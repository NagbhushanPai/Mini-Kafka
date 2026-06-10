import json
import os
import time
import threading
from threading import Lock

from .exceptions import AssignmentNotFound, ConsumerNotFound, GroupNotFound, TopicNotFoundError


class GroupManager:
    def __init__(
        self,
        broker,
        offsets_dir="offsets",
        offsets_file="consumer_offsets.json",
        heartbeat_timeout=5.0,
        health_check_interval=2.0,
    ):
        self.broker = broker
        self.offsets_dir = offsets_dir
        self.offsets_file = os.path.join(offsets_dir, offsets_file)
        self.heartbeat_timeout = heartbeat_timeout
        self.health_check_interval = health_check_interval
        self.lock = Lock()
        self.groups = {}
        self.dead_consumers_detected = 0
        self._health_stop = threading.Event()
        self._health_thread = None
        self._load_offsets()

    def _load_offsets(self):
        if not os.path.exists(self.offsets_file):
            return
        with open(self.offsets_file, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        self.groups = {}
        for group_id, group_data in data.items():
            topic = group_data.get("topic")
            members = group_data.get("members", {})
            offsets = group_data.get("offsets", {})
            assignments = {int(partition): consumer_id for partition, consumer_id in group_data.get("assignments", {}).items()}
            now = time.time()
            heartbeats = group_data.get("heartbeats", {})
            self.groups[group_id] = {
                "topic": topic,
                "members": set(members),
                "assignments": assignments,
                "offsets": offsets,
                "heartbeats": {
                    consumer_id: heartbeats.get(consumer_id, now)
                    for consumer_id in members
                },
            }

    def start_health_monitor(self):
        if self._health_thread and self._health_thread.is_alive():
            return
        self._health_stop.clear()
        self._health_thread = threading.Thread(target=self._health_worker, daemon=True)
        self._health_thread.start()

    def stop_health_monitor(self):
        self._health_stop.set()
        thread = self._health_thread
        if thread and thread.is_alive():
            thread.join(timeout=self.health_check_interval + 1)

    def _health_worker(self):
        while not self._health_stop.wait(self.health_check_interval):
            self.scan_for_dead_consumers()

    def _persist(self):
        os.makedirs(self.offsets_dir, exist_ok=True)
        payload = {}
        for group_id, group in self.groups.items():
            payload[group_id] = {
                "topic": group["topic"],
                "members": sorted(group["members"]),
                "assignments": {str(partition): consumer_id for partition, consumer_id in group["assignments"].items()},
                "offsets": group["offsets"],
                "heartbeats": group["heartbeats"],
            }
        with open(self.offsets_file, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)

    def _remove_dead_members(self, group):
        now = time.time()
        dead_members = [
            consumer_id
            for consumer_id in group["members"]
            if now - group["heartbeats"].get(consumer_id, 0) > self.heartbeat_timeout
        ]
        if not dead_members:
            return False
        for consumer_id in dead_members:
            group["members"].discard(consumer_id)
            group["heartbeats"].pop(consumer_id, None)
            for partition, owner in list(group["assignments"].items()):
                if owner == consumer_id:
                    del group["assignments"][partition]
            self.dead_consumers_detected += 1
        return True

    def _cleanup_dead_groups(self):
        changed = False
        for group_id in list(self.groups.keys()):
            group = self.groups[group_id]
            if self._remove_dead_members(group):
                changed = True
                if not group["members"]:
                    del self.groups[group_id]
                else:
                    self._rebalance(group_id)
        return changed

    def scan_for_dead_consumers(self):
        with self.lock:
            changed = self._cleanup_dead_groups()
            if changed:
                self._persist()
            return changed

    def join_group(self, group_id, consumer_id, topic):
        with self.lock:
            self._cleanup_dead_groups()
            if topic not in self.broker.topics:
                raise TopicNotFoundError(f"Topic '{topic}' not found")
            group = self.groups.setdefault(
                group_id,
                {"topic": topic, "members": set(), "assignments": {}, "offsets": {}, "heartbeats": {}},
            )
            group["topic"] = topic
            group["members"].add(consumer_id)
            group["heartbeats"][consumer_id] = time.time()
            self._rebalance(group_id)
            self._persist()
            return self.get_assignments(group_id, consumer_id)

    def leave_group(self, group_id, consumer_id):
        with self.lock:
            self._cleanup_dead_groups()
            group = self.groups.get(group_id)
            if group is None:
                raise GroupNotFound(f"Group '{group_id}' not found")
            if consumer_id not in group["members"]:
                raise ConsumerNotFound(f"Consumer '{consumer_id}' not found in group '{group_id}'")
            group["members"].remove(consumer_id)
            group["heartbeats"].pop(consumer_id, None)
            for partition, owner in list(group["assignments"].items()):
                if owner == consumer_id:
                    del group["assignments"][partition]
            if not group["members"]:
                del self.groups[group_id]
            else:
                self._rebalance(group_id)
            self._persist()
            return {"status": "success"}

    def commit_offset(self, group_id, topic, partition, offset):
        with self.lock:
            self._cleanup_dead_groups()
            group = self._get_group(group_id, topic)
            group["offsets"][f"{topic}-{partition}"] = offset
            self._persist()
            return {"status": "success"}

    def get_offset(self, group_id, topic, partition):
        with self.lock:
            self._cleanup_dead_groups()
            group = self._get_group(group_id, topic)
            key = f"{topic}-{partition}"
            if key not in group["offsets"]:
                raise AssignmentNotFound(f"Offset for partition '{partition}' not found")
            return {"status": "success", "offset": group["offsets"][key]}

    def consume_assigned(self, group_id, consumer_id, batch_size):
        with self.lock:
            self._cleanup_dead_groups()
            group = self._get_group(group_id)
            if consumer_id not in group["members"]:
                raise ConsumerNotFound(f"Consumer '{consumer_id}' not found in group '{group_id}'")
            topic = group["topic"]
            assigned = [p for p, owner in group["assignments"].items() if owner == consumer_id]
            records = []
            for partition in sorted(assigned):
                offset = group["offsets"].get(f"{topic}-{partition}", 0)
                messages = self.broker.consume(topic, partition, offset, batch_size)
                for message in messages:
                    records.append(
                        {
                            "partition": partition,
                            "offset": message["offset"],
                            "key": message["key"],
                            "value": message["value"],
                        }
                    )
            return {"status": "success", "records": records}

    def heartbeat(self, group_id, consumer_id):
        with self.lock:
            self._cleanup_dead_groups()
            group = self.groups.get(group_id)
            if group is None:
                raise GroupNotFound(f"Group '{group_id}' not found")
            if consumer_id not in group["members"]:
                raise ConsumerNotFound(f"Consumer '{consumer_id}' not found in group '{group_id}'")
            group["heartbeats"][consumer_id] = time.time()
            self._persist()
            return {"status": "success"}

    def group_state(self, group_id):
        with self.lock:
            self._cleanup_dead_groups()
            group = self.groups.get(group_id)
            if group is None:
                raise GroupNotFound(f"Group '{group_id}' not found")
            return {
                "status": "success",
                "group_id": group_id,
                "topic": group["topic"],
                "members": sorted(group["members"]),
                "assignments": {str(partition): owner for partition, owner in group["assignments"].items()},
                "offsets": dict(group["offsets"]),
                "heartbeats": dict(group["heartbeats"]),
            }

    def get_assignments(self, group_id, consumer_id=None):
        group = self._get_group(group_id)
        assignments = group["assignments"]
        if consumer_id is None:
            return assignments
        return [partition for partition, owner in assignments.items() if owner == consumer_id]

    def _rebalance(self, group_id):
        group = self.groups[group_id]
        topic = group["topic"]
        partition_count = len(self.broker.topics[topic].partitions)
        consumers = sorted(group["members"])
        group["assignments"] = {}
        if not consumers:
            return
        for partition in range(partition_count):
            group["assignments"][partition] = consumers[partition % len(consumers)]

    def _get_group(self, group_id, topic=None):
        group = self.groups.get(group_id)
        if group is None:
            raise GroupNotFound(f"Group '{group_id}' not found")
        if topic is not None and group["topic"] != topic:
            raise GroupNotFound(f"Group '{group_id}' not found for topic '{topic}'")
        return group
