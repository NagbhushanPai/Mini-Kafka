import json
import os
from threading import Lock

from .exceptions import AssignmentNotFound, ConsumerNotFound, GroupNotFound, TopicNotFoundError


class GroupManager:
    def __init__(self, broker, offsets_dir="offsets", offsets_file="consumer_offsets.json"):
        self.broker = broker
        self.offsets_dir = offsets_dir
        self.offsets_file = os.path.join(offsets_dir, offsets_file)
        self.lock = Lock()
        self.groups = {}
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
            self.groups[group_id] = {
                "topic": topic,
                "members": set(members),
                "assignments": assignments,
                "offsets": offsets,
            }

    def _persist(self):
        os.makedirs(self.offsets_dir, exist_ok=True)
        payload = {}
        for group_id, group in self.groups.items():
            payload[group_id] = {
                "topic": group["topic"],
                "members": sorted(group["members"]),
                "assignments": {str(partition): consumer_id for partition, consumer_id in group["assignments"].items()},
                "offsets": group["offsets"],
            }
        with open(self.offsets_file, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)

    def join_group(self, group_id, consumer_id, topic):
        with self.lock:
            if topic not in self.broker.topics:
                raise TopicNotFoundError(f"Topic '{topic}' not found")
            group = self.groups.setdefault(
                group_id,
                {"topic": topic, "members": set(), "assignments": {}, "offsets": {}},
            )
            group["topic"] = topic
            group["members"].add(consumer_id)
            self._rebalance(group_id)
            self._persist()
            return self.get_assignments(group_id, consumer_id)

    def leave_group(self, group_id, consumer_id):
        with self.lock:
            group = self.groups.get(group_id)
            if group is None:
                raise GroupNotFound(f"Group '{group_id}' not found")
            if consumer_id not in group["members"]:
                raise ConsumerNotFound(f"Consumer '{consumer_id}' not found in group '{group_id}'")
            group["members"].remove(consumer_id)
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
            group = self._get_group(group_id, topic)
            group["offsets"][f"{topic}-{partition}"] = offset
            self._persist()
            return {"status": "success"}

    def get_offset(self, group_id, topic, partition):
        with self.lock:
            group = self._get_group(group_id, topic)
            key = f"{topic}-{partition}"
            if key not in group["offsets"]:
                raise AssignmentNotFound(f"Offset for partition '{partition}' not found")
            return {"status": "success", "offset": group["offsets"][key]}

    def consume_assigned(self, group_id, consumer_id, batch_size):
        with self.lock:
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
