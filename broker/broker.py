import os

from .exceptions import PartitionNotFoundError, TopicAlreadyExistsError, TopicNotFoundError
from .group_manager import GroupManager
from .offset_manager import OffsetManager
from .topic import Topic


class Broker:
    def __init__(self, data_dir="data", offsets_dir=None):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.offsets_dir = offsets_dir or os.path.join(os.path.dirname(os.path.abspath(self.data_dir)), "offsets")
        self.topics = {}
        self._load_topics()
        self.group_manager = GroupManager(self, offsets_dir=self.offsets_dir)
        self.offset_manager = OffsetManager(self.group_manager)

    def _load_topics(self):
        for name in os.listdir(self.data_dir):
            topic_dir = os.path.join(self.data_dir, name)
            if not os.path.isdir(topic_dir):
                continue
            log_files = sorted(
                file_name
                for file_name in os.listdir(topic_dir)
                if file_name.startswith("partition_") and file_name.endswith(".log")
            )
            if not log_files:
                continue
            self.topics[name] = Topic(name, len(log_files), self.data_dir)

    def create_topic(self, name, partitions):
        if partitions <= 0:
            raise ValueError("partitions must be greater than zero")
        if name in self.topics or os.path.exists(os.path.join(self.data_dir, name)):
            raise TopicAlreadyExistsError(f"Topic '{name}' already exists")
        self.topics[name] = Topic(name, partitions, self.data_dir)

    def produce(self, topic, key, value):
        topic_obj = self.topics.get(topic)
        if topic_obj is None:
            raise TopicNotFoundError(f"Topic '{topic}' not found")
        partition = topic_obj.get_partition(key)
        message = partition.append(key, value)
        return {"topic": topic, "partition": partition.partition_id, "offset": message["offset"]}

    def consume(self, topic, partition, offset, batch_size):
        topic_obj = self.topics.get(topic)
        if topic_obj is None:
            raise TopicNotFoundError(f"Topic '{topic}' not found")
        if partition < 0 or partition >= len(topic_obj.partitions):
            raise PartitionNotFoundError(f"Partition '{partition}' not found")
        return topic_obj.partitions[partition].read(offset, batch_size)

    def join_group(self, group_id, consumer_id, topic):
        return self.group_manager.join_group(group_id, consumer_id, topic)

    def leave_group(self, group_id, consumer_id):
        return self.group_manager.leave_group(group_id, consumer_id)

    def commit_offset(self, group_id, topic, partition, offset):
        return self.offset_manager.commit_offset(group_id, topic, partition, offset)

    def get_offset(self, group_id, topic, partition):
        return self.offset_manager.get_offset(group_id, topic, partition)

    def consume_assigned(self, group_id, consumer_id, batch_size):
        return self.group_manager.consume_assigned(group_id, consumer_id, batch_size)

    def heartbeat(self, group_id, consumer_id):
        return self.group_manager.heartbeat(group_id, consumer_id)

    def group_state(self, group_id):
        return self.group_manager.group_state(group_id)

    def metrics(self):
        topics = len(self.topics)
        partitions = sum(len(topic.partitions) for topic in self.topics.values())
        active_groups = len(self.group_manager.groups)
        active_consumers = sum(len(group["members"]) for group in self.group_manager.groups.values())
        messages_stored = 0
        partition_offsets = {}
        for topic in self.topics.values():
            partition_offsets[topic.name] = {}
            for partition in topic.partitions:
                messages_stored += len(partition.read(0, 10**9))
                partition_offsets[topic.name][partition.partition_id] = partition.next_offset
        committed_offsets = {}
        for group_id, group in self.group_manager.groups.items():
            committed_offsets[group_id] = dict(group["offsets"])
        return {
            "status": "success",
            "topics": topics,
            "partitions": partitions,
            "active_groups": active_groups,
            "active_consumers": active_consumers,
            "messages_stored": messages_stored,
            "partition_offsets": partition_offsets,
            "committed_offsets": committed_offsets,
        }
