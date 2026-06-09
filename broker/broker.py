import os

from .exceptions import PartitionNotFoundError, TopicAlreadyExistsError, TopicNotFoundError
from .topic import Topic


class Broker:
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.topics = {}
        self._load_topics()

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

