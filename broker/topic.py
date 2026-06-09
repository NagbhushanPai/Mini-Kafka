import hashlib
import os

from .partition import Partition


class Topic:
    def __init__(self, name, partitions, data_dir="data"):
        self.name = name
        self.data_dir = data_dir
        self.partitions = []
        topic_dir = os.path.join(data_dir, name)
        os.makedirs(topic_dir, exist_ok=True)
        for partition_id in range(partitions):
            log_file_path = os.path.join(topic_dir, f"partition_{partition_id}.log")
            self.partitions.append(Partition(partition_id, log_file_path))

    def _stable_partition_index(self, key):
        digest = hashlib.sha256(str(key).encode("utf-8")).hexdigest()
        return int(digest, 16) % len(self.partitions)

    def get_partition(self, key):
        return self.partitions[self._stable_partition_index(key)]

