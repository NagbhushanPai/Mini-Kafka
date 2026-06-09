from .storage import StorageEngine


class Partition:
    def __init__(self, partition_id, log_file_path, storage_engine=None):
        self.partition_id = partition_id
        self.log_file_path = log_file_path
        self.storage_engine = storage_engine or StorageEngine()
        self.next_offset = self._load_next_offset()

    def _load_next_offset(self):
        messages = self.storage_engine.read_messages(self.log_file_path, 0, 10**9)
        if not messages:
            return 0
        return messages[-1]["offset"] + 1

    def append(self, key, value):
        message = {"offset": self.next_offset, "key": key, "value": value}
        self.storage_engine.append_message(self.log_file_path, message)
        self.next_offset += 1
        return message

    def read(self, start_offset, batch_size):
        return self.storage_engine.read_messages(self.log_file_path, start_offset, batch_size)

