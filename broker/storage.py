import json
import os


class StorageEngine:
    def append_message(self, file_path, message):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(message, separators=(",", ":")) + "\n")

    def read_messages(self, file_path, start_offset, batch_size):
        if not os.path.exists(file_path):
            return []

        messages = []
        with open(file_path, "r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                record = json.loads(line)
                payload = record.get("payload", record)
                if payload["offset"] < start_offset:
                    continue
                messages.append(payload)
                if len(messages) >= batch_size:
                    break
        return messages
