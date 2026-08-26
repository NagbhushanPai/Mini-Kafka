import json
import os
import shutil
import uuid
from datetime import datetime

from broker.storage import StorageEngine


class SessionStore:
    """Archives the broker's on-disk data before a reset so nothing is lost.

    Each archive is a self-contained, timestamped snapshot under sessions/,
    completely separate from the live data/ and offsets/ directories reset()
    clears, and from reports/ (benchmark results) which this never touches.
    """

    def __init__(self, data_dir="data", offsets_file=os.path.join("offsets", "consumer_offsets.json"), sessions_dir="sessions"):
        self.data_dir = data_dir
        self.offsets_file = offsets_file
        self.sessions_dir = sessions_dir
        self._engine = StorageEngine()

    def archive_current(self):
        session_id = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
        dest = os.path.join(self.sessions_dir, session_id)
        os.makedirs(dest, exist_ok=True)

        data_dest = os.path.join(dest, "data")
        has_data = os.path.isdir(self.data_dir) and os.listdir(self.data_dir)
        if has_data:
            shutil.copytree(self.data_dir, data_dest)

        if os.path.exists(self.offsets_file):
            shutil.copy2(self.offsets_file, os.path.join(dest, "consumer_offsets.json"))

        topics = self._scan_topics(data_dest) if has_data else []
        metadata = {
            "session_id": session_id,
            "archived_at": datetime.now().isoformat(timespec="seconds"),
            "topics": topics,
            "total_messages": sum(sum(p["message_count"] for p in t["partitions"]) for t in topics),
        }
        with open(os.path.join(dest, "metadata.json"), "w", encoding="utf-8") as handle:
            json.dump(metadata, handle, indent=2)
        return metadata

    def _scan_topics(self, data_dest):
        topics = []
        for name in sorted(os.listdir(data_dest)):
            topic_dir = os.path.join(data_dest, name)
            if not os.path.isdir(topic_dir):
                continue
            partitions = []
            log_files = sorted(f for f in os.listdir(topic_dir) if f.startswith("partition_") and f.endswith(".log"))
            for file_name in log_files:
                path = os.path.join(topic_dir, file_name)
                messages = self._engine.read_messages(path, 0, 10**9)
                partition_id = int(file_name[len("partition_"):-len(".log")])
                partitions.append(
                    {
                        "partition_id": partition_id,
                        "message_count": len(messages),
                        "sample_messages": messages[:5],
                    }
                )
            topics.append({"name": name, "partitions": partitions})
        return topics

    def list_sessions(self):
        if not os.path.isdir(self.sessions_dir):
            return []
        sessions = []
        for name in sorted(os.listdir(self.sessions_dir), reverse=True):
            metadata = self._read_metadata(name)
            if metadata is None:
                continue
            sessions.append(
                {
                    "session_id": metadata["session_id"],
                    "archived_at": metadata["archived_at"],
                    "topic_count": len(metadata.get("topics", [])),
                    "total_messages": metadata.get("total_messages", 0),
                }
            )
        return sessions

    def get_session(self, session_id):
        return self._read_metadata(session_id)

    def _read_metadata(self, session_id):
        meta_path = os.path.join(self.sessions_dir, session_id, "metadata.json")
        if not os.path.isfile(meta_path):
            return None
        with open(meta_path, "r", encoding="utf-8") as handle:
            return json.load(handle)
