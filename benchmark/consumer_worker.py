import threading
import time


class ConsumerWorker(threading.Thread):
    def __init__(self, client_factory, group_id, consumer_id, topic, batch_size, metrics, target_messages):
        super().__init__(daemon=True)
        self.client_factory = client_factory
        self.group_id = group_id
        self.consumer_id = consumer_id
        self.topic = topic
        self.batch_size = batch_size
        self.metrics = metrics
        self.target_messages = target_messages

    def run(self):
        client = self.client_factory()
        try:
            join = client.join_group(self.group_id, self.consumer_id, self.topic)
            if join.get("status") != "success":
                raise RuntimeError(join)
            while True:
                response = client.consume_assigned(self.group_id, self.consumer_id, self.batch_size)
                if response.get("status") != "success":
                    raise RuntimeError(response)
                records = response["records"]
                if not records:
                    if self.metrics.messages_consumed >= self.target_messages:
                        break
                    time.sleep(0.01)
                    continue
                now = time.time_ns()
                for record in records:
                    produced_at = record["value"]["timestamp"]
                    e2e_ms = (now - produced_at) / 1_000_000
                    committed_offset = record["offset"] + 1
                    self.metrics.record_consumed(e2e_ms, lag=0)
                    client.commit_offset(
                        self.group_id,
                        self.topic,
                        record["partition"],
                        committed_offset,
                    )
        finally:
            try:
                client.leave_group(self.group_id, self.consumer_id)
            finally:
                client.close()
