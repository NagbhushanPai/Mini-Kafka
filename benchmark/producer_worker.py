import threading
import time
import uuid


class ProducerWorker(threading.Thread):
    def __init__(self, client_factory, topic, total_messages, worker_id, metrics, partition_hint=None, stop_event=None):
        super().__init__(daemon=True)
        self.client_factory = client_factory
        self.topic = topic
        self.total_messages = total_messages
        self.worker_id = worker_id
        self.metrics = metrics
        self.partition_hint = partition_hint
        self.stop_event = stop_event

    def run(self):
        client = self.client_factory()
        try:
            for _ in range(self.total_messages):
                if self.stop_event is not None and self.stop_event.is_set():
                    break
                message_id = str(uuid.uuid4())
                started = time.perf_counter()
                response = client.produce(
                    self.topic,
                    key=message_id,
                    value={
                        "message_id": message_id,
                        "timestamp": time.time_ns(),
                        "payload": "benchmark-data",
                    },
                )
                elapsed_ms = (time.perf_counter() - started) * 1000
                if response.get("status") != "success":
                    raise RuntimeError(response)
                self.metrics.record_produced(response["partition"], elapsed_ms)
        finally:
            client.close()
