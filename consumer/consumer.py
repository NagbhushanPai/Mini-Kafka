import threading
import time


class Consumer:
    heartbeat_interval = 5.0

    def __init__(self, client, group_id, consumer_id, topic, heartbeat_interval=None):
        self.client = client
        self.group_id = group_id
        self.consumer_id = consumer_id
        self.topic = topic
        if heartbeat_interval is not None:
            self.heartbeat_interval = heartbeat_interval
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread = None

    def join_group(self):
        response = self.client.join_group(self.group_id, self.consumer_id, self.topic)
        if response.get("status") == "success":
            self._start_heartbeat_loop()
        return response

    def leave_group(self):
        self._stop_heartbeat_loop()
        return self.client.leave_group(self.group_id, self.consumer_id)

    def poll(self, batch_size=10):
        return self.client.consume_assigned(self.group_id, self.consumer_id, batch_size)

    def _start_heartbeat_loop(self):
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            return
        self._heartbeat_stop.clear()
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_worker, daemon=True)
        self._heartbeat_thread.start()

    def _stop_heartbeat_loop(self):
        self._heartbeat_stop.set()

    def _heartbeat_worker(self):
        while not self._heartbeat_stop.wait(self.heartbeat_interval):
            try:
                self.client.heartbeat(self.group_id, self.consumer_id)
            except Exception:
                break
