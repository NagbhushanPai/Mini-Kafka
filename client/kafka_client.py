import socket
import threading

from network.protocol import recv_json, send_json


class KafkaClient:
    def __init__(self, host="localhost", port=9092):
        self.host = host
        self.port = port
        self._socket = socket.create_connection((self.host, self.port))
        self._lock = threading.Lock()

    def close(self):
        if self._socket is not None:
            self._socket.close()
            self._socket = None

    def _request(self, payload):
        with self._lock:
            send_json(self._socket, payload)
            response = recv_json(self._socket)
            if response is None:
                raise ConnectionError("Broker closed the connection")
            return response

    def create_topic(self, name, partitions):
        return self._request({"version": 1, "type": "create_topic", "name": name, "partitions": partitions})

    def produce(self, topic, key, value):
        return self._request({"version": 1, "type": "produce", "topic": topic, "key": key, "value": value})

    def consume(self, topic, partition, offset, batch_size):
        return self._request(
            {
                "version": 1,
                "type": "consume",
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "batch_size": batch_size,
            }
        )

    def join_group(self, group_id, consumer_id, topic):
        return self._request(
            {
                "version": 1,
                "type": "join_group",
                "group_id": group_id,
                "consumer_id": consumer_id,
                "topic": topic,
            }
        )

    def leave_group(self, group_id, consumer_id):
        return self._request({"version": 1, "type": "leave_group", "group_id": group_id, "consumer_id": consumer_id})

    def commit_offset(self, group_id, topic, partition, offset):
        return self._request(
            {
                "version": 1,
                "type": "commit_offset",
                "group_id": group_id,
                "topic": topic,
                "partition": partition,
                "offset": offset,
            }
        )

    def get_offset(self, group_id, topic, partition):
        return self._request(
            {
                "version": 1,
                "type": "get_offset",
                "group_id": group_id,
                "topic": topic,
                "partition": partition,
            }
        )

    def consume_assigned(self, group_id, consumer_id, batch_size):
        return self._request(
            {
                "version": 1,
                "type": "consume_assigned",
                "group_id": group_id,
                "consumer_id": consumer_id,
                "batch_size": batch_size,
            }
        )

    def heartbeat(self, group_id, consumer_id):
        return self._request(
            {
                "version": 1,
                "type": "heartbeat",
                "group_id": group_id,
                "consumer_id": consumer_id,
            }
        )

    def group_state(self, group_id):
        return self._request({"version": 1, "type": "group_state", "group_id": group_id})

    def metrics(self):
        return self._request({"version": 1, "type": "metrics"})
