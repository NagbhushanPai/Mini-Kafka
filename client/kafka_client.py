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

