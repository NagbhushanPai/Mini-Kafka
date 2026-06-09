import socket
import tempfile
import threading
import time
import unittest

from broker.server import BrokerServer
from client.kafka_client import KafkaClient
from consumer.consumer import Consumer
from producer.producer import Producer


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("localhost", 0))
        return sock.getsockname()[1]


class NetworkingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.port = _find_free_port()
        self.server = BrokerServer(port=self.port, data_dir=self.tmp.name)
        self.thread = threading.Thread(target=self.server.start, daemon=True)
        self.thread.start()
        time.sleep(0.2)

    def tearDown(self):
        self.server.stop()
        self.tmp.cleanup()

    def test_create_produce_consume_over_tcp(self):
        client = KafkaClient(port=self.port)
        try:
            self.assertEqual(client.create_topic("orders", 3)["status"], "success")
            produce_result = client.produce("orders", "user1", "hello")
            self.assertEqual(produce_result["status"], "success")
            messages = client.consume("orders", produce_result["partition"], 0, 10)
            self.assertEqual(messages["status"], "success")
            self.assertEqual(messages["messages"], [{"offset": 0, "key": "user1", "value": "hello"}])
        finally:
            client.close()

    def test_persistence_survives_server_restart(self):
        client = KafkaClient(port=self.port)
        try:
            client.create_topic("orders", 3)
            first = client.produce("orders", "user1", "hello")
            client.produce("orders", "user1", "world")
            partition = first["partition"]
        finally:
            client.close()

        self.server.stop()
        self.thread.join(timeout=1)

        restarted = BrokerServer(port=self.port, data_dir=self.tmp.name)
        restarted_thread = threading.Thread(target=restarted.start, daemon=True)
        restarted_thread.start()
        time.sleep(0.2)

        restarted_client = KafkaClient(port=self.port)
        try:
            response = restarted_client.consume("orders", partition, 0, 10)
            self.assertEqual(response["status"], "success")
            self.assertEqual(
                response["messages"],
                [
                    {"offset": 0, "key": "user1", "value": "hello"},
                    {"offset": 1, "key": "user1", "value": "world"},
                ],
            )
        finally:
            restarted_client.close()
            restarted.stop()
            restarted_thread.join(timeout=1)

    def test_multiple_clients_can_connect(self):
        clients = [KafkaClient(port=self.port) for _ in range(4)]
        try:
            results = []

            def worker(client, idx):
                results.append(client.create_topic(f"topic_{idx}", 1)["status"])

            threads = [threading.Thread(target=worker, args=(client, idx)) for idx, client in enumerate(clients)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            self.assertEqual(results, ["success"] * 4)
        finally:
            for client in clients:
                client.close()

    def test_producer_and_consumer_use_tcp_client(self):
        client = KafkaClient(port=self.port)
        producer = Producer(client)
        consumer = Consumer(client)
        try:
            client.create_topic("orders", 3)
            result = producer.send("orders", "user1", "hello")
            self.assertEqual(result["status"], "success")
            messages = consumer.poll("orders", result["partition"], 0, 10)
            self.assertEqual(messages["messages"][0]["value"], "hello")
        finally:
            client.close()


if __name__ == "__main__":
    unittest.main()

