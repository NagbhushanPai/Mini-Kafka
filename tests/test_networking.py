import socket
import json
import logging
import os
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
        consumer = Consumer(client, group_id="orders-processors", consumer_id="consumer-a", topic="orders")
        try:
            client.create_topic("orders", 3)
            join_response = consumer.join_group()
            self.assertEqual(join_response["status"], "success")
            result = producer.send("orders", "user1", "hello")
            self.assertEqual(result["status"], "success")
            messages = consumer.poll(batch_size=10)
            self.assertEqual(messages["status"], "success")
            self.assertTrue(any(record["value"] == "hello" for record in messages["records"]))
        finally:
            client.close()

    def test_consumer_group_join_commit_and_restart_recovery(self):
        client = KafkaClient(port=self.port)
        consumer = Consumer(client, group_id="orders-processors", consumer_id="consumer-a", topic="orders")
        committed_partition = None
        committed_offset = None
        try:
            self.assertEqual(client.create_topic("orders", 3)["status"], "success")
            self.assertEqual(consumer.join_group()["status"], "success")
            self.assertEqual(client.produce("orders", "user1", "hello")["status"], "success")
            records = consumer.poll(batch_size=10)["records"]
            self.assertTrue(records)
            committed_partition = records[0]["partition"]
            committed_offset = records[0]["offset"] + 1
            committed = client.commit_offset("orders-processors", "orders", committed_partition, committed_offset)
            self.assertEqual(committed["status"], "success")
            offset = client.get_offset("orders-processors", "orders", committed_partition)
            self.assertEqual(offset["offset"], committed_offset)
        finally:
            client.close()

        self.server.stop()
        self.thread.join(timeout=1)

        restarted = BrokerServer(port=self.port, data_dir=self.tmp.name)
        restarted_thread = threading.Thread(target=restarted.start, daemon=True)
        restarted_thread.start()
        time.sleep(0.2)

        client = KafkaClient(port=self.port)
        consumer = Consumer(client, group_id="orders-processors", consumer_id="consumer-a", topic="orders")
        try:
            self.assertEqual(consumer.join_group()["status"], "success")
            offset = client.get_offset("orders-processors", "orders", committed_partition)
            self.assertEqual(offset["status"], "success")
            self.assertEqual(offset["offset"], committed_offset)
        finally:
            try:
                consumer.leave_group()
            except Exception:
                pass
            client.close()
            restarted.stop()
            restarted_thread.join(timeout=1)

    @unittest.skipUnless(os.environ.get("RUN_STRESS_TEST") == "1", "Set RUN_STRESS_TEST=1 to run the stress test")
    def test_stress_10_producers_5_consumers_50k_messages_with_logs(self):
        logger = logging.getLogger("mini_kafka.stress")
        logger.setLevel(logging.INFO)
        logger.propagate = False
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        logger.handlers = [handler]

        total_messages = 50000
        producer_threads = 10
        consumer_threads = 5
        partitions = 8
        produced_ranges = []
        seen_offsets = {}
        seen_lock = threading.Lock()

        client = KafkaClient(port=self.port)
        try:
            logger.info("Creating stress topic")
            response = client.create_topic("stress", partitions)
            self.assertEqual(response["status"], "success")
        finally:
            client.close()

        def producer_worker(index):
            start = (total_messages * index) // producer_threads
            end = (total_messages * (index + 1)) // producer_threads
            produced_ranges.append((index, start, end))
            logger.info("Producer %s handling range [%s, %s)", index, start, end)
            producer_client = KafkaClient(port=self.port)
            try:
                for value_index in range(start, end):
                    resp = producer_client.produce("stress", f"key-{value_index}", f"value-{value_index}")
                    if resp.get("status") != "success":
                        raise AssertionError(resp)
                logger.info("Producer %s completed %s messages", index, end - start)
            finally:
                producer_client.close()

        def consumer_worker(index, partition_ids):
            logger.info("Consumer %s handling partitions %s", index, partition_ids)
            consumer_client = KafkaClient(port=self.port)
            try:
                for partition_id in partition_ids:
                    offset = 0
                    partition_seen = seen_offsets.setdefault(partition_id, set())
                    while True:
                        resp = consumer_client.consume("stress", partition_id, offset, 250)
                        if resp.get("status") != "success":
                            raise AssertionError(resp)
                        messages = resp["messages"]
                        if not messages:
                            break
                        with seen_lock:
                            for message in messages:
                                if message["offset"] in partition_seen:
                                    raise AssertionError(
                                        f"duplicate offset in partition {partition_id}: {message['offset']}"
                                    )
                                partition_seen.add(message["offset"])
                        logger.info(
                            "Consumer %s read partition=%s batch=%s next_offset=%s",
                            index,
                            partition_id,
                            len(messages),
                            messages[-1]["offset"] + 1,
                        )
                        offset = messages[-1]["offset"] + 1
                logger.info("Consumer %s completed", index)
            finally:
                consumer_client.close()

        producer_threads_list = [
            threading.Thread(target=producer_worker, args=(index,)) for index in range(producer_threads)
        ]
        start_time = time.time()
        logger.info("Starting %s producer threads", producer_threads)
        for thread in producer_threads_list:
            thread.start()
        for thread in producer_threads_list:
            thread.join()
        logger.info("All producers finished in %.2fs", time.time() - start_time)

        for partition_id in range(partitions):
            path = os.path.join(self.tmp.name, "stress", f"partition_{partition_id}.log")
            self.assertTrue(os.path.exists(path), path)
            with open(path, "r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    record = json.loads(line)
                    self.assertEqual(record["version"], 1)
                    self.assertEqual(record["type"], "produce")
                    self.assertIn("payload", record)

        partition_assignments = [list(range(i, partitions, consumer_threads)) for i in range(consumer_threads)]
        consumer_threads_list = [
            threading.Thread(target=consumer_worker, args=(index, partition_ids))
            for index, partition_ids in enumerate(partition_assignments)
        ]
        logger.info("Starting %s consumer threads", consumer_threads)
        for thread in consumer_threads_list:
            thread.start()
        for thread in consumer_threads_list:
            thread.join()
        logger.info("All consumers finished")

        total_read = sum(len(values) for values in seen_offsets.values())
        self.assertEqual(total_read, total_messages)

        self.server.stop()
        self.thread.join(timeout=1)
        restarted = BrokerServer(port=self.port, data_dir=self.tmp.name)
        restarted_thread = threading.Thread(target=restarted.start, daemon=True)
        restarted_thread.start()
        time.sleep(0.2)
        restarted_client = KafkaClient(port=self.port)
        try:
            logger.info("Verifying restart recovery")
            response = restarted_client.consume("stress", 0, 0, 5)
            self.assertEqual(response["status"], "success")
            self.assertTrue(response["messages"])
        finally:
            restarted_client.close()
            restarted.stop()
            restarted_thread.join(timeout=1)

        logger.info("Stress test complete: produced=%s consumed=%s", total_messages, total_read)

    def test_consumer_groups_multiple_members_round_robin_assignment(self):
        client_a = KafkaClient(port=self.port)
        client_b = KafkaClient(port=self.port)
        client_c = KafkaClient(port=self.port)
        consumer_a = Consumer(client_a, group_id="orders-processors", consumer_id="consumer-a", topic="orders")
        consumer_b = Consumer(client_b, group_id="orders-processors", consumer_id="consumer-b", topic="orders")
        consumer_c = Consumer(client_c, group_id="orders-processors", consumer_id="consumer-c", topic="orders")
        try:
            self.assertEqual(client_a.create_topic("orders", 4)["status"], "success")
            self.assertEqual(consumer_a.join_group()["status"], "success")
            self.assertEqual(consumer_b.join_group()["status"], "success")
            self.assertEqual(consumer_c.join_group()["status"], "success")

            assigned_a = consumer_a.join_group()["assigned_partitions"]
            assigned_b = consumer_b.join_group()["assigned_partitions"]
            assigned_c = consumer_c.join_group()["assigned_partitions"]

            all_assigned = sorted(assigned_a + assigned_b + assigned_c)
            self.assertEqual(all_assigned, [0, 1, 2, 3])
            self.assertEqual(len(set(all_assigned)), 4)
        finally:
            for consumer in (consumer_a, consumer_b, consumer_c):
                try:
                    consumer.leave_group()
                except Exception:
                    pass
            client_a.close()
            client_b.close()
            client_c.close()


if __name__ == "__main__":
    unittest.main()
