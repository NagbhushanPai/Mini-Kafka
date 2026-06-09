import tempfile
import unittest

from broker.broker import Broker
from broker.exceptions import PartitionNotFoundError, TopicAlreadyExistsError, TopicNotFoundError
from consumer.consumer import Consumer
from producer.producer import Producer


class MiniKafkaTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.broker = Broker(data_dir=self.tmp.name)
        self.producer = Producer(self.broker)
        self.consumer = Consumer(self.broker)

    def tearDown(self):
        self.tmp.cleanup()

    def test_create_topic(self):
        self.broker.create_topic("orders", 3)
        self.assertIn("orders", self.broker.topics)

    def test_duplicate_topic_raises(self):
        self.broker.create_topic("orders", 3)
        with self.assertRaises(TopicAlreadyExistsError):
            self.broker.create_topic("orders", 3)

    def test_invalid_topic_config_raises(self):
        with self.assertRaises(ValueError):
            self.broker.create_topic("orders", 0)

    def test_produce_and_consume(self):
        self.broker.create_topic("orders", 3)
        result = self.producer.send("orders", "user1", "hello")
        messages = self.consumer.poll("orders", result["partition"], 0, 10)
        self.assertEqual(messages, [{"offset": 0, "key": "user1", "value": "hello"}])

    def test_topic_not_found(self):
        with self.assertRaises(TopicNotFoundError):
            self.producer.send("missing", "user1", "hello")

    def test_partition_not_found(self):
        self.broker.create_topic("orders", 3)
        with self.assertRaises(PartitionNotFoundError):
            self.consumer.poll("orders", 10, 0, 10)

    def test_same_key_stays_same_partition(self):
        self.broker.create_topic("orders", 3)
        first = self.producer.send("orders", "user1", "message1")
        second = self.producer.send("orders", "user1", "message2")
        third = self.producer.send("orders", "user1", "message3")
        self.assertEqual(first["partition"], second["partition"])
        self.assertEqual(second["partition"], third["partition"])
        messages = self.consumer.poll("orders", first["partition"], 0, 10)
        self.assertEqual([m["value"] for m in messages], ["message1", "message2", "message3"])

    def test_offsets_are_sequential(self):
        self.broker.create_topic("orders", 3)
        for idx in range(5):
            self.producer.send("orders", f"user{idx}", f"message{idx}")
        partition = self.broker.topics["orders"].get_partition("user1").partition_id
        messages = self.consumer.poll("orders", partition, 0, 100)
        offsets = [message["offset"] for message in messages]
        self.assertEqual(offsets, list(range(len(offsets))))

    def test_persistence_survives_broker_restart(self):
        self.broker.create_topic("orders", 3)
        first = self.producer.send("orders", "user1", "hello")
        second = self.producer.send("orders", "user1", "world")

        restarted_broker = Broker(data_dir=self.tmp.name)
        restarted_consumer = Consumer(restarted_broker)
        partition = first["partition"]

        self.assertIn("orders", restarted_broker.topics)
        self.assertEqual(restarted_broker.topics["orders"].partitions[partition].next_offset, 2)

        messages = restarted_consumer.poll("orders", partition, 0, 10)
        self.assertEqual(
            messages,
            [
                {"offset": first["offset"], "key": "user1", "value": "hello"},
                {"offset": second["offset"], "key": "user1", "value": "world"},
            ],
        )


if __name__ == "__main__":
    unittest.main()
