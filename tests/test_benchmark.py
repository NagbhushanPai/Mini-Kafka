import json
import tempfile
import unittest
from collections import defaultdict
import os

from benchmark.benchmark_runner import BenchmarkRunner
from benchmark.metrics import MetricsCollector


class FakeClient:
    def __init__(self, store):
        self.store = store

    def close(self):
        return None

    def create_topic(self, name, partitions):
        self.store["topic"] = name
        self.store["partitions"] = partitions
        self.store["messages"] = defaultdict(list)
        self.store["offsets"] = defaultdict(int)
        self.store["next_partition"] = 0
        return {"status": "success"}

    def produce(self, topic, key, value):
        partition = self.store["next_partition"] % self.store["partitions"]
        self.store["next_partition"] += 1
        offset = self.store["offsets"][partition]
        self.store["offsets"][partition] += 1
        self.store["messages"][partition].append({"offset": offset, "key": key, "value": value})
        return {"status": "success", "topic": topic, "partition": partition, "offset": offset}

    def join_group(self, group_id, consumer_id, topic):
        self.store["consumer_topic"] = topic
        return {"status": "success", "assigned_partitions": list(range(self.store["partitions"]))}

    def consume_assigned(self, group_id, consumer_id, batch_size):
        records = []
        for partition in range(self.store["partitions"]):
            for message in self.store["messages"][partition][:batch_size]:
                records.append({"partition": partition, **message})
            self.store["messages"][partition] = self.store["messages"][partition][batch_size:]
        return {"status": "success", "records": records}

    def commit_offset(self, group_id, topic, partition, offset):
        return {"status": "success"}

    def leave_group(self, group_id, consumer_id):
        return {"status": "success"}

    def metrics(self):
        return {
            "status": "success",
            "topics": 1,
            "partitions": self.store.get("partitions", 0),
            "active_groups": 1,
            "active_consumers": 1,
            "messages_stored": sum(len(messages) for messages in self.store.get("messages", {}).values()),
        }


class BenchmarkTests(unittest.TestCase):
    def test_metrics_collection(self):
        metrics = MetricsCollector()
        metrics.start()
        metrics.record_produced(0, 1.5)
        metrics.record_produced(1, 2.5)
        metrics.record_consumed(3.0, lag=4)
        metrics.stop()

        summary = metrics.summary()
        self.assertEqual(summary["messages_produced"], 2)
        self.assertEqual(summary["messages_consumed"], 1)
        self.assertEqual(len(metrics.producer_latencies), 2)
        self.assertEqual(len(metrics.end_to_end_latencies), 1)
        self.assertEqual(summary["consumer_lag"], 4)

    def test_percentiles_are_deterministic(self):
        metrics = MetricsCollector()
        values = [1, 2, 3, 4, 5, 100]
        self.assertEqual(metrics.percentile(values, 50), 3.0)
        self.assertEqual(metrics.percentile(values, 95), 100.0)
        self.assertEqual(metrics.percentile(values, 99), 100.0)

    def test_end_to_end_latency_uses_timestamps(self):
        metrics = MetricsCollector()
        produced_at = 1_000_000_000
        received_at = 1_015_000_000
        metrics.record_consumed((received_at - produced_at) / 1_000_000, lag=0)
        self.assertAlmostEqual(metrics.end_to_end_latencies[0], 15.0)

    def test_full_benchmark_run_writes_report(self):
        store = {}

        def client_factory():
            return FakeClient(store)

        with tempfile.TemporaryDirectory() as tmpdir:
            runner = BenchmarkRunner(client_factory, reports_dir=tmpdir)
            runner.run("baseline")

            report_path = os.path.join(tmpdir, "benchmark_results.json")
            with open(report_path, "r", encoding="utf-8") as handle:
                report = json.load(handle)

        self.assertEqual(report["messages_produced"], 10000)
        self.assertEqual(report["messages_consumed"], 10000)
        self.assertIn("throughput", report)
        self.assertIn("broker_metrics", report)


if __name__ == "__main__":
    unittest.main()
