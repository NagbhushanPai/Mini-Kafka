import json
import logging
import os

from .consumer_worker import ConsumerWorker
from .metrics import MetricsCollector
from .producer_worker import ProducerWorker
from .scenarios import SCENARIOS


class BenchmarkRunner:
    def __init__(self, client_factory, reports_dir="reports", logger=None):
        self.client_factory = client_factory
        self.reports_dir = reports_dir
        self.logger = logger or logging.getLogger("mini_kafka.benchmark")

    def run(self, scenario_name):
        scenario = SCENARIOS[scenario_name]
        metrics = MetricsCollector()
        metrics.start()
        self.logger.info("Benchmark started")
        self.logger.info("Producers launched: %s", scenario["producers"])
        self.logger.info("Consumers launched: %s", scenario["consumers"])

        client = self.client_factory()
        topic = f"benchmark-{scenario_name}"
        try:
            response = client.create_topic(topic, scenario["partitions"])
            if response.get("status") != "success":
                raise RuntimeError(response)
        finally:
            client.close()

        messages_per_producer = scenario["messages"] // scenario["producers"]
        producer_workers = [
            ProducerWorker(self.client_factory, topic, messages_per_producer, idx, metrics)
            for idx in range(scenario["producers"])
        ]
        consumer_workers = [
            ConsumerWorker(
                self.client_factory,
                f"{topic}-group",
                f"consumer-{idx}",
                topic,
                100,
                metrics,
                scenario["messages"],
            )
            for idx in range(scenario["consumers"])
        ]

        for worker in consumer_workers:
            worker.start()
        for worker in producer_workers:
            worker.start()
        for worker in producer_workers:
            worker.join()
        for worker in consumer_workers:
            worker.join()

        metrics.stop()
        report = metrics.summary()
        broker_metrics = self._broker_metrics()
        report["broker_metrics"] = broker_metrics
        report["consumer_lag"] = self._consumer_lag(broker_metrics, topic, f"{topic}-group")
        self._save_report(report)
        self.logger.info("Messages produced: %s", report["messages_produced"])
        self.logger.info("Throughput: %.2f msg/sec", report["throughput"])
        self.logger.info("Benchmark completed")
        return report

    def _broker_metrics(self):
        client = self.client_factory()
        try:
            return client.metrics()
        finally:
            client.close()

    def _save_report(self, report):
        os.makedirs(self.reports_dir, exist_ok=True)
        with open(os.path.join(self.reports_dir, "benchmark_results.json"), "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, sort_keys=True)

    def _consumer_lag(self, broker_metrics, topic, group_id):
        partition_offsets = broker_metrics.get("partition_offsets", {}).get(topic, {})
        committed_offsets = broker_metrics.get("committed_offsets", {}).get(group_id, {})
        lag = 0
        for partition, latest_offset in partition_offsets.items():
            committed = committed_offsets.get(f"{topic}-{partition}", 0)
            lag = max(lag, max(0, latest_offset - committed))
        return lag
