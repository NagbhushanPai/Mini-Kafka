import json
import logging
import os
import threading

from .consumer_worker import ConsumerWorker
from .metrics import MetricsCollector
from .producer_worker import ProducerWorker
from .scenarios import SCENARIOS


class BenchmarkRunner:
    def __init__(self, client_factory, reports_dir="reports", logger=None, progress_callback=None):
        self.client_factory = client_factory
        self.reports_dir = reports_dir
        self.logger = logger or logging.getLogger("mini_kafka.benchmark")
        self.progress_callback = progress_callback or (lambda *_args, **_kwargs: None)

    def run(self, scenario_name, stop_event=None):
        scenario = SCENARIOS[scenario_name]
        stop_event = stop_event if stop_event is not None else threading.Event()
        metrics = MetricsCollector()
        metrics.start()
        self.progress_callback("starting", "Connecting to broker", 0, scenario["messages"])
        self.logger.info("Benchmark started")
        self.logger.info("Producers launched: %s", scenario["producers"])
        self.logger.info("Consumers launched: %s", scenario["consumers"])

        client = self.client_factory()
        topic = f"benchmark-{scenario_name}"
        try:
            response = client.create_topic(topic, scenario["partitions"])
            if response.get("status") != "success" and response.get("error") != "TopicAlreadyExists":
                raise RuntimeError(response)
        finally:
            client.close()
        self.progress_callback("setup", "Benchmark topic created", 0, scenario["messages"])

        if stop_event.is_set():
            return self._cancelled(metrics, scenario)

        messages_per_producer = scenario["messages"] // scenario["producers"]
        producer_workers = [
            ProducerWorker(self.client_factory, topic, messages_per_producer, idx, metrics, stop_event=stop_event)
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
                stop_event=stop_event,
            )
            for idx in range(scenario["consumers"])
        ]

        for worker in consumer_workers:
            worker.start()
        self.progress_callback("running", f"Started {scenario['consumers']} consumers", 0, scenario["messages"])
        for worker in producer_workers:
            worker.start()
        self.progress_callback("running", f"Started {scenario['producers']} producers", 0, scenario["messages"])

        self._await_workers(
            producer_workers, lambda: f"Producing messages ({metrics.messages_produced}/{scenario['messages']} written)", metrics, scenario,
        )
        self.progress_callback(
            "running",
            "Cancelling: waiting for consumers to stop" if stop_event.is_set() else "Producers finished; consumers are draining messages",
            metrics.messages_produced,
            scenario["messages"],
        )
        self._await_workers(
            consumer_workers, lambda: f"Consuming messages ({metrics.messages_consumed}/{scenario['messages']} read)", metrics, scenario,
        )

        metrics.stop()

        if stop_event.is_set():
            return self._cancelled(metrics, scenario)

        report = metrics.summary()
        broker_metrics = self._broker_metrics()
        report["broker_metrics"] = broker_metrics
        report["consumer_lag"] = self._consumer_lag(broker_metrics, topic, f"{topic}-group")
        self.progress_callback("complete", "Benchmark complete", report["messages_produced"], scenario["messages"], report)
        self._save_report(report)
        self.logger.info("Messages produced: %s", report["messages_produced"])
        self.logger.info("Throughput: %.2f msg/sec", report["throughput"])
        self.logger.info("Benchmark completed")
        return report

    def _await_workers(self, workers, message_fn, metrics, scenario):
        while any(worker.is_alive() for worker in workers):
            for worker in workers:
                worker.join(timeout=0.25)
            self.progress_callback("running", message_fn(), metrics.messages_produced, scenario["messages"])

    def _cancelled(self, metrics, scenario):
        metrics.stop()
        self.progress_callback("cancelled", "Benchmark cancelled", metrics.messages_produced, scenario["messages"])
        self.logger.info("Benchmark cancelled")
        return None

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
