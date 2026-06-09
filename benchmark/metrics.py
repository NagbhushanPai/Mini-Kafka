import math
import statistics
import time
from threading import Lock


class MetricsCollector:
    def __init__(self):
        self._lock = Lock()
        self.messages_produced = 0
        self.messages_consumed = 0
        self.producer_latencies = []
        self.end_to_end_latencies = []
        self.consumer_lag = []
        self.partition_counts = {}
        self._started_at = None
        self._finished_at = None

    def start(self):
        self._started_at = time.perf_counter()

    def stop(self):
        self._finished_at = time.perf_counter()

    @property
    def duration_seconds(self):
        if self._started_at is None:
            return 0.0
        end = self._finished_at if self._finished_at is not None else time.perf_counter()
        return max(0.0, end - self._started_at)

    def record_produced(self, partition, latency_ms):
        with self._lock:
            self.messages_produced += 1
            self.producer_latencies.append(latency_ms)
            self.partition_counts[partition] = self.partition_counts.get(partition, 0) + 1

    def record_consumed(self, latency_ms, lag=0):
        with self._lock:
            self.messages_consumed += 1
            self.end_to_end_latencies.append(latency_ms)
            self.consumer_lag.append(lag)

    def percentile(self, values, pct):
        if not values:
            return 0.0
        ordered = sorted(values)
        if len(ordered) == 1:
            return float(ordered[0])
        rank = math.ceil((pct / 100) * len(ordered))
        index = max(0, min(len(ordered) - 1, rank - 1))
        return float(ordered[index])

    def summary(self):
        duration = self.duration_seconds
        throughput = self.messages_produced / duration if duration else 0.0
        return {
            "throughput": throughput,
            "avg_producer_latency_ms": statistics.mean(self.producer_latencies) if self.producer_latencies else 0.0,
            "p50_producer_latency_ms": self.percentile(self.producer_latencies, 50),
            "p95_producer_latency_ms": self.percentile(self.producer_latencies, 95),
            "p99_producer_latency_ms": self.percentile(self.producer_latencies, 99),
            "max_producer_latency_ms": max(self.producer_latencies) if self.producer_latencies else 0.0,
            "avg_e2e_latency_ms": statistics.mean(self.end_to_end_latencies) if self.end_to_end_latencies else 0.0,
            "p50_e2e_latency_ms": self.percentile(self.end_to_end_latencies, 50),
            "p95_e2e_latency_ms": self.percentile(self.end_to_end_latencies, 95),
            "p99_e2e_latency_ms": self.percentile(self.end_to_end_latencies, 99),
            "max_e2e_latency_ms": max(self.end_to_end_latencies) if self.end_to_end_latencies else 0.0,
            "consumer_lag": max(self.consumer_lag) if self.consumer_lag else 0,
            "messages_produced": self.messages_produced,
            "messages_consumed": self.messages_consumed,
            "duration_seconds": duration,
            "partition_counts": dict(self.partition_counts),
        }
