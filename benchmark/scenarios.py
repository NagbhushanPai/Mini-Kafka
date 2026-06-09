SCENARIOS = {
    "baseline": {"producers": 1, "consumers": 1, "messages": 10000, "partitions": 4},
    "moderate": {"producers": 5, "consumers": 5, "messages": 50000, "partitions": 8},
    "high_concurrency": {"producers": 10, "consumers": 20, "messages": 100000, "partitions": 16},
    "partition_stress": {"producers": 20, "consumers": 20, "messages": 100000, "partitions": 16},
}
