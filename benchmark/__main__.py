import argparse
import json
import logging
import sys

from client.kafka_client import KafkaClient

from .benchmark_runner import BenchmarkRunner
from .scenarios import SCENARIOS


def build_parser():
    parser = argparse.ArgumentParser(description="Run MiniKafka benchmark scenarios")
    parser.add_argument(
        "scenario",
        nargs="?",
        help="Benchmark scenario name (use --list to view available scenarios)",
    )
    parser.add_argument("--host", default="localhost", help="Broker host")
    parser.add_argument("--port", type=int, default=9092, help="Broker port")
    parser.add_argument(
        "--reports-dir",
        default="reports",
        help="Directory where benchmark_results.json will be written",
    )
    parser.add_argument("--list", action="store_true", help="List available benchmark scenarios")
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.list:
        print(json.dumps(SCENARIOS, indent=2, sort_keys=True))
        return 0

    if not args.scenario:
        parser.error("scenario is required unless --list is used")

    if args.scenario not in SCENARIOS:
        available = ", ".join(sorted(SCENARIOS))
        parser.error(f"unknown scenario '{args.scenario}'. Available scenarios: {available}")

    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

    def client_factory():
        return KafkaClient(host=args.host, port=args.port)

    runner = BenchmarkRunner(client_factory, reports_dir=args.reports_dir)
    report = runner.run(args.scenario)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
