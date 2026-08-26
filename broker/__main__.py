import argparse

from .server import BrokerServer


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run the Mini Kafka broker")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=9092)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--offsets-dir", default=None)
    args = parser.parse_args(argv)
    BrokerServer(
        host=args.host,
        port=args.port,
        data_dir=args.data_dir,
        offsets_dir=args.offsets_dir,
    ).start()


if __name__ == "__main__":
    main()
