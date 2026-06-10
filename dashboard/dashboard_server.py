import argparse
import json
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from client.kafka_client import KafkaClient


class DashboardAPI:
    def __init__(self, host="localhost", port=9092, data_dir="data"):
        self.host = host
        self.port = port
        self.data_dir = data_dir

    def _client(self):
        return KafkaClient(host=self.host, port=self.port)

    def snapshot(self):
        client = self._client()
        try:
            metrics = client.metrics()
            topics = []
            for topic_name, partitions in sorted(metrics.get("partition_offsets", {}).items()):
                partition_rows = []
                for partition_id, next_offset in sorted(partitions.items(), key=lambda item: int(item[0])):
                    messages = client.consume(topic_name, int(partition_id), 0, 5)
                    partition_rows.append(
                        {
                            "partition_id": int(partition_id),
                            "next_offset": next_offset,
                            "message_count": len(messages),
                            "sample_messages": messages,
                        }
                    )
                topics.append(
                    {
                        "name": topic_name,
                        "partitions": partition_rows,
                    }
                )

            return {
                "status": "success",
                "metrics": metrics,
                "topics": topics,
            }
        finally:
            client.close()


def build_parser():
    parser = argparse.ArgumentParser(description="Run the Mini Kafka dashboard")
    parser.add_argument("--host", default="localhost", help="Broker host")
    parser.add_argument("--port", type=int, default=8000, help="Dashboard port")
    parser.add_argument("--broker-host", default="localhost", help="Broker host for the dashboard backend")
    parser.add_argument("--broker-port", type=int, default=9092, help="Broker port for the dashboard backend")
    parser.add_argument("--data-dir", default="data", help="Broker data directory")
    return parser


class DashboardHandler(BaseHTTPRequestHandler):
    api = None
    root_dir = None

    def _send_json(self, payload, status=HTTPStatus.OK):
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path, content_type):
        with open(path, "rb") as handle:
            body = handle.read()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/snapshot":
            try:
                self._send_json(self.api.snapshot())
            except ConnectionError as exc:
                self._send_json({"status": "error", "message": str(exc)}, status=HTTPStatus.SERVICE_UNAVAILABLE)
            return

        if parsed.path in ("/", "/index.html"):
            self._send_file(os.path.join(self.root_dir, "index.html"), "text/html; charset=utf-8")
            return
        if parsed.path == "/styles.css":
            self._send_file(os.path.join(self.root_dir, "styles.css"), "text/css; charset=utf-8")
            return
        if parsed.path == "/app.js":
            self._send_file(os.path.join(self.root_dir, "app.js"), "application/javascript; charset=utf-8")
            return

        self.send_error(HTTPStatus.NOT_FOUND)

    def log_message(self, format, *args):
        return


def main(argv=None):
    args = build_parser().parse_args(argv)
    api = DashboardAPI(host=args.broker_host, port=args.broker_port, data_dir=args.data_dir)
    DashboardHandler.api = api
    DashboardHandler.root_dir = os.path.join(os.path.dirname(__file__), "static")
    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    print(f"MiniKafka Dashboard running at http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
