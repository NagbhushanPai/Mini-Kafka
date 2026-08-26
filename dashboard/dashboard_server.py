import argparse
import json
import os
import threading
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from client.kafka_client import KafkaClient
from benchmark.benchmark_runner import BenchmarkRunner
from benchmark.scenarios import SCENARIOS
from dashboard.sessions import SessionStore

BENCHMARK_JOBS = {}


class DashboardAPI:
    def __init__(self, host="localhost", port=9092, data_dir="data", offsets_file=os.path.join("offsets", "consumer_offsets.json"), sessions_dir="sessions"):
        self.host = host
        self.port = port
        self.data_dir = data_dir
        self.sessions = SessionStore(data_dir=data_dir, offsets_file=offsets_file, sessions_dir=sessions_dir)

    def _client(self):
        return KafkaClient(host=self.host, port=self.port)

    def reset(self):
        session = self.sessions.archive_current()
        client = self._client()
        try:
            client.reset()
        finally:
            client.close()
        return {"status": "success", "archived_session": session}

    def start_benchmark(self, scenario):
        job_id = str(uuid.uuid4())
        job = {"id": job_id, "status": "queued", "message": "Waiting to start", "progress": 0, "total": SCENARIOS[scenario]["messages"]}
        BENCHMARK_JOBS[job_id] = job
        def run():
            try:
                def progress(status, message, current, total, report=None):
                    job.update({"status": status, "message": message, "current": current, "total": total, "progress": round(current / total * 100, 1) if total else 0})
                    if report is not None: job["report"] = report
                runner = BenchmarkRunner(lambda: self._client(), progress_callback=progress)
                runner.run(scenario)
            except Exception as exc:
                job.update({"status": "error", "message": str(exc)})
        threading.Thread(target=run, daemon=True).start()
        return job

    def snapshot(self):
        client = self._client()
        try:
            metrics = client.metrics()
            topics = []
            for topic_name, partitions in sorted(metrics.get("partition_offsets", {}).items()):
                partition_rows = []
                for partition_id, next_offset in sorted(partitions.items(), key=lambda item: int(item[0])):
                    payload = client.consume(topic_name, int(partition_id), 0, 5)
                    if isinstance(payload, dict):
                        messages = payload.get("messages", [])
                    else:
                        messages = payload
                    if not isinstance(messages, list):
                        messages = []
                    partition_rows.append(
                        {
                            "partition_id": int(partition_id),
                            "next_offset": int(next_offset),
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
    parser.add_argument("--offsets-file", default=os.path.join("offsets", "consumer_offsets.json"), help="Broker offsets file")
    parser.add_argument("--sessions-dir", default="sessions", help="Directory to archive reset snapshots into")
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

        if parsed.path == "/api/messages":
            try:
                query = dict(item.split("=", 1) for item in parsed.query.split("&") if "=" in item)
                client = self.api._client()
                messages = client.consume(query["topic"], int(query.get("partition", 0)), int(query.get("offset", 0)), int(query.get("limit", 20)))
                client.close()
                self._send_json({"status": "success", "messages": messages})
            except Exception as exc:
                self._send_json({"status": "error", "message": str(exc)}, HTTPStatus.BAD_REQUEST)
            return
        if parsed.path == "/api/benchmarks/scenarios":
            self._send_json({"status": "success", "scenarios": SCENARIOS})
            return
        if parsed.path == "/api/sessions":
            self._send_json({"status": "success", "sessions": self.api.sessions.list_sessions()})
            return
        if parsed.path.startswith("/api/sessions/"):
            session_id = parsed.path[len("/api/sessions/"):]
            session = self.api.sessions.get_session(session_id)
            self._send_json(
                {"status": "success", "session": session} if session else {"status": "error", "message": "Session not found"},
                HTTPStatus.OK if session else HTTPStatus.NOT_FOUND,
            )
            return
        if parsed.path.startswith("/api/benchmarks/"):
            job = BENCHMARK_JOBS.get(parsed.path.rsplit("/", 1)[-1])
            self._send_json(job or {"status": "error", "message": "Benchmark not found"}, HTTPStatus.OK if job else HTTPStatus.NOT_FOUND)
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

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            payload = json.loads(self.rfile.read(length) or b"{}")
            client = self.api._client()
            try:
                if self.path == "/api/topics":
                    result = client.create_topic(payload["name"], int(payload["partitions"]))
                elif self.path == "/api/benchmarks/run":
                    client.close()
                    result = self.api.start_benchmark(payload["scenario"])
                    self._send_json(result, HTTPStatus.ACCEPTED)
                    return
                elif self.path == "/api/reset":
                    client.close()
                    result = self.api.reset()
                    self._send_json(result)
                    return
                elif self.path == "/api/messages":
                    value = payload.get("value")
                    if isinstance(value, str):
                        try:
                            value = json.loads(value)
                        except json.JSONDecodeError:
                            pass
                    result = client.produce(payload["topic"], payload.get("key", ""), value)
                elif self.path == "/api/groups/join":
                    result = client.join_group(payload["group_id"], payload["consumer_id"], payload["topic"])
                elif self.path == "/api/groups/poll":
                    result = client.consume_assigned(payload["group_id"], payload["consumer_id"], int(payload.get("batch_size", 10)))
                elif self.path == "/api/groups/leave":
                    result = client.leave_group(payload["group_id"], payload["consumer_id"])
                else:
                    self._send_json({"status": "error", "message": "Not found"}, HTTPStatus.NOT_FOUND)
                    return
            finally:
                client.close()
            self._send_json(result)
        except Exception as exc:
            self._send_json({"status": "error", "message": str(exc)}, HTTPStatus.BAD_REQUEST)

    def log_message(self, format, *args):
        return


def main(argv=None):
    args = build_parser().parse_args(argv)
    api = DashboardAPI(host=args.broker_host, port=args.broker_port, data_dir=args.data_dir, offsets_file=args.offsets_file, sessions_dir=args.sessions_dir)
    DashboardHandler.api = api
    DashboardHandler.root_dir = os.path.join(os.path.dirname(__file__), "static")
    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    print(f"MiniKafka Dashboard running at http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
