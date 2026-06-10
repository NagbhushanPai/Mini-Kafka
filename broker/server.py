import socket
import threading

from network.protocol import recv_json, send_json

from .broker import Broker
from .request_handler import RequestHandler


class BrokerServer:
    def __init__(
        self,
        host="localhost",
        port=9092,
        data_dir="data",
        offsets_dir=None,
        heartbeat_timeout=5.0,
        health_check_interval=2.0,
    ):
        self.host = host
        self.port = port
        self.broker = Broker(
            data_dir=data_dir,
            offsets_dir=offsets_dir,
            heartbeat_timeout=heartbeat_timeout,
            health_check_interval=health_check_interval,
        )
        self.handler = RequestHandler(self.broker)
        self._server_socket = None
        self._stop_event = threading.Event()

    def start(self):
        self.broker.group_manager.start_health_monitor()
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen()
        print("MiniKafka Broker Started")
        print(f"Listening on {self.host}:{self.port}")

        while not self._stop_event.is_set():
            try:
                client_socket, _ = self._server_socket.accept()
            except OSError:
                break
            threading.Thread(target=self._handle_client, args=(client_socket,), daemon=True).start()

    def stop(self):
        self._stop_event.set()
        self.broker.group_manager.stop_health_monitor()
        if self._server_socket is not None:
            self._server_socket.close()

    def _handle_client(self, client_socket):
        with client_socket:
            while True:
                request = recv_json(client_socket)
                if request is None:
                    break
                response = self.handler.handle(request)
                send_json(client_socket, response)
