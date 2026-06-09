import socket
import unittest

from network.protocol import recv_json, send_json


class ProtocolTests(unittest.TestCase):
    def test_send_and_recv_json(self):
        left, right = socket.socketpair()
        try:
            send_json(left, {"hello": "world", "n": 1})
            self.assertEqual(recv_json(right), {"hello": "world", "n": 1})
        finally:
            left.close()
            right.close()


if __name__ == "__main__":
    unittest.main()

