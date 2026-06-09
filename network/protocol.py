import json
import socket as socket_module


def send_json(sock, data):
    payload = json.dumps(data, separators=(",", ":")).encode("utf-8") + b"\n"
    sock.sendall(payload)


def recv_json(sock):
    buffer = bytearray()
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            if not buffer:
                return None
            break
        buffer.extend(chunk)
        newline_index = buffer.find(b"\n")
        if newline_index != -1:
            line = bytes(buffer[:newline_index])
            return json.loads(line.decode("utf-8"))
    return json.loads(bytes(buffer).decode("utf-8"))

