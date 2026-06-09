import json
import weakref


_recv_buffers = weakref.WeakKeyDictionary()


def send_json(sock, data):
    payload = json.dumps(data, separators=(",", ":")).encode("utf-8") + b"\n"
    sock.sendall(payload)


def recv_json(sock):
    buffer = _recv_buffers.setdefault(sock, bytearray())
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
            del buffer[: newline_index + 1]
            return json.loads(line.decode("utf-8"))
    return json.loads(bytes(buffer).decode("utf-8"))
