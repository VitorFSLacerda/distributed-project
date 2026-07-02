import socket
import json
import time

MAX_RETRIES = 6
_INITIAL_BACKOFF = 0.2   # seconds; doubles each attempt


def send_message(host, port, payload):
    data = (json.dumps(payload) + "\n").encode()
    backoff = _INITIAL_BACKOFF
    for attempt in range(MAX_RETRIES):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3.0)
            sock.connect((host, port))
            sock.sendall(data)
            sock.close()
            return
        except (ConnectionRefusedError, OSError, socket.timeout):
            sock.close()
            if attempt < MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2
