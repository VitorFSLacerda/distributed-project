import os
import time
import threading

from config import NODES
from server import start_server
from client import send_message
from lamport import LamportClock

clock = LamportClock()

NODE_ID = int(os.getenv("NODE_ID"))

HOST, PORT = NODES[NODE_ID]

print(
    f"Node {NODE_ID} started",
    flush=True
)

server_thread = threading.Thread(
    target=start_server,
    args=("0.0.0.0", PORT, NODE_ID, clock),
    daemon=True
)

server_thread.start()

time.sleep(3)

if NODE_ID == 1:

    timestamp = clock.send_event()

    send_message(
        "node2",
        5002,
        {
            "sender": NODE_ID,
            "clock": timestamp,
            "message": "Hello from node1"
        }
    )

    print(
        f"[Node {NODE_ID}] Sent message with clock {timestamp}",
        flush=True
    )

while True:
    time.sleep(1)