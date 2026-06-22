import os
import time
import threading

from config import NODES
from server import start_server
from lamport import LamportClock
from mutual_exclusion import RicartAgrawala

clock = LamportClock()

NODE_ID = int(os.getenv("NODE_ID"))

mutex = RicartAgrawala(
    NODE_ID,
    len(NODES),
    clock
)

HOST, PORT = NODES[NODE_ID]

print(
    f"Node {NODE_ID} started",
    flush=True
)

server_thread = threading.Thread(
    target=start_server,
    args=("0.0.0.0", PORT, NODE_ID, clock, mutex),
    daemon=True
)

server_thread.start()

time.sleep(3)

if NODE_ID == 1:
    time.sleep(1)
    mutex.request_critical_section()

elif NODE_ID == 2:
    time.sleep(1)
    mutex.request_critical_section()

while True:
    time.sleep(1)