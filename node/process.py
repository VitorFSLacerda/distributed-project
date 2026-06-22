import os
import time
import threading

from config.config import NODES
from communication.server import start_server
from algorithms.lamport import LamportClock
from algorithms.mutual_exclusion import RicartAgrawala
from algorithms.leader_election import BullyElection
from utils.logger import section, log


NODE_ID = int(os.getenv("NODE_ID"))

clock = LamportClock(NODE_ID)

mutex = RicartAgrawala(NODE_ID, len(NODES), clock)
election = BullyElection(NODE_ID)

HOST, PORT = NODES[NODE_ID]

section(NODE_ID, "NODE INIT")
log(NODE_ID, "INIT", host=HOST, port=PORT)

threading.Thread(
    target=start_server,
    args=("0.0.0.0", PORT, NODE_ID, clock, mutex, election),
    daemon=True
).start()

time.sleep(3)

log(NODE_ID, "SYSTEM_READY")

if NODE_ID in (1, 2):
    time.sleep(1)
    mutex.request_critical_section()

if NODE_ID == 1:
    time.sleep(5)
    election.start_election()

while True:
    time.sleep(1)