import os
import time
import random
import threading

from config.config import NODES
from communication.server import start_server
from algorithms.lamport import LamportClock
from algorithms.mutual_exclusion import RicartAgrawala, RELEASED
from algorithms.leader_election import BullyElection
from utils.logger import section, log


NODE_ID  = int(os.getenv("NODE_ID"))
SCENARIO = os.getenv("SCENARIO", "mutex_contention")

clock    = LamportClock(NODE_ID)
mutex    = RicartAgrawala(NODE_ID, len(NODES), clock)
election = BullyElection(NODE_ID)

HOST, PORT = NODES[NODE_ID]

section(NODE_ID, f"NODE INIT | SCENARIO={SCENARIO}")
log(NODE_ID, "INIT", host=HOST, port=PORT)

threading.Thread(
    target=start_server,
    args=("0.0.0.0", PORT, NODE_ID, clock, mutex, election),
    daemon=True,
).start()

# Short deterministic pause so all four servers finish bind/listen.
# The retry+backoff in client.py covers any remaining startup race.
time.sleep(1)

log(NODE_ID, "SYSTEM_READY", scenario=SCENARIO)


# ──────────────────────────────────────────────────────────────────────────────
# Scenarios
# ──────────────────────────────────────────────────────────────────────────────

def scenario_mutex_contention():
    """
    All four nodes compete for the critical section three times each with
    random delays.  shared_resource.txt proves no two ENTER lines overlap.
    """
    for round_num in range(1, 4):
        delay = random.uniform(0.0, 2.0)
        time.sleep(delay)
        log(NODE_ID, "ROUND_START", round=round_num)
        mutex.request_critical_section()
        # Spin until released before starting the next round.
        while mutex.state != RELEASED:
            time.sleep(0.1)
        log(NODE_ID, "ROUND_END", round=round_num)
        time.sleep(random.uniform(0.5, 1.5))


def scenario_election_normal():
    """
    Node 1 triggers an election.  Node 4 (highest ID) must win and broadcast
    a single COORDINATOR message.
    """
    if NODE_ID == 1:
        time.sleep(1)
        election.start_election()


def scenario_election_failure():
    """
    Node 4 wins the initial election, then exits (simulating a crash).
    The heartbeat monitor on remaining nodes detects the absence and triggers
    a new election.  Node 3 (next highest) should become the new leader.
    """
    if NODE_ID == 4:
        time.sleep(1)
        election.start_election()
        time.sleep(4)                      # allow COORDINATOR to propagate
        log(NODE_ID, "LEADER_CRASH_SIMULATED")
        os._exit(0)                        # hard exit — simulates crash
    # Nodes 1-3: receive COORDINATOR from node 4, start heartbeat monitor,
    # detect failure, and re-elect automatically — no explicit code needed here.


def scenario_lamport():
    """
    Causal chain: node 1 → node 2 → node 3.
    Node 1 requests CS first; its REQUEST carries timestamp T1.
    Node 2 receives it (clock advances to T1+1), then requests CS with T2 > T1.
    Node 3 receives node 2's REQUEST (clock advances again), then requests CS
    with T3 > T2.  LAMPORT_RECV log lines show the strictly increasing chain.
    """
    if NODE_ID in (1, 2, 3):
        delay = (NODE_ID - 1) * 2.0    # stagger: N1 at t=0, N2 at t=2, N3 at t=4
        time.sleep(delay)
        log(NODE_ID, "LAMPORT_DEMO_REQUEST")
        mutex.request_critical_section()
        while mutex.state != RELEASED:
            time.sleep(0.1)


SCENARIOS = {
    "mutex_contention":  scenario_mutex_contention,
    "election_normal":   scenario_election_normal,
    "election_failure":  scenario_election_failure,
    "lamport":           scenario_lamport,
}

runner = SCENARIOS.get(SCENARIO)
if runner:
    threading.Thread(target=runner, daemon=False).start()
else:
    log(NODE_ID, "UNKNOWN_SCENARIO", scenario=SCENARIO,
        valid=list(SCENARIOS.keys()))

while True:
    time.sleep(1)
