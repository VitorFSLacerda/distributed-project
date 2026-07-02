import json
import socket
import threading
import time

from communication.client import send_message
from config.config import NODES
from utils.logger import log

ELECTION    = "ELECTION"
OK          = "OK"
COORDINATOR = "COORDINATOR"
HEARTBEAT   = "HEARTBEAT"

_HB_INTERVAL = 2.0   # seconds between liveness probes
_HB_TIMEOUT  = 2.0   # TCP connect timeout for each probe


class BullyElection:

    def __init__(self, node_id):
        self.node_id = node_id
        self.leader = None
        self.election_in_progress = False
        self._announced = False   # idempotency guard: true once COORDINATOR sent
        self.lock = threading.Lock()

    # ------------------------------------------------------------------ public

    def start_election(self):
        with self.lock:
            if self.election_in_progress:
                return   # already running — ignore
            self.election_in_progress = True
            self._announced = False
            self.leader = None

        log(self.node_id, "ELECTION_START")

        higher = [nid for nid in NODES if nid > self.node_id]

        if not higher:
            # Highest node — win immediately without waiting.
            self._become_leader()
            return

        log(self.node_id, "CONTACT_HIGHER", nodes=higher)

        for nid in higher:
            host, port = NODES[nid]
            try:
                send_message(host, port, {
                    "type": ELECTION,
                    "sender": self.node_id,
                    "clock": 0,
                })
                log(self.node_id, "SEND_ELECTION", to=nid)
            except Exception:
                log(self.node_id, "SEND_ELECTION_FAIL", to=nid)

        threading.Thread(target=self._wait_for_ok, daemon=True).start()

    def receive_election(self, sender_id):
        log(self.node_id, "RECV_ELECTION", from_node=sender_id)

        host, port = NODES[sender_id]
        try:
            send_message(host, port, {
                "type": OK,
                "sender": self.node_id,
                "clock": 0,
            })
            log(self.node_id, "SEND_OK", to=sender_id)
        except Exception:
            log(self.node_id, "SEND_OK_FAIL", to=sender_id)

        with self.lock:
            is_leader = self.leader == self.node_id
            already_running = self.election_in_progress

        if is_leader:
            # Re-assert: sender missed our COORDINATOR.
            try:
                send_message(host, port, {
                    "type": COORDINATOR,
                    "sender": self.node_id,
                    "clock": 0,
                })
                log(self.node_id, "REASSERT_COORDINATOR", to=sender_id)
            except Exception:
                pass
        elif not already_running:
            self.start_election()

    def receive_ok(self):
        log(self.node_id, "RECV_OK")
        with self.lock:
            self.election_in_progress = False

    def receive_coordinator(self, leader_id):
        with self.lock:
            if self.leader == leader_id:
                return   # duplicate — already applied
            self.leader = leader_id
            self.election_in_progress = False

        log(self.node_id, "NEW_LEADER", leader=leader_id)
        self._start_heartbeat_monitor(leader_id)

    # ----------------------------------------------------------------- private

    def _wait_for_ok(self):
        time.sleep(2)
        with self.lock:
            still_waiting = self.election_in_progress and not self._announced
        if still_waiting:
            log(self.node_id, "NO_OK_TIMEOUT")
            self._become_leader()

    def _become_leader(self):
        with self.lock:
            if self._announced:
                return   # idempotent — announce only once
            self._announced = True
            self.leader = self.node_id
            self.election_in_progress = False

        log(self.node_id, "I_AM_LEADER")

        for nid, (host, port) in NODES.items():
            if nid == self.node_id:
                continue
            try:
                send_message(host, port, {
                    "type": COORDINATOR,
                    "sender": self.node_id,
                    "clock": 0,
                })
                log(self.node_id, "SEND_COORDINATOR", to=nid)
            except Exception:
                log(self.node_id, "SEND_COORDINATOR_FAIL", to=nid)

    def _start_heartbeat_monitor(self, leader_id):
        """Periodically probe the leader; trigger re-election on failure."""
        def monitor():
            while True:
                time.sleep(_HB_INTERVAL)
                with self.lock:
                    current = self.leader
                if current != leader_id:
                    return   # leader changed — this monitor is stale

                host, port = NODES[leader_id]
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(_HB_TIMEOUT)
                    sock.connect((host, port))
                    sock.sendall((json.dumps({
                        "type": HEARTBEAT,
                        "sender": self.node_id,
                        "clock": 0,
                    }) + "\n").encode())
                    sock.close()
                except (ConnectionRefusedError, OSError, socket.timeout):
                    log(self.node_id, "LEADER_UNREACHABLE", leader=leader_id)
                    with self.lock:
                        if self.leader == leader_id:
                            self.leader = None
                    self.start_election()
                    return   # new monitor will be started after new election

        threading.Thread(target=monitor, daemon=True).start()
