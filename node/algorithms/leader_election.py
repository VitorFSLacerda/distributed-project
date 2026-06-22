import threading
import time

from communication.client import send_message
from config.config import NODES
from utils.logger import log


ELECTION = "ELECTION"
OK = "OK"
COORDINATOR = "COORDINATOR"


class BullyElection:

    def __init__(self, node_id):
        self.node_id = node_id
        self.leader = None
        self.election_in_progress = False
        self.lock = threading.Lock()

    def start_election(self):

        with self.lock:
            self.election_in_progress = True

        log(self.node_id, "ELECTION_START")

        higher = [nid for nid in NODES if nid > self.node_id]

        if not higher:
            self.become_leader()
            return

        log(self.node_id, "CONTACT_HIGHER", nodes=higher)

        for nid in higher:
            host, port = NODES[nid]

            send_message(host, port, {
                "type": ELECTION,
                "sender": self.node_id,
                "clock": 0
            })

            log(self.node_id, "SEND_ELECTION", to=nid)

        threading.Thread(target=self.wait_for_ok, daemon=True).start()

    def wait_for_ok(self):

        time.sleep(2)

        with self.lock:
            if self.election_in_progress:
                log(self.node_id, "NO_OK -> BECOME_LEADER")
                self.become_leader()

    def receive_election(self, sender_id):

        log(self.node_id, "RECV_ELECTION", from_node=sender_id)

        host, port = NODES[sender_id]

        send_message(host, port, {
            "type": OK,
            "sender": self.node_id,
            "clock": 0
        })

        log(self.node_id, "SEND_OK", to=sender_id)

        if not self.election_in_progress:
            self.start_election()

    def receive_ok(self):

        log(self.node_id, "RECV_OK")

        with self.lock:
            self.election_in_progress = False

    def become_leader(self):

        with self.lock:
            self.leader = self.node_id
            self.election_in_progress = False

        log(self.node_id, "I_AM_LEADER")

        for nid, (host, port) in NODES.items():

            if nid == self.node_id:
                continue

            send_message(host, port, {
                "type": COORDINATOR,
                "sender": self.node_id,
                "clock": 0
            })

            log(self.node_id, "SEND_COORDINATOR", to=nid)

    def receive_coordinator(self, leader_id):

        self.leader = leader_id
        self.election_in_progress = False

        log(self.node_id, "NEW_LEADER", leader=leader_id)