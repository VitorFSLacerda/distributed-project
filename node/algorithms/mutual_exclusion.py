from config.config import NODES
from communication.client import send_message
from utils.logger import log

import threading
import time


class RicartAgrawala:

    def __init__(self, node_id, total_nodes, clock):

        self.node_id = node_id
        self.total_nodes = total_nodes
        self.clock = clock

        self.requesting = False
        self.request_timestamp = None

        self.replies_received = 0
        self.deferred_replies = set()

        self.lock = threading.Lock()

    def request_critical_section(self):

        self.requesting = True
        self.request_timestamp = self.clock.send_event()
        self.replies_received = 0

        log(self.node_id, "REQ_CS", ts=self.request_timestamp)

        for nid, (host, port) in NODES.items():

            if nid == self.node_id:
                continue

            send_message(host, port, {
                "type": "REQUEST",
                "sender": self.node_id,
                "clock": self.request_timestamp
            })

            log(self.node_id, "SEND_REQUEST", to=nid)

    def handle_request(self, sender_id, timestamp):

        should_reply = (
            not self.requesting
            or (timestamp, sender_id) < (self.request_timestamp, self.node_id)
        )

        if should_reply:

            ts = self.clock.send_event()

            host, port = NODES[sender_id]

            send_message(host, port, {
                "type": "REPLY",
                "sender": self.node_id,
                "clock": ts
            })

            log(self.node_id, "SEND_REPLY", to=sender_id)

        else:
            self.deferred_replies.add(sender_id)
            log(self.node_id, "DEFER_REPLY", to=sender_id)

    def handle_reply(self, sender_id):

        with self.lock:
            self.replies_received += 1
            count = self.replies_received

        log(
            self.node_id,
            "RECV_REPLY",
            from_node=sender_id,
            replies=f"{count}/{self.total_nodes - 1}"
        )

        if count == self.total_nodes - 1:

            log(self.node_id, "ENTER_CS")

            threading.Thread(
                target=self.execute_critical_section,
                daemon=True
            ).start()

    def execute_critical_section(self):

        log(self.node_id, "IN_CS")

        with open("shared_resource.txt", "a") as f:
            f.write(f"N{self.node_id} ENTER {time.time()}\n")
            time.sleep(5)
            f.write(f"N{self.node_id} EXIT {time.time()}\n")

        log(self.node_id, "EXIT_CS")

        self.release_critical_section()

    def release_critical_section(self):

        self.requesting = False
        self.request_timestamp = None

        log(self.node_id, "RELEASE_CS")

        for nid in self.deferred_replies:

            host, port = NODES[nid]
            ts = self.clock.send_event()

            send_message(host, port, {
                "type": "REPLY",
                "sender": self.node_id,
                "clock": ts
            })

            log(self.node_id, "SEND_DEFERRED_REPLY", to=nid)

        self.deferred_replies.clear()