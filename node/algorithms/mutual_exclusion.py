import threading
import time

from config.config import NODES
from communication.client import send_message
from utils.logger import log

RELEASED = "RELEASED"
WANTED   = "WANTED"
HELD     = "HELD"


class RicartAgrawala:

    def __init__(self, node_id, total_nodes, clock):
        self.node_id = node_id
        self.total_nodes = total_nodes
        self.clock = clock

        self.state = RELEASED          # RELEASED | WANTED | HELD
        self.request_timestamp = None
        self.replies_received = 0
        self.deferred_replies = set()

        self.lock = threading.Lock()

    # ------------------------------------------------------------------ public

    def request_critical_section(self):
        # Capture timestamp before entering lock so clock.send_event() (which
        # has its own lock) never nests inside self.lock.
        ts = self.clock.send_event()

        with self.lock:
            self.state = WANTED
            self.request_timestamp = ts
            self.replies_received = 0

        log(self.node_id, "REQ_CS", ts=ts)

        for nid, (host, port) in NODES.items():
            if nid == self.node_id:
                continue
            send_message(host, port, {
                "type": "REQUEST",
                "sender": self.node_id,
                "clock": ts,
            })
            log(self.node_id, "SEND_REQUEST", to=nid)

    def handle_request(self, sender_id, timestamp):
        with self.lock:
            # Defer when we hold the CS, or when we have priority (lower tuple).
            should_defer = (
                self.state == HELD
                or (
                    self.state == WANTED
                    and (self.request_timestamp, self.node_id) < (timestamp, sender_id)
                )
            )
            if should_defer:
                self.deferred_replies.add(sender_id)

        if should_defer:
            log(self.node_id, "DEFER_REPLY", to=sender_id)
            return

        # Send reply outside the lock — no network I/O while holding lock.
        ts = self.clock.send_event()
        host, port = NODES[sender_id]
        send_message(host, port, {
            "type": "REPLY",
            "sender": self.node_id,
            "clock": ts,
        })
        log(self.node_id, "SEND_REPLY", to=sender_id)

    def handle_reply(self, sender_id):
        with self.lock:
            self.replies_received += 1
            count = self.replies_received
            needed = self.total_nodes - 1
            # Transition to HELD exactly once, atomically.
            enter_now = (count == needed and self.state == WANTED)
            if enter_now:
                self.state = HELD

        log(self.node_id, "RECV_REPLY",
            from_node=sender_id, replies=f"{count}/{needed}")

        if enter_now:
            log(self.node_id, "ENTER_CS")
            threading.Thread(
                target=self.execute_critical_section,
                daemon=True,
            ).start()

    # ----------------------------------------------------------------- private

    def execute_critical_section(self):
        log(self.node_id, "IN_CS")
        with open("shared_resource.txt", "a") as f:
            f.write(f"N{self.node_id} ENTER {time.time():.3f}\n")
            time.sleep(2)
            f.write(f"N{self.node_id} EXIT  {time.time():.3f}\n")
        log(self.node_id, "EXIT_CS")
        self._release()

    def _release(self):
        with self.lock:
            self.state = RELEASED
            self.request_timestamp = None
            deferred = list(self.deferred_replies)
            self.deferred_replies.clear()

        log(self.node_id, "RELEASE_CS")

        for nid in deferred:
            host, port = NODES[nid]
            ts = self.clock.send_event()
            send_message(host, port, {
                "type": "REPLY",
                "sender": self.node_id,
                "clock": ts,
            })
            log(self.node_id, "SEND_DEFERRED_REPLY", to=nid)
