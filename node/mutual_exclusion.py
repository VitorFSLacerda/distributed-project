from config import NODES
from client import send_message

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

        print(
            f"[Node {self.node_id}] REQUEST timestamp={self.request_timestamp}",
            flush=True
        )

        for node_id, (host, port) in NODES.items():

            if node_id == self.node_id:
                continue

            send_message(
                host,
                port,
                {
                    "type": "REQUEST",
                    "sender": self.node_id,
                    "clock": self.request_timestamp
                }
            )

            print(
                f"[Node {self.node_id}] REQUEST sent to Node {node_id}",
                flush=True
            )

    def handle_request(self, sender_id, timestamp):

        should_reply = False

        if not self.requesting:
            should_reply = True

        elif (timestamp, sender_id) < (
            self.request_timestamp,
            self.node_id
        ):
            should_reply = True

        if should_reply:

            reply_timestamp = self.clock.send_event()

            host, port = NODES[sender_id]

            send_message(
                host,
                port,
                {
                    "type": "REPLY",
                    "sender": self.node_id,
                    "clock": reply_timestamp
                }
            )

            print(
                f"[Node {self.node_id}] REPLY sent to Node {sender_id}",
                flush=True
            )

        else:

            self.deferred_replies.add(sender_id)

            print(
                f"[Node {self.node_id}] Deferred REPLY to Node {sender_id}",
                flush=True
            )

    def handle_reply(self, sender_id):

        with self.lock:

            self.replies_received += 1

            current_replies = self.replies_received

        print(
            f"[Node {self.node_id}] REPLY received from Node {sender_id}",
            flush=True
        )

        if current_replies == self.total_nodes - 1:

            print(
                f"[Node {self.node_id}] ALL REPLIES RECEIVED",
                flush=True
            )

            print(
                f"[Node {self.node_id}] ENTERING CRITICAL SECTION",
                flush=True
            )

            threading.Thread(
                target=self.execute_critical_section,
                daemon=True
            ).start()

    def execute_critical_section(self):

        print(
            f"[Node {self.node_id}] INSIDE CRITICAL SECTION",
            flush=True
        )

        with open("shared_resource.txt", "a") as file:

            file.write(
                f"Node {self.node_id} entered CS at {time.time()}\n"
            )

            file.flush()

            time.sleep(5)

            file.write(
                f"Node {self.node_id} left CS at {time.time()}\n"
            )

        print(
            f"[Node {self.node_id}] LEAVING CRITICAL SECTION",
            flush=True
        )

        self.release_critical_section()

    def release_critical_section(self):

        self.requesting = False
        self.request_timestamp = None

        for node_id in self.deferred_replies:

            host, port = NODES[node_id]

            timestamp = self.clock.send_event()

            send_message(
                host,
                port,
                {
                    "type": "REPLY",
                    "sender": self.node_id,
                    "clock": timestamp
                }
            )

            print(
                f"[Node {self.node_id}] Deferred REPLY sent to Node {node_id}",
                flush=True
            )

        self.deferred_replies.clear()