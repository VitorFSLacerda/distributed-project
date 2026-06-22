from utils.logger import log


class LamportClock:

    def __init__(self, node_id=None):
        self.time = 0
        self.node_id = node_id

    def increment(self):
        self.time += 1

    def send_event(self):
        self.increment()
        log(self.node_id, "LAMPORT_SEND", clock=self.time)
        return self.time

    def receive_event(self, received_time):
        old = self.time
        self.time = max(self.time, received_time) + 1

        log(
            self.node_id,
            "LAMPORT_RECV",
            received=received_time,
            before=old,
            after=self.time
        )

    def get_time(self):
        return self.time