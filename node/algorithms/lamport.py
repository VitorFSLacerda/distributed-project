import threading
from utils.logger import log


class LamportClock:

    def __init__(self, node_id=None):
        self.node_id = node_id
        self._time = 0
        self._lock = threading.Lock()

    def send_event(self):
        with self._lock:
            self._time += 1
            t = self._time
        log(self.node_id, "LAMPORT_SEND", clock=t)
        return t

    def receive_event(self, received_time):
        with self._lock:
            old = self._time
            self._time = max(self._time, received_time) + 1
            new = self._time
        log(self.node_id, "LAMPORT_RECV", received=received_time, before=old, after=new)

    def get_time(self):
        with self._lock:
            return self._time
