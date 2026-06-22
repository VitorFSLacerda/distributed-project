class LamportClock:

    def __init__(self):
        self.time = 0

    def increment(self):
        self.time += 1

    def send_event(self):
        self.increment()
        return self.time

    def receive_event(self, received_time):
        self.time = max(self.time, received_time) + 1

    def get_time(self):
        return self.time