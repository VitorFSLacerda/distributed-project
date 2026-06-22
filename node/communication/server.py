import socket
import json

from algorithms.leader_election import (
    ELECTION,
    OK,
    COORDINATOR
)
from utils.logger import log


def start_server(host, port, node_id, clock, mutex, election=None):

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen()

    log(node_id, "SERVER_START", port=port)

    while True:

        conn, _ = server.accept()
        data = conn.recv(1024).decode()

        if not data:
            conn.close()
            continue

        message = json.loads(data)

        msg_type = message["type"]
        sender = message["sender"]
        recv_clock = message["clock"]

        log(node_id, "RECV_MSG", type=msg_type, from_node=sender)

        clock.receive_event(recv_clock)

        if msg_type == "REQUEST":
            mutex.handle_request(sender, recv_clock)

        elif msg_type == "REPLY":
            mutex.handle_reply(sender)

        elif msg_type == ELECTION:
            if election:
                election.receive_election(sender)

        elif msg_type == OK:
            if election:
                election.receive_ok()

        elif msg_type == COORDINATOR:
            if election:
                election.receive_coordinator(sender)

        conn.close()