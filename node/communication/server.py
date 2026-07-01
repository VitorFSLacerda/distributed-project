import socket
import json
import threading

from algorithms.leader_election import ELECTION, OK, COORDINATOR, HEARTBEAT
from utils.logger import log


def _recv_line(conn):
    """Read bytes until newline; returns decoded string or '' on closed conn."""
    buf = b""
    while True:
        chunk = conn.recv(4096)
        if not chunk:
            return ""
        buf += chunk
        if b"\n" in buf:
            return buf.split(b"\n", 1)[0].decode()


def _handle(conn, node_id, clock, mutex, election):
    try:
        data = _recv_line(conn)
        if not data:
            return

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
        elif msg_type == HEARTBEAT:
            pass   # liveness probe — no action needed
    except Exception as e:
        log(node_id, "HANDLER_ERROR", error=str(e))
    finally:
        conn.close()


def start_server(host, port, node_id, clock, mutex, election=None):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen()

    log(node_id, "SERVER_START", port=port)

    while True:
        conn, _ = server.accept()
        threading.Thread(
            target=_handle,
            args=(conn, node_id, clock, mutex, election),
            daemon=True,
        ).start()
