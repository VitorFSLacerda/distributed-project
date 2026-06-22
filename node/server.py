import socket
import json

def start_server(host, port, node_id, clock):
    server = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    server.bind((host, port))
    server.listen()

    print(
        f"[Node {node_id}] Listening on {port}",
        flush=True
    )

    while True:
        conn, addr = server.accept()

        data = conn.recv(1024).decode()

        message = json.loads(data)

        received_clock = message["clock"]

        print(
            f"[Node {node_id}] Clock before = {clock.get_time()}",
            flush=True
        )

        clock.receive_event(received_clock)

        print(
            f"[Node {node_id}] Received from Node {message['sender']}",
            flush=True
        )

        print(
            f"[Node {node_id}] Clock after = {clock.get_time()}",
            flush=True
        )

        conn.close()