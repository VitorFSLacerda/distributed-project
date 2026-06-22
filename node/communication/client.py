import socket
import json


def send_message(host, port, payload):

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((host, port))

    client.send(json.dumps(payload).encode())
    client.close()