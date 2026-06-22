def log(node_id, event, **fields):
    extras = " ".join(f"{k}={v}" for k, v in fields.items())
    if extras:
        print(f"N{node_id} | {event} | {extras}", flush=True)
    else:
        print(f"N{node_id} | {event}", flush=True)


def section(node_id, title):
    print("\n" + "━" * 60, flush=True)
    print(f"N{node_id} | {title}", flush=True)
    print("━" * 60 + "\n", flush=True)