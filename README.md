# MC714 — Distributed Systems Project

Implementation of three distributed algorithms using **Python + TCP sockets + Docker**:

| Algorithm | File |
|---|---|
| Lamport Logical Clock | `node/algorithms/lamport.py` |
| Ricart-Agrawala Mutual Exclusion | `node/algorithms/mutual_exclusion.py` |
| Bully Leader Election | `node/algorithms/leader_election.py` |

Communication is done exclusively via real TCP socket messages (`node/communication/`).  
`shared_resource.txt` is the *resource* being protected — it is not a communication channel.

---

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) 24+
- [Docker Compose](https://docs.docker.com/compose/install/) v2+ (`docker compose` — note: no hyphen)

---

## Build

```bash
docker compose build
```

Rebuild after any code change:

```bash
docker compose build --no-cache
```

---

## Running scenarios

Select a scenario with the `SCENARIO` environment variable:

| Value | What it demonstrates |
|---|---|
| `mutex_contention` *(default)* | All 4 nodes request the CS 3 times each with random delays — proves no overlapping access |
| `election_normal` | Node 1 triggers an election; node 4 (highest ID) wins |
| `election_failure` | Node 4 wins, then crashes; heartbeat detects absence; node 3 wins re-election |
| `lamport` | Causal chain N1→N2→N3 with strictly increasing Lamport timestamps |

### Linux / macOS

```bash
# Default scenario
docker compose up --build

# Specific scenario
SCENARIO=election_failure docker compose up --build
```

### Windows (PowerShell)

```powershell
# Default scenario
docker compose up --build

# Specific scenario
$env:SCENARIO="election_failure"; docker compose up --build
```

### Stop and remove containers

```bash
docker compose down
```

---

## Observing output

### Live logs (all nodes)

```bash
docker compose logs -f
```

### Logs for a single node

```bash
docker compose logs -f node3
```

### Critical section audit

`shared_resource.txt` records every entry and exit from the critical section:

```
N2 ENTER 1720000001.234
N2 EXIT  1720000003.241
N3 ENTER 1720000003.512
N3 EXIT  1720000005.520
```

Safety property: every `ENTER` must be followed by an `EXIT` from the same node
before any other node's `ENTER` appears.

```bash
cat shared_resource.txt
```

Clear between runs:

```bash
echo "" > shared_resource.txt    # Linux/macOS
echo. > shared_resource.txt      # Windows CMD
"" | Out-File shared_resource.txt -Encoding utf8  # PowerShell
```

---

## Algorithm notes

### Lamport Logical Clock
Every message carries a logical timestamp.  
On receive: `clock = max(local_clock, received_clock) + 1`.  
Ensures that if event *a* causally precedes *b*, then `timestamp(a) < timestamp(b)`.

### Ricart-Agrawala Mutual Exclusion
State machine per node: `RELEASED → WANTED → HELD → RELEASED`.

- **RELEASED**: not interested in the CS.
- **WANTED**: broadcasts REQUEST to all peers; defers incoming requests only when
  this node has priority `(own_timestamp, own_id) < (sender_timestamp, sender_id)`.
- **HELD**: inside the CS; defers *all* incoming requests unconditionally.
- On exit: sends deferred REPLY messages and transitions back to RELEASED.

### Bully Leader Election
Any node may trigger an election by sending ELECTION to all nodes with higher IDs.  
A higher-ID node responds with OK and starts its own election.  
The node that receives no OK within a timeout announces itself via COORDINATOR.  
Non-leaders monitor the leader with periodic heartbeat probes; on failure they
start a new election automatically.
