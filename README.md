# Aegian

A distributed, strongly-consistent key-value store built from scratch in Go, implementing the **Raft consensus algorithm**.

*Aegian* — guards one consistent state.

---

## What it is

Aegian is a cluster of nodes that together act as a single reliable key-value store. Data is replicated across all nodes, so the system survives node failures without losing committed writes. It chooses **consistency over availability** (the CP side of the CAP theorem): when a majority of nodes can't agree, it refuses to serve rather than risk returning a wrong answer.

## Features

- **Leader election** with randomized timeouts and term-based voting.
- **Log replication** with the Raft consistency check, so all node logs converge to identical.
- **Strong consistency (CP):** a write is only acknowledged once a majority of nodes have committed it.
- **Crash recovery:** each node persists its term, vote, and log to disk (via bbolt) and restores on restart.
- **Fault tolerance:** a 3-node cluster survives one node failing and elects a new leader automatically.
- **HTTP API:** simple `GET` / `PUT` / `DELETE` over HTTP, with writes routed to the leader.
- **One-command cluster:** runs as a 3-node cluster via Docker Compose.

## Tech stack

- **Go** — the implementation language.
- **gRPC + Protocol Buffers** — node-to-node consensus communication.
- **bbolt** — embedded on-disk persistence for Raft state.
- **net/http** — the client-facing API.
- **Docker + Docker Compose** — packaging and running the cluster.

---

## Running the cluster

Requires Docker (and Docker Desktop on Windows/macOS).

```bash
docker compose up --build
```

This starts a 3-node cluster. The nodes elect a leader, then sit idle waiting for client commands. The nodes are reachable from the host at:

- node 1 → `localhost:3001`
- node 2 → `localhost:3002`
- node 3 → `localhost:3003`

To stop the cluster:

```bash
docker compose down
```

## Usage

Writes must go to the current leader. Writing to a follower returns a redirect message pointing at the leader.

```bash
# Store a value (send to the leader)
curl -X PUT localhost:3002/aegian/name -d "vimal"

# Read it back from any node
curl localhost:3001/aegian/name

# Delete it
curl -X DELETE localhost:3002/aegian/name
```

### Testing fault tolerance

Kill the leader and watch the remaining nodes elect a new one, with data intact:

```bash
docker compose stop node2      # stop the current leader
curl localhost:3001/aegian/name   # data still available from the survivors
docker compose start node2     # node rejoins and catches up
```

---

## Architecture

Each node runs the same binary and contains:

- **Raft core** — leader election, log replication, commit logic.
- **gRPC server** — handles node-to-node consensus RPCs (`RequestVote`, `AppendEntries`).
- **HTTP server** — handles client `GET`/`PUT`/`DELETE` requests.
- **Storage layer** — persists term, vote, and log to disk.

## License

MIT
