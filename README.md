# streams

A persistent event engine in a single binary. **streams** is an append-only log
service exposed over a clean, JSON-first HTTP API: write events to named **boxes**,
read them back by sequence, fan them out with **routers**, and watch many boxes at
once over a single Server-Sent Events connection — all from one static binary on one
machine, backed by a write-ahead log on local NVMe. It is the persistence layer for
job queues, pub/sub, and durable event streams, with a design that makes data loss
**always explicit, never silent**.

> Status: **design phase**. This repository currently contains the complete API and
> architecture specification (see [docs/](docs/)). The implementation follows the
> [roadmap](docs/ROADMAP.md) in four phases.

---

## Mental model

Five concepts. That's the whole product.

- **Box** — a named, append-only log of records ordered by a monotonic `seq`. Think
  inbox/outbox. A box is created lazily on first write (or explicitly with config).
- **Record** — one immutable event in a box. The server assigns it a `$seq` (u64) and
  `$ts` (ms). It carries an opaque `data` payload, and optionally a `tag` (for
  read-time deletion), a `node` (origin id, for loop prevention), and small `meta`.
- **seq** — the cursor. Reads are "give me everything after `from_seq`." There is no
  opaque cursor token for box reads: the monotonic `seq` *is* the cursor. The client
  owns its position; advancing it is the ack.
- **Router** — a server-side forwarding rule `source → dest`. Every record appended to
  `source` is copied into `dest`. Routers fan out, and because the origin `node` rides
  through untouched, N symmetric nodes can mirror to each other without echo or loops.
- **Tombstone** — the explicit "you missed data" signal. If records you wanted were
  evicted (cap) or expired (TTL) before you read them, the read returns an in-band
  tombstone with the exact `[gap_from, gap_to]` range — at HTTP 200, never silently.

The load-bearing invariant: **capacity-driven loss you didn't ask for always produces
a tombstone; content-based removal you did ask for (tag-deletion, your own node's
events) is silently filtered.** Mixing those would make the gap alarm useless.

---

## Quickstart

Assume the binary is running on `localhost:4000` with auth disabled (dev mode).

### 1. Create a box (optional — first write auto-creates it)

```bash
curl -X PUT localhost:4000/v0/boxes/jobs \
  -H 'content-type: application/json' \
  -d '{ "durable": true, "cap_records": 1000000, "ttl_ms": 0 }'
```

```json
{ "box": "jobs", "created": true,
  "config": { "ttl_ms": 0, "cap_records": 1000000, "cap_bytes": 0,
              "discard": "old", "durable": true, "priority": null,
              "auto_priority": true, "auto_create": true,
              "idempotency_window_ms": 120000, "dedupe_node": true },
  "performance": { "server_total_ms": 0.22 } }
```

### 2. Write records (server assigns the seqs)

```bash
curl -X POST localhost:4000/v0/boxes/jobs \
  -H 'content-type: application/json' \
  -d '{ "node": "worker-eu-1",
        "records": [
          { "data": { "url": "s3://b/a.png", "w": 256 }, "tag": "tenant42:job-9001" },
          { "data": { "url": "s3://b/b.png", "w": 512 }, "tag": "tenant42:job-9002" }
        ] }'
```

```json
{ "box": "jobs", "first_seq": 1, "last_seq": 2, "seqs": [1, 2],
  "head_seq": 2, "count": 2, "created": false, "deduped": false,
  "performance": { "server_total_ms": 0.62, "fsync_ms": 0.39 } }
```

### 3. Read current state (head, earliest, count, config)

```bash
curl localhost:4000/v0/boxes/jobs
```

```json
{ "box": "jobs", "head_seq": 2, "earliest_seq": 1, "next_seq": 3,
  "count": 2, "bytes": 184, "effective_priority": 500, "config": { "...": "..." },
  "performance": { "server_total_ms": 0.05 } }
```

### 4. Read the difference from a cursor (batched, with tombstones)

```bash
curl -X POST localhost:4000/v0/boxes/jobs/diff \
  -H 'content-type: application/json' \
  -d '{ "from_seq": 0, "limit": 500, "node": "worker-eu-1" }'
```

```json
{ "box": "jobs",
  "records": [
    { "$seq": 1, "$ts": 1748470000123, "$tag": "tenant42:job-9001",
      "data": { "url": "s3://b/a.png", "w": 256 } },
    { "$seq": 2, "$ts": 1748470000140, "$tag": "tenant42:job-9002",
      "data": { "url": "s3://b/b.png", "w": 512 } }
  ],
  "next_from_seq": 2, "head_seq": 2, "earliest_seq": 1,
  "caught_up": true, "tombstone": null, "lag": 0,
  "performance": { "server_total_ms": 0.30 } }
```

(Records written by `worker-eu-1` are filtered out when `worker-eu-1` reads — loop
prevention. The cursor still advances past them.)

### 5. Watch many boxes over one SSE stream

```bash
# Step 1: create the watch session (carries the full subscription)
curl -X POST localhost:4000/v0/watch \
  -H 'content-type: application/json' \
  -d '{ "node": "worker-eu-1",
        "boxes": { "jobs": { "from_seq": 0 }, "events": { "tail": true } } }'
# -> { "wid": "wid_8f3ac21be9", "stream_url": "/v0/watch/wid_8f3ac21be9", ... }

# Step 2: open the stream (EventSource-compatible)
curl -N localhost:4000/v0/watch/wid_8f3ac21be9
```

```
retry: 2000

id: eyJqb2JzIjoxfQ
event: record
data: {"box":"jobs","records":[{"$seq":1,"$ts":1748470000123,"data":{"url":"s3://b/a.png"}}],"from_seq":0,"to_seq":1,"head_seq":2}

id: eyJqb2JzIjoyfQ
event: caught-up
data: {"box":"jobs","head_seq":2}

: hb 1748470015000
```

### 6. Cancel jobs by tag (read-time deletion)

```bash
# cancel one job
curl -X POST localhost:4000/v0/boxes/jobs/delete \
  -H 'content-type: application/json' \
  -d '{ "filters": [["tag", "Eq", "tenant42:job-9001"]] }'

# cancel an entire tenant (prefix wildcard)
curl -X POST localhost:4000/v0/boxes/jobs/delete \
  -H 'content-type: application/json' \
  -d '{ "filters": [["tag", "Glob", "tenant42:*"]] }'
```

---

## Phase 2 — running it

The current build is the **phase-2 in-memory server**: the complete `/v0` API,
correct semantics, but all state lives in RAM. **A restart loses all data** (this
is expected and documented; durability lands in phase 4).

```bash
# build the single binary
cargo build --release

# run it (defaults to 0.0.0.0:4000, auth disabled in dev mode)
./target/release/streams
```

Configuration is read from the environment:

| Variable | Default | Meaning |
|---|---|---|
| `STREAMS_PORT` | `4000` | Listen port (`STREAMS_HOST` may set a full `host:port`). |
| `STREAMS_API_KEYS` | _(unset)_ | Comma-separated bearer keys. Unset ⇒ **auth disabled** (dev mode). |
| `STREAMS_DATA_DIR` | _(unset)_ | Accepted placeholder; unused in phase 2 (in-memory), wired in phase 4. |
| `RUST_LOG` | `info` | Tracing filter. |

`durable: true` is accepted but is a no-op fast path in phase 2 (`fsync_ms` is
reported as `0.0`). The server shuts down gracefully on `SIGINT`/`SIGTERM`. The
quickstart commands above work verbatim against this build.

---

## Use-case recipes

### Job queue (Bull-style)

```bash
curl -X PUT localhost:4000/v0/boxes/jobs -d '{ "durable": true, "cap_records": 0 }'
```
Producers `POST /v0/boxes/jobs`. Each worker calls
`POST /v0/boxes/jobs/diff {from_seq, node:"worker-N", limit:50}`, processes the batch,
then persists `next_from_seq` as its ack (cursor-advance = ack-all). Cancel a job with a
tag delete; cancel a tenant with a `tag*` prefix delete. Durable + unbounded cap means
nothing is lost; replay is just reading from an earlier `from_seq`.

### Pub/sub (Redis-style, weak guarantees)

```bash
curl -X PUT localhost:4000/v0/boxes/feed \
  -d '{ "ttl_ms": 5000, "cap_records": 10000, "discard": "old", "durable": false }'
curl -X PUT localhost:4000/v0/routers/feed-to-a -d '{ "source":"feed", "dest":"sub-a" }'
curl -X PUT localhost:4000/v0/routers/feed-to-b -d '{ "source":"feed", "dest":"sub-b" }'
```
Subscribers `POST /v0/watch` on `sub-a` / `sub-b` with `tail: true`. Small cap + TTL keep
memory bounded; subscribers tolerate gaps, which arrive as explicit `tombstone` frames.

### Strong delivery / replay

```bash
curl -X PUT localhost:4000/v0/boxes/ledger \
  -d '{ "durable": true, "cap_records": 0, "ttl_ms": 0, "discard": "reject" }'
```
Unbounded + durable + `discard:"reject"` means eviction is impossible and TTL is off, so
there is no tombstone source at all — guaranteed no silent loss. Consumers persist their
cursor and replay from the last acked `from_seq`. If a cap is ever configured and hit,
`discard:"reject"` fails the producer's write synchronously rather than dropping data.

---

## Features

- **Append-only boxes** with server-assigned monotonic `seq` and `ts`.
- **Batched diff reads** — bounded batches, a plain `seq` cursor, in-band tombstones.
- **Explicit gap detection** — cap eviction and TTL expiry crossing a cursor always
  yield a tombstone with the exact missed range and a `reason`.
- **Read-time tag deletion** — cancel records by exact tag or `tag*` prefix; an
  efficient read-time filter, not a physical delete.
- **Node loop-prevention** — a node never receives back events it produced, making
  N-way multi-master fan-out safe by construction.
- **Routers** — server-side `source → dest` forwarding, at-least-once, per-source FIFO,
  cycle-rejecting by default.
- **Multiplexed SSE** — watch many boxes over one resumable connection with composite
  cursors, named events, heartbeats, and tombstones.
- **Per-box durability** — `fsync`-on-commit for durable boxes, group-committed fast
  path for the rest.
- **Priority + elastic throttling** — manual or recency-based auto priority; under CPU
  pressure delivery degrades in latency, never in correctness.
- **Single static binary** — WAL + segments on local NVMe, restartable at any instant;
  only data not yet in the WAL is lost.

---

## Build phases

1. **Define API + docs** — this repository. The contract the implementation must satisfy.
2. **In-memory server** — the complete, correct API with no WAL; not yet scalable or persistent.
3. **Tests + benchmarks** — maximum-coverage unit tests and a baseline benchmark suite.
4. **Make it scalable** — WAL, group commit, segments, recovery, priority scheduler,
   elastic throttling — while staying a single restartable process.

See [docs/ROADMAP.md](docs/ROADMAP.md) for acceptance criteria per phase.

---

## Documentation

- [docs/API.md](docs/API.md) — complete `/v0` HTTP API reference (the contract).
- [docs/DESIGN.md](docs/DESIGN.md) — data model & semantics: seq, watermarks, tombstones,
  tag-deletion, node loop-prevention, routers, priority.
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — storage, WAL, group commit, segments,
  recovery, scheduler, concurrency, crate choices, latency budget.
- [docs/ROADMAP.md](docs/ROADMAP.md) — build phases, acceptance criteria, benchmark plan.
