# streams

A persistent event engine in a single binary. **streams** is an append-only log
service exposed over a clean, JSON-first HTTP API: write events to named **boxes**,
read them back by sequence, fan them out with **routers**, and watch many boxes at
once over a single Server-Sent Events connection — all from one static binary on one
machine, backed by a write-ahead log on local NVMe. It is the persistence layer for
job queues, pub/sub, and durable event streams, with a design that makes data loss
**always explicit, never silent**.

> Status: **implemented and durable.** The full `/v0` API is built and tested
> (boxes, diff reads, deletes, routers, multiplexed SSE, and lease-based queues),
> backed by a write-ahead log with group commit, segments, snapshots, and
> crash-recovery replay on a single restartable process. A `durable` box's writes
> are **fsync-gated** — an acknowledged write is committed, and a restart replays
> the WAL to recover it. See [docs/](docs/) for the specification and the
> [roadmap](docs/ROADMAP.md) for how it was built.

---

## Mental model

Five concepts. That's the whole product.

- **Box** — a named, append-only log of records ordered by a monotonic `seq`. Think
  inbox/outbox. A box is created lazily on first write (or explicitly with config).
- **Record** — one immutable event in a box. The server assigns it a `$seq` (u64) and
  `$ts` (ms). It carries an opaque `data` payload, and optionally a `tag` (a match key
  for deletion), a `node` (origin id, for loop prevention), and small `meta`.
- **seq** — the cursor. Reads are "give me everything after `from_seq`." There is no
  opaque cursor token for box reads: the monotonic `seq` *is* the cursor. The client
  owns its position; advancing it is the ack.
- **Router** — a server-side forwarding rule `source → dest`. Every record appended to
  `source` is copied into `dest`. Routers fan out, and because the origin `node` rides
  through untouched, N symmetric nodes can mirror to each other without echo or loops.
- **Tombstone** — the explicit "you missed data" signal. If records you wanted were
  evicted (cap) or expired (TTL) before you read them, the read returns an in-band
  tombstone with the exact `[gap_from, gap_to]` range — at HTTP 200, never silently.
- **Delete** — a permanent, asynchronous, point-in-time removal of records, by `seq`
  range and/or by `tag` match. A delete is **silent** (never a tombstone) and takes
  effect immediately on all reads; the physical memory/disk is reclaimed lazily in the
  background. It only removes records that exist at call time — it is not a standing
  filter, so future records are never affected.

The load-bearing invariant: **involuntary capacity-driven loss you didn't ask for
(cap eviction, TTL expiry) always produces a tombstone; voluntary removal you did ask
for (permanent delete, your own node's events) is silently filtered.** Mixing those
would make the gap alarm useless. A delete advances the box's `earliest_seq` (first
live seq) but never its `evict_floor` (the cap/TTL tombstone trigger), so reading
across a purely-deleted gap is silent while reading below an evicted floor tombstones.

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
# -> { "wid": "wid_BuRguGorNdVFWNQULz-rrw", "stream_url": "/v0/watch/wid_BuRguGorNdVFWNQULz-rrw", ... }
# The wid is an unguessable random capability: possessing it authorizes the GET
# stream (so EventSource needs no api key in the URL). When auth is on, the wid
# is bound to the key that created it.

# Step 2: open the stream (EventSource-compatible)
curl -N localhost:4000/v0/watch/wid_BuRguGorNdVFWNQULz-rrw
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

### 6. Delete records (permanent, point-in-time, by seq and/or tag)

```bash
# cancel one job (exact tag match — removes records present right now)
curl -X POST localhost:4000/v0/boxes/jobs/delete \
  -H 'content-type: application/json' \
  -d '{ "match": ["tag", "Eq", "tenant42:job-9001"] }'

# cancel an entire tenant (trailing-prefix match)
curl -X POST localhost:4000/v0/boxes/jobs/delete \
  -H 'content-type: application/json' \
  -d '{ "match": ["tag", "Glob", "tenant42:*"] }'
```

```json
{ "box": "jobs", "deleted": 1, "earliest_seq": 3, "head_seq": 2, "count": 0, "bytes": 0,
  "performance": { "server_total_ms": 0.12 } }
```

(Both records carried `tenant42:` tags, so after the two deletes the box is empty:
`count` 0 and `earliest_seq` = `head_seq + 1` = 3. `deleted` is the count removed by
*this* call.)

The delete is **permanent** (no un-delete), **silent** (no tombstone), takes effect
**immediately** on all reads, and is **point-in-time**: a `match`-only delete is
bounded by the current head, so a job enqueued a moment later by an in-flight producer
is *not* deleted. Three patterns:

```bash
# Snapshot / compaction: drop everything before a seq (e.g. after a checkpoint)
curl -X POST localhost:4000/v0/boxes/jobs/delete -d '{ "before_seq": 480000 }'

# Message update: publish v2, then delete the prior versions but keep the new one
curl -X POST localhost:4000/v0/boxes/chat/delete \
  -d '{ "match": ["tag", "Eq", "msg-123"], "before_seq": 5012 }'   # 5012 = seq of v2

# Chat revoke: a kicked user's whole sub-stream (prefix), point-in-time
curl -X POST localhost:4000/v0/boxes/chat/delete \
  -d '{ "match": ["tag", "Glob", "chat-42:*"] }'
```

---

## Running it

The build is a **durable single binary**: the complete `/v0` API backed by a
write-ahead log on local disk. On start it opens the data directory, loads the
latest snapshot, and **replays the WAL forward** (truncating any torn tail) before
serving — so an acknowledged durable write survives a restart. The readiness gate
(`GET /v0/ready`) returns `503` during replay and `200` once recovery completes.

```bash
# build the single binary
cargo build --release

# run it (defaults to 127.0.0.1:4000 loopback, auth disabled in dev mode;
# WAL + segments + snapshots live under ./streams-data)
./target/release/streams
```

Configuration is read from the environment:

| Variable | Default | Meaning |
|---|---|---|
| `STREAMS_HOST` | `127.0.0.1` | Bind host (loopback-only by default; may also be a full `host:port`). |
| `STREAMS_PORT` | `4000` | Listen port. |
| `STREAMS_API_KEYS` | _(unset)_ | Comma-separated bearer keys (constant-time compared). Unset ⇒ **auth disabled** (dev mode). |
| `STREAMS_ALLOW_INSECURE_NO_AUTH` | `0` | Required to start on a **non-loopback** bind with **no** keys — otherwise the server refuses to start (it would be an open, unauthenticated event store). |
| `STREAMS_DATA_DIR` | `./streams-data` | Directory for the WAL, segments, and snapshots. Replayed on start; a missing/empty dir is a fresh start. |
| `STREAMS_COLD_DIR` | _(unset)_ | Optional cold-tier directory. Set ⇒ sealed segments past the hot-retention bound relocate here off the hot path; unset ⇒ tiering disabled (everything stays hot). |
| `RUST_LOG` | `info` | Tracing filter. |

Durability is **per box**: `durable: true` makes a write block on `fsync` before
it is acknowledged (the reported `fsync_ms` is real), and that write is recovered
by WAL replay after a crash. `durable: false` boxes take a group-committed fast
path and report `fsync_ms` as `0.0` — they trade recovery for latency. Either way,
**an acknowledged write is published; a write that fails to commit publishes
nothing visible** (no readable-but-not-durable state). The server shuts down
gracefully on `SIGINT`/`SIGTERM`, writing a final snapshot so a clean restart
starts from a current checkpoint. The quickstart commands above work verbatim.

### Security

- **Default bind is loopback** (`127.0.0.1:4000`), so an unconfigured server is never
  accidentally a public, unauthenticated event store. Binding a **non-loopback** address with
  **no** `STREAMS_API_KEYS` makes the server **refuse to start** unless you set
  `STREAMS_ALLOW_INSECURE_NO_AUTH=1` (it logs the reason loudly).
- **Bearer keys** are constant-time compared. A valid key has full access (no scopes yet).
- **Watch streams** are gated by an **unguessable `wid` capability** minted by the
  authenticated `POST /v0/watch`; the GET stream needs no api key in the URL. The dev-only
  `?token=` query fallback exists but leaks via logs — prefer the `Authorization` header.
- **streams speaks plain HTTP** (no built-in TLS). For any non-loopback exposure, run it
  behind a **TLS-terminating reverse proxy** (or bind loopback). TLS and per-key scopes /
  tenant isolation are **planned** — see `docs/API.md` §0.2 / §0.11.

---

## Use-case recipes

### Job queue (Bull-style)

```bash
curl -X PUT localhost:4000/v0/boxes/jobs -d '{ "durable": true, "cap_records": 0 }'
```
Producers `POST /v0/boxes/jobs`. Each worker calls
`POST /v0/boxes/jobs/diff {from_seq, node:"worker-N", limit:50}`, processes the batch,
then persists `next_from_seq` as its ack (cursor-advance = ack-all). Cancel a job with a
`match ["tag","Eq",jobid]` delete; cancel a tenant with a `match ["tag","Glob","tenant*"]`
delete (both permanent and point-in-time). Durable + unbounded cap means nothing is lost
to eviction; replay is just reading from an earlier `from_seq`.

For competing workers that need leases and redelivery rather than a shared cursor,
create the box with `"type": "queue"` instead and use claim/ack/nack/extend (or the
`/work` auto-claim SSE stream): jobs are leased with a visibility timeout, redelivered
if not acked, and optionally dead-lettered. See [docs/API.md](docs/API.md) §10.

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
- **Permanent deletion** — remove records by `seq` range (`before_seq`) and/or by tag
  (exact or `tag*` prefix), backed by a per-box tag index for efficiency. Permanent,
  silent (never a tombstone), effective immediately on reads, point-in-time (never
  affects future records), with lazy background physical reclaim.
- **Node loop-prevention** — a node never receives back events it produced, making
  N-way multi-master fan-out safe by construction.
- **Routers** — server-side `source → dest` forwarding, at-least-once, per-source FIFO,
  cycle-rejecting by default. Forwarded copies are **durable by construction** (they go
  through the same WAL append path as user writes, so they recover on restart).
- **Lease-based queues** — set `type: "queue"` to layer claim/ack/nack/extend (and a
  `/work` auto-claim SSE stream) on the same log: visibility-timeout leases,
  coalesced fair fan-out, redelivery, and optional dead-lettering (see API §10).
- **Multiplexed SSE** — watch many boxes over one resumable connection with composite
  cursors, named events, heartbeats, and tombstones.
- **Per-box durability** — `fsync`-on-commit for durable boxes (WAL-first: an
  acknowledged write is committed; nothing visible is ever un-durable), group-committed
  fast path for the rest. Crash-recovery via snapshot + WAL replay on start.
- **Priority + elastic throttling** — manual or recency-based auto priority; under CPU
  pressure delivery degrades in latency, never in correctness.
- **Single static binary** — WAL + segments on local NVMe, restartable at any instant;
  only data not yet in the WAL is lost.

---

## How it was built

The API and its semantics were fixed first and never changed; persistence and
scalability were added *underneath* that contract.

1. **Define API + docs** — the contract the implementation satisfies (`docs/`). ✅
2. **In-memory server** — the complete, correct `/v0` API. ✅
3. **Tests + benchmarks** — maximum-coverage tests and a baseline benchmark suite. ✅
4. **Make it durable + scalable** — WAL, group commit, segments, snapshots, crash
   recovery, priority scheduler, elastic throttling — all on one restartable
   process. ✅
5. **Lease-based queues** — claim/ack/nack/extend + `/work` stream on the same log. ✅
6. **Tiered storage** — optional hot→cold segment relocation off the hot path. ✅

See [docs/ROADMAP.md](docs/ROADMAP.md) for the original phase plan and acceptance
criteria.

---

## Documentation

- [docs/API.md](docs/API.md) — complete `/v0` HTTP API reference (the contract).
- [docs/DESIGN.md](docs/DESIGN.md) — data model & semantics: seq, dual watermark
  (`evict_floor`/`earliest_seq`), tombstones, permanent deletion, node loop-prevention,
  routers, priority.
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — storage, WAL, group commit, segments,
  recovery, scheduler, concurrency, crate choices, latency budget.
- [docs/ROADMAP.md](docs/ROADMAP.md) — build phases, acceptance criteria, benchmark plan.
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md) — recorded Phase-2/3 in-memory baseline numbers (hardware, methodology, every applicable benchmark-plan metric).
