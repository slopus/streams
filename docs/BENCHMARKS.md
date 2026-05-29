# Benchmarks — Phase-2/3 In-Memory BASELINE

This document records the **initial baseline** performance numbers for the
Phase-2 in-memory `streams` server, captured during Phase 3. All data lives in
RAM: there is no WAL, no segment store, and no fsync. These numbers therefore
represent the *engine + HTTP + SSE* cost with durability removed from the
critical path.

> Phase 4 (persistence: WAL, group commit, segments, recovery) will **append a
> second column** to every table here so the cost of durability is explicit
> against this baseline. Do not edit the baseline numbers below — add new
> columns/sections.

---

## Environment

| | |
|---|---|
| **CPU** | Apple M4 Max |
| **Cores** | 16 |
| **RAM** | 128 GiB (137438953472 bytes) |
| **OS** | Darwin 25.2.0 (macOS) |
| **rustc** | rustc 1.92.0 (ded5c06cf 2025-12-08) |
| **Edition** | 2021 |
| **Build flags** | `--release` (optimized) |
| **NVMe** | n/a for this baseline — in-memory build, no disk I/O on the data path |

Hardware gathered via `sysctl -n machdep.cpu.brand_string hw.ncpu hw.memsize`,
OS via `uname -sr`, toolchain via `rustc --version`.

---

## Methodology

Two layers, matching the ROADMAP benchmark plan:

1. **Criterion micro-benchmarks** (`benches/engine.rs`) — call the engine API
   directly in-process (`Engine::write`, `Engine::diff`,
   `BoxState::matching_live_seqs`, `BoxState::apply_delete`, cap-eviction via
   `enforce_retention`). No HTTP, no network. Criterion's default config: 3 s
   warm-up, 100 samples per bench. Reported value is the **median** of the
   estimate interval. Throughput is Criterion's `Throughput::Elements` over the
   batch size. These isolate raw CPU cost of the hot paths.

2. **Live end-to-end HTTP macro-benchmarks** (`streams-probe bench`) — run
   against the **release binary** bound on an ephemeral localhost port over real
   HTTP (loopback, keep-alive). Latencies are wall-clock; percentiles via sort +
   linear interpolation. Run size: **50 000 writes**, SSE windows at **1 / 10 /
   100** watchers. SSE write→deliver latency is measured with a shared
   monotonic epoch stamp embedded in each write payload, so it is a true
   end-to-end write-to-delivery interval with no clock skew. The full run
   completes in well under a minute (~7 s).

Time-dependent **correctness** (TTL expiry, priority recency, watermark
invariants) is *not* measured here; it is verified deterministically in the
engine unit tests and the proptest suite using the injectable `TestClock`. The
live tool naturally uses real time for *latency* measurement, which is expected
and fine.

Reproduce:

```bash
# micro
cargo bench --bench engine

# macro: boot release server, then probe
STREAMS_PORT=4090 ./target/release/streams &
./target/release/streams-probe conformance http://localhost:4090   # must exit 0
./target/release/streams-probe bench       http://localhost:4090 --json
```

---

## 1. Criterion micro-benchmarks (engine, in-process)

Median time per criterion run; throughput derived from batch size.

### Append (`Engine::write`, fresh box per batch)

| Payload | Batch | Median time | Throughput (records/s) |
|---|---:|---:|---:|
| 64 B   | 1    | 873 ns   | ~1.15 M |
| 64 B   | 10   | 2.86 µs  | ~3.50 M |
| 64 B   | 100  | 23.6 µs  | ~4.24 M |
| 64 B   | 1000 | 248.6 µs | ~4.02 M |
| 1 KiB  | 1    | 1.26 µs  | ~0.79 M |
| 1 KiB  | 10   | 7.20 µs  | ~1.39 M |
| 1 KiB  | 100  | 69.2 µs  | ~1.45 M |
| 1 KiB  | 1000 | 718.1 µs | ~1.39 M |

Batching amortizes per-call overhead strongly (1→100 records is ~6x cheaper per
record). 64 KiB payloads were not run as a separate micro-bench; the per-record
trend at 1 KiB shows the path is allocation/copy-bound at large payloads.

### getDifference (`Engine::diff` from seq 0, warm 10 k-record box)

| Limit | Median time | Throughput (records/s) |
|---:|---:|---:|
| 1    | 167.8 ns | ~6.0 M |
| 256  | 18.45 µs | ~13.7 M |
| 1000 | 74.4 µs  | ~13.1 M |

### Tag-index match (`matching_live_seqs`, 10 k records / 100 tags)

| Pattern | Median time |
|---|---:|
| exact (`Eq`, single posting list) | 266 ns |
| prefix (`Glob` `tenant:*`, range scan all 100 tags) | 67.9 µs |

### Cap eviction (`Engine::write` into a full `discard:old`, cap=10 k box)

| Batch | Median time | Throughput (records/s) |
|---:|---:|---:|
| 1   | 474 ns  | ~2.11 M |
| 100 | 25.8 µs | ~3.88 M |

### Delete (`BoxState::apply_delete`, fresh warm 10 k box per iter)

| Selector | Median time | Throughput (records/s) |
|---|---:|---:|
| `before_seq` all (prefix delete of all 10 k) | 2.60 ms | ~3.85 M |
| `match` exact (tag `Eq`, ~5 k matched) | 1.53 ms | ~3.29 M |

---

## 2. Live end-to-end HTTP macro-benchmarks (`streams-probe bench`)

Release binary over loopback HTTP. 50 000 writes. Latencies in milliseconds.

### Write-ack latency (single record, n=5000)

| p50 | p99 | p999 | max |
|---:|---:|---:|---:|
| 0.045 ms | 0.077 ms | 0.143 ms | 0.341 ms |

### Write throughput (16 concurrent writers, batch=100)

| Records acked | Elapsed | Throughput |
|---:|---:|---:|
| 49 600 | ~0.011 s | **~4.66 M records/s** |

### getDifference (latency + throughput over HTTP)

| Limit | p50 | p99 | p999 | calls/s | records/s |
|---:|---:|---:|---:|---:|---:|
| 1    | 0.043 ms | 0.124 ms | 0.258 ms | 21 069 | 21 069 |
| 256  | 0.163 ms | 0.221 ms | 0.246 ms | 5 446  | ~1.39 M |
| 1000 | 0.503 ms | 0.555 ms | 0.598 ms | 1 697  | ~1.70 M |

Tail (caught-up, near-head) read latency, n=2000: p50 **0.045 ms**, p99 0.076 ms,
p999 0.090 ms.

### SSE fan-out (write → deliver latency, 1 writer × N watchers)

| Watchers | Deliveries | p50 | p99 | p999 | max |
|---:|---:|---:|---:|---:|---:|
| 1   | 50   | 0.193 ms | 0.425 ms | 0.502 ms | 0.511 ms |
| 10  | 500  | 0.286 ms | 0.557 ms | 0.572 ms | 0.578 ms |
| 100 | 5000 | 0.939 ms | 1.847 ms | 2.408 ms | 2.451 ms |

**1–5 ms `eventual` delivery target: MET** at the tested load. At 100 watchers
the p50 is ~0.94 ms and p99 ~1.85 ms — comfortably within the 1–5 ms target;
even the worst observed delivery (2.45 ms) is inside the budget. The 1000-watcher
case from the ROADMAP plan was not run (probe defaults to 1/10/100); the 1→100
trend is roughly linear and stays well under budget. SSE fan-out scale
(connection churn, memory per idle connection) is not separately measured in the
in-memory baseline.

### Router forwarding overhead (src → dst write-to-visible)

| Path | p50 | p99 | p999 |
|---|---:|---:|---:|
| direct write+read baseline | 0.089 ms | 0.123 ms | 0.138 ms |
| forwarded (1-hop router) | 0.092 ms | 0.187 ms | 0.237 ms |
| **added latency (p50)** | **~0.002 ms** | | |

Router forwarding adds only single-digit microseconds at the median on the
in-memory build (synchronous in-process fan-out).

---

## 3. ROADMAP benchmark-plan coverage

Every applicable metric from the ROADMAP §"Benchmark plan" table, mapped to the
baseline numbers above. Metrics that require persistence or the full scheduler
are deferred to Phase 4 (the persistent build), since the Phase-2/3 in-memory
build has no disk path, no recovery, and no governor.

| Metric | Status (in-memory baseline) | Where |
|---|---|---|
| Write throughput | ~4.66 M rec/s (HTTP, 16 writers ×100); ~4.0 M rec/s micro at 64 B | §1, §2 |
| Append latency p50/p99/p999 | 0.045 / 0.077 / 0.143 ms (single record, HTTP); non-durable only | §2 |
| getDifference throughput | up to ~1.70 M rec/s (limit 1000, HTTP); ~13.7 M rec/s micro | §1, §2 |
| getDifference latency p50/p99 | tail 0.045 / 0.076 ms; deep (limit 1000) 0.503 / 0.555 ms | §2 |
| SSE fan-out latency p50/p99 | 1/10/100 watchers; p50 0.19–0.94 ms, p99 0.43–1.85 ms — 1–5 ms target MET | §2 |
| Router forwarding | ~0.002 ms added p50 latency; forwarding throughput bounded by append | §2 |
| Eviction / TTL cost | cap-evict micro 474 ns/rec (batch 1); TTL correctness in unit/proptest | §1 |
| **SSE fan-out scale** (churn, mem/idle conn) | Deferred — Phase 4 | — |
| **Recovery time** | Not applicable — in-memory build has no WAL/segments | Phase 4 |
| **Throttling behavior** | Not applicable — no governor/elastic throttle wired in Phase 2/3 | Phase 4 |
| **Memory footprint** (bytes/record) | Not separately measured in baseline | Phase 4 |
| **Durable** append latency / fsync group-commit | Not applicable — no fsync path | Phase 4 |
| getDifference cold (mmap fault) vs warm | Not applicable — no segment store; all reads warm | Phase 4 |

---

## Notes

- All micro numbers are criterion medians from a clean `cargo bench --bench
  engine` run on an otherwise-idle machine; expect a few percent run-to-run
  variance (criterion reported most changes within its noise threshold).
- The macro numbers are a single representative `streams-probe bench` run; they
  are loopback-HTTP figures and include the full axum/hyper request path.
- `streams-probe conformance` passed 89/89 checks (exit 0) against this same
  release binary, so the contract these numbers were measured against is the
  documented `/v0` contract.
