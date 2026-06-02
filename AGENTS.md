# Agent Instructions

- Do not introduce explicit alternate modes, protocol modes, or separate media/data-plane modes unless the user explicitly asks for them.
- For RAM-only live media work, make the vanilla `durability:"ephemeral"` topic path work well. Preserve monotonic sequence numbers and existing topic contracts.
- Do not use generic topic names like `job` or `jobs` in examples. Use a more specific topic name that reflects the use case.
- Testing default: run the quick gate, not the full gate. Quick is `./scripts/test-quick.sh`; it is intended to stay under one minute and covers formatting, unit tests, smoke tests, and core topic/diff/error API integration tests. It deliberately skips fault-injection, failpoint, crash-matrix, proptest/fuzz, benchmark, docs-build, and deeper queue/router/SSE/WebSocket matrix work.
- Full testing is `./scripts/test-full.sh`. Do not run it by default. Run it only when the user explicitly asks for full testing, before release/landing, or when a change touches durability/recovery/WAL/snapshot/segment/fault-test behavior where the heavy corpus is relevant.
- The exhaustive crash matrix is not part of either default test command. Run it only on an explicit request by setting `TOPICS_TEST_EXHAUSTIVE=1` with the relevant full/failpoint command.
