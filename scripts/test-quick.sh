#!/usr/bin/env bash
set -euo pipefail

# Basic, sub-minute confidence gate for routine work.
# Intentionally skips fault injection, failpoints, crash matrices, proptest/fuzz,
# benchmark sweeps, docs builds, and deeper queue/router/SSE/WebSocket matrices.

cargo fmt --check

cargo test --lib --bins

cargo test \
  --test smoke \
  --test harness_smoke \
  --test integration_topics \
  --test integration_diff \
  --test integration_errors
