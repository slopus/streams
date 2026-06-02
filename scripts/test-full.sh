#!/usr/bin/env bash
set -euo pipefail

# Heavy bounded gate for release/high-risk changes.
# Do not run by default for routine edits. This still excludes the opt-in
# TOPICS_TEST_EXHAUSTIVE crash matrix unless the caller sets that env var.

cargo fmt --check

# Catches bench/example/test target compile failures without running every target.
cargo test --all-targets --no-run

# Default runtime suite.
cargo test

# Hostile filesystem, crash-recovery, and durability corpus.
cargo test --features test-fs --tests

# Failpoint-driven crash harnesses.
cargo test --features test-fs,failpoints --test crash_harness --test crash_snapshot_delete_race -- --test-threads=1

# Docs app type/build check.
(
  cd docs-app
  npm run build
)
