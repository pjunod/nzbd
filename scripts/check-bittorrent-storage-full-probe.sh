#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 rust-target-triple" >&2
  exit 2
fi

readonly target="$1"
readonly exact_test='m0_storage_full_probe::storage_full_write_is_torrent_scoped'
export CARGO_TERM_COLOR=never

contains_exact_ignored_test() {
  grep -Fxq "$exact_test: test" <<<"$1"
}

# Discriminating control: the same exact-match guard must reject a stale or
# differently classified entry before it is trusted for the real discovery.
if contains_exact_ignored_test "$exact_test: benchmark"; then
  echo "storage-full discovery guard accepted a non-test entry" >&2
  exit 1
fi
echo "storage-full discovery guard negative control passed"

test_list="$(cargo test --locked -p nzbd-torrent --release \
  --target "$target" --lib -- --list --ignored)"
readonly test_list
if ! contains_exact_ignored_test "$test_list"; then
  echo "ignored storage-full proof test was not discovered: $exact_test" >&2
  exit 1
fi

set +e
test_output="$(cargo test --locked -p nzbd-torrent --release \
  --target "$target" --lib "$exact_test" \
  -- --exact --ignored --nocapture 2>&1)"
readonly cargo_status="$?"
set -e
readonly test_output
printf '%s\n' "$test_output"
if [[ "$cargo_status" -ne 0 ]]; then
  exit "$cargo_status"
fi

if ! grep -Eq '^test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; [0-9]+ filtered out; finished in ' <<<"$test_output"; then
  echo "storage-full proof did not execute exactly one passing test" >&2
  exit 1
fi
