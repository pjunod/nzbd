#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 rust-target-triple" >&2
  exit 2
fi

readonly target="$1"
readonly write_test='m0_storage_full_probe_tests::storage_full_write_is_torrent_scoped'
readonly sizing_test='m0_storage_full_probe_tests::storage_full_during_file_sizing_is_fail_closed'
export CARGO_TERM_COLOR=never

contains_exact_ignored_test() {
  local exact_test="$1"
  local discovered_tests="$2"
  grep -Fxq "$exact_test: test" <<<"$discovered_tests"
}

# Discriminating control: the same exact-match guard must reject a stale or
# differently classified entry before it is trusted for the real discovery.
for proof_test in "$write_test" "$sizing_test"; do
  if contains_exact_ignored_test "$proof_test" "$proof_test: benchmark"; then
    echo "storage-full discovery guard accepted a non-test entry: $proof_test" >&2
    exit 1
  fi
done
echo "storage-full discovery guard negative control passed"

test_list="$(cargo test --locked -p nzbd-torrent --release \
  --target "$target" --lib -- --list --ignored)"
readonly test_list

run_exact_ignored_test() {
  local exact_test="$1"
  if ! contains_exact_ignored_test "$exact_test" "$test_list"; then
    echo "ignored storage-full proof test was not discovered: $exact_test" >&2
    exit 1
  fi

  local test_output
  local cargo_status
  set +e
  test_output="$(cargo test --locked -p nzbd-torrent --release \
    --target "$target" --lib "$exact_test" \
    -- --exact --ignored --nocapture 2>&1)"
  cargo_status="$?"
  set -e
  printf '%s\n' "$test_output"
  if [[ "$cargo_status" -ne 0 ]]; then
    exit "$cargo_status"
  fi

  if ! grep -Eq '^test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; [0-9]+ filtered out; finished in ' <<<"$test_output"; then
    echo "storage-full proof did not execute exactly one passing test: $exact_test" >&2
    exit 1
  fi
}

run_exact_ignored_test "$write_test"
run_exact_ignored_test "$sizing_test"
