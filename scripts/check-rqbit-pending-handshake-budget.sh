#!/usr/bin/env bash

set -euo pipefail

readonly stable_base="00b97485160ff5b5aa2b379ea0815d568ec665f0"
readonly main_base="4e5f94cbcf1d57ec500885c77cf1e24d70232d89"

if [[ $# -ne 1 ]]; then
  echo "usage: $0 /path/to/clean/rqbit-v8.1.1-or-documented-main" >&2
  exit 2
fi

readonly source_dir="$1"
readonly repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
source_head="$(git -C "$source_dir" rev-parse HEAD)"
readonly source_head

case "$source_head" in
  "$stable_base")
    readonly source_variant="stable"
    ;;
  *)
    if ! git -C "$source_dir" cat-file -e "$main_base^{commit}" 2>/dev/null; then
      echo "rqbit main base $main_base is missing; use a full main checkout" >&2
      exit 1
    fi
    if ! git -C "$source_dir" merge-base --is-ancestor "$main_base" "$source_head"; then
      echo "expected rqbit v8.1.1 at $stable_base or a descendant of main base $main_base; found $source_head" >&2
      exit 1
    fi
    readonly source_variant="main"
    ;;
esac

echo "verifying rqbit $source_variant pending-handshake budget against $source_head"

readonly work_dir="$(mktemp -d "${TMPDIR:-/tmp}/nzbd-rqbit-handshake-budget.XXXXXX")"
trap 'rm -rf -- "$work_dir"' EXIT

git clone --quiet --no-hardlinks "$source_dir" "$work_dir/rqbit"

if [[ "$source_variant" == "stable" ]]; then
  readonly patch_file="contrib/rqbit/0009-bound-pending-incoming-handshakes.patch"
  git -C "$work_dir/rqbit" apply --check "$repository_root/$patch_file"
  git -C "$work_dir/rqbit" apply "$repository_root/$patch_file"

  readonly session_source="$work_dir/rqbit/crates/librqbit/src/session.rs"
  for invariant in \
    'MAX_PENDING_INCOMING_HANDSHAKE_CHECKS: usize = 256' \
    'accept_incoming_handshake(&l, futs.len())' \
    'pending_handshake_budget_blocks_and_resumes_listener_accepts'
  do
    if ! grep -Fq "$invariant" "$session_source"; then
      echo "stable pending-handshake patch is missing invariant: $invariant" >&2
      exit 1
    fi
  done
else
  readonly listen_source="$work_dir/rqbit/crates/librqbit/src/listen.rs"
  readonly session_source="$work_dir/rqbit/crates/librqbit/src/session.rs"
  for invariant in \
    'DEFAULT_MAX_PENDING_INCOMING_HANDSHAKE_CHECKS: usize = 256' \
    'pub max_pending_incoming_handshake_checks: usize' \
    'futs.len() < max_pending_incoming_handshake_checks'
  do
    if ! grep -Fq "$invariant" "$listen_source" "$session_source"; then
      echo "rqbit main pending-handshake boundary is missing invariant: $invariant" >&2
      exit 1
    fi
  done
fi

(
  cd "$work_dir/rqbit"
  cargo fmt --all -- --check
  if [[ "$source_variant" == "stable" ]]; then
    readonly exact_test='session::tests::pending_handshake_budget_blocks_and_resumes_listener_accepts'
    test_list="$(cargo test -p librqbit --lib -- --list)"
    readonly test_list
    if ! grep -Fxq "$exact_test: test" <<<"$test_list"; then
      echo "stable pending-handshake proof test was not discovered: $exact_test" >&2
      exit 1
    fi
    cargo test -p librqbit --lib "$exact_test" -- --exact --nocapture
  else
    cargo check -p librqbit --all-targets
  fi
)
