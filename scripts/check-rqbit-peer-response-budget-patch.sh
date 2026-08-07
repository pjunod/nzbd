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
    readonly patch_file="contrib/rqbit/0012-bound-peer-response-backlog.patch"
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
    readonly patch_file="contrib/rqbit/0013-bound-peer-response-backlog-main.patch"
    readonly source_variant="main"
    ;;
esac

echo "verifying rqbit $source_variant peer-response-budget patch against $source_head"

readonly work_dir="$(mktemp -d "${TMPDIR:-/tmp}/nzbd-rqbit-peer-response-budget.XXXXXX")"
trap 'rm -rf -- "$work_dir"' EXIT

git clone --quiet --no-hardlinks "$source_dir" "$work_dir/rqbit"
if git -C "$work_dir/rqbit" apply --check "$repository_root/$patch_file"; then
  git -C "$work_dir/rqbit" apply "$repository_root/$patch_file"
elif [[ "$source_variant" == "main" ]]; then
  echo "direct apply drifted; attempting a three-way apply against $source_head" >&2
  git -C "$work_dir/rqbit" apply --3way "$repository_root/$patch_file"
else
  echo "stable v8.1.1 patch no longer applies to its pinned base" >&2
  exit 1
fi

readonly peer_connection_source="$work_dir/rqbit/crates/librqbit/src/peer_connection.rs"
readonly live_source="$work_dir/rqbit/crates/librqbit/src/torrent_state/live/mod.rs"

for invariant in \
  'MAX_PENDING_PEER_RESPONSES_PER_PEER: usize = 128' \
  'peer_responses_sem' \
  'acquire_peer_response_permit' \
  'WriterRequest::ReadChunkRequest(ci, permit)' \
  'peer_response_permit_spans_scheduler_and_writer_queues'
do
  if ! grep -Fq "$invariant" "$peer_connection_source" "$live_source"; then
    echo "peer-response-budget patch is missing invariant: $invariant" >&2
    exit 1
  fi
done

case "$source_variant" in
  stable)
    grep -Fq 'MessageWithPermit(MessageOwned, OwnedSemaphorePermit)' "$peer_connection_source"
    grep -Fq 'WriterRequest::MessageWithPermit' "$live_source"
    ;;
  main)
    grep -Fq 'UtMetadata(UtMetadata<ByteBufOwned>, OwnedSemaphorePermit)' "$peer_connection_source"
    grep -Fq 'WriterRequest::UtMetadata(' "$live_source"
    ;;
esac

(
  cd "$work_dir/rqbit"
  cargo fmt --all -- --check
  readonly exact_test='torrent_state::live::peer_response_budget_tests::peer_response_permit_spans_scheduler_and_writer_queues'
  test_list="$(cargo test -p librqbit --lib -- --list)"
  readonly test_list
  if ! grep -Fxq "$exact_test: test" <<<"$test_list"; then
    echo "peer-response-budget proof test was not discovered: $exact_test" >&2
    exit 1
  fi
  cargo test -p librqbit --lib "$exact_test" -- --exact --nocapture
)
