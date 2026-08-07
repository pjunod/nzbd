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
    readonly patch_file="contrib/rqbit/0007-bound-session-peers.patch"
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
    readonly patch_file="contrib/rqbit/0008-bound-session-peers-main.patch"
    readonly source_variant="main"
    ;;
esac

echo "verifying rqbit $source_variant session-peer-budget patch against $source_head"

readonly work_dir="$(mktemp -d "${TMPDIR:-/tmp}/nzbd-rqbit-peer-budget.XXXXXX")"
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

readonly session_source="$work_dir/rqbit/crates/librqbit/src/session.rs"
readonly live_source="$work_dir/rqbit/crates/librqbit/src/torrent_state/live/mod.rs"

for invariant in \
  'peer_limit_total: Option<usize>' \
  'peer_semaphore_total' \
  'PeerSemaphores' \
  'paused.shared.options.peer_limit.unwrap_or(128)' \
  'session.peer_semaphore_total.clone()' \
  'self.peer_semaphores.try_acquire()' \
  'state.peer_semaphores.acquire().await' \
  'per_torrent_limit_is_exact' \
  'session_limit_is_shared_between_torrents' \
  'waiting_peer_resumes_after_session_permit_is_released'
do
  if ! grep -Fq "$invariant" "$session_source" "$live_source"; then
    echo "session peer-budget patch is missing invariant: $invariant" >&2
    exit 1
  fi
done

(
  cd "$work_dir/rqbit"
  cargo fmt --all -- --check
  cargo test -p librqbit --lib peer_semaphore_tests
)
