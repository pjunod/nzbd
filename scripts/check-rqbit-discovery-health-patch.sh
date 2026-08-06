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
    readonly patch_file="contrib/rqbit/0003-expose-per-torrent-discovery-health.patch"
    readonly source_variant="stable"
    readonly tracker_health_source="crates/tracker_comms/src/tracker_health.rs"
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
    readonly patch_file="contrib/rqbit/0004-expose-per-torrent-discovery-health-main.patch"
    readonly source_variant="main"
    readonly tracker_health_source="crates/tracker_comms/src/tracker_comms.rs"
    ;;
esac

echo "verifying rqbit $source_variant discovery-health patch against $source_head"

readonly work_dir="$(mktemp -d "${TMPDIR:-/tmp}/nzbd-rqbit-discovery-health.XXXXXX")"
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

if grep -A 1 -E '^#\[derive\([^]]*Debug[^]]*\)\]$' "$work_dir/rqbit/$tracker_health_source" |
  grep -q '^pub struct TrackerHealth '; then
  echo "TrackerHealth must use its credential-safe hand-written Debug implementation" >&2
  exit 1
fi

(
  cd "$work_dir/rqbit"
  cargo fmt --all -- --check
  cargo test -p librqbit --lib --no-default-features --features rust-tls
  if [[ "$source_variant" == "stable" ]]; then
    cargo test -p librqbit-dht --lib --no-default-features --features sha1-ring
    cargo test -p librqbit-tracker-comms --lib --no-default-features --features sha1-ring
  else
    cargo test -p librqbit-dht --lib
    cargo test -p librqbit-tracker-comms --lib
  fi
)
