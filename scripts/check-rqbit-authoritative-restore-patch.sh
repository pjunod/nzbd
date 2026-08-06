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
    readonly patch_file="contrib/rqbit/0001-allow-persistence-without-auto-restore.patch"
    ;;
  "$main_base")
    readonly patch_file="contrib/rqbit/0002-allow-persistence-without-auto-restore-main.patch"
    ;;
  *)
    echo "expected rqbit v8.1.1 at $stable_base or main at $main_base; found $source_head" >&2
    exit 1
    ;;
esac

readonly work_dir="$(mktemp -d "${TMPDIR:-/tmp}/nzbd-rqbit-patch.XXXXXX")"
trap 'rm -rf -- "$work_dir"' EXIT

git clone --quiet --no-hardlinks "$source_dir" "$work_dir/rqbit"
git -C "$work_dir/rqbit" apply "$repository_root/$patch_file"

(
  cd "$work_dir/rqbit"
  cargo fmt --all -- --check
  cargo test -p librqbit \
    persistence_can_skip_implicit_admission_and_restore_an_authoritative_subset \
    --no-default-features \
    --features rust-tls \
    -- --nocapture
)
