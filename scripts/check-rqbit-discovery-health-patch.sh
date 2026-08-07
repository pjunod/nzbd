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

  readonly librqbit_tests=(
    'discovery_health::tests::tracker_endpoints_are_redacted_in_public_snapshot'
    'discovery_health::tests::private_torrent_reports_dht_suppression'
    'session::tests::public_discovery_health_is_redacted'
  )
  readonly dht_tests=(
    'dht::health_tests::request_peers_health_tracks_current_run'
  )
  if [[ "$source_variant" == "stable" ]]; then
    tracker_tests=(
      'tracker_comms::health_tests::http_failure_is_classified_without_exposing_tracker_secrets'
      'tracker_health::tests::tracker_health_redacts_endpoint_and_retains_safe_failure'
      'tracker_health::tests::unsupported_tracker_remains_rejected_when_other_monitors_stop'
    )
  else
    tracker_tests=(
      'tracker_comms::health_tests::http_failure_is_classified_without_exposing_tracker_secrets'
      'tracker_comms::health_tests::tracker_health_redacts_endpoint_and_retains_safe_failure'
      'tracker_comms::health_tests::unsupported_tracker_remains_rejected_when_other_monitors_stop'
      'tracker_comms::health_tests::safe_endpoint_formats_ipv6_without_path_or_query'
    )
  fi
  readonly tracker_tests

  librqbit_list="$(cargo test -p librqbit --lib \
    --no-default-features --features rust-tls -- --list)"
  if [[ "$source_variant" == "stable" ]]; then
    dht_list="$(cargo test -p librqbit-dht --lib \
      --no-default-features --features sha1-ring -- --list)"
    tracker_list="$(cargo test -p librqbit-tracker-comms --lib \
      --no-default-features --features sha1-ring -- --list)"
  else
    dht_list="$(cargo test -p librqbit-dht --lib -- --list)"
    tracker_list="$(cargo test -p librqbit-tracker-comms --lib -- --list)"
  fi
  readonly librqbit_list dht_list tracker_list

  for exact_test in "${librqbit_tests[@]}"; do
    if ! grep -Fxq "$exact_test: test" <<<"$librqbit_list"; then
      echo "librqbit discovery-health proof test was not discovered: $exact_test" >&2
      exit 1
    fi
    cargo test -p librqbit --lib "$exact_test" \
      --no-default-features --features rust-tls -- --exact --nocapture
  done
  for exact_test in "${dht_tests[@]}"; do
    if ! grep -Fxq "$exact_test: test" <<<"$dht_list"; then
      echo "DHT discovery-health proof test was not discovered: $exact_test" >&2
      exit 1
    fi
    if [[ "$source_variant" == "stable" ]]; then
      cargo test -p librqbit-dht --lib "$exact_test" \
        --no-default-features --features sha1-ring -- --exact --nocapture
    else
      cargo test -p librqbit-dht --lib "$exact_test" -- --exact --nocapture
    fi
  done
  for exact_test in "${tracker_tests[@]}"; do
    if ! grep -Fxq "$exact_test: test" <<<"$tracker_list"; then
      echo "tracker discovery-health proof test was not discovered: $exact_test" >&2
      exit 1
    fi
    if [[ "$source_variant" == "stable" ]]; then
      cargo test -p librqbit-tracker-comms --lib "$exact_test" \
        --no-default-features --features sha1-ring -- --exact --nocapture
    else
      cargo test -p librqbit-tracker-comms --lib "$exact_test" -- --exact --nocapture
    fi
  done
)
