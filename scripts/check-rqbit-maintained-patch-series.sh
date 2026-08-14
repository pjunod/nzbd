#!/usr/bin/env bash
set -euo pipefail

readonly repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly series_file="$repository_root/contrib/rqbit/maintained-series.txt"
readonly checksum_file="$repository_root/contrib/rqbit/upstream-v8.1.1.sha256"
readonly vendor_dir="$repository_root/contrib/rqbit/vendor"
readonly upstream_url="https://github.com/ikatson/rqbit/archive/refs/tags/v8.1.1.tar.gz"
readonly expected_series=$'0001-allow-persistence-without-auto-restore.patch\n0005-bound-tracker-requests.patch\n0007-bound-session-peers.patch\n0009-bound-pending-incoming-handshakes.patch\n0010-bound-known-peer-records.patch\n0012-bound-peer-response-backlog.patch\n0014-bound-discovery-pressure.patch\n0016-limit-peer-metadata-before-allocation.patch\n0018-propagate-file-sizing-errors.patch'

if [[ "$(<"$series_file")" != "$expected_series" ]]; then
  echo "maintained rqbit patch order or membership changed unexpectedly" >&2
  diff -u <(printf '%s\n' "$expected_series") "$series_file" || true
  exit 1
fi

readonly work_dir="$(mktemp -d "${TMPDIR:-/tmp}/nzbd-rqbit-series.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

archive="${RQBIT_UPSTREAM_ARCHIVE:-$work_dir/rqbit-v8.1.1.tar.gz}"
if [[ -z "${RQBIT_UPSTREAM_ARCHIVE:-}" ]]; then
  curl --fail --location --silent --show-error "$upstream_url" --output "$archive"
fi

if command -v sha256sum >/dev/null 2>&1; then
  (cd "$(dirname "$archive")" && sed "s#  rqbit-v8.1.1.tar.gz#  $(basename "$archive")#" "$checksum_file" | sha256sum -c - >/dev/null)
else
  (cd "$(dirname "$archive")" && sed "s#  rqbit-v8.1.1.tar.gz#  $(basename "$archive")#" "$checksum_file" | shasum -a 256 -c - >/dev/null)
fi

tar -xzf "$archive" -C "$work_dir"
readonly source_dir="$work_dir/rqbit-8.1.1"
git -C "$source_dir" init --quiet
git -C "$source_dir" add --all

while IFS= read -r patch_name; do
  patch_file="$repository_root/contrib/rqbit/$patch_name"
  git -C "$source_dir" apply --check "$patch_file"
  git -C "$source_dir" apply "$patch_file"
  git -C "$source_dir" diff --check
  git -C "$source_dir" add --all
done <"$series_file"

if ! diff -u "$source_dir/LICENSE" "$vendor_dir/LICENSE" \
  || ! diff -u "$source_dir/README.md" "$vendor_dir/README.md" \
  || ! diff -ru "$source_dir/crates" "$vendor_dir/crates"; then
  echo "checked-in rqbit vendor tree drifted from v8.1.1 plus maintained-series.txt" >&2
  exit 1
fi

if [[ "${RQBIT_SERIES_DERIVE_ONLY:-0}" == "1" ]]; then
  echo "rqbit maintained patch series: checksum, ordered apply, and vendor drift checks passed"
  exit 0
fi

(
  cd "$source_dir"
  cargo fmt --all -- --check

  librqbit_tests="$(cargo test -p librqbit --lib --no-default-features --features rust-tls -- --list)"
  tracker_tests="$(cargo test -p librqbit-tracker-comms --lib --no-default-features --features sha1-ring -- --list)"
  dht_tests="$(cargo test -p librqbit-dht --lib --no-default-features --features sha1-ring -- --list)"
  readonly librqbit_tests tracker_tests dht_tests

  readonly -a expected_librqbit_tests=(
    'tests::persistence::persistence_can_skip_implicit_admission_and_restore_an_authoritative_subset'
    'torrent_state::live::peer_semaphore_tests::per_torrent_limit_is_exact'
    'torrent_state::live::peer_semaphore_tests::session_limit_is_shared_between_torrents'
    'torrent_state::live::peer_semaphore_tests::waiting_peer_resumes_after_session_permit_is_released'
    'session::tests::pending_handshake_budget_blocks_and_resumes_listener_accepts'
    'torrent_state::live::peers::known_peer_budget_tests::per_torrent_limit_is_exact_and_released'
    'torrent_state::live::peers::known_peer_budget_tests::session_limit_is_shared_between_torrents'
    'torrent_state::live::peers::known_peer_budget_tests::peer_records_hold_slots_until_removed'
    'torrent_state::live::peers::known_peer_budget_tests::failed_shared_acquisition_releases_the_local_slot'
    'torrent_state::live::peers::known_peer_budget_tests::alternate_outgoing_address_is_queued_once_with_record_handle'
    'peer_connection::peer_response_writer_tests::production_writer_holds_permit_until_socket_write_finishes'
    'torrent_state::live::peer_response_budget_tests::production_admission_spans_scheduler_and_writer_queues'
    'dht_utils::tests::production_metadata_queues_close_at_exact_boundaries'
    'peer_info_reader::tests::configured_metadata_limit_is_checked_before_allocation'
    'torrent_state::initializing::tests::file_sizing_error_stops_initialization'
  )
  for exact_test in "${expected_librqbit_tests[@]}"; do
    if ! grep -Fxq "$exact_test: test" <<<"$librqbit_tests"; then
      echo "maintained rqbit proof test was not discovered: $exact_test" >&2
      exit 1
    fi
  done
  for exact_test in \
    'tracker_comms::tests::http_tracker_response_is_bounded_and_timed' \
    'tracker_comms::tests::hostile_tracker_intervals_are_clamped'
  do
    if ! grep -Fxq "$exact_test: test" <<<"$tracker_tests"; then
      echo "maintained rqbit tracker proof was not discovered: $exact_test" >&2
      exit 1
    fi
  done
  if ! grep -Fxq 'dht::queue_budget_tests::queue_budgets_close_at_exact_boundaries: test' <<<"$dht_tests"; then
    echo "maintained rqbit DHT pressure proof was not discovered" >&2
    exit 1
  fi

  cargo test -p librqbit --lib --no-default-features --features rust-tls
  cargo test -p librqbit-tracker-comms --lib --no-default-features --features sha1-ring
  cargo test -p librqbit-dht --lib --no-default-features --features sha1-ring
  cargo check --workspace --exclude rqbit-desktop
)

echo "rqbit maintained patch series: nine revert-sensitive contracts passed"
