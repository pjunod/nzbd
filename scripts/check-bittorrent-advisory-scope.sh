#!/usr/bin/env bash
set -euo pipefail

# Gate 9 permits three exact RustSec exceptions only while their affected code
# is unreachable in nzbd's accepted configuration. Cargo's inverse trees make
# dependency and feature drift visible before prose can go stale.

package_set() {
  sed -E 's/ \([^)]*\)( \(\*\))?$//; s/ \(\*\)$//' |
    grep -E '^[[:alnum:]_.+-]+ v[^ ]+$' |
    LC_ALL=C sort -u
}

require_package_set() {
  local label="$1"
  local actual="$2"
  local expected="$3"
  if [[ "$actual" != "$expected" ]]; then
    echo "$label dependency path changed" >&2
    echo 'expected:' >&2
    echo "$expected" >&2
    echo 'actual:' >&2
    echo "$actual" >&2
    exit 1
  fi
}

quick_xml_tree="$(
  cargo tree --locked --workspace --target all -e normal \
    -i quick-xml@0.37.5 --prefix none
)"
quick_xml_packages="$(package_set <<<"$quick_xml_tree")"
expected_quick_xml_packages="$(LC_ALL=C sort <<'EOF'
librqbit v8.1.1
librqbit-upnp v1.0.0
nzbd-torrent v0.2.0
quick-xml v0.37.5
EOF
)"
require_package_set \
  'RUSTSEC-2026-0194/0195 quick-xml' \
  "$quick_xml_packages" \
  "$expected_quick_xml_packages"

time_tree="$(
  cargo tree --locked --workspace --target all -e features \
    -i time@0.3.41 --prefix none
)"
if grep -qx 'time feature "parsing"' <<<"$time_tree"; then
  echo 'RUSTSEC-2026-0009 parsing code is enabled in time 0.3.41' >&2
  exit 1
fi
time_packages="$(package_set <<<"$time_tree")"
expected_time_packages="$(LC_ALL=C sort <<'EOF'
nzbd v0.2.0
rcgen v0.13.2
time v0.3.41
yasna v0.5.2
EOF
)"
require_package_set \
  'RUSTSEC-2026-0009 time' \
  "$time_packages" \
  "$expected_time_packages"

option_ext_tree="$(
  cargo tree --locked --workspace --target all -e normal \
    -i option-ext@0.2.0 --prefix none
)"
option_ext_packages="$(package_set <<<"$option_ext_tree")"
expected_option_ext_packages="$(LC_ALL=C sort <<'EOF'
directories v6.0.0
dirs-sys v0.5.0
librqbit v8.1.1
librqbit-core v5.0.0
librqbit-dht v5.3.1
librqbit-peer-protocol v4.3.0
librqbit-tracker-comms v3.0.0
nzbd-torrent v0.2.0
option-ext v0.2.0
EOF
)"
require_package_set \
  'MPL-2.0 option-ext exception' \
  "$option_ext_packages" \
  "$expected_option_ext_packages"

ignored_advisories="$(
  sed -n '/^ignore = \[/,/^\]/p' deny.toml |
    grep -Eo 'RUSTSEC-[0-9]{4}-[0-9]{4}' |
    LC_ALL=C sort -u
)"
expected_ignored_advisories="$(LC_ALL=C sort <<'EOF'
RUSTSEC-2026-0009
RUSTSEC-2026-0194
RUSTSEC-2026-0195
EOF
)"
if [[ "$ignored_advisories" != "$expected_ignored_advisories" ]]; then
  echo 'the exact reviewed RustSec exception set changed' >&2
  echo 'expected:' >&2
  echo "$expected_ignored_advisories" >&2
  echo 'actual:' >&2
  echo "$ignored_advisories" >&2
  exit 1
fi

echo 'BitTorrent advisory scope: exact paths, disabled UPnP, no time parsing'
