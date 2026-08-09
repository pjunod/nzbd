#!/usr/bin/env bash
set -euo pipefail

# Repository policy permits three exact RustSec exceptions and one exact
# MPL-2.0 package only while their reviewed dependency and feature boundaries
# remain intact. Fail closed on output drift, but do not make a workspace
# version bump look like a third-party dependency change.

workspace_package_names="$(
  cargo tree --locked --workspace --depth 0 --prefix none |
    sed -E 's/ v[^ ]+.*$//' |
    grep -E '^[[:alnum:]_.+-]+$' |
    LC_ALL=C sort -u
)"

package_set() {
  local line
  local name

  sed -E 's/ \([^)]*\)( \(\*\))?$//; s/ \(\*\)$//' |
    while IFS= read -r line; do
      if [[ ! "$line" =~ ^[[:alnum:]_.+-]+[[:space:]]v[^[:space:]]+$ ]]; then
        continue
      fi

      name="${line%% v*}"
      if grep -Fqx -- "$name" <<<"$workspace_package_names"; then
        printf '%s (workspace)\n' "$name"
      else
        printf '%s\n' "$line"
      fi
    done |
    LC_ALL=C sort -u
}

feature_set() {
  sed -E 's/ \(\*\)$//' |
    grep -E '^time feature "' |
    LC_ALL=C sort -u || true
}

require_exact_set() {
  local label="$1"
  local subject="$2"
  local actual="$3"
  local expected="$4"

  if [[ "$actual" != "$expected" ]]; then
    echo "$label $subject changed" >&2
    echo 'expected:' >&2
    echo "$expected" >&2
    echo 'actual:' >&2
    echo "$actual" >&2
    exit 1
  fi
}

inverse_tree() {
  local label="$1"
  local package_spec="$2"
  local edge_kinds="$3"
  local remedy="$4"
  local output

  if ! output="$(
    cargo tree --locked --workspace --target all -e "$edge_kinds" \
      -i "$package_spec" --prefix none 2>&1
  )"; then
    if grep -Fq 'did not match any packages' <<<"$output"; then
      echo "$label: $package_spec is absent from the locked graph." >&2
      echo "remedy: $remedy; do not update the expected set blindly." >&2
    else
      echo "$label: cargo could not inspect $package_spec." >&2
      echo "$output" >&2
    fi
    exit 1
  fi

  printf '%s\n' "$output"
}

quick_xml_tree="$(
  inverse_tree \
    'RUSTSEC-2026-0194/0195 quick-xml scope' \
    'quick-xml@0.37.5' \
    'normal' \
    're-run cargo deny, then remove or replace both quick-xml ignores in deny.toml and update the gate 9 evidence'
)"
quick_xml_packages="$(package_set <<<"$quick_xml_tree")"
expected_quick_xml_packages="$(LC_ALL=C sort <<'EOF'
librqbit v8.1.1
librqbit-upnp v1.0.0
nzbd-torrent (workspace)
quick-xml v0.37.5
EOF
)"
require_exact_set \
  'RUSTSEC-2026-0194/0195 quick-xml' \
  'dependency package set' \
  "$quick_xml_packages" \
  "$expected_quick_xml_packages"

time_tree="$(
  inverse_tree \
    'RUSTSEC-2026-0009 time scope' \
    'time@0.3.41' \
    'features' \
    're-run cargo deny, then remove or replace the time ignore in deny.toml and update the reviewed MSRV evidence'
)"
time_packages="$(package_set <<<"$time_tree")"
expected_time_packages="$(LC_ALL=C sort <<'EOF'
nzbd (workspace)
nzbd-torrent (workspace)
rcgen v0.13.2
time v0.3.41
yasna v0.5.2
EOF
)"
require_exact_set \
  'RUSTSEC-2026-0009 time' \
  'dependency package set' \
  "$time_packages" \
  "$expected_time_packages"

time_features="$(feature_set <<<"$time_tree")"
expected_time_features="$(LC_ALL=C sort <<'EOF'
time feature "alloc"
time feature "std"
EOF
)"
require_exact_set \
  'RUSTSEC-2026-0009 time' \
  'feature set' \
  "$time_features" \
  "$expected_time_features"

option_ext_tree="$(
  inverse_tree \
    'MPL-2.0 option-ext scope' \
    'option-ext@0.2.0' \
    'normal' \
    'remove or replace the exact option-ext license exception in deny.toml and update the gate 9 evidence'
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
nzbd-torrent (workspace)
option-ext v0.2.0
EOF
)"
require_exact_set \
  'MPL-2.0 option-ext exception' \
  'dependency package set' \
  "$option_ext_packages" \
  "$expected_option_ext_packages"

ignored_advisories="$(
  sed -n '/^ignore = \[/,/^\]/p' deny.toml |
    grep -Eo 'RUSTSEC-[0-9]{4}-[0-9]{4}' |
    LC_ALL=C sort -u || true
)"
expected_ignored_advisories="$(LC_ALL=C sort <<'EOF'
RUSTSEC-2026-0009
RUSTSEC-2026-0194
RUSTSEC-2026-0195
EOF
)"
require_exact_set \
  'repository RustSec exceptions' \
  'reviewed identifier set' \
  "$ignored_advisories" \
  "$expected_ignored_advisories"

echo 'Reviewed dependency exceptions: exact package, feature, license, and advisory sets'
