#!/usr/bin/env bash
set -euo pipefail

# ADR-19 treats the embedded engine and its TLS shape as a correctness
# boundary. Keep this check shell-only so it runs under the MSRV toolchain
# before any adapter code or peer test starts.
readonly repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bt_normal_tree="$(cargo tree --locked -p nzbd-torrent -e normal --target all --prefix none)"
bt_feature_tree="$(cargo tree --locked -p nzbd-torrent -e features -i librqbit)"
bt_source_tree="$(cargo tree --locked -p nzbd-torrent -e normal)"
daemon_normal_tree="$(cargo tree --locked -p nzbd -e normal --target all --prefix none)"

bt_librqbit_versions="$(grep -E '^librqbit v' <<<"$bt_normal_tree" | sed -E 's/ \(.*\)$//' | sort -u || true)"
if [[ "$bt_librqbit_versions" != "librqbit v8.1.1" ]]; then
  echo "BitTorrent M0 requires exactly librqbit v8.1.1; found:" >&2
  echo "$bt_librqbit_versions" >&2
  exit 1
fi

if ! grep -Fq 'path = "contrib/rqbit/vendor/crates/librqbit"' "$repository_root/Cargo.toml" 2>/dev/null; then
  echo 'BitTorrent M0 must consume the checked-in maintained rqbit vendor tree' >&2
  exit 1
fi

if ! grep -F 'librqbit v8.1.1 (' <<<"$bt_source_tree" | grep -Fq 'contrib/rqbit/vendor/crates/librqbit'; then
  echo 'resolved librqbit is not the checked-in maintained v8.1.1 source' >&2
  exit 1
fi

if ! grep -q 'librqbit feature "rust-tls"' <<<"$bt_feature_tree"; then
  echo 'librqbit rust-tls feature is not enabled' >&2
  exit 1
fi

if grep -q 'librqbit feature "default"' <<<"$bt_feature_tree"; then
  echo 'librqbit default features must remain disabled' >&2
  exit 1
fi

if grep -Eq '^(openssl|openssl-sys|native-tls) v' <<<"$bt_normal_tree"; then
  echo 'OpenSSL/native-tls entered the nzbd-torrent normal dependency graph:' >&2
  grep -E '^(openssl|openssl-sys|native-tls) v' <<<"$bt_normal_tree" >&2
  exit 1
fi

daemon_torrent_packages="$(grep -E '^(nzbd-torrent|librqbit)' <<<"$daemon_normal_tree" | sort -u || true)"
if [[ -n "$daemon_torrent_packages" ]]; then
  echo 'BitTorrent M0 is a no-go: the production daemon dependency graph contains:' >&2
  echo "$daemon_torrent_packages" >&2
  exit 1
fi

echo 'BitTorrent dependency policy: maintained librqbit 8.1.1 series, rust-tls only, no OpenSSL; daemon graph remains dormant'
