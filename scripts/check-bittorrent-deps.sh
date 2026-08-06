#!/usr/bin/env bash
set -euo pipefail

# ADR-19 treats the embedded engine and its TLS shape as a correctness
# boundary. Keep this check shell-only so it runs under the MSRV toolchain
# before any adapter code or peer test starts.
bt_normal_tree="$(cargo tree --locked -p nzbd-torrent -e normal --target all --prefix none)"
bt_feature_tree="$(cargo tree --locked -p nzbd-torrent -e features -i librqbit)"

bt_librqbit_versions="$(grep -E '^librqbit v' <<<"$bt_normal_tree" | sort -u || true)"
if [[ "$bt_librqbit_versions" != "librqbit v8.1.1" ]]; then
  echo "BitTorrent M0 requires exactly librqbit v8.1.1; found:" >&2
  echo "$bt_librqbit_versions" >&2
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

echo 'BitTorrent dependency policy: librqbit 8.1.1, rust-tls only, no OpenSSL'
