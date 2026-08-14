#!/usr/bin/env bash
set -euo pipefail

# The fuzz crate is intentionally outside the product workspace so
# libfuzzer-sys never enters a release graph. Keep every shared package on the
# reviewed product lock and admit only the two pinned, test-only additions.
lock_identities() {
  awk '
    function emit() {
      if (name != "") {
        print name "\t" version "\t" source
      }
    }

    $1 == "[[package]]" {
      emit()
      name = ""
      version = ""
      source = "(path)"
      next
    }

    $1 == "name" {
      name = $3
      gsub(/"/, "", name)
      next
    }

    $1 == "version" {
      version = $3
      gsub(/"/, "", version)
      next
    }

    $1 == "source" {
      source = $3
      gsub(/"/, "", source)
    }

    END { emit() }
  ' "$1" | LC_ALL=C sort -u
}

if ! grep -Fq 'libfuzzer-sys = "=0.4.13"' fuzz/Cargo.toml; then
  echo 'fuzz/Cargo.toml must pin libfuzzer-sys exactly to 0.4.13' >&2
  exit 1
fi

if ! grep -Fq 'features = ["fuzzing"]' fuzz/Cargo.toml; then
  echo 'the fuzz crate must use nzbd-torrent through its fuzzing feature' >&2
  exit 1
fi

if ! grep -Fq 'path = "../contrib/rqbit/vendor/crates/librqbit"' fuzz/Cargo.toml; then
  echo 'the fuzz crate must use the same maintained rqbit vendor as the product lock' >&2
  exit 1
fi

cargo metadata \
  --locked \
  --manifest-path fuzz/Cargo.toml \
  --format-version 1 >/dev/null

expected_fuzz_only=$'arbitrary\t1.4.2\tregistry+https://github.com/rust-lang/crates.io-index\nlibfuzzer-sys\t0.4.13\tregistry+https://github.com/rust-lang/crates.io-index\nnzbd-torrent-fuzz\t0.0.0\t(path)'
actual_fuzz_only="$(
  comm -13 \
    <(lock_identities Cargo.lock) \
    <(lock_identities fuzz/Cargo.lock)
)"

if [[ "$actual_fuzz_only" != "$expected_fuzz_only" ]]; then
  echo 'unexpected package identities entered the isolated fuzz lock:' >&2
  printf '%s\n' "$actual_fuzz_only" >&2
  exit 1
fi

echo 'BitTorrent fuzz dependency policy: product lock plus pinned test-only tooling'
