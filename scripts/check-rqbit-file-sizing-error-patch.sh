#!/usr/bin/env bash

set -euo pipefail

readonly stable_base="00b97485160ff5b5aa2b379ea0815d568ec665f0"
readonly main_base="4e5f94cbcf1d57ec500885c77cf1e24d70232d89"
readonly sizing_test="torrent_state::initializing::tests::file_sizing_error_stops_initialization"
readonly pause_race_test="torrent_state::tests::pause_race_preserves_non_cancellation_initialization_errors"

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
    readonly patch_file="contrib/rqbit/0018-propagate-file-sizing-errors.patch"
    readonly source_variant="stable"
    readonly -a exact_tests=("$sizing_test")
    readonly initialization_boundary='spawn_block_in_place\(\|\| \{[[:space:]]*ensure_selected_file_lengths\([[:space:]]*self\.files\.as_ref\(\),[[:space:]]*&self\.metadata\.file_infos,[[:space:]]*self\.only_files\.as_deref\(\),[[:space:]]*\)[[:space:]]*\}\)\?;'
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
    readonly patch_file="contrib/rqbit/0019-propagate-file-sizing-errors-main.patch"
    readonly source_variant="main"
    readonly -a exact_tests=("$sizing_test" "$pause_race_test")
    readonly initialization_boundary='block_in_place_with_semaphore\(\|\| \{[[:space:]]*ensure_selected_file_lengths\([[:space:]]*self\.files\.as_ref\(\),[[:space:]]*&self\.metadata\.file_infos,[[:space:]]*self\.only_files\.as_deref\(\),[[:space:]]*\)[[:space:]]*\}\)[[:space:]]*\.await\?;'
    ;;
esac

echo "verifying rqbit $source_variant file-sizing error patch against $source_head"

readonly work_dir="$(mktemp -d "${TMPDIR:-/tmp}/nzbd-rqbit-sizing-error.XXXXXX")"
trap 'rm -rf -- "$work_dir"' EXIT

git clone --quiet --no-hardlinks "$source_dir" "$work_dir/rqbit"
# Cloning a repository whose worktree is detached follows its configured
# default branch, not necessarily the commit checked out in that worktree.
# Reproduce the exact source that was classified above before applying the
# contribution patch; CI intentionally supplies detached SHA checkouts.
git -C "$work_dir/rqbit" checkout --quiet --detach "$source_head"
if git -C "$work_dir/rqbit" apply --check "$repository_root/$patch_file"; then
  git -C "$work_dir/rqbit" apply "$repository_root/$patch_file"
elif [[ "$source_variant" == "main" ]]; then
  echo "direct apply drifted; attempting a three-way apply against $source_head" >&2
  git -C "$work_dir/rqbit" apply --3way "$repository_root/$patch_file"
else
  echo "stable v8.1.1 patch no longer applies to its pinned base" >&2
  exit 1
fi

readonly source_file="$work_dir/rqbit/crates/librqbit/src/torrent_state/initializing.rs"
readonly normalized_source="$work_dir/initializing.normalized.rs"
tr '\n' ' ' <"$source_file" >"$normalized_source"
for invariant in \
  'fn ensure_selected_file_lengths(' \
  '.ensure_file_length(idx, file_info.len)' \
  'self.only_files.as_deref(),' \
  'fn file_sizing_error_stops_initialization()'
do
  if ! grep -Fq "$invariant" "$source_file"; then
    echo "file-sizing error patch is missing invariant: $invariant" >&2
    exit 1
  fi
done

if grep -Fq 'if let Err(err) = self.files.ensure_file_length' "$source_file"; then
  echo "file-sizing error patch still swallows the initialization failure" >&2
  exit 1
fi

if ! grep -Eq "$initialization_boundary" "$normalized_source"; then
  echo "file-sizing helper result is not propagated at the $source_variant initialization boundary" >&2
  exit 1
fi

if [[ "$source_variant" == "main" ]]; then
  readonly state_file="$work_dir/rqbit/crates/librqbit/src/torrent_state/mod.rs"
  readonly file_ops="$work_dir/rqbit/crates/librqbit/src/file_ops.rs"
  readonly normalized_state="$work_dir/torrent-state.normalized.rs"
  tr '\n' ' ' <"$state_file" >"$normalized_state"
  for invariant in \
    'struct InitialCheckPaused;' \
    'return Err(InitialCheckPaused.into());' \
    'fn should_suppress_initial_check_error(' \
    'fn pause_race_preserves_non_cancellation_initialization_errors()'
  do
    if ! grep -Fq "$invariant" "$state_file" "$file_ops"; then
      echo "rqbit main pause-race fix is missing invariant: $invariant" >&2
      exit 1
    fi
  done
  readonly pause_boundary='if should_suppress_initial_check_error\([[:space:]]*init\.is_pause_requested\(\),[[:space:]]*&err,[[:space:]]*\)[[:space:]]*\{'
  if ! grep -Eq "$pause_boundary" "$normalized_state"; then
    echo "rqbit main does not classify the actual initialization error boundary" >&2
    exit 1
  fi
  if [[ "$(grep -Fc 'init.is_pause_requested()' "$state_file")" -ne 1 ]]; then
    echo "rqbit main initialization error branch has an unreviewed pause-request fallback" >&2
    exit 1
  fi
fi

(
  cd "$work_dir/rqbit"
  cargo fmt --all -- --check

  test_list="$(cargo test -p librqbit --lib \
    --no-default-features \
    --features rust-tls \
    -- --list)"
  readonly test_list
  for exact_test in "${exact_tests[@]}"; do
    if ! grep -Fxq "$exact_test: test" <<<"$test_list"; then
      echo "file-sizing error proof test was not discovered: $exact_test" >&2
      exit 1
    fi

    set +e
    test_output="$(cargo test -p librqbit --lib "$exact_test" \
      --no-default-features \
      --features rust-tls \
      -- --exact --nocapture 2>&1)"
    test_status=$?
    set -e
    printf '%s\n' "$test_output"
    if [[ $test_status -ne 0 ]]; then
      exit "$test_status"
    fi
    if ! grep -Fq 'test result: ok. 1 passed; 0 failed; 0 ignored' <<<"$test_output"; then
      echo "file-sizing error proof did not execute exactly one passing test: $exact_test" >&2
      exit 1
    fi
  done

  cargo check -p librqbit --no-default-features --features rust-tls
)
