# rqbit pull-request draft — propagate file-sizing failures

**Status:** ready for human review and editing; do not submit verbatim ·
**Target:** [ikatson/rqbit](https://github.com/ikatson/rqbit/pulls)

## Proposed title

Stop torrent initialization when file sizing fails

## Proposed body

### Problem

Torrent initialization currently catches an error returned by
`TorrentStorage::ensure_file_length`, writes a warning log, and continues into
the paused/live state machine. An embedding application can therefore see a
successfully initialized torrent even when the selected payload file could not
be allocated or sized.

rqbit's only indication for errors such as ENOSPC or EDQUOT is a warning log.
Torrent stats and control state do not carry the failure, so callers
cannot reliably pause new work or report the affected torrent from the public
control plane.

### Change

Extract the selected-file sizing loop into a small fallible helper. Preserve
the existing selection and padding-file behavior, but attach file name and
requested length as context and return the first sizing failure. The existing
initialization state transition propagates an error returned by its blocking
task, so the torrent enters the normal error path instead of being reported as
successfully paused. On current main, give explicit checksum cancellation a
typed marker and suppress only that marker when a pause overlaps
initialization; a real sizing or I/O error remains fatal even when the pause
flag is set.

This does not add a new public API or storage policy. It makes the existing
`TorrentStorage` failure contract effective at the initialization boundary.

### Compatibility and safety

- Successful file sizing follows the same selected-file order and skips the
  same padding entries.
- The first sizing failure stops initialization; later files are not touched.
- A pause can still cancel the checksum loop, but it cannot turn an overlapping
  storage failure into a successful paused initialization.
- Error context contains only the metainfo file name and requested byte count.
  Callers remain responsible for display-safe path handling at their boundary.
- The change does not decide whether an application should pause, retry,
  delete, or continue seeding already-verified data after ENOSPC.

### Test

The focused unit test supplies a `TorrentStorage` whose first
`ensure_file_length` call returns an injected `StorageFull` error. It proves
that the call targets file 0 at 262,144 bytes, occurs exactly once, and returns
an error chain containing the file name, requested length, and storage cause.

The repository verifier first requires the exact test name in Cargo's test
list, then requires an exact one-test pass summary. A mutation inside the
helper made the test fail with `unwrap_err()` receiving `Ok(())`; restoring
error propagation made the same exact test pass. The focused test calls the
helper directly, so the verifier also requires the blocking initialization
call site to propagate that helper result. A mutation that discarded it there
left the unit test green but failed the call-site assertion.

Current main has a second exact proof for the pause race. With the pause flag
set, an injected `StorageFull` remains non-suppressible; the typed checksum
cancellation marker is suppressible only while that flag is set. The verifier
also pins that classifier at the actual initialization error branch.

### Validation

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo clippy --all-targets
cargo test --workspace
```

The nzbd contribution kit also validates exact v8.1.1 and the documented
current-main base as blocking Rust-TLS legs, then checks the moving main tip as
advisory pull-request drift evidence. Stable 8.1.1 may emit unrelated
warnings under newer Clippy versions; the focused test, formatting check, and
library compile remain the candidate's reproducible gates.

### AI assistance disclosure

OpenAI Codex assisted with research, implementation, tests, mutation checking,
and this draft. **Human contributor: replace this bolded note only after
reviewing and editing the final PR and confirming you can explain every changed
code path and test without AI assistance. Keep the disclosure above.**
