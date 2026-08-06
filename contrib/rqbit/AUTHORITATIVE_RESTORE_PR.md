# rqbit pull-request draft — persistence without implicit admission

**Status:** ready for human review and editing; do not submit verbatim ·
**Target:** [ikatson/rqbit](https://github.com/ikatson/rqbit/pulls)

## Proposed title

Allow persistent sessions to skip implicit torrent restore

## Proposed body

### Problem

When persistence is configured, session construction implicitly admits every
stored torrent before returning. An embedding application with its own durable
queue therefore cannot keep rqbit fast-resume data while deciding which
records are authoritative and eligible to restore.

Disabling persistence is not equivalent: it discards the resume state that
makes restart safe and fast. Deleting rqbit's store before construction also
throws away valid records before the application can reconcile them.

### Change

Add `SessionOptions::disable_auto_restore`, defaulting to `false`. When set,
session construction keeps persistence active but skips only the constructor's
implicit admission loop. The caller can then explicitly add the authoritative
subset with each persisted `preferred_id`.

The default preserves existing behavior: callers that do not set the option
still restore every stored record.

### Test

The focused test:

1. persists two paused torrents;
2. constructs a persistent session with automatic restore disabled and proves
   it starts empty;
3. explicitly restores only the selected record under its original ID; and
4. constructs a default session and proves both records are still restored.

The change does not expose rqbit's private persistence schema, delete records,
or claim to alter payload verification. It only separates opening the store
from admitting every stored torrent.

### Validation

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo clippy --all-targets
cargo test --workspace
```

### AI assistance disclosure

OpenAI Codex assisted with research, implementation, tests, and this draft.
**Human contributor: replace this sentence only after reviewing and editing
the final PR and confirming you can explain every changed code path and test
without AI assistance.**
