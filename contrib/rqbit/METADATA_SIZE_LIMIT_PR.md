# rqbit pull-request draft — bound peer metadata before allocation

**Status:** ready for human review and editing; do not submit verbatim ·
**Target:** [ikatson/rqbit](https://github.com/ikatson/rqbit/pulls)

## Proposed title

Allow callers to cap BEP 9 metadata before allocation

## Proposed body

### Problem

The peer metadata reader currently accepts a peer-advertised BEP 9 metadata
size up to a fixed 32 MiB and allocates that buffer before an embedding
application can inspect the metainfo. An application with a smaller
hostile-input limit therefore cannot enforce its policy before allocation
when adding a magnet.

The existing 32 MiB ceiling is a useful safe default and should remain the
behavior for callers that do not opt in to a smaller limit.

### Change

Add `PeerConnectionOptions::max_metadata_size: Option<u32>`. Session defaults
and per-add peer options merge through the existing peer-options path. The
metadata reader uses the configured value, or the existing 32 MiB default,
when processing the extended handshake.

The size check now runs before allocating the metadata buffer and before
sending unchoke, interested, or piece-request messages. A peer that advertises
more than the configured maximum is rejected without starting the metadata
exchange.

The option lives in `PeerConnectionOptions` rather than directly on
`AddTorrentOptions` because that object already carries session and per-add
settings into every peer connection used for magnet metadata. I am happy to
move the field if rqbit prefers a dedicated metadata-fetch contract.

### Compatibility and security

- `None` preserves the existing 32 MiB ceiling.
- The optional serde field keeps older serialized option documents valid.
- The limit changes only BEP 9 metadata fetched from peers. It does not change
  payload piece sizes or the parser's validation responsibilities.
- The rejection message reports only numeric sizes; it contains no peer,
  tracker, magnet, or credential material.

### Test

The focused unit test proves that a buffer exactly at the configured limit is
accepted and that a value one byte above it is rejected before the allocation
constructor can create a buffer. The nzbd verifier also checks the production
option and pre-allocation call site structurally, requires the exact test name
to appear in Cargo's test list, and then runs it with `--exact`; a renamed or
missing test cannot pass by executing zero tests.

### Validation

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo clippy --all-targets
cargo test --workspace
```

### AI assistance disclosure

OpenAI Codex assisted with research, implementation, tests, and this draft.
**Human contributor: replace this bolded note only after reviewing and
editing the final PR and confirming you can explain every changed code path
and test without AI assistance. Keep the disclosure above.**
