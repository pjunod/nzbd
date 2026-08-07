# rqbit PR draft — bound tracker requests and hostile announce intervals

**Status:** human review required; do not submit verbatim · **Targets:** rqbit
main patch `0006`; stable patch `0005` is reproduction evidence only

## Summary

- stream HTTP tracker response bodies with a 1 MiB hard limit;
- give the complete HTTP tracker request and body read a 30-second deadline;
- clamp tracker-provided HTTP and UDP announce intervals to at least 60 seconds;
- retain the existing explicit `force_tracker_interval` override for callers
  that intentionally need a different interval; and
- test the exact response limit, declared and chunked overflow, timeout, and
  interval boundaries on loopback.

## Problem

Tracker monitors are long-lived work created from untrusted metainfo. Current
rqbit main and v8.1.1 call `Response::bytes()`, which buffers an HTTP tracker
body without a size limit. The request has no tracker-owned deadline. Both
lines also accept tracker-provided announce intervals below one minute; v8.1.1
and main currently clamp UDP only to five seconds, while HTTP accepts zero.

One hostile tracker can therefore retain a request indefinitely, consume
memory proportional to its response, or make one monitor announce in a tight
loop. Caller-side URL and tracker-count limits cannot close those response-side
boundaries.

## Design

`fetch_http_tracker_response` owns the complete request deadline and streams
decoded chunks into a checked vector. It rejects an oversized declared
`Content-Length` before buffering and independently checks every received
chunk, so missing or false length headers do not bypass the limit.

The 60-second floor applies only to unforced tracker responses. Existing
callers that deliberately set `force_tracker_interval` keep the exact override
semantics. Retry backoff is unchanged.

The constants are intentionally local to tracker communications in this first
candidate. If maintainers prefer public configuration, the behavior and tests
can be retained while moving the values into a reviewed options type.

## Verification

Apply `0006-bound-tracker-requests-main.patch` to the documented rqbit-main
base and run:

```bash
cargo fmt --all -- --check
cargo test -p librqbit-tracker-comms --lib
```

The nzbd verifier runs those gates against current main and repeats the focused
suite against exact v8.1.1. The stable evidence also passes under Rust 1.85.

## Human submission checklist

1. Read the response loop and interval policy in both HTTP and UDP paths.
2. Decide whether 30 seconds, 1 MiB, and 60 seconds are acceptable upstream
   defaults or should be public options.
3. Rebase the main patch onto the current rqbit head and rerun its full
   contributor gates.
4. Edit this draft in your own words and add the repository-required AI
   assistance disclosure.

**Do not remove this warning without human review:** rqbit's contribution
policy requires the submitter to understand, review, edit, and disclose
AI-assisted work. This file is a preparation artifact, not authorization to
open an upstream PR.
