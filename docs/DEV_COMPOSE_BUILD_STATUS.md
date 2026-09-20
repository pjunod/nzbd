# Dev Compose build status — local image ownership

**Status:** adversarial review clean, qualification pending · **Branch:**
`codex/fix-dev-compose-build` · **Updated:** 2026-09-19

Companion to [STATUS.md](../STATUS.md), which owns the project-wide ledger.
This page records the isolated fix for the dev Compose build failure reported on
`nuc3`; it is not a general Docker operations guide.

## Current position — implementation complete, qualification deferred

| Workstream | State | Next observable result |
|---|---|---|
| Reproduction and diagnosis | complete | The image-only discovery service is identified as the registry pull owner |
| Compose image ownership | implemented | Both services resolve the same local build definition |
| Regression contract | implemented | A source-level test rejects an image-only dev discovery service |
| Adversarial review | complete | No actionable P0–P3 findings |
| Fast-lane qualification | pending | Focused config tests and Compose model validation pass after review remediation |
| Merge | pending | Reviewed, green head lands on `main` |

## Working rules — one review and one qualification pass

- Work happens in an isolated clone at
  `/private/tmp/nzbd-codex-compose-build`; user checkouts remain untouched.
- The implementation receives one adversarial review only when the complete
  diff is ready to merge.
- Review findings are addressed before the fast-lane tests run. Full-suite
  failures remain for the separate batching process requested by the operator.
- The change adds no runtime feature gate. It repairs build ownership in the
  developer Compose model.

## Decisions applied during implementation

1. **Share the build mapping with a YAML anchor.** Both services need an
   explicit `build` entry, while one definition prevents their Dockerfile,
   context, and version argument from drifting.
2. **Keep `depends_on` for runtime readiness only.** Compose resolves images
   before container startup ordering, so health-based dependency ordering
   cannot make an image-only service consume a concurrently built tag.
3. **Pin the regression at the shipped-recipe boundary.** The failure came
   from the Compose contract, so the test inspects that contract instead of
   exercising unrelated daemon behavior.

## Activity log

| Date | Change | Evidence |
|---|---|---|
| 2026-09-19 | Reproduced the resolved Compose model from the reported checkout | Only `nzbd` owned a build; `nzbd-discovery` referenced `nzbd:dev` as an image |
| 2026-09-19 | Moved work into an isolated GitHub clone | Clean branch `codex/fix-dev-compose-build` from `origin/main` |
| 2026-09-19 | Shared the build definition and added the regression contract | Source inspection complete; tests intentionally deferred until after review |
| 2026-09-19 | Completed the single adversarial merge-readiness review | No actionable P0–P3 correctness, compatibility, test, documentation, or side-effect findings |
