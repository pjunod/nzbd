#!/usr/bin/env bash
set -euo pipefail

review_doc="docs/BITTORRENT_RELEASE_REVIEW.md"
proposal="docs/BITTORRENT_PROPOSAL.md"
m0_report="docs/BITTORRENT_M0_REPORT.md"
gate9_review="docs/BITTORRENT_GATE9_REVIEW.md"
readme="README.md"

require_literal() {
  local file="$1"
  local literal="$2"

  if ! grep -Fq -- "$literal" "$file"; then
    echo "BitTorrent release-review drift: $file is missing: $literal" >&2
    exit 1
  fi
}

require_count() {
  local file="$1"
  local pattern="$2"
  local expected="$3"
  local actual

  actual="$(grep -cE -- "$pattern" "$file" || true)"
  if [[ "$actual" != "$expected" ]]; then
    echo "BitTorrent release-review drift: $file has $actual lines matching /$pattern/, expected $expected" >&2
    exit 1
  fi
}

for file in "$review_doc" "$proposal" "$m0_report" "$gate9_review" "$readme"; do
  if [[ ! -f "$file" ]]; then
    echo "BitTorrent release-review drift: missing $file" >&2
    exit 1
  fi
done

require_literal "$review_doc" '**Status:** pre-release review surface; production BitTorrent remains disabled'
require_literal "$review_doc" '> **No-go:** do not add a production switch or weaken the daemon-isolation'

for heading in \
  '## 2. Public traffic' \
  '## 3. Ports' \
  '## 4. Paths' \
  '## 5. Seeding' \
  '## 6. Deletion' \
  '## 7. Release evidence' \
  '## 8. Sign-off and stop conditions'; do
  require_literal "$review_doc" "$heading"
done

require_literal "$m0_report" '| 7. Public observability | **Fail** |'
require_literal "$m0_report" '| 8. nzbd-authoritative persistence | **Fail** |'
require_literal "$m0_report" '| 9. Resource, package, and license delta | **Partial** |'
require_literal "$proposal" '[pre-release operations review](BITTORRENT_RELEASE_REVIEW.md)'
require_literal "$readme" '[docs/BITTORRENT_RELEASE_REVIEW.md](docs/BITTORRENT_RELEASE_REVIEW.md)'

# The gate 9 human disposition is a recorded decision, not a rerunnable check.
# Losing it would silently return the gate to "awaiting a reviewer" while the
# proposal and M0 report still claim the boundaries were accepted.
require_literal "$gate9_review" '### 4.1 Recorded disposition — accepted 2026-08-14'
require_literal "$gate9_review" '**Status:** all eleven dispositions recorded in §4.1; gate 9 remains Partial'
require_literal "$gate9_review" 'reply `APPROVE RECOMMENDED DEFAULTS`, 2026-08-14T00:30:03Z'
require_literal "$gate9_review" 'https://github.com/pjunod/nzbd/issues/83#issuecomment-5287959702'
require_count "$gate9_review" '^\| *[0-9]+ \| \*\*Accepted' 11

# Acceptance of items 6-11 accepted a requirement, not its implementation.
# Gate 9 stays Partial until an accepted stable release enforces every boundary.
require_literal "$gate9_review" 'It does not move gate 9 to Pass.'
require_literal "$m0_report" 'gate 9 review brief §4.1'
require_literal "$proposal" 'Its §4.1 records the accepted disposition'

echo 'BitTorrent release-review policy: no-go state, operator review domains, and the recorded gate 9 disposition are explicit'
