#!/usr/bin/env bash
set -euo pipefail

review_doc="docs/BITTORRENT_RELEASE_REVIEW.md"
proposal="docs/BITTORRENT_PROPOSAL.md"
m0_report="docs/BITTORRENT_M0_REPORT.md"
readme="README.md"

require_literal() {
  local file="$1"
  local literal="$2"

  if ! grep -Fq -- "$literal" "$file"; then
    echo "BitTorrent release-review drift: $file is missing: $literal" >&2
    exit 1
  fi
}

for file in "$review_doc" "$proposal" "$m0_report" "$readme"; do
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

echo 'BitTorrent release-review policy: no-go state and operator review domains are explicit'
