#!/usr/bin/env python3
"""Self-hosted status badges — no external badge service, works on private
repos. Generates shields-style flat SVG badges plus a shields.io endpoint
JSON, published to the `badges` branch by .github/workflows/coverage.yml
and referenced from README.md.

Usage:
    coverage-badges.py --coverage 87.3 --tests 87 --out badges-out
"""

import argparse
import json
import pathlib

FONT_WIDTH = 6.6  # ~Verdana 11px average glyph width
PADDING = 10


def color_for(percent: float) -> str:
    if percent >= 80.0:
        return "#4c1"  # brightgreen
    if percent >= 60.0:
        return "#a4a61d"  # yellowgreen
    if percent >= 40.0:
        return "#dfb317"  # yellow
    return "#e05d44"  # red


def text_width(s: str) -> int:
    return round(len(s) * FONT_WIDTH) + PADDING


def badge(label: str, value: str, color: str) -> str:
    lw = text_width(label)
    vw = text_width(value)
    w = lw + vw
    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="20" role="img" aria-label="{label}: {value}">
  <title>{label}: {value}</title>
  <linearGradient id="s" x2="0" y2="100%">
    <stop offset="0" stop-color="#bbb" stop-opacity=".1"/>
    <stop offset="1" stop-opacity=".1"/>
  </linearGradient>
  <clipPath id="r"><rect width="{w}" height="20" rx="3" fill="#fff"/></clipPath>
  <g clip-path="url(#r)">
    <rect width="{lw}" height="20" fill="#555"/>
    <rect x="{lw}" width="{vw}" height="20" fill="{color}"/>
    <rect width="{w}" height="20" fill="url(#s)"/>
  </g>
  <g fill="#fff" text-anchor="middle" font-family="Verdana,Geneva,DejaVu Sans,sans-serif" font-size="110" text-rendering="geometricPrecision">
    <text x="{lw * 5}" y="150" transform="scale(.1)" fill="#010101" fill-opacity=".3" textLength="{(lw - PADDING) * 10}">{label}</text>
    <text x="{lw * 5}" y="140" transform="scale(.1)" textLength="{(lw - PADDING) * 10}">{label}</text>
    <text x="{lw * 10 + vw * 5}" y="150" transform="scale(.1)" fill="#010101" fill-opacity=".3" textLength="{(vw - PADDING) * 10}">{value}</text>
    <text x="{lw * 10 + vw * 5}" y="140" transform="scale(.1)" textLength="{(vw - PADDING) * 10}">{value}</text>
  </g>
</svg>
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--coverage", type=float, required=True, help="line coverage percent")
    ap.add_argument("--tests", type=int, required=True, help="tests passed")
    ap.add_argument("--out", type=pathlib.Path, required=True)
    args = ap.parse_args()

    out: pathlib.Path = args.out
    out.mkdir(parents=True, exist_ok=True)

    cov = f"{args.coverage:.1f}%"
    color = color_for(args.coverage)
    (out / "coverage.svg").write_text(badge("coverage", cov, color))
    # The Tests workflow badge shows pass/fail; this one shows the count
    # (it only regenerates when the instrumented suite passed).
    (out / "tests.svg").write_text(badge("tests", f"{args.tests} passed", "#4c1"))
    # shields.io endpoint schema, for anyone who prefers
    # https://img.shields.io/endpoint?url=<raw badges branch>/coverage.json
    (out / "coverage.json").write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "label": "coverage",
                "message": cov,
                "color": color.lstrip("#"),
            }
        )
    )
    print(f"badges written to {out}: coverage {cov}, {args.tests} tests")


if __name__ == "__main__":
    main()
