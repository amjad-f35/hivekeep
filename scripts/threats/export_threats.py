#!/usr/bin/env python3
"""Export the threat tables of a Markdown threat model as JSON.

Every Markdown table whose header row has a "Threat" column is read. Columns
recognised (case-insensitive): Threat ID, Priority, Threat, Mitigating
Invariants, Suggested Concrete Mitigations, Notes. Each row with a non-empty
threat becomes one JSON object; Markdown backslash escapes are removed.
Threat IDs must look like T-NN and be unique across the document.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

# Header text -> JSON key, in output order.
COLUMNS = {
    "threat id": "threat_id",
    "priority": "priority",
    "threat": "threat",
    "mitigating invariants": "mitigating_invariants",
    "suggested concrete mitigations": "suggested_concrete_mitigations",
    "notes": "notes",
}
THREAT_ID = re.compile(r"^T-\d+$")
_ESCAPED_PIPE = "\x00"


def unescape(text: str) -> str:
    """Drop Markdown backslash escapes (``\\|`` -> ``|``)."""
    return re.sub(r"\\(.)", r"\1", text)


def split_row(line: str) -> list[str]:
    """Cells of a ``| a | b |`` row, honouring escaped pipes."""
    guarded = line.replace("\\|", _ESCAPED_PIPE)
    return [c.strip().replace(_ESCAPED_PIPE, "\\|") for c in guarded.split("|")[1:-1]]


def is_separator(cells: list[str]) -> bool:
    return all(c.replace("-", "").replace(":", "").strip() == "" for c in cells)


def extract(markdown: str) -> list[dict[str, str]]:
    """Parse every threat table in ``markdown``."""
    threats: list[dict[str, str]] = []
    columns: dict[str, int] | None = None
    for raw in markdown.splitlines():
        line = raw.strip()
        if not line.startswith("|"):
            columns = None
            continue
        cells = split_row(line)
        if columns is None:
            names = [unescape(c).lower() for c in cells]
            if "threat" in names:
                columns = {COLUMNS[n]: i for i, n in enumerate(names) if n in COLUMNS}
            continue
        if is_separator(cells):
            continue
        idx = columns["threat"]
        if idx >= len(cells) or not unescape(cells[idx]):
            continue
        row = {}
        for key in COLUMNS.values():
            i = columns.get(key)
            if i is not None and i < len(cells):
                row[key] = unescape(cells[i])
        threats.append(row)

    seen: set[str] = set()
    for t in threats:
        tid = t.get("threat_id", "")
        if not THREAT_ID.match(tid):
            raise ValueError(f"threat id {tid!r} must look like T-NN (e.g. T-01)")
        if tid in seen:
            raise ValueError(f"duplicate threat id {tid}")
        seen.add(tid)
    return threats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--md", default="docs/threat-model.md", help="threat model Markdown")
    parser.add_argument("--out", default="docs/threats.json", help="JSON output path")
    args = parser.parse_args(argv)

    md = Path(args.md)
    if not md.exists():
        print(f"error: {md} not found", file=sys.stderr)
        return 1
    try:
        threats = extract(md.read_text(encoding="utf-8"))
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    Path(args.out).write_text(json.dumps(threats, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {len(threats)} threats from {md} to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
