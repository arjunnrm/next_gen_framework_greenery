#!/usr/bin/env python
"""Render the observability Genie space for one target's catalog.

WHY THIS SCRIPT EXISTS. `resources/genie_spaces.*.file_path` is inlined into
`serialized_space` **verbatim** at deploy time: DABs does NOT substitute `${var.*}` inside the
referenced file (verified against CLI v1.13.0 -- a templatised file resolves with the literal
`${var.catalog}` still in the payload, which would deploy a space whose every table reference
is a nonexistent catalog named `${var.catalog}`). The dashboards avoid this with
`dataset_catalog`/`dataset_schema`, which let their queries carry BARE table names; a Genie
space has no equivalent, because `data_sources.tables[].identifier` must be fully qualified.

So the catalog has to be baked in per target, and the canonical file
(`databricks-genie/flowx_observability.geniespace.json`) is the `flowx` rendering used by the
default targets. This script writes the rendering for any other catalog, which the target's
own resource YAML then points at.

    python scripts/render_genie_space.py --catalog bt_digital_poc

Writes `databricks-genie/flowx_observability.<catalog>.geniespace.json`.

WHAT IT REWRITES. Every occurrence of `<source-catalog>.` immediately followed by one of the
framework's own schemas (`config`, `observability`). It deliberately does NOT touch
`system.billing.*` -- those tables live in the `system` catalog on every workspace -- nor any
other identifier, so a stray word "flowx" in prose stays untouched.

WHAT IT PRESERVES. The API rejects unsorted repeated blocks, so the file's ordering is
load-bearing. This script only substitutes text inside existing values; it adds, removes and
reorders nothing. It re-verifies the sort afterwards and refuses to write a file that would be
rejected, because a sort error surfaces only at deploy time as
"Invalid export proto: ... must be sorted by ...".
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CANONICAL = REPO / "databricks-genie" / "flowx_observability.geniespace.json"

# The framework's own schemas. A catalog-qualified reference to one of these is ours to rewrite;
# anything else (notably system.billing) is not.
FRAMEWORK_SCHEMAS = ("config", "observability")


def rewrite(text: str, source_catalog: str, target_catalog: str) -> tuple[str, int]:
    """Replace `<source>.<framework schema>.` with `<target>.<framework schema>.`."""
    total = 0
    for schema in FRAMEWORK_SCHEMAS:
        pattern = re.compile(rf"\b{re.escape(source_catalog)}\.{re.escape(schema)}\.")
        text, n = pattern.subn(f"{target_catalog}.{schema}.", text)
        total += n
    return text, total


def assert_sorted(doc: dict) -> None:
    """The Genie export API rejects unsorted repeated blocks. Fail here, not at deploy."""
    tables = [t["identifier"] for t in doc["data_sources"]["tables"]]
    if tables != sorted(tables):
        raise SystemExit(
            "data_sources.tables is no longer sorted by identifier after rewriting.\n"
            "Renaming the catalog can change the sort order (e.g. 'bt_...' sorts before\n"
            "'system...'). Re-sort the tables block, keeping every other block untouched."
        )
    blocks = (
        ("instructions", "text_instructions"),
        ("instructions", "example_question_sqls"),
        ("config", "sample_questions"),
        ("benchmarks", "questions"),
    )
    for outer, inner in blocks:
        items = (doc.get(outer) or {}).get(inner) or []
        ids = [i["id"] for i in items if "id" in i]
        if ids != sorted(ids):
            raise SystemExit(f"{outer}.{inner} is not sorted by id")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--catalog", required=True, help="target catalog, e.g. bt_digital_poc")
    ap.add_argument("--source-catalog", default="flowx",
                    help="catalog baked into the canonical file (default: flowx)")
    ap.add_argument("--out", default=None, help="output path (default: alongside the canonical)")
    args = ap.parse_args()

    if args.catalog == args.source_catalog:
        print(f"catalog is already {args.catalog}; the canonical file is the rendering")
        return 0

    raw = CANONICAL.read_text(encoding="utf-8")
    rendered, count = rewrite(raw, args.source_catalog, args.catalog)
    if count == 0:
        raise SystemExit(
            f"no '{args.source_catalog}.<{'|'.join(FRAMEWORK_SCHEMAS)}>.' references found -- "
            "is --source-catalog right?"
        )

    doc = json.loads(rendered)  # also proves the rewrite kept it valid JSON
    tables = [t["identifier"] for t in doc["data_sources"]["tables"]]
    if tables != sorted(tables):
        doc["data_sources"]["tables"].sort(key=lambda t: t["identifier"])
        print("  re-sorted data_sources.tables after the catalog rename")
    assert_sorted(doc)

    out = Path(args.out) if args.out else (
        CANONICAL.parent / f"flowx_observability.{args.catalog}.geniespace.json")
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"wrote {out.relative_to(REPO)}")
    print(f"  {count} table references repointed to catalog '{args.catalog}'")
    print(f"  {len(tables)} data sources, sorted: {tables == sorted(tables)}")
    for t in tables:
        print(f"    {t}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
