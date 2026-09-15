#!/usr/bin/env python
"""Build the docs and fail on any MkDocs WARNING outside the frozen archive tree.

``mkdocs build --strict`` is the obvious tool, but the ``docs/archive/`` tree is a
deliberately-frozen copy of superseded documents whose internal links were broken when
they were archived, and it is kept for provenance rather than repaired. ``--strict`` would
fail on those forever. This script draws the line where it belongs: every warning about a
living page fails the build; a warning whose *source* page lives under ``archive/`` is
counted and reported but tolerated.

    python scripts/check_docs_warnings.py            # build + check
    python scripts/check_docs_warnings.py --no-build # check the last build's log instead

Exit code 0 when the living tree is warning-free, 1 otherwise.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
LOG = REPO / "site" / ".mkdocs-build.log"

# A warning is attributed to the page named in "Doc file '<path>'" when present; anything
# without that prefix (plugin/config warnings) is always living-tree.
_DOC_FILE = re.compile(r"Doc file '([^']+)'")


def classify(lines: list[str]) -> tuple[list[str], list[str]]:
    living: list[str] = []
    archived: list[str] = []
    for line in lines:
        if "WARNING" not in line:
            continue
        m = _DOC_FILE.search(line)
        if m and m.group(1).startswith("archive/"):
            archived.append(line.rstrip())
        else:
            living.append(line.rstrip())
    return living, archived


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-build", action="store_true", help="re-check the saved log instead of rebuilding")
    args = ap.parse_args()

    if args.no_build and LOG.exists():
        output = LOG.read_text(encoding="utf-8", errors="replace")
    else:
        proc = subprocess.run(
            [sys.executable, "-m", "mkdocs", "build", "--clean"],
            cwd=str(REPO),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        output = (proc.stdout or "") + (proc.stderr or "")
        LOG.parent.mkdir(parents=True, exist_ok=True)
        LOG.write_text(output, encoding="utf-8")
        if proc.returncode != 0:
            print(output[-4000:])
            print("mkdocs build failed", file=sys.stderr)
            return 1

    living, archived = classify(output.splitlines())
    print(f"mkdocs warnings: {len(living)} on living pages, {len(archived)} inside archive/ (tolerated)")
    if living:
        print("\nWarnings that must be fixed:", file=sys.stderr)
        for line in living:
            print("  " + line, file=sys.stderr)
        return 1
    print("living documentation tree is warning-free.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
