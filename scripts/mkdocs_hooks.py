"""MkDocs hooks for the Metaflow documentation hub.

Registered in ``mkdocs.yml`` under ``hooks:``. MkDocs imports this module and calls the
``on_*`` functions at the matching lifecycle points, so the derived reference pages are
regenerated as part of every ``mkdocs build`` without anyone remembering to run a script.

    docs/reference/json/*   <- databricks-app/config/attribute_knowledge.json
                               + databricks-app/config/attribute_faqs.json
                               + onboarding_templates/onboarding_spec.schema.json
                               + spec_validator.py (removed-key registry)
    docs/reference/code/*   <- src/flowx (AST, no imports)
    docs/UC*/               <- BT_Usecase/<UC>/docs

Two deliberate limits:

* Regeneration runs for ``mkdocs build`` only, never ``mkdocs serve``. The generator writes
  into ``docs/``, and a write inside ``docs_dir`` during ``serve`` re-triggers the watcher —
  an infinite rebuild loop. Under ``serve`` the committed output is used as-is; run
  ``python scripts/build_docs_reference.py`` by hand when you change a source.
* A regeneration failure is a WARNING, not a build failure. The reference derives from the
  Databricks App's config; a docs-only checkout without ``node`` on PATH must still build
  from the committed pages. CI enforces freshness separately with
  ``python scripts/build_docs_reference.py --check``.

Set ``FLOWX_DOCS_SKIP_REGEN=1`` to skip regeneration explicitly (CI does this after the
``--check`` step has already proven the committed output current).
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from pathlib import Path

log = logging.getLogger("mkdocs.hooks.flowx")

REPO = Path(__file__).resolve().parent.parent
GENERATOR = REPO / "scripts" / "build_docs_reference.py"


def on_startup(command: str, dirty: bool) -> None:  # noqa: ARG001 - MkDocs signature
    if command != "build":
        log.info("[flowx] mkdocs %s: skipping reference regeneration (build-only)", command)
        return
    if os.environ.get("FLOWX_DOCS_SKIP_REGEN") == "1":
        log.info("[flowx] FLOWX_DOCS_SKIP_REGEN=1: using committed reference pages")
        return
    if not GENERATOR.exists():
        log.warning("[flowx] generator not found at %s; using committed reference pages", GENERATOR)
        return

    started = time.time()
    proc = subprocess.run(
        [sys.executable, str(GENERATOR)],
        cwd=str(REPO),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    elapsed = time.time() - started
    if proc.returncode != 0:
        log.warning(
            "[flowx] reference regeneration failed (exit %s) after %.1fs; building from the "
            "committed pages instead.\n%s",
            proc.returncode,
            elapsed,
            (proc.stderr or proc.stdout or "").strip()[-2000:],
        )
        return
    pages = [line.strip() for line in proc.stdout.splitlines() if line.strip().startswith("docs")]
    log.info("[flowx] regenerated %d derived pages in %.1fs", len(pages), elapsed)


def on_config(config):  # noqa: ANN001 - MkDocs passes its own config object
    """Expose the framework version to templates as ``config.extra.flowx_version``."""
    version = "unknown"
    pyproject = REPO / "pyproject.toml"
    if pyproject.exists():
        for line in pyproject.read_text(encoding="utf-8").splitlines():
            if line.strip().startswith("version"):
                version = line.split("=", 1)[1].strip().strip('"').strip("'")
                break
    config.extra = dict(config.extra or {})
    config.extra.setdefault("flowx_version", version)
    return config
