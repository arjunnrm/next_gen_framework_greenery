"""Branding accessor for the framework and the Onboarding App.

Loads ``branding.json`` (resolved relative to ``__file__``, so it works from any
working directory) and exposes the values through small typed accessors.

Dependency-free and stdlib-only on purpose: a generated copy of this module runs
inside the Databricks App, where only ``databricks-app/requirements.txt`` is
installed and no third-party branding library is available.

Usage::

    from branding.branding import get, env_var, load_branding

    get("framework.display_name")        # "Metaflow"
    get("app.title")                     # "Metaflow Onboarding"
    env_var("SPEC_CATALOG")              # "METAFLOW_SPEC_CATALOG"
    load_branding()                      # the whole dict

Nothing here reads or writes the repo outside ``branding.json``. Rebranding a
*generated* file is the job of ``scripts/apply_branding.py``; this module is
what *hand-maintained* Python reads at runtime.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

__all__ = [
    "BRANDING_PATH",
    "BrandingError",
    "load_branding",
    "get",
    "env_var",
    "env_prefix",
    "framework_name",
    "framework_slug",
    "vendor_name",
    "app_title",
    "app_deployed_name",
    "docs_site_name",
    "logo_assets",
    "reload",
]

#: Absolute path to the JSON config, resolved from this file's own location.
BRANDING_PATH: Path = Path(__file__).resolve().parent / "branding.json"

_CACHE: Optional[Dict[str, Any]] = None

# Keys that must exist for any consumer to be meaningful. Kept deliberately
# small: a missing optional key should raise at the call site with a clear dotted
# path, not fail the whole load.
_REQUIRED: List[str] = [
    "framework.display_name",
    "framework.slug",
    "vendor.display_name",
    "app.title",
    "app.deployed_name",
    "env.prefix",
    "docs.site_name",
]


class BrandingError(RuntimeError):
    """Raised when branding.json is missing, malformed, or incomplete."""


def _dig(data: Dict[str, Any], dotted: str) -> Any:
    node: Any = data
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            raise KeyError(dotted)
        node = node[part]
    return node


def load_branding(path: Optional[os.PathLike] = None, *, refresh: bool = False) -> Dict[str, Any]:
    """Return the parsed branding config as a plain dict.

    The result is cached after the first successful load. Pass ``refresh=True``
    (or call :func:`reload`) to re-read from disk.
    """
    global _CACHE
    if _CACHE is not None and not refresh and path is None:
        return _CACHE

    target = Path(path) if path is not None else BRANDING_PATH
    try:
        raw = target.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise BrandingError(
            f"branding config not found at {target}. "
            "It ships with the repo at branding/branding.json; inside the Databricks App "
            "it is the generated copy written by scripts/apply_branding.py."
        ) from exc

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise BrandingError(f"branding config at {target} is not valid JSON: {exc}") from exc

    if not isinstance(data, dict):
        raise BrandingError(f"branding config at {target} must be a JSON object, got {type(data).__name__}")

    missing = []
    for key in _REQUIRED:
        try:
            _dig(data, key)
        except KeyError:
            missing.append(key)
    if missing:
        raise BrandingError(f"branding config at {target} is missing required keys: {', '.join(missing)}")

    if path is None:
        _CACHE = data
    return data


def reload() -> Dict[str, Any]:
    """Drop the cache and re-read branding.json from disk."""
    return load_branding(refresh=True)


def get(dotted: str, default: Any = None) -> Any:
    """Look up a dotted path, e.g. ``get("app.title")``.

    Raises :class:`BrandingError` for an unknown path when no ``default`` is
    given, so a typo fails loudly instead of silently rendering ``None``.
    """
    data = load_branding()
    try:
        return _dig(data, dotted)
    except KeyError:
        if default is not None:
            return default
        raise BrandingError(f"no branding key {dotted!r} in {BRANDING_PATH}") from None


def env_prefix() -> str:
    """The environment-variable prefix, e.g. ``"METAFLOW"``."""
    return str(get("env.prefix")).strip().strip("_").upper()


def env_var(name: str) -> str:
    """Build a prefixed environment variable name.

    ``env_var("SPEC_CATALOG") == "METAFLOW_SPEC_CATALOG"``

    Already-prefixed names are returned unchanged, so this is safe to apply
    twice::

        env_var("METAFLOW_SPEC_CATALOG") == "METAFLOW_SPEC_CATALOG"
    """
    prefix = env_prefix()
    clean = str(name).strip().strip("_").upper()
    if not clean:
        raise BrandingError("env_var() needs a non-empty name")
    if clean == prefix or clean.startswith(prefix + "_"):
        return clean
    return f"{prefix}_{clean}"


# -- small typed accessors -------------------------------------------------

def framework_name() -> str:
    """Framework display name, e.g. ``"Metaflow"``."""
    return str(get("framework.display_name"))


def framework_slug() -> str:
    """Lowercase slug used in deployed object names, e.g. ``"metaflow"``."""
    return str(get("framework.slug"))


def vendor_name() -> str:
    """Vendor / company display name, e.g. ``"NRM Analytix"``."""
    return str(get("vendor.display_name"))


def app_title() -> str:
    """Onboarding App title, e.g. ``"Metaflow Onboarding"``."""
    return str(get("app.title"))


def app_deployed_name() -> str:
    """Databricks App resource name, e.g. ``"metaflow-onboarding"``."""
    return str(get("app.deployed_name"))


def docs_site_name() -> str:
    """MkDocs ``site_name``, e.g. ``"Metaflow"``."""
    return str(get("docs.site_name"))


def logo_assets() -> Dict[str, str]:
    """Published logo filenames keyed by theme, e.g.
    ``{"light": "logo-light.png", "dark": "logo-dark.png"}``."""
    return {
        "light": str(get("logo.light.published")),
        "dark": str(get("logo.dark.published")),
    }


if __name__ == "__main__":  # pragma: no cover - manual smoke check
    print(f"config       : {BRANDING_PATH}")
    print(f"framework    : {framework_name()} ({framework_slug()})")
    print(f"vendor       : {vendor_name()}")
    print(f"app title    : {app_title()}")
    print(f"app deployed : {app_deployed_name()}")
    print(f"docs site    : {docs_site_name()}")
    print(f"env prefix   : {env_prefix()}")
    print(f"env_var      : env_var('SPEC_CATALOG') -> {env_var('SPEC_CATALOG')}")
    print(f"logos        : {logo_assets()}")
