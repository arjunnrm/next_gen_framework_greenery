#!/usr/bin/env python3
"""Regenerate every DERIVED branding artifact from ``branding/branding.json``.

This script only ever *writes generated files*. It never rewrites hand-maintained
source with a regex -- that is the pattern this repo has been burned by twice
(a blanket ``sed`` mangled Python identifiers; a DOTALL ``re.sub`` swallowed ~650
lines of Builder.jsx). Hand-maintained files read the branding config at runtime
instead; see branding/README.md for the split.

What it generates
-----------------
1. ``databricks-app/web/src/branding.js``
   The frontend accessor. Vite's dev server refuses to serve files outside its
   root (databricks-app/web/), so the frontend cannot import branding/branding.json
   across that boundary -- it must be generated INTO web/src/. See README.

2. ``databricks-app/server/branding_generated.py``
   The app-side copy of the config. The app resource sets
   ``source_code_path: "../../databricks-app"``, so a top-level branding/ directory
   is never uploaded to Databricks Apps. This generated module has no imports
   outside the stdlib and embeds the values directly, so it works at runtime.

3. ``databricks-app/web/public/logo-light.png`` and ``logo-dark.png``
   Cropped to the wordmark bounding box with the flat background turned
   transparent, from the sources named in branding.json.

4. ``docs/assets/logo.png``
   The MkDocs theme logo (the dark-background variant reads correctly on
   Material's coloured header).

Usage
-----
    python scripts/apply_branding.py            # write
    python scripts/apply_branding.py --check    # verify, exit 1 if stale (CI)

Stdlib only: the PNG crop/alpha pass is implemented here with ``zlib`` + ``struct``
because Pillow is not a dependency of this repo.
"""

from __future__ import annotations

import argparse
import json
import struct
import sys
import zlib
from pathlib import Path
from typing import Dict, List, Optional, Tuple

REPO = Path(__file__).resolve().parent.parent
BRANDING_JSON = REPO / "branding" / "branding.json"

WEB_BRANDING_JS = REPO / "databricks-app" / "web" / "src" / "branding.js"
SERVER_BRANDING_PY = REPO / "databricks-app" / "server" / "branding_generated.py"
WEB_PUBLIC = REPO / "databricks-app" / "web" / "public"
DOCS_ASSETS = REPO / "docs" / "assets"

BANNER_JS = "// GENERATED FILE - do not edit. Produced by scripts/apply_branding.py from branding/branding.json."
BANNER_PY = "GENERATED FILE - do not edit. Produced by scripts/apply_branding.py from branding/branding.json."


# --------------------------------------------------------------------------
# minimal stdlib PNG reader / writer
# --------------------------------------------------------------------------

class Png:
    """A decoded, non-interlaced PNG as flat RGBA rows."""

    def __init__(self, width: int, height: int, rgba: bytearray) -> None:
        self.width = width
        self.height = height
        self.rgba = rgba  # width*height*4 bytes

    def pixel(self, x: int, y: int) -> Tuple[int, int, int, int]:
        o = (y * self.width + x) * 4
        return self.rgba[o], self.rgba[o + 1], self.rgba[o + 2], self.rgba[o + 3]


def _unfilter(raw: bytes, height: int, bpp: int, stride: int) -> bytes:
    out = bytearray()
    prev = bytearray(stride)
    p = 0
    for _ in range(height):
        ft = raw[p]
        p += 1
        line = bytearray(raw[p:p + stride])
        p += stride
        if ft == 1:
            for i in range(bpp, stride):
                line[i] = (line[i] + line[i - bpp]) & 255
        elif ft == 2:
            for i in range(stride):
                line[i] = (line[i] + prev[i]) & 255
        elif ft == 3:
            for i in range(stride):
                a = line[i - bpp] if i >= bpp else 0
                line[i] = (line[i] + ((a + prev[i]) >> 1)) & 255
        elif ft == 4:
            for i in range(stride):
                a = line[i - bpp] if i >= bpp else 0
                b = prev[i]
                c = prev[i - bpp] if i >= bpp else 0
                pa, pb, pc = abs(b - c), abs(a - c), abs(a + b - 2 * c)
                pr = a if (pa <= pb and pa <= pc) else (b if pb <= pc else c)
                line[i] = (line[i] + pr) & 255
        elif ft != 0:
            raise ValueError("unsupported PNG filter type " + str(ft))
        out += line
        prev = line
    return bytes(out)


def read_png(path: Path) -> Png:
    """Decode a non-interlaced 8-bit PNG (greyscale/RGB/palette/alpha) to RGBA."""
    data = path.read_bytes()
    if data[:8] != b"\x89PNG\r\n\x1a\n":
        raise ValueError(str(path) + " is not a PNG")

    pos, idat = 8, bytearray()
    ihdr = plte = trns = None
    while pos < len(data):
        (length,) = struct.unpack(">I", data[pos:pos + 4])
        ctype = data[pos + 4:pos + 8]
        chunk = data[pos + 8:pos + 8 + length]
        if ctype == b"IHDR":
            ihdr = struct.unpack(">IIBBBBB", chunk)
        elif ctype == b"IDAT":
            idat += chunk
        elif ctype == b"PLTE":
            plte = chunk
        elif ctype == b"tRNS":
            trns = chunk
        elif ctype == b"IEND":
            break
        pos += 12 + length

    if ihdr is None:
        raise ValueError(str(path) + " has no IHDR")
    width, height, depth, colour, _comp, _filt, interlace = ihdr
    if depth != 8:
        raise ValueError(str(path) + ": only 8-bit PNGs are supported (got " + str(depth) + ")")
    if interlace:
        raise ValueError(str(path) + ": interlaced PNGs are not supported")

    channels = {0: 1, 2: 3, 3: 1, 4: 2, 6: 4}[colour]
    stride = width * channels
    px = _unfilter(zlib.decompress(bytes(idat)), height, channels, stride)

    rgba = bytearray(width * height * 4)
    for y in range(height):
        row = y * stride
        dst = y * width * 4
        for x in range(width):
            s = row + x * channels
            d = dst + x * 4
            if colour == 3:
                i = px[s]
                rgba[d] = plte[i * 3]
                rgba[d + 1] = plte[i * 3 + 1]
                rgba[d + 2] = plte[i * 3 + 2]
                rgba[d + 3] = trns[i] if trns and i < len(trns) else 255
            elif colour == 2:
                rgba[d:d + 3] = px[s:s + 3]
                rgba[d + 3] = 255
            elif colour == 6:
                rgba[d:d + 4] = px[s:s + 4]
            elif colour == 0:
                v = px[s]
                rgba[d] = rgba[d + 1] = rgba[d + 2] = v
                rgba[d + 3] = 255
            else:  # colour == 4, greyscale + alpha
                v = px[s]
                rgba[d] = rgba[d + 1] = rgba[d + 2] = v
                rgba[d + 3] = px[s + 1]
    return Png(width, height, rgba)


def encode_png(img: Png) -> bytes:
    """Encode RGBA as a deterministic 8-bit PNG (filter 0, fixed zlib level).

    Determinism matters: ``--check`` compares the encoded bytes against what is
    on disk, so the same input must always produce byte-identical output.
    """
    raw = bytearray()
    for y in range(img.height):
        raw.append(0)
        o = y * img.width * 4
        raw += img.rgba[o:o + img.width * 4]

    def chunk(tag: bytes, payload: bytes) -> bytes:
        return (
            struct.pack(">I", len(payload))
            + tag
            + payload
            + struct.pack(">I", zlib.crc32(tag + payload) & 0xFFFFFFFF)
        )

    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", img.width, img.height, 8, 6, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(bytes(raw), 9))
        + chunk(b"IEND", b"")
    )


def crop_and_alpha(
    img: Png,
    background: List[int],
    tolerance: int,
    do_crop: bool,
    do_alpha: bool,
) -> Png:
    """Crop to the non-background bounding box and make the flat background transparent.

    The supplied logos are 16:9 with heavy padding and an opaque baked-in
    background; at the header's 22px height the untouched asset renders as a tiny
    glyph inside a solid box. Cropping to the wordmark and dropping the background
    is what makes them usable.
    """
    bg = tuple(background[:3])
    w, h = img.width, img.height

    if do_crop:
        minx, miny, maxx, maxy = w, h, -1, -1
        for y in range(h):
            for x in range(w):
                r, g, b, a = img.pixel(x, y)
                if a == 0:
                    continue
                if abs(r - bg[0]) + abs(g - bg[1]) + abs(b - bg[2]) > tolerance:
                    if x < minx:
                        minx = x
                    if x > maxx:
                        maxx = x
                    if y < miny:
                        miny = y
                    if y > maxy:
                        maxy = y
        if maxx < 0:
            raise ValueError("logo appears to be entirely background - check logo.<variant>.background")
    else:
        minx, miny, maxx, maxy = 0, 0, w - 1, h - 1

    nw, nh = maxx - minx + 1, maxy - miny + 1
    out = bytearray(nw * nh * 4)
    feather = tolerance * 3
    for y in range(nh):
        for x in range(nw):
            r, g, b, a = img.pixel(minx + x, miny + y)
            if do_alpha:
                dist = abs(r - bg[0]) + abs(g - bg[1]) + abs(b - bg[2])
                if dist <= tolerance:
                    a = 0
                elif feather > 0 and dist < tolerance + feather:
                    # feather the anti-aliased rim so edges do not look cut out
                    a = min(a, int(255 * (dist - tolerance) / feather))
            d = (y * nw + x) * 4
            out[d] = r
            out[d + 1] = g
            out[d + 2] = b
            out[d + 3] = a
    return Png(nw, nh, out)


# --------------------------------------------------------------------------
# generators
# --------------------------------------------------------------------------

def render_branding_js(cfg: dict) -> str:
    payload = {
        "frameworkName": cfg["framework"]["display_name"],
        "frameworkSlug": cfg["framework"]["slug"],
        "vendorName": cfg["vendor"]["display_name"],
        "appTitle": cfg["app"]["title"],
        "builderSubtitle": cfg["framework"]["builder_subtitle"],
        "tagline": cfg["framework"]["tagline"],
        "logoAlt": cfg["logo"]["alt_text"],
        "logoLight": "/" + cfg["logo"]["light"]["published"],
        "logoDark": "/" + cfg["logo"]["dark"]["published"],
    }
    lines = [
        BANNER_JS,
        "// Vite serves only files under databricks-app/web/, so the frontend cannot import",
        "// branding/branding.json across that boundary. This module is generated into web/src/ instead.",
        "",
        "export const branding = " + json.dumps(payload, indent=2, ensure_ascii=False) + ";",
        "",
        "export default branding;",
        "",
    ]
    return "\n".join(lines)


def _py_literal(value: object, indent: int = 0) -> str:
    """Render a JSON-derived value as a *Python* literal.

    ``json.dumps`` would emit ``true``/``false``/``null``, which are not Python
    names -- embedding that in a .py file raises NameError at import. ``pprint``
    reflows unpredictably, which would break ``--check`` determinism, so this
    walks the structure and emits stable, deterministic Python source.
    """
    pad = " " * indent
    inner = " " * (indent + 4)
    if isinstance(value, dict):
        if not value:
            return "{}"
        lines = ["{"]
        for key in sorted(value):
            lines.append(inner + repr(str(key)) + ": " + _py_literal(value[key], indent + 4) + ",")
        lines.append(pad + "}")
        return "\n".join(lines)
    if isinstance(value, list):
        if not value:
            return "[]"
        lines = ["["]
        for item in value:
            lines.append(inner + _py_literal(item, indent + 4) + ",")
        lines.append(pad + "]")
        return "\n".join(lines)
    # bool must precede int: bool is a subclass of int
    if isinstance(value, bool) or value is None or isinstance(value, (int, float, str)):
        return repr(value)
    raise TypeError("cannot render " + type(value).__name__ + " as a Python literal")


def render_branding_py(cfg: dict) -> str:
    prefix = str(cfg["env"]["prefix"]).strip().strip("_").upper()
    body = _py_literal(cfg)
    parts = [
        '"""' + BANNER_PY,
        "",
        "The Databricks App uploads only ``databricks-app/`` (the app resource sets",
        '``source_code_path: \"../../databricks-app\"``), so the repo-root ``branding/``',
        "package does not exist at runtime. This generated module embeds the same values",
        "and has no imports outside the stdlib.",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        "from typing import Any, Dict",
        "",
        "BRANDING: Dict[str, Any] = " + body,
        "",
        'ENV_PREFIX = "' + prefix + '"',
        "",
        'FRAMEWORK_NAME = BRANDING["framework"]["display_name"]',
        'FRAMEWORK_SLUG = BRANDING["framework"]["slug"]',
        'VENDOR_NAME = BRANDING["vendor"]["display_name"]',
        'APP_TITLE = BRANDING["app"]["title"]',
        'APP_DEPLOYED_NAME = BRANDING["app"]["deployed_name"]',
        'APP_RUNNING_MESSAGE = BRANDING["app"]["running_message"]',
        'DOCS_SITE_NAME = BRANDING["docs"]["site_name"]',
        'DOCS_EXTERNAL_BASE_URL = BRANDING["docs"]["external_base_url"]',
        "",
        "",
        "def get(dotted: str, default: Any = None) -> Any:",
        '    """Look up a dotted path, e.g. ``get("app.title")``."""',
        "    node: Any = BRANDING",
        '    for part in dotted.split("."):',
        "        if not isinstance(node, dict) or part not in node:",
        "            return default",
        "        node = node[part]",
        "    return node",
        "",
        "",
        "def env_var(name: str) -> str:",
        '    """Build a prefixed environment variable name.',
        "",
        '    ``env_var("SPEC_CATALOG") == "' + prefix + '_SPEC_CATALOG"``. Already-prefixed',
        "    names are returned unchanged, so this is safe to apply twice.",
        '    """',
        '    clean = str(name).strip().strip("_").upper()',
        "    if not clean:",
        '        raise ValueError("env_var() needs a non-empty name")',
        '    if clean == ENV_PREFIX or clean.startswith(ENV_PREFIX + "_"):',
        "        return clean",
        '    return ENV_PREFIX + "_" + clean',
        "",
    ]
    return "\n".join(parts)


# --------------------------------------------------------------------------
# driver
# --------------------------------------------------------------------------

class Runner:
    def __init__(self, check: bool) -> None:
        self.check = check
        self.written: List[str] = []
        self.unchanged: List[str] = []
        self.stale: List[str] = []

    def rel(self, path: Path) -> str:
        try:
            return path.relative_to(REPO).as_posix()
        except ValueError:
            return str(path)

    def emit_text(self, path: Path, content: str) -> None:
        current = None
        if path.exists():
            current = path.read_text(encoding="utf-8")
        if current == content:
            self.unchanged.append(self.rel(path))
            return
        if self.check:
            self.stale.append(self.rel(path))
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(content)
        self.written.append(self.rel(path))

    def emit_bytes(self, path: Path, content: bytes) -> None:
        current = path.read_bytes() if path.exists() else None
        if current == content:
            self.unchanged.append(self.rel(path))
            return
        if self.check:
            self.stale.append(self.rel(path))
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        self.written.append(self.rel(path))


def build_logo_bytes(cfg: dict, variant: str) -> bytes:
    """Crop + alpha one logo variant and return the encoded PNG bytes."""
    spec = cfg["logo"][variant]
    src = REPO / cfg["logo"]["source_dir"] / spec["source"]
    if not src.exists():
        raise FileNotFoundError(
            "logo source " + str(src) + " not found. branding.json names it as logo."
            + variant + ".source; the filename contains a space and an uppercase "
            ".PNG extension - keep it verbatim."
        )
    img = read_png(src)
    out = crop_and_alpha(
        img,
        spec.get("background", [255, 255, 255]),
        int(cfg["logo"].get("alpha_tolerance", 24)),
        bool(cfg["logo"].get("crop_to_content", True)),
        bool(cfg["logo"].get("background_to_alpha", True)),
    )
    return encode_png(out)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="Regenerate derived branding artifacts from branding/branding.json"
    )
    ap.add_argument(
        "--check",
        action="store_true",
        help="verify only; exit 1 if any derived file is stale",
    )
    args = ap.parse_args(argv)

    if not BRANDING_JSON.exists():
        print("ERROR: " + str(BRANDING_JSON) + " not found", file=sys.stderr)
        return 2
    cfg = json.loads(BRANDING_JSON.read_text(encoding="utf-8"))

    run = Runner(args.check)

    # 1 + 2: generated accessors
    run.emit_text(WEB_BRANDING_JS, render_branding_js(cfg))
    run.emit_text(SERVER_BRANDING_PY, render_branding_py(cfg))

    # 3: web logo assets (light + dark), cropped with a transparent background
    light = build_logo_bytes(cfg, "light")
    dark = build_logo_bytes(cfg, "dark")
    run.emit_bytes(WEB_PUBLIC / cfg["logo"]["light"]["published"], light)
    run.emit_bytes(WEB_PUBLIC / cfg["logo"]["dark"]["published"], dark)

    # 4: docs logo for MkDocs (theme.logo: assets/logo.png, relative to docs_dir).
    # Material renders it on a coloured header, so the dark-background variant is right.
    run.emit_bytes(DOCS_ASSETS / "logo.png", dark)

    label = "CHECK" if args.check else "APPLY"
    print("apply_branding [" + label + "]  config: " + run.rel(BRANDING_JSON))
    print(
        "  brand: " + cfg["framework"]["display_name"]
        + "  vendor: " + cfg["vendor"]["display_name"]
        + "  app: " + cfg["app"]["deployed_name"]
        + "  env prefix: " + cfg["env"]["prefix"] + "_"
    )
    for name in run.written:
        print("  wrote     " + name)
    for name in run.unchanged:
        print("  unchanged " + name)
    for name in run.stale:
        print("  STALE     " + name)

    if args.check and run.stale:
        print(
            "\n" + str(len(run.stale)) + " derived file(s) are stale. "
            "Run: python scripts/apply_branding.py",
            file=sys.stderr,
        )
        return 1

    if args.check:
        print("\nAll derived branding artifacts are current.")
    else:
        print(
            "\n" + str(len(run.written)) + " written, "
            + str(len(run.unchanged)) + " already current."
        )
        print(
            "Next: npm run build in databricks-app/web "
            "(dist/ is git-tracked and is what the server serves)."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
