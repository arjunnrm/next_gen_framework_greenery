# Databricks notebook source
# MAGIC %md
# MAGIC # UC3 / UC6 / UC7 -- Environment Setup and File Staging
# MAGIC
# MAGIC One notebook that takes a workspace folder full of manually uploaded source files and
# MAGIC turns it into a fully provisioned, correctly populated landing estate for the three
# MAGIC use cases. It does four things, in order:
# MAGIC
# MAGIC | Stage | What it does |
# MAGIC |---|---|
# MAGIC | **Provisioning** | `CREATE SCHEMA IF NOT EXISTS` / `CREATE VOLUME IF NOT EXISTS` for every schema and volume the three use cases need, then `dbutils.fs.mkdirs` for the directory tree inside each volume. |
# MAGIC | **Secrets** | Registers the placeholder secret keys the framework expects, showing **both** the Unity Catalog three-level pattern and the classic workspace-scope fallback. |
# MAGIC | **Staging** | Walks the workspace upload folder recursively, unpacks `.zip` containers, classifies every file against a data-driven rule table and copies it to its destination volume folder. |
# MAGIC | **Validation** | Asserts every expected folder exists, reports per-use-case file counts, and displays the complete action log. |
# MAGIC
# MAGIC ## Deliberately NOT a Databricks Asset Bundle
# MAGIC
# MAGIC The execution team uploads source files by hand into **one** Databricks Workspace folder
# MAGIC (`workspace_staging_path`). Nothing here is deployed by `bundle deploy`; the notebook is
# MAGIC imported and run directly. This is why the landing volumes are also **not** declared as
# MAGIC bundle resources -- `bundle destroy` must never be able to reach source data.
# MAGIC
# MAGIC ## Idempotency
# MAGIC
# MAGIC Safe to re-run end to end. Every DDL is `IF NOT EXISTS`, every `mkdirs` is a no-op on an
# MAGIC existing path, secret-scope creation catches `RESOURCE_ALREADY_EXISTS`, and file copies
# MAGIC overwrite by default with a size verification after each one.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites and permissions
# MAGIC
# MAGIC Confirm every line below **before** running. The notebook fails loudly rather than
# MAGIC half-provisioning, but a missing grant wastes a run.
# MAGIC
# MAGIC ### Unity Catalog privileges on the target catalog (default `flowx`)
# MAGIC
# MAGIC | Privilege | Needed for |
# MAGIC |---|---|
# MAGIC | `USE CATALOG` on `<target_catalog>` | Everything. |
# MAGIC | `CREATE SCHEMA` on `<target_catalog>` | Creating `silver` / `gold` / `landing` / `observability` / `config` if absent. |
# MAGIC | `USE SCHEMA` + `CREATE VOLUME` on each schema | Creating `uc_3`, `uc_6`, `uc_7`, `app_logs`, `wheels`. |
# MAGIC | `READ VOLUME` + `WRITE VOLUME` on each landing volume | Staging files into it. |
# MAGIC
# MAGIC ### Secrets
# MAGIC
# MAGIC * **UC secrets path** -- `CREATE SECRET` on the `<catalog>.config` schema, plus a metastore
# MAGIC   with Unity Catalog secrets enabled (DBR 17.3 LTS+ or serverless environment version 4+).
# MAGIC * **Classic-scope fallback** -- workspace admin, or `CAN_MANAGE` on the target scope. The
# MAGIC   framework falls back to a classic scope only after the UC lookup has already failed; see
# MAGIC   `crypto/secrets.py::_fallback_scope_names`.
# MAGIC * Actual secret **values** are never set by this notebook and never printed. It registers
# MAGIC   placeholders only; an operator supplies the real passphrase out of band.
# MAGIC
# MAGIC ### External volumes (optional)
# MAGIC
# MAGIC If you override a volume to `EXTERNAL`, an external location must already exist covering
# MAGIC the target URL, and you need `CREATE EXTERNAL VOLUME` on the schema plus `READ FILES` /
# MAGIC `WRITE FILES` on that external location. Managed is the default and is what UC3, UC6 and
# MAGIC UC7 were all built against.
# MAGIC
# MAGIC ### Files
# MAGIC
# MAGIC The source files must **already be uploaded** to `workspace_staging_path` before this runs.
# MAGIC Subfolders are fine and are used as a routing hint. Nothing is downloaded from anywhere.
# MAGIC
# MAGIC ### Compute
# MAGIC
# MAGIC Serverless or a Unity Catalog-enabled cluster on DBR 14.3 LTS or later. The workspace-files
# MAGIC FUSE mount (`/Workspace/...` readable by `os`/`shutil`) is required for the primary staging
# MAGIC path; a Workspace Export API fallback is included for runtimes where it is unavailable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters (widgets)
# MAGIC
# MAGIC **Two required inputs only.** Schema and volume names are *not* widgets: they are derived
# MAGIC from a single hardcoded topology dictionary in the next cell, because they are contractual
# MAGIC (`docs/UC3/BUILD_CONTRACT.md` section 1, `docs/UC6/BUILD_CONTRACT.md` section 1,
# MAGIC `docs/UC7/UC7_MASTER_DOCUMENT.md` section 3.3). Making them typeable would let a typo
# MAGIC silently create a parallel, empty estate that no onboarding spec points at.
# MAGIC
# MAGIC Four convenience widgets are added, and each earns its place:
# MAGIC
# MAGIC * `dry_run` -- plan the whole run and log every intended action without creating or
# MAGIC   copying anything. This is how you review the routing decisions before the first real run,
# MAGIC   and how you re-check them after adding a new file naming pattern.
# MAGIC * `use_cases` -- a multiselect. The three use cases are onboarded by different teams on
# MAGIC   different days; re-running everything to add one UC6 file is wasteful, and restricting
# MAGIC   the blast radius is worth one widget.
# MAGIC * `move_after_copy` -- copy is the default; move (delete the workspace original) is opt-in.
# MAGIC * `external_volume_locations` -- optional JSON overriding chosen volumes to `EXTERNAL`.
# MAGIC   Empty by default, so every volume is `MANAGED`.

# COMMAND ----------

dbutils.widgets.text("target_catalog", "flowx", "1. Target Unity Catalog")
dbutils.widgets.text("workspace_staging_path", "/Workspace/Shared/flowx_upload", "2. Workspace upload folder")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "3. Dry run (plan only)")
dbutils.widgets.multiselect("use_cases", "UC3,UC6,UC7", ["UC3", "UC6", "UC7"], "4. Use cases to set up")
dbutils.widgets.dropdown("move_after_copy", "false", ["true", "false"], "5. Move (delete source) after copy")
dbutils.widgets.text("external_volume_locations", "{}", "6. External volume overrides (JSON)")

# COMMAND ----------

import json
import logging
import os
import re
import shutil
import zipfile
from dataclasses import dataclass
from datetime import date
from typing import Callable, Dict, Iterator, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("setup_uc3_uc6_uc7_environment")

CATALOG: str = dbutils.widgets.get("target_catalog").strip()
WORKSPACE_STAGING_PATH: str = dbutils.widgets.get("workspace_staging_path").strip().rstrip("/")
DRY_RUN: bool = dbutils.widgets.get("dry_run").strip().lower() == "true"
MOVE_AFTER_COPY: bool = dbutils.widgets.get("move_after_copy").strip().lower() == "true"
SELECTED_USE_CASES: List[str] = [
    part.strip().upper() for part in dbutils.widgets.get("use_cases").split(",") if part.strip()
]

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

if not CATALOG:
    raise ValueError("The 'target_catalog' widget must be set to a valid Unity Catalog name.")
if not _IDENTIFIER_PATTERN.match(CATALOG):
    # Same strictness as crypto/secrets.py::assert_safe_identifier. The catalog name is
    # interpolated into DDL, so it is validated before it ever reaches spark.sql().
    raise ValueError(f"'target_catalog' is not a safe SQL identifier: {CATALOG!r}")
if not WORKSPACE_STAGING_PATH:
    raise ValueError("The 'workspace_staging_path' widget must point at the workspace upload folder.")
if not SELECTED_USE_CASES:
    raise ValueError("At least one use case must be selected in the 'use_cases' widget.")

_unknown_use_cases = sorted(set(SELECTED_USE_CASES) - {"UC3", "UC6", "UC7"})
if _unknown_use_cases:
    raise ValueError(f"Unrecognised use case(s) in the 'use_cases' widget: {_unknown_use_cases}")

try:
    EXTERNAL_VOLUME_LOCATIONS: Dict[str, str] = json.loads(
        dbutils.widgets.get("external_volume_locations").strip() or "{}"
    )
except json.JSONDecodeError as exc:
    raise ValueError(
        "The 'external_volume_locations' widget must contain a JSON object mapping "
        "'<schema>.<volume>' to a storage URL, for example "
        "{\"landing.uc_7\": \"abfss://landing@acct.dfs.core.windows.net/uc7\"}. "
        f"Parse error: {exc}"
    ) from exc

if not isinstance(EXTERNAL_VOLUME_LOCATIONS, dict):
    raise ValueError("'external_volume_locations' must be a JSON object, not a list or scalar.")

logger.info(
    "Catalog=%s | upload folder=%s | use cases=%s | dry_run=%s | move_after_copy=%s | external overrides=%d",
    CATALOG,
    WORKSPACE_STAGING_PATH,
    ",".join(SELECTED_USE_CASES),
    DRY_RUN,
    MOVE_AFTER_COPY,
    len(EXTERNAL_VOLUME_LOCATIONS),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Topology -- the single source of truth
# MAGIC
# MAGIC Every schema, volume and directory for the three use cases, in one dictionary. Nothing
# MAGIC below this cell hardcodes a schema or volume name; everything derives from `TOPOLOGY`.
# MAGIC The catalog is always the `CATALOG` parameter, never a literal.
# MAGIC
# MAGIC ### Where each use case lands, and why
# MAGIC
# MAGIC | Use case | Schemas | Landing volume | Notes |
# MAGIC |---|---|---|---|
# MAGIC | **UC3** | `staging`, `bronze` | `staging.uc_3` | Both schemas already exist and are **reused** -- UC3 distinguishes itself by table name and by the `uc_3` volume, not by dedicated schemas (build contract, revised 2026-09-05). |
# MAGIC | **UC6** | `bronze`, `silver`, `gold` | `staging.uc_6` | `silver` and `gold` are created here if absent. |
# MAGIC | **UC7** | `bronze` | `landing.uc_7` | UC7 has its own `landing` schema; the volume is MANAGED and deliberately outside the bundle. |
# MAGIC | **Shared** | `observability`, `config` | `observability.app_logs`, `config.wheels` | Run telemetry and the framework wheel. |
# MAGIC
# MAGIC ### UC3 volume layout
# MAGIC
# MAGIC ```
# MAGIC /Volumes/<catalog>/staging/uc_3/streaming/<table>/<table>_stream.csv
# MAGIC /Volumes/<catalog>/staging/uc_3/batch/<table>/batch_date=YYYY-MM-DD/<table>_batch.csv
# MAGIC ```
# MAGIC
# MAGIC The `batch/<table>/batch_date=*/` folders must exist before a file is copied into them:
# MAGIC a UC volume copy does **not** create intermediate directories, it fails with
# MAGIC `no such directory` (UC3 build contract, section on the volume directory tree).
# MAGIC
# MAGIC ### UC6 volume layout
# MAGIC
# MAGIC ```
# MAGIC /Volumes/<catalog>/staging/uc_6/raw/         <- all six source files land here, per cycle
# MAGIC /Volumes/<catalog>/staging/uc_6/archive/     <- originals moved here after a successful ingest
# MAGIC /Volumes/<catalog>/staging/uc_6/output/      <- the four generated output files
# MAGIC /Volumes/<catalog>/staging/uc_6/_schemas/    <- Auto Loader schema locations
# MAGIC /Volumes/<catalog>/staging/uc_6/_extracted/  <- decrypted/decompressed staging for the EA file
# MAGIC ```
# MAGIC
# MAGIC The `_schemas/` and `_extracted/` per-source subfolders match the `schema_location` and
# MAGIC `path` values in `onboarding/uc6/uc6_ea_flood_warning.json` exactly. They are created here
# MAGIC so the first pipeline update does not race to create them.
# MAGIC
# MAGIC ### UC7 volume layout
# MAGIC
# MAGIC ```
# MAGIC /Volumes/<catalog>/landing/uc_7/raw/<EMSC|PSGW|SGSN|TAP>/
# MAGIC /Volumes/<catalog>/landing/uc_7/asn_schema/       <- the .asn1 module files
# MAGIC /Volumes/<catalog>/landing/uc_7/_schemas/<table>/ <- Auto Loader schema checkpoints
# MAGIC /Volumes/<catalog>/landing/uc_7/output_sample/
# MAGIC /Volumes/<catalog>/landing/uc_7/archive/
# MAGIC ```

# COMMAND ----------

# Framework wheel version. Used only to build the wheels/<version>/ directory; it does not
# install anything. Keep in step with pyproject.toml's [project] version.
FRAMEWORK_WHEEL_VERSION = "0.0.3"

# UC3 tables, in the order the build contract declares them.
UC3_TABLES = ["physical_device", "customer", "subscriber"]

# The four UC7 network elements actually onboarded. SMSC and MMSC are deliberately excluded --
# their payloads are CSV text, not ASN.1, and there is no SMSC.asn1 module at all.
UC7_ELEMENTS = ["EMSC", "PSGW", "SGSN", "TAP"]

# UC7 bronze table names, used for the Auto Loader _schemas/<table>/ checkpoint folders.
UC7_SCHEMA_CHECKPOINT_TABLES = ["emsc_cdr_raw", "psgw_cdr_raw", "sgsn_cdr_raw", "tap310_raw"]

# UC6 logical sources. Each gets its own _schemas/ and _extracted/ subfolder, matching the
# spec's schema_location and source_config.path values verbatim.
UC6_SOURCES = [
    "ea_request",
    "css_account",
    "css_account_address",
    "css_subscription",
    "jt_customer",
    "excalibur_address",
]

# Default batch_date partition for UC3 batch files whose name carries no date. Overridden
# per file whenever a YYYYMMDD or YYYY-MM-DD token is present in the filename.
DEFAULT_BATCH_DATE = date.today().isoformat()


def _uc3_directories() -> List[str]:
    """Streaming and batch directory tree for UC3, relative to the uc_3 volume root."""
    directories = ["streaming", "batch"]
    for table in UC3_TABLES:
        directories.append(f"streaming/{table}")
        directories.append(f"batch/{table}")
        directories.append(f"batch/{table}/batch_date={DEFAULT_BATCH_DATE}")
    return directories


def _uc6_directories() -> List[str]:
    directories = ["raw", "archive", "output", "_schemas", "_extracted"]
    for source in UC6_SOURCES:
        directories.append(f"_schemas/{source}")
        directories.append(f"_extracted/{source}")
    return directories


def _uc7_directories() -> List[str]:
    directories = ["raw", "asn_schema", "_schemas", "output_sample", "archive"]
    directories.extend(f"raw/{element}" for element in UC7_ELEMENTS)
    directories.extend(f"_schemas/{table}" for table in UC7_SCHEMA_CHECKPOINT_TABLES)
    return directories


# TOPOLOGY[use_case] = {
#   "schemas":  schemas that must exist (created IF NOT EXISTS),
#   "volumes":  [(schema, volume, [subdirectories relative to the volume root]), ...],
# }
# "SHARED" is always provisioned regardless of the use_cases widget: observability and config
# are framework-level, and every one of the three use cases writes telemetry to app_logs.
TOPOLOGY: Dict[str, Dict[str, object]] = {
    "SHARED": {
        "schemas": ["config", "observability"],
        "volumes": [
            ("observability", "app_logs", ["streaming_cdc", "batch_recon"]),
            ("config", "wheels", [FRAMEWORK_WHEEL_VERSION]),
        ],
    },
    "UC3": {
        # staging and bronze already exist on metaflow_v7 and are reused, not replaced.
        # CREATE SCHEMA IF NOT EXISTS is a no-op against them and keeps a fresh workspace working.
        "schemas": ["staging", "bronze"],
        "volumes": [("staging", "uc_3", _uc3_directories())],
    },
    "UC6": {
        "schemas": ["bronze", "silver", "gold", "staging"],
        "volumes": [("staging", "uc_6", _uc6_directories())],
    },
    "UC7": {
        "schemas": ["bronze", "landing"],
        "volumes": [("landing", "uc_7", _uc7_directories())],
    },
}

# Provision SHARED first so config/ exists before the secrets stage touches it.
ACTIVE_USE_CASES: List[str] = ["SHARED"] + [uc for uc in ("UC3", "UC6", "UC7") if uc in SELECTED_USE_CASES]

logger.info("Active provisioning groups: %s", ", ".join(ACTIVE_USE_CASES))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Secret placeholders the framework expects
# MAGIC
# MAGIC Every secret in this framework is referenced as `{secret_catalog, secret_schema, secret_key}`
# MAGIC -- one shape, resolved by `crypto/secrets.py::resolve_secret_value`. That helper tries the
# MAGIC Unity Catalog three-level lookup first and, only if it fails (a metastore with UC secrets
# MAGIC switched off raises `UC_SECRETS_NOT_ENABLED`), falls back to a classic workspace scope,
# MAGIC trying these spellings in order:
# MAGIC
# MAGIC ```
# MAGIC <catalog>.<schema>   e.g. flowx.config
# MAGIC <catalog>_<schema>   e.g. flowx_config
# MAGIC <schema>             e.g. config
# MAGIC <catalog>            e.g. flowx
# MAGIC ```
# MAGIC
# MAGIC This notebook therefore creates the classic scope under the **most specific** of those
# MAGIC names, `<catalog>.<schema>`, so the fallback resolves on the first attempt.

# COMMAND ----------

@dataclass(frozen=True)
class SecretPlaceholder:
    """One secret the framework expects to be resolvable at pipeline runtime."""

    secret_schema: str
    secret_key: str
    use_case: str
    purpose: str


SECRET_PLACEHOLDERS: List[SecretPlaceholder] = [
    SecretPlaceholder(
        secret_schema="config",
        secret_key="pgpkey",
        use_case="UC6",
        purpose=(
            "GPG symmetric passphrase. Decrypts the inbound EE_*-REQUEST_*.csv.gz.gpg envelope "
            "and encrypts the two outbound telephone-list exports."
        ),
    ),
    SecretPlaceholder(
        secret_schema="security",
        secret_key="pii_encryption_key",
        use_case="SHARED",
        purpose=(
            "AES key for encrypted_columns / decrypted_columns. Not referenced by the current "
            "UC3/UC6/UC7 specs, registered so a column-encryption flow can be enabled without a "
            "second provisioning pass."
        ),
    ),
]

# Only the secret schemas actually needed by the selected use cases.
_needed_secret_schemas = sorted(
    {
        placeholder.secret_schema
        for placeholder in SECRET_PLACEHOLDERS
        if placeholder.use_case == "SHARED" or placeholder.use_case in SELECTED_USE_CASES
    }
)

logger.info("Secret schemas in scope: %s", ", ".join(_needed_secret_schemas))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Routing rules -- data-driven, ordered, first match wins
# MAGIC
# MAGIC Every rule is a regex over the file's path **relative to the upload folder** (so a
# MAGIC subfolder name such as `SGSN/` is a legitimate routing hint), plus a resolver that turns
# MAGIC the match into a destination directory. Rules are evaluated in order and the first match
# MAGIC wins, so the specific ones come before the general ones.
# MAGIC
# MAGIC ### Compression and encryption -- what is unpacked and what is not
# MAGIC
# MAGIC | Container | Action | Why |
# MAGIC |---|---|---|
# MAGIC | `.zip` | **Unpacked.** Members are re-classified individually and routed on their own names. | A `.zip` is only ever a transport container for the upload. No use case ingests a `.zip` from these landing folders. |
# MAGIC | `.dat.gz` | **Left compressed.** Copied byte for byte. | UC6 reads these through Auto Loader, which decompresses gzip natively. Decompressing here would break the `CSS_*.dat.gz` / `CM_*.dat.gz` file patterns in the spec, and every one of those patterns ends in `.dat.gz`. |
# MAGIC | `.csv.gz.gpg` | **Left compressed and encrypted.** Copied byte for byte. | The framework decrypts the GPG envelope using the `<catalog>.config.pgpkey` secret and decompresses the gzip member itself, inside the pipeline update, writing to `_extracted/ea_request/`. Decrypting here would strip the very step under test and would require the passphrase in the notebook. |
# MAGIC | `.gz` (bare, not `.dat.gz`) | **Left compressed.** | Same reasoning; no rule decompresses anything. |
# MAGIC
# MAGIC **Nothing is ever decrypted by this notebook.** The `.gpg` bytes are moved, never opened.
# MAGIC
# MAGIC ### The rules
# MAGIC
# MAGIC | Order | Matches | Destination |
# MAGIC |---|---|---|
# MAGIC | 1 | `*.asn1`, `*.asn` | `landing/uc_7/asn_schema/` |
# MAGIC | 2 | Name or parent folder starting `EMSC`/`PSGW`/`SGSN`/`TAP` | `landing/uc_7/raw/<ELEMENT>/` |
# MAGIC | 3 | `*.csv.gz.gpg`, `EE_*` | `staging/uc_6/raw/` |
# MAGIC | 4 | `CM_*`, `CSS_*`, `EE_*`, any `*.dat.gz` | `staging/uc_6/raw/` |
# MAGIC | 5 | `<table>_stream.csv` | `staging/uc_3/streaming/<table>/` |
# MAGIC | 6 | `<table>_batch.csv` | `staging/uc_3/batch/<table>/batch_date=<derived>/` |
# MAGIC | -- | anything else | `staging/_unrouted/` with a warning |
# MAGIC
# MAGIC Rule 2 is checked **before** the UC6 rules because a TAP file has no extension at all, and
# MAGIC rule 1 is first because an `.asn1` module is a schema, not a CDR, even when it sits in an
# MAGIC element-named folder.
# MAGIC
# MAGIC ### Unrouted files are quarantined, never dropped and never fatal
# MAGIC
# MAGIC A file matching no rule is copied to `/Volumes/<catalog>/staging/_unrouted/` and logged as
# MAGIC a warning. It is counted in the summary and reported by the validation cell. Failing the
# MAGIC whole run on one unrecognised file would strand the other fifty; silently skipping it would
# MAGIC be worse still, because the pipeline would then run against incomplete input and report
# MAGIC success.

# COMMAND ----------

VOLUME_ROOT = f"/Volumes/{CATALOG}"

UC3_STAGING_ROOT = f"{VOLUME_ROOT}/staging/uc_3"
UC6_STAGING_ROOT = f"{VOLUME_ROOT}/staging/uc_6"
UC7_LANDING_ROOT = f"{VOLUME_ROOT}/landing/uc_7"
UNROUTED_ROOT = f"{VOLUME_ROOT}/staging/_unrouted"

# YYYY-MM-DD or YYYYMMDD anywhere in the filename, e.g. CM_..._20250127_00000048.dat.gz
# or EE_2026-08-20-REQUEST_1OF1.csv.gz.gpg.
_DATE_TOKEN_PATTERN = re.compile(r"(?<!\d)(20\d{2})-?(\d{2})-?(\d{2})(?!\d)")

# An explicit batch_date=YYYY-MM-DD folder already present in the uploaded tree. This is what
# scripts/generate_uc3_test_data.py emits, so it is trusted ahead of any filename token.
_BATCH_DATE_FOLDER_PATTERN = re.compile(r"batch_date=(\d{4}-\d{2}-\d{2})(?:/|$)", re.IGNORECASE)


def derive_batch_date(relative_path: str) -> str:
    """Derive the ``batch_date=`` partition value for a UC3 batch file.

    Three sources, in descending order of trust:

    1. An explicit ``batch_date=YYYY-MM-DD`` folder already present in the uploaded path. The
       framework's own generator (``scripts/generate_uc3_test_data.py``) writes exactly this
       layout, so an operator who uploads the generated tree keeps its partitioning verbatim.
    2. A date token embedded in the filename, e.g. ``customer_batch_20250127.csv``.
    3. Today's date, so a file carrying no date at all still lands somewhere deterministic
       rather than failing the run. This case is warned about, because it silently invents a
       partition value and a reviewer needs to see that in the log.
    """
    explicit = _BATCH_DATE_FOLDER_PATTERN.search(relative_path.replace("\\", "/"))
    if explicit:
        return explicit.group(1)

    match = _DATE_TOKEN_PATTERN.search(os.path.basename(relative_path))
    if match:
        year, month, day = match.groups()
        try:
            return date(int(year), int(month), int(day)).isoformat()
        except ValueError:
            # A token such as 20259999 parses as digits but is not a real date.
            logger.warning(
                "'%s' contains a date-shaped token that is not a valid date; using the "
                "default batch_date '%s'.",
                relative_path,
                DEFAULT_BATCH_DATE,
            )
            return DEFAULT_BATCH_DATE

    logger.warning(
        "'%s' carries no batch_date folder and no date in its name; defaulting its partition "
        "to '%s'. Upload it under a batch_date=YYYY-MM-DD folder to control this explicitly.",
        relative_path,
        DEFAULT_BATCH_DATE,
    )
    return DEFAULT_BATCH_DATE


def _uc3_table_from(file_name: str, kind: str) -> Optional[str]:
    """Extract the UC3 table name from a ``<table>_stream.csv`` / ``<table>_batch.csv`` name.

    An optional date tail is tolerated, so ``customer_batch.csv`` and
    ``customer_batch_20250127.csv`` both resolve to ``customer``.

    Returns ``None`` when the stem is not one of the three contracted tables, which sends the
    file to the unrouted quarantine rather than inventing a folder for a typo. This is
    deliberate: ``widget_stream.csv`` is far more likely to be a mistake than a fourth table.
    """
    match = re.match(
        rf"^(?P<table>.+?)_{kind}(?:[_-]\d{{4}}-?\d{{2}}-?\d{{2}})?\.csv$",
        file_name,
        re.IGNORECASE,
    )
    if not match:
        return None
    table = match.group("table").lower()
    return table if table in UC3_TABLES else None


@dataclass(frozen=True)
class RoutingRule:
    """One entry in the ordered routing table.

    ``pattern`` is matched (case-insensitively, via ``search``) against the file's path
    relative to the upload folder, so both the filename and any parent folder can drive the
    decision. ``resolver`` receives that relative path and the bare filename and returns the
    destination directory, or ``None`` to decline the match and fall through to the next rule.
    """

    rule_id: str
    use_case: str
    pattern: re.Pattern
    resolver: Callable[[str, str], Optional[str]]
    description: str


def _resolve_uc7_asn_schema(relative_path: str, file_name: str) -> Optional[str]:
    return f"{UC7_LANDING_ROOT}/asn_schema"


def _resolve_uc7_raw(relative_path: str, file_name: str) -> Optional[str]:
    """Route a CDR file to raw/<ELEMENT>/ using the filename first, then the parent folder.

    UC7 files have no consistent extension -- ``.raw``, ``.fin`` and extensionless (TAP) all
    occur -- so the element token is the only reliable signal.
    """
    upper_name = file_name.upper()
    for element in UC7_ELEMENTS:
        if upper_name.startswith(element):
            return f"{UC7_LANDING_ROOT}/raw/{element}"
    upper_path = relative_path.upper().replace("\\", "/")
    for element in UC7_ELEMENTS:
        # A parent folder named exactly after the element, e.g. uploads/SGSN/somefile.fin.
        if f"/{element}/" in f"/{upper_path}":
            return f"{UC7_LANDING_ROOT}/raw/{element}"
    return None


def _resolve_uc6_raw(relative_path: str, file_name: str) -> Optional[str]:
    return f"{UC6_STAGING_ROOT}/raw"


def _resolve_uc3_streaming(relative_path: str, file_name: str) -> Optional[str]:
    table = _uc3_table_from(file_name, "stream")
    if table is None:
        return None
    return f"{UC3_STAGING_ROOT}/streaming/{table}"


def _resolve_uc3_batch(relative_path: str, file_name: str) -> Optional[str]:
    table = _uc3_table_from(file_name, "batch")
    if table is None:
        return None
    batch_date = derive_batch_date(relative_path)
    return f"{UC3_STAGING_ROOT}/batch/{table}/batch_date={batch_date}"


# Ordered. First match wins. Add a new rule by appending a RoutingRule -- nothing else changes.
ROUTING_RULES: List[RoutingRule] = [
    RoutingRule(
        rule_id="uc7_asn_module",
        use_case="UC7",
        pattern=re.compile(r"\.asn1?$", re.IGNORECASE),
        resolver=_resolve_uc7_asn_schema,
        description="ASN.1 schema module (.asn1/.asn) -> uc_7/asn_schema/",
    ),
    RoutingRule(
        rule_id="uc7_cdr_raw",
        use_case="UC7",
        pattern=re.compile(r"(^|/)(EMSC|PSGW|SGSN|TAP)", re.IGNORECASE),
        resolver=_resolve_uc7_raw,
        description="CDR payload named for, or filed under, a network element -> uc_7/raw/<ELEMENT>/",
    ),
    RoutingRule(
        rule_id="uc6_encrypted_request",
        use_case="UC6",
        pattern=re.compile(r"\.csv\.gz\.gpg$|(^|/)EE_", re.IGNORECASE),
        resolver=_resolve_uc6_raw,
        description="EA flood-risk request, gzip+GPG, left encrypted -> uc_6/raw/",
    ),
    RoutingRule(
        rule_id="uc6_gzip_feed",
        use_case="UC6",
        pattern=re.compile(r"(^|/)(CM_|CSS_)|\.dat\.gz$", re.IGNORECASE),
        resolver=_resolve_uc6_raw,
        description="CSS / Excalibur / JT gzip feed, left compressed -> uc_6/raw/",
    ),
    RoutingRule(
        rule_id="uc3_streaming_csv",
        use_case="UC3",
        pattern=re.compile(r"_stream(?:[_-]\d{4}-?\d{2}-?\d{2})?\.csv$", re.IGNORECASE),
        resolver=_resolve_uc3_streaming,
        description="UC3 streaming CSV -> uc_3/streaming/<table>/",
    ),
    RoutingRule(
        rule_id="uc3_batch_csv",
        use_case="UC3",
        pattern=re.compile(r"_batch(?:[_-]\d{4}-?\d{2}-?\d{2})?\.csv$", re.IGNORECASE),
        resolver=_resolve_uc3_batch,
        description="UC3 batch CSV -> uc_3/batch/<table>/batch_date=<derived>/",
    ),
]

logger.info("Loaded %d routing rules.", len(ROUTING_RULES))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Helper functions
# MAGIC
# MAGIC ### Why `dbutils.fs.ls` is not used to enumerate the upload folder
# MAGIC
# MAGIC `dbutils.fs` speaks DBFS and object-store URIs. A `/Workspace/...` path is neither: it is
# MAGIC the workspace-object tree, served to the driver through a **FUSE mount**, and its objects
# MAGIC are notebooks, directories and files rather than blobs. Pointed at a workspace path,
# MAGIC `dbutils.fs.ls` either resolves it against DBFS (returning nothing, or an unrelated DBFS
# MAGIC path of the same name), or raises. Worse, the failure mode when it "works" is a silent
# MAGIC empty listing -- the notebook would report zero files and exit successfully.
# MAGIC
# MAGIC The reliable route on modern DBR is plain Python: `os.walk` / `os.path.getsize` / `open()`
# MAGIC against the `/Workspace` FUSE mount, which is exactly how `09a_store_sample_config.py`
# MAGIC already reads bundle-synced specs in this repo. That is the primary implementation below.
# MAGIC
# MAGIC ### Fallback: the Workspace Export API
# MAGIC
# MAGIC The FUSE mount is unavailable on some restricted runtimes and on a few older DBR versions.
# MAGIC `_walk_workspace_via_api` covers that case with `WorkspaceClient.workspace.list` plus
# MAGIC `workspace.download`, which returns raw bytes for a `FILE` object. It is only consulted
# MAGIC when the mount is genuinely not readable -- it is slower and it materialises each file in
# MAGIC driver memory, so it is not the default.
# MAGIC
# MAGIC The API path treats `NOTEBOOK` objects as out of scope: a `.py`/`.sql` uploaded as a
# MAGIC notebook is a source artefact, not data, and exporting it would silently reformat it.

# COMMAND ----------

@dataclass
class ActionRecord:
    """One row of the run log. Every provisioning, secret and file action appends one."""

    stage: str
    action: str
    source_path: str
    destination_path: str
    size_bytes: int
    use_case: str
    rule_id: str
    status: str
    detail: str


ACTION_LOG: List[ActionRecord] = []


def record(
    stage: str,
    action: str,
    status: str,
    *,
    source_path: str = "",
    destination_path: str = "",
    size_bytes: int = 0,
    use_case: str = "",
    rule_id: str = "",
    detail: str = "",
) -> None:
    """Append one action to the run log and log it at the level its status implies."""
    entry = ActionRecord(
        stage=stage,
        action=action,
        source_path=source_path,
        destination_path=destination_path,
        size_bytes=int(size_bytes),
        use_case=use_case,
        rule_id=rule_id,
        status=status,
        detail=detail,
    )
    ACTION_LOG.append(entry)
    message = "[%s] %s %s -> %s (%s)%s"
    arguments = (
        stage,
        action,
        source_path or "-",
        destination_path or "-",
        status,
        f" {detail}" if detail else "",
    )
    if status in ("FAILED", "UNROUTED"):
        logger.warning(message, *arguments)
    else:
        logger.info(message, *arguments)


def execute_ddl(statement: str, description: str, *, use_case: str, stage: str = "provisioning") -> None:
    """Execute one DDL statement, tolerating only a concurrent-creation race.

    Every statement here is already ``IF NOT EXISTS``, so the only exception worth swallowing
    is a genuine race with another session creating the same object between the existence
    check and the create. Anything else -- a missing grant, a bad catalog, a malformed
    LOCATION -- is re-raised with the failing description attached.
    """
    if DRY_RUN:
        record(stage, "ddl", "SKIPPED_DRY_RUN", destination_path=description, use_case=use_case, detail=statement)
        return
    try:
        spark.sql(statement)
    except Exception as exc:  # noqa: BLE001
        text = str(exc).upper()
        if "ALREADY EXISTS" in text or "SCHEMA_ALREADY_EXISTS" in text or "RESOURCE_ALREADY_EXISTS" in text:
            record(stage, "ddl", "ALREADY_EXISTS", destination_path=description, use_case=use_case)
            logger.info("'%s' was created concurrently by another session; continuing.", description)
            return
        record(stage, "ddl", "FAILED", destination_path=description, use_case=use_case, detail=str(exc))
        raise RuntimeError(f"DDL execution failed for '{description}': {exc}") from exc
    record(stage, "ddl", "APPLIED", destination_path=description, use_case=use_case)


def make_directory(path: str, *, use_case: str) -> None:
    """Create a directory inside a UC volume. ``dbutils.fs.mkdirs`` is already idempotent."""
    if DRY_RUN:
        record("provisioning", "mkdirs", "SKIPPED_DRY_RUN", destination_path=path, use_case=use_case)
        return
    try:
        dbutils.fs.mkdirs(path)
    except Exception as exc:  # noqa: BLE001
        record("provisioning", "mkdirs", "FAILED", destination_path=path, use_case=use_case, detail=str(exc))
        raise RuntimeError(f"Could not create volume directory '{path}': {exc}") from exc
    record("provisioning", "mkdirs", "CREATED", destination_path=path, use_case=use_case)


def volume_path_exists(path: str) -> bool:
    """True when a path inside a UC volume exists.

    ``dbutils.fs.ls`` is correct here -- unlike the workspace tree, ``/Volumes/...`` is a real
    filesystem path that the dbutils layer resolves natively. A missing path raises rather
    than returning empty, so the exception is the answer, not an error.
    """
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as exc:  # noqa: BLE001
        if "not found" in str(exc).lower() or "does not exist" in str(exc).lower():
            return False
        # Anything else -- a permission failure, for instance -- is a real problem and must
        # not be reported as "the folder is missing".
        raise


def count_files_in_volume(path: str) -> int:
    """Recursive file count under a volume path. Returns 0 for a path that does not exist.

    Walks with ``dbutils.fs.ls`` rather than reading anything: no file contents are loaded and
    no DataFrame is collected, so this stays cheap even against a folder of 42 MB CDR files.
    """
    total = 0
    pending = [path]
    while pending:
        current = pending.pop()
        try:
            entries = dbutils.fs.ls(current)
        except Exception as exc:  # noqa: BLE001
            if "not found" in str(exc).lower() or "does not exist" in str(exc).lower():
                continue
            raise
        for entry in entries:
            if entry.isDir():
                pending.append(entry.path)
            else:
                total += 1
    return total

# COMMAND ----------

def _walk_workspace_via_fuse(root: str) -> Iterator[Tuple[str, str, int]]:
    """Yield ``(absolute_path, path_relative_to_root, size_bytes)`` via the FUSE mount."""
    for directory, _subdirectories, file_names in os.walk(root):
        for file_name in sorted(file_names):
            absolute = os.path.join(directory, file_name)
            relative = os.path.relpath(absolute, root).replace("\\", "/")
            try:
                size = os.path.getsize(absolute)
            except OSError as exc:
                logger.warning("Could not stat '%s' (%s); skipping it.", absolute, exc)
                record(
                    "staging",
                    "enumerate",
                    "FAILED",
                    source_path=absolute,
                    detail=f"stat failed: {exc}",
                )
                continue
            yield absolute, relative, size


def _walk_workspace_via_api(root: str) -> Iterator[Tuple[str, str, int]]:
    """Fallback enumeration through the Workspace Export API.

    Each file is downloaded to a driver-local temp copy so the rest of the pipeline can treat
    it exactly like a FUSE path. Only ``FILE`` objects are yielded: a ``NOTEBOOK`` object is
    source code, not data, and exporting one would rewrite its content.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.workspace import ObjectType

    client = WorkspaceClient()
    local_root = "/local_disk0/tmp/flowx_workspace_export"
    os.makedirs(local_root, exist_ok=True)

    pending = [root]
    while pending:
        current = pending.pop()
        for obj in client.workspace.list(current):
            if obj.object_type == ObjectType.DIRECTORY:
                pending.append(obj.path)
                continue
            if obj.object_type != ObjectType.FILE:
                logger.warning(
                    "Skipping workspace object '%s' of type %s -- only FILE objects are staged. "
                    "If this is a data file, re-upload it as a file rather than a notebook.",
                    obj.path,
                    obj.object_type,
                )
                continue
            relative = obj.path[len(root):].lstrip("/")
            local_path = os.path.join(local_root, relative.replace("/", os.sep))
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with client.workspace.download(obj.path) as stream:
                payload = stream.read()
            with open(local_path, "wb") as handle:
                handle.write(payload)
            yield local_path, relative, len(payload)


def enumerate_uploaded_files(root: str) -> List[Tuple[str, str, int]]:
    """Enumerate every uploaded file, preferring the FUSE mount and falling back to the API."""
    if os.path.isdir(root):
        files = list(_walk_workspace_via_fuse(root))
        logger.info("Enumerated %d file(s) under '%s' via the workspace FUSE mount.", len(files), root)
        return files

    logger.warning(
        "'%s' is not readable as a directory on this runtime; falling back to the Workspace "
        "Export API. This materialises each file on the driver, so it is slower.",
        root,
    )
    try:
        files = list(_walk_workspace_via_api(root))
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            f"Could not enumerate the upload folder '{root}' through either the workspace FUSE "
            f"mount or the Workspace Export API. Confirm the folder exists, that the path is "
            f"spelled with the /Workspace prefix, and that this principal can read it. "
            f"Underlying error: {exc}"
        ) from exc

    logger.info("Enumerated %d file(s) under '%s' via the Workspace Export API.", len(files), root)
    return files

# COMMAND ----------

ZIP_EXPANSION_ROOT = "/local_disk0/tmp/flowx_zip_expansion"


def expand_zip_archive(archive_path: str, relative_path: str) -> List[Tuple[str, str, int]]:
    """Expand one ``.zip`` container into driver-local files for individual routing.

    Only ``.zip`` is expanded. ``.gz``, ``.dat.gz`` and ``.csv.gz.gpg`` are transport formats
    the framework itself unwraps during ingestion, so they are copied byte for byte -- see the
    compression table in section 4.

    Zip-slip is guarded explicitly: a member whose normalised path escapes the extraction root
    is refused rather than written.
    """
    expanded: List[Tuple[str, str, int]] = []
    archive_stem = os.path.splitext(os.path.basename(archive_path))[0]
    destination_root = os.path.join(ZIP_EXPANSION_ROOT, archive_stem)
    os.makedirs(destination_root, exist_ok=True)

    try:
        with zipfile.ZipFile(archive_path) as archive:
            for member in archive.infolist():
                if member.is_dir():
                    continue
                member_name = member.filename.replace("\\", "/")
                target = os.path.normpath(os.path.join(destination_root, member_name))
                if not target.startswith(os.path.normpath(destination_root) + os.sep):
                    record(
                        "staging",
                        "unzip",
                        "FAILED",
                        source_path=f"{archive_path}!{member_name}",
                        detail="Refused: member path escapes the extraction directory (zip slip).",
                    )
                    raise RuntimeError(
                        f"Archive '{archive_path}' contains member '{member_name}' whose path "
                        "escapes the extraction directory. Refusing to extract it."
                    )
                os.makedirs(os.path.dirname(target), exist_ok=True)
                with archive.open(member) as source, open(target, "wb") as sink:
                    shutil.copyfileobj(source, sink)
                member_relative = f"{os.path.dirname(relative_path)}/{member_name}".lstrip("/")
                size = os.path.getsize(target)
                expanded.append((target, member_relative, size))
                record(
                    "staging",
                    "unzip",
                    "EXTRACTED",
                    source_path=f"{archive_path}!{member_name}",
                    destination_path=target,
                    size_bytes=size,
                )
    except zipfile.BadZipFile as exc:
        record("staging", "unzip", "FAILED", source_path=archive_path, detail=f"Not a valid zip: {exc}")
        raise RuntimeError(f"'{archive_path}' has a .zip extension but is not a valid archive: {exc}") from exc

    logger.info("Expanded '%s' into %d member file(s).", archive_path, len(expanded))
    return expanded


def classify(relative_path: str) -> Tuple[Optional[str], str, str]:
    """Return ``(destination_directory, use_case, rule_id)`` for one file.

    ``destination_directory`` is ``None`` only when no rule matched, which routes the file to
    the unrouted quarantine. A rule whose regex matches but whose resolver declines (an
    unrecognised UC3 table name, say) falls through to the next rule.
    """
    file_name = os.path.basename(relative_path)
    for rule in ROUTING_RULES:
        if rule.use_case not in SELECTED_USE_CASES:
            continue
        if not rule.pattern.search(relative_path) and not rule.pattern.search(file_name):
            continue
        destination = rule.resolver(relative_path, file_name)
        if destination is not None:
            return destination, rule.use_case, rule.rule_id
    return None, "UNROUTED", "none"


def stage_file(local_path: str, relative_path: str, size_bytes: int) -> None:
    """Route one file to its destination volume folder, copying and verifying by size."""
    file_name = os.path.basename(relative_path)
    destination_directory, use_case, rule_id = classify(relative_path)

    if destination_directory is None:
        destination_directory = UNROUTED_ROOT
        record(
            "staging",
            "route",
            "UNROUTED",
            source_path=relative_path,
            destination_path=destination_directory,
            size_bytes=size_bytes,
            use_case="UNROUTED",
            detail=(
                "No routing rule matched. Copied to the quarantine folder for review; add a "
                "RoutingRule for this pattern if it is a genuine source file."
            ),
        )

    destination_path = f"{destination_directory}/{file_name}"

    if DRY_RUN:
        record(
            "staging",
            "copy",
            "SKIPPED_DRY_RUN",
            source_path=relative_path,
            destination_path=destination_path,
            size_bytes=size_bytes,
            use_case=use_case,
            rule_id=rule_id,
        )
        return

    # The destination folder is pre-created by the provisioning stage for every contracted
    # path, but a derived batch_date= partition or the quarantine folder may be new. A UC
    # volume copy does not create intermediate directories -- it fails with "no such
    # directory" -- so the mkdirs is not optional.
    dbutils.fs.mkdirs(destination_directory)

    try:
        # Streamed byte-for-byte copy through the FUSE mounts on both sides. shutil.copyfileobj
        # never holds the whole file in memory, which matters for the 42 MB SGSN payload.
        with open(local_path, "rb") as source, open(destination_path, "wb") as sink:
            shutil.copyfileobj(source, sink, length=8 * 1024 * 1024)
    except OSError as exc:
        record(
            "staging",
            "copy",
            "FAILED",
            source_path=relative_path,
            destination_path=destination_path,
            size_bytes=size_bytes,
            use_case=use_case,
            rule_id=rule_id,
            detail=str(exc),
        )
        raise RuntimeError(f"Failed to copy '{local_path}' to '{destination_path}': {exc}") from exc

    written = os.path.getsize(destination_path)
    if written != size_bytes:
        record(
            "staging",
            "verify",
            "FAILED",
            source_path=relative_path,
            destination_path=destination_path,
            size_bytes=written,
            use_case=use_case,
            rule_id=rule_id,
            detail=f"Size mismatch: source {size_bytes} bytes, destination {written} bytes.",
        )
        raise RuntimeError(
            f"Copy verification failed for '{destination_path}': expected {size_bytes} bytes, "
            f"found {written}. The destination file has been left in place for inspection."
        )

    record(
        "staging",
        "copy",
        "COPIED",
        source_path=relative_path,
        destination_path=destination_path,
        size_bytes=written,
        use_case=use_case,
        rule_id=rule_id,
    )

    if MOVE_AFTER_COPY:
        try:
            os.remove(local_path)
        except OSError as exc:
            # The copy succeeded and was verified, so the staging outcome is sound. Failing to
            # remove the original is a housekeeping problem, reported but not fatal.
            record(
                "staging",
                "remove_source",
                "FAILED",
                source_path=local_path,
                use_case=use_case,
                rule_id=rule_id,
                detail=f"Copy verified but the source could not be removed: {exc}",
            )
            logger.warning("Copy of '%s' verified, but the source could not be removed: %s", local_path, exc)
        else:
            record("staging", "remove_source", "REMOVED", source_path=local_path, use_case=use_case, rule_id=rule_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Provisioning -- schemas, volumes, directory tree
# MAGIC
# MAGIC Managed volumes by default. To make one external instead, pass a JSON object in the
# MAGIC `external_volume_locations` widget keyed by `<schema>.<volume>`:
# MAGIC
# MAGIC ```json
# MAGIC {"landing.uc_7": "abfss://landing@myaccount.dfs.core.windows.net/uc7"}
# MAGIC ```
# MAGIC
# MAGIC which renders as:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS <catalog>.landing.uc_7
# MAGIC   LOCATION 'abfss://landing@myaccount.dfs.core.windows.net/uc7'
# MAGIC ```
# MAGIC
# MAGIC **Prerequisite for that path:** an external location must already exist covering the URL,
# MAGIC backed by a storage credential, and this principal needs `CREATE EXTERNAL VOLUME` on the
# MAGIC schema plus `READ FILES` / `WRITE FILES` on the external location. Without the external
# MAGIC location the statement fails; Unity Catalog will not create one implicitly. Managed is the
# MAGIC recommendation and is what all three use cases were built and tested against -- UC7's
# MAGIC `flowx.landing.uc_7` in particular is documented as MANAGED, resolving to the workspace's
# MAGIC account-managed storage.

# COMMAND ----------

def build_volume_ddl(schema: str, volume: str) -> Tuple[str, str]:
    """Return ``(ddl_statement, description)`` for one volume, managed unless overridden."""
    qualified = f"{CATALOG}.{schema}.{volume}"
    location = EXTERNAL_VOLUME_LOCATIONS.get(f"{schema}.{volume}")
    if location:
        if "'" in location:
            raise ValueError(f"External volume location for '{schema}.{volume}' contains a quote: {location!r}")
        statement = f"CREATE EXTERNAL VOLUME IF NOT EXISTS {qualified} LOCATION '{location}'"
        return statement, f"create external volume {qualified} at {location}"
    return f"CREATE VOLUME IF NOT EXISTS {qualified}", f"create managed volume {qualified}"


provisioned_schemas: set = set()
provisioned_volume_roots: List[Tuple[str, str, str]] = []

for use_case in ACTIVE_USE_CASES:
    definition = TOPOLOGY[use_case]

    for schema in definition["schemas"]:  # type: ignore[index]
        if schema in provisioned_schemas:
            continue
        if not _IDENTIFIER_PATTERN.match(schema):
            raise ValueError(f"Topology schema name is not a safe identifier: {schema!r}")
        execute_ddl(
            f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}",
            f"create schema {CATALOG}.{schema}",
            use_case=use_case,
        )
        provisioned_schemas.add(schema)

    for schema, volume, subdirectories in definition["volumes"]:  # type: ignore[misc]
        statement, description = build_volume_ddl(schema, volume)
        execute_ddl(statement, description, use_case=use_case)
        volume_root = f"{VOLUME_ROOT}/{schema}/{volume}"
        provisioned_volume_roots.append((use_case, f"{schema}.{volume}", volume_root))
        make_directory(volume_root, use_case=use_case)
        for subdirectory in subdirectories:
            make_directory(f"{volume_root}/{subdirectory}", use_case=use_case)

# The unrouted quarantine lives under the staging schema, which every path above guarantees
# exists whenever any use case is selected. Created unconditionally so the staging stage never
# has to decide whether it can quarantine a file.
if "staging" not in provisioned_schemas:
    execute_ddl(
        f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.staging",
        f"create schema {CATALOG}.staging",
        use_case="SHARED",
    )
    provisioned_schemas.add("staging")

execute_ddl(
    f"CREATE VOLUME IF NOT EXISTS {CATALOG}.staging._unrouted",
    f"create managed volume {CATALOG}.staging._unrouted",
    use_case="SHARED",
)
make_directory(UNROUTED_ROOT, use_case="SHARED")

logger.info(
    "Provisioning complete: %d schema(s), %d volume(s).",
    len(provisioned_schemas),
    len(provisioned_volume_roots) + 1,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Secrets -- both patterns
# MAGIC
# MAGIC ### Pattern A -- Unity Catalog three-level secrets (preferred)
# MAGIC
# MAGIC A UC secret is addressed `catalog.schema.key` and read with
# MAGIC `dbutils.secrets.get(catalog=, schema=, key=)`. There is deliberately **no** SQL-callable
# MAGIC resolution for it: the SQL `secret(scope, key)` function only reaches classic workspace
# MAGIC scopes, and embedding a literal `secret(...)` call in a query executed inside a Lakeflow
# MAGIC pipeline's graph-definition context corrupts the query result through the credential
# MAGIC redaction machinery. UC secrets cannot hit that bug even in principle.
# MAGIC
# MAGIC Requires DBR 17.3 LTS+ or serverless environment version 4+.
# MAGIC
# MAGIC ### Pattern B -- classic workspace scope (the fallback)
# MAGIC
# MAGIC `crypto/secrets.py::resolve_secret_value` tries the UC lookup, and on failure -- a
# MAGIC metastore with UC secrets switched off raises `UC_SECRETS_NOT_ENABLED` -- retries against
# MAGIC classic scopes named, in order, `<catalog>.<schema>`, `<catalog>_<schema>`, `<schema>`,
# MAGIC `<catalog>`. This notebook creates the **first** of those, so the fallback resolves
# MAGIC immediately with no ambiguity.
# MAGIC
# MAGIC The spec shape never changes: it is always `{secret_catalog, secret_schema, secret_key}`.
# MAGIC Only the resolution widens.
# MAGIC
# MAGIC ### What this cell does and does not do
# MAGIC
# MAGIC It creates the scope and reports which keys are **missing**. It does **not** set any
# MAGIC value: a real passphrase must not pass through a notebook cell, a run log or a job
# MAGIC parameter. An operator sets it out of band with, for example:
# MAGIC
# MAGIC ```bash
# MAGIC # Unity Catalog secret
# MAGIC databricks secrets put-secret --catalog flowx --schema config --key pgpkey
# MAGIC # classic workspace scope
# MAGIC databricks secrets put-secret flowx.config pgpkey
# MAGIC ```
# MAGIC
# MAGIC No secret value is ever read into a variable, logged or displayed here. The verification
# MAGIC below checks only for the key's **presence**.

# COMMAND ----------

def _workspace_client():
    """Construct a WorkspaceClient from the ambient notebook credentials.

    The SDK resolves host and token from the notebook context automatically on Databricks
    compute, so no token is read, held or printed by this notebook.
    """
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def ensure_classic_scope(scope_name: str) -> str:
    """Create a classic workspace secret scope, tolerating one that already exists.

    Returns a status string for the run log. ``RESOURCE_ALREADY_EXISTS`` is the documented
    response when the scope is present, which is the normal outcome on every re-run.
    """
    if DRY_RUN:
        return "SKIPPED_DRY_RUN"
    try:
        client = _workspace_client()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not construct a WorkspaceClient (%s); skipping classic scope creation.", exc)
        return "SKIPPED_NO_CLIENT"

    try:
        client.secrets.create_scope(scope=scope_name)
    except Exception as exc:  # noqa: BLE001
        text = str(exc).upper()
        if "RESOURCE_ALREADY_EXISTS" in text or "ALREADY EXISTS" in text:
            return "ALREADY_EXISTS"
        if "PERMISSION_DENIED" in text or "UNAUTHORIZED" in text or "FORBIDDEN" in text:
            # Not fatal: the UC secret may already cover this, and a workspace admin can create
            # the scope separately. Surfaced clearly rather than swallowed.
            logger.warning(
                "Not permitted to create the classic secret scope '%s' (%s). If Unity Catalog "
                "secrets are enabled this does not matter; otherwise ask a workspace admin to "
                "create it.",
                scope_name,
                exc,
            )
            return "PERMISSION_DENIED"
        raise RuntimeError(f"Failed to create the classic secret scope '{scope_name}': {exc}") from exc
    return "CREATED"


def uc_secret_exists(secret_schema: str, secret_key: str) -> bool:
    """True when the Unity Catalog secret resolves. The value is never retained or logged."""
    try:
        dbutils.secrets.get(catalog=CATALOG, schema=secret_schema, key=secret_key)
        return True
    except Exception as exc:  # noqa: BLE001
        logger.info(
            "Unity Catalog secret '%s.%s.%s' is not resolvable (%s).",
            CATALOG,
            secret_schema,
            secret_key,
            type(exc).__name__,
        )
        return False


def classic_secret_exists(scope_name: str, secret_key: str) -> bool:
    """True when the key is present in the classic scope. Listed, never read."""
    try:
        client = _workspace_client()
        return any(item.key == secret_key for item in client.secrets.list_secrets(scope=scope_name))
    except Exception as exc:  # noqa: BLE001
        logger.info("Could not list the classic scope '%s' (%s).", scope_name, type(exc).__name__)
        return False


missing_secrets: List[str] = []

for secret_schema in _needed_secret_schemas:
    # Pattern A prerequisite: the schema that will hold the UC secret.
    execute_ddl(
        f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{secret_schema}",
        f"create schema {CATALOG}.{secret_schema} (secret holder)",
        use_case="SHARED",
        stage="secrets",
    )

    # Pattern B: the most specific classic scope name the framework's fallback will try.
    scope_name = f"{CATALOG}.{secret_schema}"
    scope_status = ensure_classic_scope(scope_name)
    record("secrets", "create_scope", scope_status, destination_path=scope_name, use_case="SHARED")

for placeholder in SECRET_PLACEHOLDERS:
    if placeholder.use_case != "SHARED" and placeholder.use_case not in SELECTED_USE_CASES:
        continue

    label = f"{CATALOG}.{placeholder.secret_schema}.{placeholder.secret_key}"
    scope_name = f"{CATALOG}.{placeholder.secret_schema}"

    if DRY_RUN:
        record("secrets", "check_secret", "SKIPPED_DRY_RUN", destination_path=label, use_case=placeholder.use_case)
        continue

    if uc_secret_exists(placeholder.secret_schema, placeholder.secret_key):
        record("secrets", "check_secret", "PRESENT_UC", destination_path=label, use_case=placeholder.use_case)
    elif classic_secret_exists(scope_name, placeholder.secret_key):
        record(
            "secrets",
            "check_secret",
            "PRESENT_CLASSIC",
            destination_path=label,
            use_case=placeholder.use_case,
            detail=f"Resolvable from the classic scope '{scope_name}'.",
        )
    else:
        missing_secrets.append(label)
        record(
            "secrets",
            "check_secret",
            "MISSING",
            destination_path=label,
            use_case=placeholder.use_case,
            detail=placeholder.purpose,
        )

if missing_secrets:
    logger.warning(
        "The following secret(s) are not yet set and must be supplied by an operator before the "
        "pipelines run: %s. Use 'databricks secrets put-secret --catalog %s --schema <schema> "
        "--key <key>' for a Unity Catalog secret, or 'databricks secrets put-secret %s.<schema> "
        "<key>' for the classic scope. Values are never set from this notebook.",
        ", ".join(missing_secrets),
        CATALOG,
        CATALOG,
    )
else:
    logger.info("Every expected secret placeholder resolves.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Staging -- enumerate, unpack, classify, copy
# MAGIC
# MAGIC Order of operations per file:
# MAGIC
# MAGIC 1. Enumerate recursively from the workspace upload folder.
# MAGIC 2. If it is a `.zip`, expand it and put the members back through the same pipeline. Only
# MAGIC    `.zip`. `.gz`, `.dat.gz` and `.csv.gz.gpg` pass through untouched.
# MAGIC 3. Classify against the ordered rule table.
# MAGIC 4. Copy to the destination volume folder, then verify by byte size.
# MAGIC 5. Optionally remove the workspace original when `move_after_copy` is set.

# COMMAND ----------

uploaded_files = enumerate_uploaded_files(WORKSPACE_STAGING_PATH)

if not uploaded_files:
    logger.warning(
        "No files found under '%s'. Provisioning and secrets have still been applied, but "
        "nothing was staged. Confirm the upload folder path and that the files are uploaded "
        "as files rather than as notebooks.",
        WORKSPACE_STAGING_PATH,
    )

# Expand zip containers first so their members are routed on their own names.
work_queue: List[Tuple[str, str, int]] = []
for absolute_path, relative_path, size_bytes in uploaded_files:
    if relative_path.lower().endswith(".zip"):
        if DRY_RUN:
            record(
                "staging",
                "unzip",
                "SKIPPED_DRY_RUN",
                source_path=relative_path,
                size_bytes=size_bytes,
                detail="Zip would be expanded and its members routed individually.",
            )
            continue
        work_queue.extend(expand_zip_archive(absolute_path, relative_path))
    else:
        work_queue.append((absolute_path, relative_path, size_bytes))

logger.info("Staging %d file(s) after zip expansion.", len(work_queue))

for absolute_path, relative_path, size_bytes in work_queue:
    stage_file(absolute_path, relative_path, size_bytes)

logger.info("Staging stage complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validation
# MAGIC
# MAGIC Asserts that every directory the topology promised actually exists, then reports the file
# MAGIC count per use case. A missing directory is a hard failure -- an onboarding spec pointing at
# MAGIC a `schema_location` that does not exist fails at pipeline runtime, well after this notebook
# MAGIC has reported success, which is exactly the failure this cell is here to prevent.
# MAGIC
# MAGIC Files in `_unrouted/` are reported as a warning rather than an error, because a stray
# MAGIC README in the upload folder is normal and should not block the run. Review them, then
# MAGIC either add a routing rule or delete them.

# COMMAND ----------

if DRY_RUN:
    logger.warning("Dry run: skipping directory validation, since nothing was created.")
    validation_failures: List[str] = []
else:
    validation_failures = []

    for use_case in ACTIVE_USE_CASES:
        for schema, volume, subdirectories in TOPOLOGY[use_case]["volumes"]:  # type: ignore[misc]
            volume_root = f"{VOLUME_ROOT}/{schema}/{volume}"
            for subdirectory in [""] + list(subdirectories):
                expected = f"{volume_root}/{subdirectory}".rstrip("/")
                if not volume_path_exists(expected):
                    validation_failures.append(expected)
                    record("validation", "check_directory", "FAILED", destination_path=expected, use_case=use_case)
                else:
                    record("validation", "check_directory", "PRESENT", destination_path=expected, use_case=use_case)

    if validation_failures:
        raise RuntimeError(
            "Expected volume directories are missing after provisioning: "
            f"{sorted(validation_failures)}. Check the CREATE VOLUME grants on the affected "
            "schema and re-run."
        )

    logger.info("Every expected volume directory is present.")

# COMMAND ----------

if DRY_RUN:
    logger.warning("Dry run: skipping file counts, since nothing was copied.")
else:
    count_targets: List[Tuple[str, str]] = []
    if "UC3" in SELECTED_USE_CASES:
        count_targets.append(("UC3", f"{UC3_STAGING_ROOT}/streaming"))
        count_targets.append(("UC3", f"{UC3_STAGING_ROOT}/batch"))
    if "UC6" in SELECTED_USE_CASES:
        count_targets.append(("UC6", f"{UC6_STAGING_ROOT}/raw"))
    if "UC7" in SELECTED_USE_CASES:
        count_targets.append(("UC7", f"{UC7_LANDING_ROOT}/raw"))
        count_targets.append(("UC7", f"{UC7_LANDING_ROOT}/asn_schema"))
    count_targets.append(("UNROUTED", UNROUTED_ROOT))

    for use_case, path in count_targets:
        file_count = count_files_in_volume(path)
        status = "EMPTY" if file_count == 0 else "POPULATED"
        record(
            "validation",
            "count_files",
            status,
            destination_path=path,
            size_bytes=file_count,
            use_case=use_case,
            detail=f"{file_count} file(s)",
        )
        if use_case == "UNROUTED" and file_count:
            logger.warning(
                "%d file(s) are sitting in the unrouted quarantine at '%s'. Review them and "
                "either add a RoutingRule or remove them.",
                file_count,
                path,
            )
        elif file_count == 0:
            logger.warning(
                "%s landing path '%s' is empty. If files were expected here, check the routing "
                "rules and the upload folder.",
                use_case,
                path,
            )

    # UC7 needs its ASN.1 modules present or every decode quarantines. Warned, not failed:
    # the modules are sometimes staged in a separate pass from the CDR payloads.
    if "UC7" in SELECTED_USE_CASES:
        asn_modules = {"EMSC.asn1", "PSGW.asn1", "SGSN.asn1", "TAP.310.asn1"}
        try:
            present = {entry.name for entry in dbutils.fs.ls(f"{UC7_LANDING_ROOT}/asn_schema")}
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not list the UC7 asn_schema folder: %s", exc)
            present = set()
        missing_modules = sorted(asn_modules - present)
        if missing_modules:
            logger.warning(
                "UC7 ASN.1 module(s) not yet staged: %s. Every record decoded against a missing "
                "module is quarantined, so upload them before running the UC7 pipeline.",
                ", ".join(missing_modules),
            )
            record(
                "validation",
                "check_asn_modules",
                "MISSING",
                destination_path=f"{UC7_LANDING_ROOT}/asn_schema",
                use_case="UC7",
                detail=f"Missing: {', '.join(missing_modules)}",
            )
        else:
            record(
                "validation",
                "check_asn_modules",
                "PRESENT",
                destination_path=f"{UC7_LANDING_ROOT}/asn_schema",
                use_case="UC7",
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Summary
# MAGIC
# MAGIC Every action taken in this run, as a DataFrame. The log is built in driver memory as the
# MAGIC run proceeds -- one small row per action, never file contents -- so turning it into a
# MAGIC DataFrame is cheap and no large dataset is collected anywhere in this notebook.

# COMMAND ----------

from pyspark.sql.types import LongType, StringType, StructField, StructType  # noqa: E402

ACTION_LOG_SCHEMA = StructType(
    [
        StructField("stage", StringType(), True),
        StructField("action", StringType(), True),
        StructField("source_path", StringType(), True),
        StructField("destination_path", StringType(), True),
        StructField("size_bytes", LongType(), True),
        StructField("use_case", StringType(), True),
        StructField("rule_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("detail", StringType(), True),
    ]
)

action_rows = [
    (
        entry.stage,
        entry.action,
        entry.source_path,
        entry.destination_path,
        int(entry.size_bytes),
        entry.use_case,
        entry.rule_id,
        entry.status,
        entry.detail,
    )
    for entry in ACTION_LOG
]

actions_df = spark.createDataFrame(action_rows, schema=ACTION_LOG_SCHEMA)
display(actions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Copy outcomes by use case and status

# COMMAND ----------

display(
    actions_df.where("stage = 'staging' AND action = 'copy'")
    .groupBy("use_case", "status")
    .count()
    .orderBy("use_case", "status")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run outcome
# MAGIC
# MAGIC A one-line verdict, plus the follow-up actions an operator still owes.

# COMMAND ----------

failed_actions = [entry for entry in ACTION_LOG if entry.status == "FAILED"]
unrouted_actions = [entry for entry in ACTION_LOG if entry.status == "UNROUTED"]
copied_actions = [entry for entry in ACTION_LOG if entry.status == "COPIED"]

summary_lines = [
    "",
    "=" * 78,
    f"  Catalog                : {CATALOG}",
    f"  Upload folder          : {WORKSPACE_STAGING_PATH}",
    f"  Use cases              : {', '.join(SELECTED_USE_CASES)}",
    f"  Mode                   : {'DRY RUN (nothing changed)' if DRY_RUN else 'APPLIED'}",
    f"  Schemas provisioned    : {len(provisioned_schemas)}",
    f"  Volumes provisioned    : {len(provisioned_volume_roots) + 1}",
    f"  Files copied           : {len(copied_actions)}",
    f"  Bytes copied           : {sum(entry.size_bytes for entry in copied_actions):,}",
    f"  Files unrouted         : {len(unrouted_actions)}",
    f"  Failed actions         : {len(failed_actions)}",
    f"  Secrets still missing  : {len(missing_secrets)}",
    "=" * 78,
]
print("\n".join(summary_lines))

if missing_secrets:
    print("\nOperator follow-up -- set these secret values out of band, never from a notebook:")
    for label in missing_secrets:
        print(f"  - {label}")

if unrouted_actions:
    print(f"\nUnrouted files quarantined in {UNROUTED_ROOT} -- review each one:")
    for entry in unrouted_actions:
        print(f"  - {entry.source_path}")

# A failed action would already have raised, so this is a belt-and-braces guard against a
# future change that records a failure without raising.
if failed_actions:
    raise RuntimeError(
        f"{len(failed_actions)} action(s) failed during this run; see the summary DataFrame "
        "above for the stage, path and detail of each."
    )

logger.info("Environment setup and file staging finished successfully.")
