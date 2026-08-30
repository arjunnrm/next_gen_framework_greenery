"""Best-effort perception/client-context capture for the onboarding audit trail.

``build_client_context_json`` is called once from ``notebooks/02_onboarding/
02_onboarding_engine.py``, before either the success or failure branch runs, and its output is
passed straight through to ``onboarding/audit_logger.py::write_audit_log_entry`` as
``client_context_json`` -- capturing who/what/where triggered this onboarding attempt
(``current_user()``, cluster/notebook tags, git commit SHA, CLI/SDK version, Spark runtime
version) independent of whether the attempt itself succeeds.

Every field is resolved independently, each in its own ``try``/``except``, and defaults to the
string ``'unknown'`` rather than propagating an exception: not every execution context
(interactive notebook vs. a Jobs task vs. a CI/CD-triggered run) populates every notebook-context
tag or environment variable, and a perception-metadata gap must never be allowed to fail the
onboarding action itself.
"""

import datetime
import json
import logging
import os
import socket
from typing import Any, Dict

from pyspark.sql import SparkSession

logger = logging.getLogger("common.onboarding.client_context")


def _safe_get_notebook_context_tag(tags: Any, key: str, default: str = "unknown") -> str:
    try:
        option = tags.get(key)
        return option.getOrElse(default)
    except Exception:  # noqa: BLE001
        return default


def build_client_context_json(spark: SparkSession, dbutils: Any) -> str:
    """Best-effort capture of onboarding perception metadata as a JSON string.

    Not every field is guaranteed to be populated by every execution context (interactive
    notebook vs. Job task vs. CI/CD-triggered run) -- unavailable fields default to
    ``'unknown'`` rather than failing the onboarding action.
    """
    context: Dict[str, str] = {"execution_timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()}

    try:
        context["user_principal"] = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not resolve current_user(): %s", exc)
        context["user_principal"] = "unknown"

    try:
        context["build_hostname"] = socket.gethostname()
    except Exception:  # noqa: BLE001
        context["build_hostname"] = "unknown"

    try:
        notebook_context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        tags = notebook_context.tags()
        context["client_ip"] = _safe_get_notebook_context_tag(tags, "browserHostName")
        context["browser_user_agent"] = _safe_get_notebook_context_tag(tags, "user_agent")
        context["cluster_id"] = _safe_get_notebook_context_tag(tags, "clusterId")
        try:
            extra_context = notebook_context.extraContext()
            context["git_commit_sha"] = extra_context.get("mostRecentGitCommitHash").getOrElse("unknown")
        except Exception:  # noqa: BLE001
            context["git_commit_sha"] = os.environ.get("GIT_COMMIT_SHA", "unknown")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Notebook context unavailable (non-interactive execution?): %s", exc)
        context.setdefault("client_ip", "unknown")
        context.setdefault("browser_user_agent", "unknown")
        context["git_commit_sha"] = os.environ.get("GIT_COMMIT_SHA", "unknown")

    context["cli_sdk_version"] = os.environ.get("DATABRICKS_CLI_VERSION", "unknown")
    try:
        context["spark_runtime_version"] = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
    except Exception:  # noqa: BLE001
        context["spark_runtime_version"] = "unknown"

    return json.dumps(context)
