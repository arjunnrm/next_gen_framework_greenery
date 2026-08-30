"""Runtime application of ``data_standardization_sql`` -- a column-expression-only allowlist.

``onboarding/spec_validator.py::_validate_data_standardization_sql`` is the primary gate,
rejecting anything that looks like a full SQL statement (``SELECT``/``FROM``/``JOIN``/
``UNION``/etc, or more than one expression per entry) at onboarding time. This module is the
runtime counterpart: it applies each already-validated column expression to the ingested
DataFrame, one ``withColumn`` per entry, replacing an existing column of the same name or
adding a new one -- e.g. ``"trim(customer_name) AS customer_name"`` normalizes an existing
column in place.
"""

import re
from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from NextGen_Metadata_Framework.lakeflow_framework.exceptions import FrameworkConfigError

# Every expression must end with an explicit "AS <column_name>" so the runtime knows which
# output column to write to, without re-deriving a name from the expression text itself.
_ALIAS_PATTERN = re.compile(r"\bAS\s+`?([A-Za-z_][A-Za-z0-9_]*)`?\s*$", re.IGNORECASE)


def apply_data_standardization_sql(df: DataFrame, expressions: Optional[List[str]]) -> DataFrame:
    """Apply each configured column expression to ``df``, in order.

    Parameters
    ----------
    df:
        Input DataFrame.
    expressions:
        List of single column-expression strings, each ending in ``AS <column_name>`` (e.g.
        ``"upper(trim(country_code)) AS country_code"``). ``None``/empty is a no-op.

    Returns
    -------
    DataFrame
        ``df`` with each expression applied via ``withColumn(alias, F.expr(expression))`` --
        overwriting an existing column of that name, or adding a new one.

    Raises
    ------
    FrameworkConfigError
        If an expression doesn't end with a resolvable ``AS <column_name>`` clause, or a
        malformed-grammar error surfaces immediately from ``F.expr(...)``/``withColumn``.
        Spark's lazy analysis means a *semantic* error (e.g. a reference to a column that
        doesn't exist on ``df``) is not guaranteed to surface here -- it appears once the
        DataFrame is actually executed inside the pipeline, as a plain, clearly-attributable
        Spark error naming the bad reference. The onboarding validator already enforces the
        restricted grammar (single column expression, no SELECT/FROM/JOIN/etc) up front.
    """
    if not expressions:
        return df

    result_df = df
    for expression in expressions:
        match = _ALIAS_PATTERN.search(expression)
        if not match:
            raise FrameworkConfigError(
                f"data_standardization_sql expression {expression!r} must end with 'AS <column_name>' "
                "naming the output column"
            )
        alias = match.group(1)
        try:
            result_df = result_df.withColumn(alias, F.expr(expression))
        except Exception as exc:  # noqa: BLE001
            raise FrameworkConfigError(
                f"Failed to apply data_standardization_sql expression {expression!r}: {exc}"
            ) from exc
    return result_df
