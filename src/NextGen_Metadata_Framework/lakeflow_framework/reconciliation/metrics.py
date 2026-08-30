"""Reconciliation run metrics -- one ``reconciliation_run_log`` row's worth of counts, per target.

:class:`ReconciliationMetrics` is the shared value object both reconciliation execution paths --
``reconciliation/appender.py::run_target_reconciliation`` (batch, one pass over the whole
dataset) and ``reconciliation/streaming.py::run_streaming_target_reconciliation`` (``foreachBatch``,
one pass per micro-batch) -- build up as they classify records (``matcher.py``), append
unmatched rows (``appender.py``), and log per-record mismatch detail (``mismatch_logging.py``),
so both paths write ``reconciliation_run_log`` through the exact same shape instead of each
assembling its own ad hoc dict.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ReconciliationMetrics:
    """The counts one ``target_configs[]`` entry's reconciliation run reports.

    Field names match ``reconciliation_run_log``'s own columns 1:1 (see
    ``control_plane/ddl_definitions.py::get_reconciliation_run_log_ddl``) so
    ``appender.py::write_run_log_entry`` can pass this straight through without remapping.
    Every field defaults to ``None`` (rather than requiring all-or-nothing construction) so a
    ``FAILED`` run -- which may not have gotten far enough to compute most of them -- can still
    log a minimal, honest entry.
    """

    source_record_count: Optional[int] = None
    target_record_count: Optional[int] = None
    matched_count: Optional[int] = None
    missing_in_target_count: Optional[int] = None
    missing_in_source_count: Optional[int] = None
    value_drift_count: Optional[int] = None
    appended_count: Optional[int] = None
    failed_count: Optional[int] = None

    def as_dict(self) -> dict:
        return {
            "source_record_count": self.source_record_count,
            "target_record_count": self.target_record_count,
            "matched_count": self.matched_count,
            "missing_in_target_count": self.missing_in_target_count,
            "missing_in_source_count": self.missing_in_source_count,
            "value_drift_count": self.value_drift_count,
            "appended_count": self.appended_count,
            "failed_count": self.failed_count,
        }
