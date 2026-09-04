"""Structured JSON logging (Phase 10) + the DLT observability engine (``dlt_observability``).

``structured_logger.py`` -- business-event JSON logging, see its own module docstring.

The ``dlt_observability`` engine (this package's other modules) runs as a downstream Workflow
task after a pipeline's ``pipeline_task`` completes: validates the run's four required task
parameters (``runtime_params.py``), resolves that task's execution window
(``task_context_resolver.py``), extracts + aggregates the DLT event log for it
(``event_log_extractor.py``), builds strict OTel ``ResourceLogs`` payloads
(``otel_payload_builder.py``), and dispatches them to every configured, enabled destination
(``destination_dispatcher.py``) resolved from the ``observability_config`` control table
(``config_loader.py``). See ``notebooks/08_observability/08_dlt_observability_engine.py`` for
the entrypoint that wires them together, and ``docs/08_observability_and_telemetry.md`` for the
full architecture writeup.

Two engines, two parameter contracts
------------------------------------
``mode`` on an ``observability_config`` row decides which engine serves a destination, and the
two are configured in genuinely different places because they have genuinely different
lifecycles:

* ``triggered`` -- a bounded Workflow task. Its inputs are **task parameters**, all four
  required: ``dataflow_group_id``, ``catalog``, ``env``, ``pipeline_task_run_id``. See
  ``runtime_params.py``, which also explains why ``pipeline_task_run_id`` must NOT be declared in
  a pipeline's ``pipeline_parameters``.
* ``continuous`` -- a standing ``continuous: true`` pipeline
  (``notebooks/06_observability_streaming/``). It has no task run to be parameterized by; it
  streams N event-log tables named by ``destination_config.event_log_tables`` on its
  ``mode: "continuous"`` observability rows, discovered through the pipeline's own
  ``dataflow.group.id`` / ``dataflow.control.catalog`` configuration. The event-log table list
  lives in the observability block, not in pipeline settings -- see ``config_loader.py`` for why.
"""
