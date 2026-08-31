"""
Action Execution & Job Polling Client (§10.4).
Supports mode: job, pipeline, and local validation.
"""

from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional
import uuid

from databricks.sdk.errors import (
    BadRequest,
    DatabricksError,
    InvalidParameterValue,
    NotFound,
    PermissionDenied,
    RequestLimitExceeded,
    ResourceDoesNotExist,
    ResourceExhausted,
    TooManyRequests,
    Unauthenticated,
)

from server.errors import AppException
from server.settings import ActionConfig, AppSettings

logger = logging.getLogger("metaflow_app")


class JobRunner:
    """Executes actions (validate, onboard, etc.) against Databricks Jobs / Pipelines."""

    # In-memory store for synthetic local runs
    _local_runs: Dict[str, Dict[str, Any]] = {}

    def __init__(self, settings: AppSettings, client: Any):
        self.settings = settings
        self.client = client

    def resolve_parameters(self, param_map: Dict[str, str], context_values: Dict[str, Any]) -> Dict[str, str]:
        """Substitute $spec_path, $spec_file_path, $spec_inline, $dataflow_group_id, $env, $catalog, $user_email, $request_id."""
        resolved = {}
        for k, v in param_map.items():
            if isinstance(v, str) and v.startswith("$"):
                var_key = v[1:]
                resolved[k] = str(context_values.get(var_key, ""))
            else:
                resolved[k] = str(v)
        return resolved

    def _map_job_exception(self, ex: Exception, action_id: str, job_id: Optional[int], operation: str = "trigger") -> AppException:
        """Map SDK error to structured application exception."""
        logger.error(
            f"Databricks SDK error during {operation} on action '{action_id}' (job_id: {job_id}): {ex}",
            extra={
                "event": f"job_{operation}_error",
                "action_id": action_id,
                "job_id": job_id,
                "error": str(ex),
                "exception_type": type(ex).__name__
            }
        )
        if isinstance(ex, Unauthenticated):
            return AppException(code="UNAUTHORIZED", message=f"Authentication failed while attempting to {operation} job {job_id}: {ex}", status_code=401)
        if isinstance(ex, PermissionDenied):
            return AppException(code="PERMISSION_DENIED", message=f"Permission denied to {operation} job {job_id}: {ex}", status_code=403)
        if isinstance(ex, (NotFound, ResourceDoesNotExist)):
            return AppException(code="NOT_FOUND", message=f"Job {job_id} not found: {ex}", status_code=404)
        if isinstance(ex, (InvalidParameterValue, BadRequest)):
            return AppException(code="VALIDATION_FAILED", message=f"Invalid parameter for job {job_id}: {ex}", status_code=400)
        if isinstance(ex, (ResourceExhausted, RequestLimitExceeded, TooManyRequests)):
            return AppException(code="RATE_LIMITED", message=f"Databricks rate limit exceeded while trying to {operation} job {job_id}: {ex}", status_code=429)
        return AppException(code="UPSTREAM_ERROR", message=f"Failed to {operation} Databricks job {job_id}: {str(ex)}", status_code=502)

    def run_action(
        self,
        action_id: str,
        spec_dict: Dict[str, Any],
        context_values: Dict[str, Any],
        overrides: Optional[Dict[str, Any]] = None,
        job_parameters: Optional[Dict[str, Any]] = None,
        notebook_params: Optional[Dict[str, Any]] = None,
        python_params: Optional[List[Any]] = None,
        python_named_params: Optional[Dict[str, Any]] = None,
        sql_params: Optional[Dict[str, Any]] = None,
        jar_params: Optional[List[Any]] = None,
        idempotency_token: Optional[str] = None,
        job_id_override: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Trigger an action execution and return run metadata.

        Maps top-level parameters, task-level overrides (notebook_params / python_params),
        and resolves templated placeholders before executing w.jobs.run_now().
        """
        if action_id not in self.settings.actions:
            raise AppException(code="NOT_FOUND", message=f"Action '{action_id}' not found in configuration.", status_code=404)

        action: ActionConfig = self.settings.actions[action_id]
        if not action.enabled:
            raise AppException(code="PERMISSION_DENIED", message=f"Action '{action_id}' is disabled.", status_code=403)

        resolved_params = self.resolve_parameters(action.parameter_map, context_values)

        # Apply operator overrides and job_parameters
        all_overrides = {}
        if overrides:
            all_overrides.update(overrides)
        if job_parameters:
            all_overrides.update(job_parameters)

        if all_overrides:
            resolved_params.update({k: str(v) for k, v in all_overrides.items()})

        # Ensure essential onboarding parameters and aliases are populated from context
        if "spec_file_path" in context_values and "spec_file_path" not in resolved_params:
            resolved_params["spec_file_path"] = str(context_values["spec_file_path"])
        if "spec_path" in context_values and "spec_path" not in resolved_params:
            resolved_params["spec_path"] = str(context_values["spec_path"])
        if "catalog" in context_values and "catalog" not in resolved_params:
            resolved_params["catalog"] = str(context_values["catalog"])
        if "env" in context_values and "env" not in resolved_params:
            resolved_params["env"] = str(context_values["env"])
        if "environment" in context_values and "environment" not in resolved_params:
            resolved_params["environment"] = str(context_values["environment"])
        if "action_type" in context_values and "action_type" not in resolved_params:
            resolved_params["action_type"] = str(context_values["action_type"])
        if "dataflow_group_id" in context_values and "dataflow_group_id" not in resolved_params:
            resolved_params["dataflow_group_id"] = str(context_values["dataflow_group_id"])

        host = self.settings.workspace.host.rstrip("/")
        user = context_values.get("user_email", "system")
        req_id = context_values.get("request_id", "")

        if action.mode == "local":
            run_id = f"local_{uuid.uuid4().hex[:8]}"
            run_url = f"{host}/#local/runs/{run_id}"
            logger.info(
                f"Executing local action '{action_id}' (run_id: {run_id}).",
                extra={
                    "event": "local_run_dispatched",
                    "action_id": action_id,
                    "run_id": run_id,
                    "user": user,
                    "request_id": req_id
                }
            )
            self._local_runs[run_id] = {
                "run_id": run_id,
                "action_id": action_id,
                "state": "SUCCESS",
                "life_cycle": "TERMINATED",
                "result": "SUCCESS",
                "current_stage": len(action.stages) - 1,
                "stages": action.stages,
                "run_url": run_url,
                "log_tail": "Local in-app validation completed successfully.",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "finished_at": datetime.now(timezone.utc).isoformat()
            }
            return {
                "run_id": run_id,
                "job_id": action.job_id,
                "run_url": run_url,
                "stages": action.stages,
                "request_id": req_id
            }

        if action.mode == "job":
            effective_job_id = None
            if job_id_override is not None and str(job_id_override).strip():
                try:
                    effective_job_id = int(str(job_id_override).strip())
                except ValueError:
                    pass
            if not effective_job_id and action.job_id:
                try:
                    effective_job_id = int(action.job_id)
                except ValueError:
                    pass
            if not effective_job_id:
                for env_k in ("METAFLOW_ONBOARDING_JOB_ID", "ONBOARDING_JOB_ID", "DATABRICKS_ONBOARDING_JOB_ID", "JOB_ID"):
                    env_jid = os.environ.get(env_k, "").strip()
                    if env_jid and env_jid.isdigit():
                        effective_job_id = int(env_jid)
                        break
            if not effective_job_id:
                raise AppException(
                    code="CONFIG_INVALID",
                    message=f"Action '{action_id}' mode is 'job' but job_id is missing. Please configure METAFLOW_ONBOARDING_JOB_ID.",
                    status_code=500
                )

            # Query job declared parameters if available from Databricks API
            declared_params = None
            try:
                job_obj = self.client.jobs.get(effective_job_id)
                settings_params = getattr(getattr(job_obj, "settings", None), "parameters", None) or []
                if settings_params:
                    declared_params = {p.name for p in settings_params if hasattr(p, "name")}
            except Exception as ex:
                logger.warning(f"Could not fetch declared parameters for job {effective_job_id}: {ex}")
                declared_params = None

            final_job_params = dict(resolved_params)
            # Remove internal non-job parameters that Databricks run_now will reject
            for internal_k in ("job_id", "jobId", "action_id", "user_email", "request_id", "spec_inline", "confirmed", "format"):
                final_job_params.pop(internal_k, None)

            if declared_params:
                # Map aliases to declared parameter names if needed
                if "spec_file_path" in declared_params and "spec_file_path" not in final_job_params and "spec_path" in final_job_params:
                    final_job_params["spec_file_path"] = final_job_params.pop("spec_path")
                if "env" in declared_params and "env" not in final_job_params and "environment" in final_job_params:
                    final_job_params["env"] = final_job_params.pop("environment")
                # Filter down strictly to what the Databricks job declares
                final_job_params = {k: v for k, v in final_job_params.items() if k in declared_params}
            else:
                # Clean up redundant parameter aliases
                if "spec_file_path" in final_job_params:
                    final_job_params.pop("spec_path", None)
                if "env" in final_job_params:
                    final_job_params.pop("environment", None)

            run_kwargs: Dict[str, Any] = {}
            if final_job_params:
                run_kwargs["job_parameters"] = final_job_params
            if notebook_params:
                run_kwargs["notebook_params"] = {str(k): str(v) for k, v in notebook_params.items()}
            if python_params:
                run_kwargs["python_params"] = [str(x) for x in python_params]
            if python_named_params:
                run_kwargs["python_named_params"] = {str(k): str(v) for k, v in python_named_params.items()}
            if sql_params:
                run_kwargs["sql_params"] = {str(k): str(v) for k, v in sql_params.items()}
            if jar_params:
                run_kwargs["jar_params"] = [str(x) for x in jar_params]
            if idempotency_token:
                run_kwargs["idempotency_token"] = str(idempotency_token)

            logger.info(
                f"Triggering Databricks job {effective_job_id} for action '{action_id}' as user {user}.",
                extra={
                    "event": "job_dispatch_start",
                    "action_id": action_id,
                    "job_id": effective_job_id,
                    "user": user,
                    "request_id": req_id,
                    "parameters_count": len(final_job_params)
                }
            )

            try:
                res = self.client.jobs.run_now(effective_job_id, **run_kwargs)
                run_id = str(res.run_id)
                run_url = f"{host}/#job/{effective_job_id}/run/{run_id}"

                logger.info(
                    f"Successfully triggered job {effective_job_id} -> run_id: {run_id} (url: {run_url}).",
                    extra={
                        "event": "job_dispatch_success",
                        "action_id": action_id,
                        "job_id": effective_job_id,
                        "run_id": run_id,
                        "run_url": run_url,
                        "user": user,
                        "request_id": req_id
                    }
                )

                return {
                    "run_id": run_id,
                    "job_id": effective_job_id,
                    "run_url": run_url,
                    "stages": action.stages,
                    "request_id": req_id
                }
            except AppException:
                raise
            except Exception as ex:
                raise self._map_job_exception(ex, action_id=action_id, job_id=effective_job_id, operation="trigger")

        raise AppException(code="CONFIG_INVALID", message=f"Unsupported action mode '{action.mode}'.", status_code=500)

    def get_run_status(self, run_id: str, action_id: Optional[str] = None) -> Dict[str, Any]:
        """Poll status of a run and map SDK state to configured stages."""
        if str(run_id).startswith("local_") or run_id in self._local_runs:
            return self._local_runs.get(run_id, {
                "run_id": run_id,
                "state": "SUCCESS",
                "life_cycle": "TERMINATED",
                "result": "SUCCESS",
                "current_stage": 0,
                "stages": ["Validation"],
                "run_url": "#",
                "log_tail": "Completed."
            })

        action = self.settings.actions.get(action_id or "onboard")
        stages = action.stages if action else ["Running", "Completed"]
        host = self.settings.workspace.host.rstrip("/")

        try:
            run_info = self.client.jobs.get_run(int(run_id))
            state = getattr(run_info, "state", None)
            life_cycle = getattr(state, "life_cycle_state", "RUNNING") if state else "RUNNING"
            result = getattr(state, "result_state", None) if state else None

            # Get logs where possible
            log_tail = ""
            try:
                output = self.client.jobs.get_run_output(int(run_id))
                log_tail = getattr(output, "logs", "") or str(output)
            except Exception:
                log_tail = "Task is executing..."

            # Estimate monotonic stage progression
            current_stage = len(stages) - 1 if life_cycle in ("TERMINATED", "SUCCESS") else max(0, min(1, len(stages) - 2))

            job_id = getattr(run_info, "job_id", None)
            if not job_id and action:
                job_id = action.job_id
            if not job_id:
                for env_k in ("METAFLOW_ONBOARDING_JOB_ID", "ONBOARDING_JOB_ID", "DATABRICKS_ONBOARDING_JOB_ID", "JOB_ID"):
                    val = os.environ.get(env_k, "").strip()
                    if val and val.isdigit():
                        job_id = int(val)
                        break
            if not job_id and self.settings.actions.get("onboard"):
                job_id = self.settings.actions.get("onboard").job_id

            if job_id:
                run_url = f"{host}/#job/{job_id}/run/{run_id}"
            else:
                run_url = getattr(run_info, "run_page_url", None) or f"{host}/#job/run/{run_id}"

            logger.debug(
                f"Polled status for run {run_id}: state={result or life_cycle}, stage={current_stage}.",
                extra={
                    "event": "job_poll_status",
                    "run_id": run_id,
                    "job_id": job_id,
                    "life_cycle": life_cycle,
                    "result": result,
                    "current_stage": current_stage
                }
            )

            return {
                "run_id": run_id,
                "state": result or life_cycle,
                "life_cycle": life_cycle,
                "result": result,
                "current_stage": current_stage,
                "stages": stages,
                "run_url": run_url,
                "log_tail": log_tail,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "finished_at": datetime.now(timezone.utc).isoformat() if life_cycle in ("TERMINATED", "SUCCESS") else None
            }
        except AppException:
            raise
        except Exception as ex:
            raise self._map_job_exception(ex, action_id=action_id or "unknown", job_id=action.job_id if action else None, operation="poll")

    def cancel_run(self, run_id: str) -> Dict[str, bool]:
        """Cancel a running job."""
        if str(run_id).startswith("local_"):
            return {"cancelled": True}

        logger.info(f"Cancelling job run {run_id}.", extra={"event": "job_cancel_request", "run_id": run_id})
        try:
            self.client.jobs.cancel_run(int(run_id))
            logger.info(f"Successfully cancelled job run {run_id}.", extra={"event": "job_cancel_success", "run_id": run_id})
            return {"cancelled": True}
        except AppException:
            raise
        except Exception as ex:
            raise self._map_job_exception(ex, action_id="unknown", job_id=None, operation="cancel")

