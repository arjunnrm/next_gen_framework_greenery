"""Custom exception hierarchy for the metadata-driven Lakeflow framework.

A typed hierarchy (instead of bare ``Exception``/``ValueError`` everywhere) lets callers
catch precisely the failure category they care about -- e.g. a Job orchestrator can choose
to retry on a transient read failure but fail fast on a configuration error.
"""


class FrameworkError(Exception):
    """Base class for all framework-raised errors."""


class FrameworkConfigError(FrameworkError):
    """Raised when control-metadata configuration is missing, malformed, or invalid."""


class SecretResolutionError(FrameworkError):
    """Raised when a Unity Catalog secret cannot be resolved."""


class CryptoError(FrameworkError):
    """Raised when AES column encryption/decryption fails."""


class ArchiveError(FrameworkError):
    """Raised when ZIP extraction or compression fails."""


class Asn1DecodeError(FrameworkError):
    """Raised when ASN.1 schema/module configuration cannot be loaded.

    Per-row binary decode failures are intentionally *not* raised through this exception:
    they are captured inline as an ``_asn1_decode_error`` column so one bad record doesn't
    abort an entire streaming micro-batch.
    """


class AbacApplicationError(FrameworkError):
    """Raised when one or more Unity Catalog row filter / column mask bindings fail."""


class CdcStrategyError(FrameworkError):
    """Raised when a CDC/materialization strategy is misconfigured or unsupported."""


class OnboardingValidationError(FrameworkError):
    """Raised when an onboarding spec fails structural or SQL-syntax validation."""


class OnboardingUpsertError(FrameworkError):
    """Raised when persisting onboarded metadata into the control tables fails."""


class ObservabilityConfigError(FrameworkError):
    """Raised when ``observability_config`` is missing, malformed, or the upstream task
    context (job/run/pipeline timestamps) cannot be resolved -- a configuration-shaped
    failure the DLT observability engine cannot recover from on its own."""


class ObservabilityDispatchError(FrameworkError):
    """Raised when telemetry dispatch to *every* configured, enabled destination failed for a
    run. A single destination's failure never raises this on its own (see
    ``observability/destination_dispatcher.py`` -- one bad destination must not block delivery
    to the others); this is reserved for the all-destinations-failed / zero-destinations-enabled
    case, which the entrypoint notebook treats as a hard failure."""
