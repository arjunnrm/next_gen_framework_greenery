<!-- GENERATED FILE — do not edit.
     Produced by scripts/build_docs_reference.py; edit the source it derives from. -->


# `lakeflow_framework`


1 modules.


## `lakeflow_framework/exceptions.py`

Custom exception hierarchy for the metadata-driven Lakeflow framework.


### class `FrameworkError`

Base class for all framework-raised errors.


### class `FrameworkConfigError`

Raised when control-metadata configuration is missing, malformed, or invalid.


### class `SecretResolutionError`

Raised when a Unity Catalog secret cannot be resolved.


### class `CryptoError`

Raised when AES column encryption/decryption fails.


### class `ArchiveError`

Raised when ZIP extraction or compression fails.


### class `Asn1DecodeError`

Raised when ASN.1 schema/module configuration cannot be loaded.


### class `AbacApplicationError`

Raised when one or more Unity Catalog row filter / column mask bindings fail.


### class `CdcStrategyError`

Raised when a CDC/materialization strategy is misconfigured or unsupported.


### class `OnboardingValidationError`

Raised when an onboarding spec fails structural or SQL-syntax validation.


### class `OnboardingUpsertError`

Raised when persisting onboarded metadata into the control tables fails.


### class `ObservabilityConfigError`

Raised when ``observability_config`` is missing, malformed, or the upstream task context (job/run/pipeline timestamps) cannot be resolved -- a configuration-shaped failure the DLT observability eng…


### class `ObservabilityDispatchError`

Raised when telemetry dispatch to *every* configured, enabled destination failed for a run.

