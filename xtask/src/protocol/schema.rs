use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

use super::{
    AttemptChange, ClockRequirement, ExceptionName, FailureOutcome, GenerationChange,
    LinearizationPrimitive, MutationClass, ObjectKind, Operation, ReasonClass, ReentryName,
    ResolverProbeTopology, State, SyncStep, TokenChange, TransitionQualification,
};

pub(super) const SCHEMA: &str = "spec/state-machine.schema.json";
const SCHEMA_CONTRACT_SHA256: &str =
    "7f1ce3181c6bd5bb876ebb0528129d4ca70d491561958c7e2f1a9ada424b8517";

pub(super) fn validate_schema(root: &Path) -> Result<(), String> {
    let bytes =
        fs::read(root.join(SCHEMA)).map_err(|error| format!("cannot read {SCHEMA}: {error}"))?;
    let schema: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|error| format!("cannot parse {SCHEMA}: {error}"))?;
    let normalized = serde_json::to_vec(&schema)
        .map_err(|error| format!("cannot normalize {SCHEMA}: {error}"))?;
    let digest = format!("{:x}", Sha256::digest(normalized));
    if digest != SCHEMA_CONTRACT_SHA256 {
        return Err(format!(
            "{SCHEMA} contract digest differs: expected {SCHEMA_CONTRACT_SHA256}, got {digest}"
        ));
    }
    validate_schema_value(&schema, "#")?;
    validate_schema_domain(
        &schema,
        "/properties/transitions/items/properties/operation/enum",
        &Operation::ALL.map(Operation::as_str),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/transitions/items/properties/source/enum",
        &State::ALL.map(State::as_str),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/transitions/items/properties/source_object_kind/enum",
        &ObjectKind::ALL.map(ObjectKind::as_str),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/transitions/items/properties/destination/enum",
        &State::DESTINATIONS.map(State::as_str),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/transitions/items/properties/destination_object_kind/enum",
        &ObjectKind::ALL.map(ObjectKind::as_str),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/transitions/items/properties/generation_change/enum",
        &GenerationChange::ALL.map(GenerationChange::as_str),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/transitions/items/properties/attempt_change/enum",
        &AttemptChange::ALL.map(AttemptChange::as_str),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/transitions/items/properties/token_change/enum",
        &TokenChange::ALL.map(TokenChange::as_str),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/transitions/items/properties/reason_class/oneOf/1/enum",
        &ReasonClass::ALL.map(ReasonClass::as_str),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/transitions/items/properties/clock_requirement/enum",
        &ClockRequirement::ALL.map(ClockRequirement::as_str),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/transitions/items/properties/required_syncs/items/enum",
        &SyncStep::ALL.map(SyncStep::as_str),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/transitions/items/properties/linearization/enum",
        &LinearizationPrimitive::TRANSITIONS.map(LinearizationPrimitive::as_str),
    )?;
    validate_schema_const(
        &schema,
        "/properties/transitions/items/properties/before_linearization_failure/const",
        FailureOutcome::NotCommitted.as_str(),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/transitions/items/properties/qualification/enum",
        &TransitionQualification::ALL.map(TransitionQualification::as_str),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/transitions/items/properties/resolver_probe_topology/enum",
        &ResolverProbeTopology::ALL.map(ResolverProbeTopology::as_str),
    )?;
    validate_schema_const(
        &schema,
        "/properties/transitions/items/properties/after_linearization_failure/const",
        FailureOutcome::OutcomeUnknown.as_str(),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/exceptions/items/properties/name/enum",
        &ExceptionName::ALL.map(ExceptionName::as_str),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/exceptions/items/properties/source_object_kind/enum",
        &ObjectKind::ALL.map(ObjectKind::as_str),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/exceptions/items/properties/destination_object_kind/enum",
        &ObjectKind::ALL.map(ObjectKind::as_str),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/exceptions/items/properties/clock_requirement/enum",
        &ClockRequirement::ALL.map(ClockRequirement::as_str),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/exceptions/items/properties/mutation_class/enum",
        &MutationClass::ALL.map(MutationClass::as_str),
    )?;
    validate_schema_const(
        &schema,
        "/properties/exceptions/items/properties/linearization/const",
        LinearizationPrimitive::RenameReplace.as_str(),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/exceptions/items/properties/required_syncs/items/enum",
        &SyncStep::ALL.map(SyncStep::as_str),
    )?;
    validate_schema_const(
        &schema,
        "/properties/exceptions/items/properties/before_linearization_failure/const",
        FailureOutcome::NotCommitted.as_str(),
    )?;
    validate_schema_const(
        &schema,
        "/properties/exceptions/items/properties/after_linearization_failure/const",
        FailureOutcome::OutcomeUnknown.as_str(),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/reentry/items/properties/name/enum",
        &ReentryName::ALL.map(ReentryName::as_str),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/reentry/items/properties/source/enum",
        &State::REENTRY_SOURCES.map(State::as_str),
    )
}

pub(super) fn validate_schema_const(
    schema: &serde_json::Value,
    pointer: &str,
    expected: &str,
) -> Result<(), String> {
    let actual = schema
        .pointer(pointer)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("{SCHEMA} has no string const at {pointer}"))?;
    if actual != expected {
        return Err(format!(
            "{SCHEMA} const at {pointer} differs from xtask: expected {expected}, got {actual}"
        ));
    }
    Ok(())
}

pub(super) fn validate_schema_domain(
    schema: &serde_json::Value,
    pointer: &str,
    expected: &[&str],
) -> Result<(), String> {
    let values = schema
        .pointer(pointer)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| format!("{SCHEMA} has no enum array at {pointer}"))?;
    let actual = values
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| format!("{SCHEMA} has a non-string enum value at {pointer}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if actual != expected {
        return Err(format!(
            "{SCHEMA} domain at {pointer} differs from xtask: expected {expected:?}, got {actual:?}"
        ));
    }
    Ok(())
}

pub(super) fn validate_schema_value(value: &serde_json::Value, path: &str) -> Result<(), String> {
    match value {
        serde_json::Value::Object(object) => {
            if object.get("type").and_then(serde_json::Value::as_str) == Some("object")
                && object.get("additionalProperties") != Some(&serde_json::Value::Bool(false))
            {
                return Err(format!(
                    "{SCHEMA} object at {path} must set additionalProperties to false"
                ));
            }
            for (key, child) in object {
                validate_schema_value(child, &format!("{path}/{key}"))?;
            }
        }
        serde_json::Value::Array(array) => {
            for (index, child) in array.iter().enumerate() {
                validate_schema_value(child, &format!("{path}/{index}"))?;
            }
        }
        _ => {}
    }
    Ok(())
}
