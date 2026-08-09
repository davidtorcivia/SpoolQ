use super::render::*;
use super::schema::{validate_schema, validate_schema_domain, validate_schema_value, SCHEMA};
use super::*;
use crate::{check_all, check_target, dispatch_command, run_command, workspace_root};
use serde_json::Value;
use std::path::Path;
use tempfile::TempDir;

fn fixture() -> StateMachineSpec {
    serde_json::from_value(fixture_value()).unwrap()
}

fn fixture_value() -> Value {
    serde_json::from_str(include_str!("../../../spec/state-machine.json")).unwrap()
}

fn temporary_workspace() -> TempDir {
    let temp = tempfile::tempdir().unwrap();
    for directory in [
        "spec",
        "generated",
        "crates/steadq-core/src",
        ".github",
        "docs",
    ] {
        fs::create_dir_all(temp.path().join(directory)).unwrap();
    }
    for (path, contents) in [
        (SPEC, include_str!("../../../spec/state-machine.json")),
        (
            SCHEMA,
            include_str!("../../../spec/state-machine.schema.json"),
        ),
        (
            GENERATED_RUST,
            include_str!("../../../generated/state-machine.rs"),
        ),
        (
            GENERATED_GO,
            include_str!("../../../generated/state-machine.go"),
        ),
        (
            GENERATED_MARKDOWN,
            include_str!("../../../generated/state-machine.md"),
        ),
        (
            CORE_RUST,
            include_str!("../../../crates/steadq-core/src/state_machine.rs"),
        ),
    ] {
        fs::write(temp.path().join(path), contents).unwrap();
    }
    for required in [
        "SECURITY.md",
        "CONTRIBUTING.md",
        ".github/CODEOWNERS",
        "docs/compatibility-policy.md",
        "docs/complexity-ledger.md",
        "docs/formal-evidence-scope.md",
        "docs/hardening-tracker.md",
        "docs/traceability.md",
    ] {
        fs::write(temp.path().join(required), "").unwrap();
    }
    temp
}

fn read(path: &Path, relative: &str) -> String {
    fs::read_to_string(path.join(relative)).unwrap()
}

#[test]
fn validates_complete_fixture() {
    validate_spec(&fixture()).unwrap();
}

#[test]
fn workspace_root_points_to_workspace() {
    let root = workspace_root().unwrap();
    assert!(root.join("Cargo.toml").is_file());
    assert!(root.join("xtask/Cargo.toml").is_file());
}

#[test]
fn rejects_unsupported_target() {
    assert_eq!(
        check_target("aarch64", "linux", "gnu", 64).unwrap_err(),
        "strict prototype target is x86_64-unknown-linux-gnu; got aarch64-linux-gnu (64-bit)"
    );
    assert_eq!(
        check_target("x86_64", "linux", "gnu", 32).unwrap_err(),
        "strict prototype target is x86_64-unknown-linux-gnu; got x86_64-linux-gnu (32-bit)"
    );
}

#[test]
fn command_dispatch_rejects_unknown_commands() {
    assert_eq!(
        run_command(Some("unknown")).unwrap_err(),
        "usage: cargo xtask <check|check-generated|generate>"
    );
    assert_eq!(
        run_command(None).unwrap_err(),
        "usage: cargo xtask <check|check-generated|generate>"
    );
}

#[test]
fn command_dispatch_runs_each_command() {
    let temp = temporary_workspace();
    for command in ["check", "check-generated", "generate"] {
        dispatch_command(Some(command), temp.path()).unwrap();
    }
}

#[test]
fn check_all_rejects_missing_baseline_artifact() {
    let temp = temporary_workspace();
    fs::remove_file(temp.path().join("SECURITY.md")).unwrap();
    assert_eq!(
        check_all(temp.path()).unwrap_err(),
        "required baseline artifact is missing: SECURITY.md"
    );
}

#[test]
fn generated_files_match_spec() {
    check_generated(&workspace_root().unwrap()).unwrap();
}

#[test]
fn generate_repairs_every_artifact() {
    let temp = temporary_workspace();
    for artifact in [GENERATED_RUST, GENERATED_GO, GENERATED_MARKDOWN, CORE_RUST] {
        fs::write(temp.path().join(artifact), "corrupt").unwrap();
    }
    assert!(check_generated(temp.path()).is_err());
    generate(temp.path()).unwrap();
    for (artifact, expected) in [
        (
            GENERATED_RUST,
            include_str!("../../../generated/state-machine.rs"),
        ),
        (
            GENERATED_GO,
            include_str!("../../../generated/state-machine.go"),
        ),
        (
            GENERATED_MARKDOWN,
            include_str!("../../../generated/state-machine.md"),
        ),
        (
            CORE_RUST,
            include_str!("../../../crates/steadq-core/src/state_machine.rs"),
        ),
    ] {
        assert_eq!(read(temp.path(), artifact), expected);
    }
}

#[test]
fn rejects_duplicate_operation() {
    let mut spec = fixture();
    spec.transitions.push(Transition {
        operation: Operation::Claim,
        source: State::Ready,
        destination: State::Leased,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Increment,
        token_change: TokenChange::New,
        reason_class: Nullable::Null,
        required_syncs: vec![SyncStep::DestinationDir],
        no_overwrite: true,
        resolution_behavior: "probe both".into(),
        notes: Nullable::Null,
    });
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "duplicate transition operation: claim"
    );
}

#[test]
fn rejects_duplicate_sync() {
    let mut spec = fixture();
    spec.transitions[0]
        .required_syncs
        .push(SyncStep::DestinationDir);
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "transition enqueue_immediate contains duplicate sync destination_dir_fsync"
    );
}

#[test]
fn rejects_overwriting_transition() {
    let mut spec = fixture();
    spec.transitions[0].no_overwrite = false;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "transition enqueue_immediate must prohibit overwrite"
    );
}

#[test]
fn generated_rust_contains_source_transition() {
    let output = render_rust(&fixture(), "fixture-digest");
    assert!(output.contains("operation: \"claim\""));
    assert!(output.contains("attempt_change: AttemptChange::Increment"));
}

#[test]
fn production_and_generated_rust_are_identical() {
    let outputs = generated_outputs(&fixture(), "fixture-digest");
    assert_eq!(outputs[0].0, GENERATED_RUST);
    assert_eq!(outputs[1].0, CORE_RUST);
    assert_eq!(outputs[0].1, outputs[1].1);
}

#[test]
fn rejects_unknown_source_field() {
    for pointer in ["", "/transitions/0", "/exceptions/0", "/reentry/0"] {
        let mut input = fixture_value();
        input
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("extra".into(), Value::Bool(true));
        assert!(
            serde_json::from_value::<StateMachineSpec>(input).is_err(),
            "accepted unknown field at {pointer}"
        );
    }
}

#[test]
fn rejects_unknown_protocol_domains() {
    for (pointer, value) in [
        ("/transitions/0/operation", "invented_operation"),
        ("/transitions/0/source", "invented_state"),
        ("/transitions/0/reason_class", "invented_reason"),
        ("/transitions/0/required_syncs/0", "invented_sync"),
        ("/exceptions/0/name", "invented_exception"),
        ("/reentry/0/name", "invented_reentry"),
    ] {
        let mut input = fixture_value();
        *input.pointer_mut(pointer).unwrap() = Value::String(value.into());
        assert!(
            serde_json::from_value::<StateMachineSpec>(input).is_err(),
            "{pointer} accepted {value}"
        );
    }
}

#[test]
fn rejects_omitted_semantic_fields() {
    for field in [
        "operation",
        "source",
        "destination",
        "generation_change",
        "attempt_change",
        "token_change",
        "reason_class",
        "required_syncs",
        "no_overwrite",
        "resolution_behavior",
        "notes",
    ] {
        let mut input = fixture_value();
        input["transitions"][0]
            .as_object_mut()
            .unwrap()
            .remove(field);
        assert!(
            serde_json::from_value::<StateMachineSpec>(input).is_err(),
            "accepted transition without {field}"
        );
    }

    for field in ["name", "description", "uses_replacing_rename"] {
        let mut input = fixture_value();
        input["exceptions"][0]
            .as_object_mut()
            .unwrap()
            .remove(field);
        assert!(
            serde_json::from_value::<StateMachineSpec>(input).is_err(),
            "accepted exception without {field}"
        );
    }

    for field in ["name", "source", "description", "creates_new_identity"] {
        let mut input = fixture_value();
        input["reentry"][0].as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<StateMachineSpec>(input).is_err(),
            "accepted reentry without {field}"
        );
    }

    for field in ["transitions", "exceptions", "reentry"] {
        let mut input = fixture_value();
        input.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<StateMachineSpec>(input).is_err(),
            "accepted root without {field}"
        );
    }
}

#[test]
fn rejects_non_destination_state() {
    let mut spec = fixture();
    spec.transitions[0].destination = State::Active;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "transition enqueue_immediate has unknown destination state active"
    );
}

#[test]
fn rejects_non_terminal_reentry_source() {
    let mut spec = fixture();
    spec.reentry[0].source = State::Ready;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "reentry requeue_dead has source state ready; expected dead"
    );
}

#[test]
fn rejects_each_cross_field_transition_mutation() {
    for (pointer, value) in [
        ("/transitions/3/source", serde_json::json!("dead")),
        ("/transitions/3/destination", serde_json::json!("receipt")),
        (
            "/transitions/3/generation_change",
            serde_json::json!("zero"),
        ),
        (
            "/transitions/3/attempt_change",
            serde_json::json!("unchanged"),
        ),
        ("/transitions/3/token_change", serde_json::json!("none")),
        (
            "/transitions/3/reason_class",
            serde_json::json!("attempts_exhausted"),
        ),
        (
            "/transitions/3/required_syncs",
            serde_json::json!(["source_dir_fsync", "destination_dir_fsync"]),
        ),
    ] {
        let mut input = fixture_value();
        *input.pointer_mut(pointer).unwrap() = value;
        let spec: StateMachineSpec = serde_json::from_value(input).unwrap();
        let error = validate_spec(&spec).unwrap_err();
        assert!(
            error.starts_with("transition claim has "),
            "{pointer} produced unexpected validation error: {error}"
        );
    }
}

#[test]
fn rejects_mismatched_reentry_name_and_source() {
    let mut input = fixture_value();
    input["reentry"][0]["source"] = serde_json::json!("quarantine");
    let spec: StateMachineSpec = serde_json::from_value(input).unwrap();
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "reentry requeue_dead has source state quarantine; expected dead"
    );
}

#[test]
fn rejects_missing_closed_domain_members() {
    let mut spec = fixture();
    spec.transitions
        .retain(|transition| transition.operation != Operation::Quarantine);
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "state-machine spec is missing transition operation: quarantine"
    );

    let mut spec = fixture();
    spec.exceptions
        .retain(|exception| exception.name != ExceptionName::WallWatermarkAdvancement);
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "state-machine spec is missing exception: wall_watermark_advancement"
    );

    let mut spec = fixture();
    spec.reentry
        .retain(|reentry| reentry.name != ReentryName::RequeueQuarantine);
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "state-machine spec is missing reentry: requeue_quarantine"
    );
}

#[test]
fn schema_closes_every_object() {
    let schema: Value =
        serde_json::from_str(include_str!("../../../spec/state-machine.schema.json")).unwrap();
    validate_schema_value(&schema, "#").unwrap();
    assert_eq!(
        schema["required"],
        serde_json::json!(["transitions", "exceptions", "reentry"])
    );
}

#[test]
fn schema_validation_rejects_open_objects() {
    let temp = temporary_workspace();
    let mut schema: Value =
        serde_json::from_str(include_str!("../../../spec/state-machine.schema.json")).unwrap();
    schema["additionalProperties"] = Value::Bool(true);
    fs::write(
        temp.path().join(SCHEMA),
        serde_json::to_vec_pretty(&schema).unwrap(),
    )
    .unwrap();
    let error = validate_schema(temp.path()).unwrap_err();
    assert!(
        error.starts_with("spec/state-machine.schema.json contract digest differs:"),
        "unexpected schema validation error: {error}"
    );

    assert_eq!(
        validate_schema_value(&schema, "#").unwrap_err(),
        "spec/state-machine.schema.json object at # must set additionalProperties to false"
    );
    let nested = serde_json::json!([{"type": "object"}]);
    assert_eq!(
        validate_schema_value(&nested, "#").unwrap_err(),
        "spec/state-machine.schema.json object at #/0 must set additionalProperties to false"
    );
}

#[test]
fn rejects_schema_domain_drift() {
    let mut schema: Value =
        serde_json::from_str(include_str!("../../../spec/state-machine.schema.json")).unwrap();
    schema["properties"]["transitions"]["items"]["properties"]["operation"]["enum"][0] =
        Value::String("invented_operation".into());
    let error = validate_schema_domain(
        &schema,
        "/properties/transitions/items/properties/operation/enum",
        &Operation::ALL.map(Operation::as_str),
    )
    .unwrap_err();
    assert!(
        error.contains("operation/enum differs from xtask"),
        "unexpected schema validation error: {error}"
    );
}
