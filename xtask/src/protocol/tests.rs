use super::render::*;
use super::schema::{
    validate_schema, validate_schema_const, validate_schema_domain, validate_schema_u64_const,
    validate_schema_value, SCHEMA,
};
use super::*;
use crate::{check_all, check_target, dispatch_command, run_command, workspace_root};
use serde_json::Value;
use std::path::Path;
use std::process::Command;
use tempfile::TempDir;

fn fixture() -> StateMachineSpec {
    serde_json::from_value(fixture_value()).unwrap()
}

fn fixture_value() -> Value {
    serde_json::from_str(include_str!("../../../spec/state-machine.json")).unwrap()
}

#[test]
fn rejects_schema_const_drift() {
    let schema = serde_json::json!({"outcome": {"const": "not_committed"}});
    assert!(validate_schema_const(&schema, "/outcome/const", "not_committed").is_ok());
    assert_eq!(
        validate_schema_const(&schema, "/outcome/const", "outcome_unknown").unwrap_err(),
        "spec/state-machine.schema.json const at /outcome/const differs from xtask: expected outcome_unknown, got not_committed"
    );
    assert_eq!(
        validate_schema_const(&schema, "/missing/const", "not_committed").unwrap_err(),
        "spec/state-machine.schema.json has no string const at /missing/const"
    );
}

#[test]
fn rejects_schema_integer_const_drift() {
    let schema = serde_json::json!({"version": {"const": 1}});
    assert!(validate_schema_u64_const(&schema, "/version/const", 1).is_ok());
    assert_eq!(
        validate_schema_u64_const(&schema, "/version/const", 3).unwrap_err(),
        "spec/state-machine.schema.json integer at /version/const differs from xtask: expected 3, got 1"
    );
    assert_eq!(
        validate_schema_u64_const(&schema, "/missing/const", 1).unwrap_err(),
        "spec/state-machine.schema.json has no unsigned integer at /missing/const"
    );
}

fn temporary_workspace() -> TempDir {
    let temp = tempfile::tempdir().unwrap();
    for directory in [
        "spec",
        "generated",
        "crates/steadq-core/src",
        ".github",
        "docs",
        "model",
        "model/namespace",
        "model/scheduling",
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
            GENERATED_TLA,
            include_str!("../../../model/SteadQProtocol.tla"),
        ),
        (
            "model/SteadQ.tla",
            include_str!("../../../model/SteadQ.tla"),
        ),
        (
            "model/SteadQ.cfg",
            include_str!("../../../model/SteadQ.cfg"),
        ),
        ("model/README.md", include_str!("../../../model/README.md")),
        (
            "model/SteadQNamespace.tla",
            include_str!("../../../model/SteadQNamespace.tla"),
        ),
        (
            "model/namespace/SteadQNamespaceOrdered.cfg",
            include_str!("../../../model/namespace/SteadQNamespaceOrdered.cfg"),
        ),
        (
            "model/namespace/SteadQNamespaceWeak.cfg",
            include_str!("../../../model/namespace/SteadQNamespaceWeak.cfg"),
        ),
        (
            "model/namespace/README.md",
            include_str!("../../../model/namespace/README.md"),
        ),
        (
            "model/SteadQScheduling.tla",
            include_str!("../../../model/SteadQScheduling.tla"),
        ),
        (
            "model/scheduling/SteadQScheduling.cfg",
            include_str!("../../../model/scheduling/SteadQScheduling.cfg"),
        ),
        (
            "model/scheduling/README.md",
            include_str!("../../../model/scheduling/README.md"),
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
    let spec = fixture();
    assert_eq!(spec.protocol, PROTOCOL_IR_IDENTITY);
    assert_eq!(spec.version, PROTOCOL_IR_VERSION);
    validate_spec(&spec).unwrap();
}

#[test]
fn rejects_unsupported_protocol_ir_identity_and_version() {
    let mut spec = fixture();
    spec.protocol = "another-protocol".into();
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "unsupported protocol IR identity: expected steadq-state-machine, got another-protocol"
    );

    for version in [
        0,
        PROTOCOL_IR_VERSION - 1,
        PROTOCOL_IR_VERSION + 1,
        u32::MAX,
    ] {
        let mut spec = fixture();
        spec.version = version;
        assert_eq!(
            validate_spec(&spec).unwrap_err(),
            format!("unsupported protocol IR version: expected 3, got {version}")
        );
    }
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
    for artifact in [
        GENERATED_RUST,
        GENERATED_GO,
        GENERATED_MARKDOWN,
        GENERATED_TLA,
        CORE_RUST,
    ] {
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
            GENERATED_TLA,
            include_str!("../../../model/SteadQProtocol.tla"),
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
        source_object_kind: ObjectKind::FullJob,
        destination: State::Leased,
        destination_object_kind: ObjectKind::FullJob,
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Increment,
        token_change: TokenChange::New,
        reason_class: Nullable::Null,
        clock_requirement: ClockRequirement::BoottimeAndAuthenticatedWallFloor,
        required_syncs: vec![SyncStep::DestinationDir],
        linearization: LinearizationPrimitive::RenameNoreplace,
        before_linearization_failure: FailureOutcome::NotCommitted,
        after_linearization_failure: FailureOutcome::OutcomeUnknown,
        resolver_probe_topology: ResolverProbeTopology::SourceAndDestination,
        qualification: TransitionQualification::None,
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
fn rejects_wrong_linearization_primitive() {
    let mut spec = fixture();
    spec.transitions[0].linearization = LinearizationPrimitive::RenameNoreplace;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "transition enqueue_immediate has linearization rename_noreplace; expected publish_noreplace"
    );
}

#[test]
fn rejects_wrong_linearization_failure_outcomes() {
    let mut spec = fixture();
    spec.transitions[0].before_linearization_failure = FailureOutcome::OutcomeUnknown;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "transition enqueue_immediate must classify pre-linearization failure as not_committed"
    );

    let mut spec = fixture();
    spec.transitions[0].after_linearization_failure = FailureOutcome::NotCommitted;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "transition enqueue_immediate must classify post-linearization failure as outcome_unknown"
    );
}

#[test]
fn rejects_wrong_clock_requirements() {
    let mut spec = fixture();
    spec.transitions[3].clock_requirement = ClockRequirement::AuthenticatedWallFloor;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "transition claim has clock requirement authenticated_wall_floor; expected boottime_and_authenticated_wall_floor"
    );

    let mut spec = fixture();
    spec.exceptions[0].clock_requirement = ClockRequirement::AuthenticatedWallFloor;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "exception receipt_compaction has clock requirement authenticated_wall_floor; expected none"
    );
}

#[test]
fn rejects_wrong_transition_qualifications() {
    let mut spec = fixture();
    spec.transitions[10].qualification = TransitionQualification::AttemptsExhausted;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "transition reap_expired_to_ready has qualification attempts_exhausted; expected attempts_remaining"
    );

    let mut spec = fixture();
    spec.transitions[12].qualification = TransitionQualification::None;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "transition quarantine has qualification none; expected raw_bytes_preserved"
    );
}

#[test]
fn rejects_wrong_resolver_probe_topology() {
    let mut spec = fixture();
    spec.transitions[0].resolver_probe_topology = ResolverProbeTopology::SourceAndDestination;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "transition enqueue_immediate has resolver probe topology source_and_destination; expected destination_only"
    );

    let mut spec = fixture();
    spec.transitions[6].resolver_probe_topology = ResolverProbeTopology::SourceAndDestination;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "transition acknowledge has resolver probe topology source_and_destination; expected receipt_candidates_and_source"
    );
}

#[test]
fn rejects_wrong_object_kinds() {
    let mut spec = fixture();
    spec.transitions[6].source_object_kind = ObjectKind::FullReceipt;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "transition acknowledge has source object kind full_receipt; expected full_job"
    );

    let mut spec = fixture();
    spec.transitions[6].destination_object_kind = ObjectKind::CompactReceipt;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "transition acknowledge has destination object kind compact_receipt; expected full_receipt"
    );

    let mut spec = fixture();
    spec.exceptions[0].source_object_kind = ObjectKind::CompactReceipt;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "exception receipt_compaction has source object kind compact_receipt; expected full_receipt"
    );

    let mut spec = fixture();
    spec.exceptions[1].destination_object_kind = ObjectKind::CompactReceipt;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "exception wall_watermark_advancement has destination object kind compact_receipt; expected watermark_record"
    );
}

#[test]
fn rejects_wrong_exception_mutation_semantics() {
    let mut spec = fixture();
    spec.exceptions[0].mutation_class = MutationClass::Unlink;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "exception receipt_compaction has mutation class unlink; expected replacing_move"
    );

    let mut spec = fixture();
    spec.exceptions[0].linearization = LinearizationPrimitive::RenameNoreplace;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "exception receipt_compaction has linearization rename_noreplace; expected rename_replace"
    );

    let mut spec = fixture();
    spec.exceptions[0].required_syncs = vec![SyncStep::File];
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "exception receipt_compaction has syncs [\"file_fsync\"]; expected [\"file_fsync\", \"same_or_destination_dir_fsync\"]"
    );

    let mut spec = fixture();
    spec.exceptions[0].before_linearization_failure = FailureOutcome::OutcomeUnknown;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "exception receipt_compaction must classify pre-linearization failure as not_committed"
    );

    let mut spec = fixture();
    spec.exceptions[0].after_linearization_failure = FailureOutcome::NotCommitted;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "exception receipt_compaction must classify post-linearization failure as outcome_unknown"
    );
}

#[test]
fn rejects_duplicate_exception_sync() {
    let mut spec = fixture();
    spec.exceptions[0]
        .required_syncs
        .push(SyncStep::SameOrDestinationDir);
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "exception receipt_compaction contains duplicate sync same_or_destination_dir_fsync"
    );
}

#[test]
fn rejects_wrong_receipt_retention_unlink_semantics() {
    let mut spec = fixture();
    spec.unlinks[0].source = State::Dead;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "unlink full_receipt_retention_deletion has source state dead; expected receipt"
    );

    let mut spec = fixture();
    spec.unlinks[0].source_object_kind = ObjectKind::CompactReceipt;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "unlink full_receipt_retention_deletion has source object kind compact_receipt; expected full_receipt"
    );

    let mut spec = fixture();
    spec.unlinks[0].source_authentication = SourceAuthentication::None;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "unlink full_receipt_retention_deletion has source authentication none; expected strict_receipt"
    );

    let mut spec = fixture();
    spec.unlinks[0].clock_requirement = ClockRequirement::None;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "unlink full_receipt_retention_deletion has clock requirement none; expected authenticated_wall_floor"
    );

    let mut spec = fixture();
    spec.unlinks[0].qualification = TransitionQualification::None;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "unlink full_receipt_retention_deletion has qualification none; expected receipt_bucket_end_plus_retention_not_after_wall_floor"
    );

    let mut spec = fixture();
    spec.unlinks[0].mutation_class = MutationClass::ReplacingMove;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "unlink full_receipt_retention_deletion has mutation class replacing_move; expected unlink"
    );

    let mut spec = fixture();
    spec.unlinks[0].linearization = LinearizationPrimitive::RenameReplace;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "unlink full_receipt_retention_deletion has linearization rename_replace; expected unlink"
    );

    let mut spec = fixture();
    spec.unlinks[0].required_syncs = vec![SyncStep::DestinationDir];
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "unlink full_receipt_retention_deletion has syncs [\"destination_dir_fsync\"]; expected [\"source_dir_fsync\"]"
    );

    let mut spec = fixture();
    spec.unlinks[0].before_linearization_failure = FailureOutcome::OutcomeUnknown;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "unlink full_receipt_retention_deletion must classify pre-linearization failure as not_committed"
    );

    let mut spec = fixture();
    spec.unlinks[0].after_linearization_failure = FailureOutcome::NotCommitted;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "unlink full_receipt_retention_deletion must classify post-linearization failure as outcome_unknown"
    );

    let mut spec = fixture();
    spec.unlinks[0].resolver_probe_topology = ResolverProbeTopology::SourceAndDestination;
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "unlink full_receipt_retention_deletion has resolver probe topology source_and_destination; expected source_presence"
    );
}

#[test]
fn rejects_duplicate_unlink_sync() {
    let mut spec = fixture();
    spec.unlinks[0].required_syncs.push(SyncStep::SourceDir);
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "unlink full_receipt_retention_deletion contains duplicate sync source_dir_fsync"
    );
}

#[test]
fn renew_requires_conditional_source_directory_barrier_in_order() {
    for (syncs, expected) in [
        (
            vec![SyncStep::SameOrDestinationDir],
            "transition renew has syncs [\"same_or_destination_dir_fsync\"]; expected [\"same_or_destination_dir_fsync\", \"source_dir_fsync_if_distinct\"]",
        ),
        (
            vec![
                SyncStep::SourceDirIfDistinct,
                SyncStep::SameOrDestinationDir,
            ],
            "transition renew has syncs [\"source_dir_fsync_if_distinct\", \"same_or_destination_dir_fsync\"]; expected [\"same_or_destination_dir_fsync\", \"source_dir_fsync_if_distinct\"]",
        ),
    ] {
        let mut spec = fixture();
        spec.transitions
            .iter_mut()
            .find(|transition| transition.operation == Operation::Renew)
            .unwrap()
            .required_syncs = syncs;
        assert_eq!(validate_spec(&spec).unwrap_err(), expected);
    }

    let mut spec = fixture();
    spec.transitions
        .iter_mut()
        .find(|transition| transition.operation == Operation::Renew)
        .unwrap()
        .required_syncs
        .push(SyncStep::SourceDirIfDistinct);
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "transition renew contains duplicate sync source_dir_fsync_if_distinct"
    );
}

#[test]
fn generated_rust_contains_source_transition() {
    let output = render_rust(&fixture(), "fixture-digest");
    assert!(output.contains("operation: Operation::Claim"));
    assert!(output.contains("source_object_kind: ObjectKind::FullJob"));
    assert!(output.contains("destination_object_kind: ObjectKind::FullReceipt"));
    assert!(output.contains("attempt_change: AttemptChange::Increment"));
    assert!(output
        .contains("required_syncs: &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory]"));
    assert!(output.contains("pub const EXCEPTIONS: &[ExceptionDef]"));
    assert!(output.contains("linearization: LinearizationPrimitive::RenameReplace"));
    assert!(output.contains("pub const UNLINKS: &[UnlinkDef]"));
    assert!(output.contains("name: UnlinkName::FullReceiptRetentionDeletion"));
    assert!(output.contains("linearization: LinearizationPrimitive::Unlink"));
    assert!(output.contains("source: State::Receipt"));
    assert!(output.contains("source_authentication: SourceAuthentication::StrictReceipt"));
    assert!(output.contains(
        "qualification: TransitionQualification::ReceiptBucketEndPlusRetentionNotAfterWallFloor"
    ));
    assert!(output.contains("resolver_probe_topology: ResolverProbeTopology::SourcePresence"));
    assert!(
        output.contains("clock_requirement: ClockRequirement::BoottimeAndAuthenticatedWallFloor")
    );
    assert!(output.contains("pub const REENTRY: &[ReentryDef]"));
}

#[test]
fn generated_tla_contains_complete_protocol_metadata() {
    let output = render_tla(&fixture(), "fixture-digest");
    assert!(output.contains("MODULE SteadQProtocol"));
    assert!(output.contains("Source SHA-256: fixture-digest"));
    assert!(output.contains("ProtocolIRIdentity == \"steadq-state-machine\""));
    assert!(output.contains("ProtocolIRVersion == 3"));
    assert!(output.contains("ProtocolOperations =="));
    assert!(output.contains("ProtocolStates =="));
    let operations = Operation::ALL
        .map(|value| format!("Operation{}", value.rust_name()))
        .join(", ");
    assert!(output.contains(&format!("ProtocolOperations == {{{operations}}}")));
    let states = State::ALL
        .map(|value| format!("State{}", value.rust_name()))
        .join(", ");
    assert!(output.contains(&format!("ProtocolStates == {{{states}}}")));
    assert_eq!(
        output.matches("    [operation |->").count(),
        Operation::ALL.len()
    );
    assert_eq!(
        output.matches("    [name |-> Exception").count(),
        ExceptionName::ALL.len()
    );
    assert_eq!(
        output.matches("    [name |-> Unlink").count(),
        UnlinkName::ALL.len()
    );
    assert!(output.contains("ProtocolUnlinks == <<"));
    assert!(output.contains("SourceAuthenticationStrictReceipt"));
    assert!(
        output.contains("TransitionQualificationReceiptBucketEndPlusRetentionNotAfterWallFloor")
    );
    assert!(output.contains("ResolverProbeTopologySourcePresence"));
    assert_eq!(
        output.matches("    [name |-> Reentry").count(),
        ReentryName::ALL.len()
    );
    assert!(output.contains("requiredSyncs |-> <<SyncStepFile, SyncStepDestinationDirectory>>"));
    assert!(output.contains("descriptionUtf8Hex |->"));
}

#[test]
fn generated_tla_hex_encodes_arbitrary_description_bytes() {
    let mut spec = fixture();
    spec.exceptions[0].description = "nul:\u{0}; unicode:\u{1f642}".into();
    let output = render_tla(&spec, "fixture-digest");
    assert!(output.contains("descriptionUtf8Hex |-> \"6e756c3a003b20756e69636f64653af09f9982\""));
    assert!(!output.contains('\u{0}'));
    assert!(!output.contains('\u{1f642}'));
}

#[test]
fn generated_rust_compiles_schema_valid_control_characters() {
    let mut spec = fixture();
    spec.exceptions[0].description = "exception\u{0}description".into();
    spec.reentry[0].description = "reentry\rdescription".into();

    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("generated.rs");
    fs::write(&source, render_rust(&spec, "fixture-digest")).unwrap();
    let output = Command::new("rustc")
        .args(["--crate-type", "lib", "--edition", "2021"])
        .arg(&source)
        .arg("-o")
        .arg(temp.path().join("generated.rlib"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "generated Rust did not compile: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_every_projection_changes(mutate: impl FnOnce(&mut StateMachineSpec)) {
    let mut changed = fixture();
    let baseline = generated_outputs(&changed, "fixture-digest");
    mutate(&mut changed);
    let changed = generated_outputs(&changed, "fixture-digest");
    for ((baseline_path, baseline), (changed_path, changed)) in baseline.iter().zip(&changed) {
        assert_eq!(baseline_path, changed_path);
        assert_ne!(
            baseline, changed,
            "{baseline_path} omitted a protocol IR field"
        );
    }
}

#[test]
fn every_transition_field_affects_every_projection() {
    assert_every_projection_changes(|spec| {
        spec.transitions[0].operation = Operation::Claim;
    });
    assert_every_projection_changes(|spec| {
        spec.transitions[0].source = State::Ready;
    });
    assert_every_projection_changes(|spec| {
        spec.transitions[0].source_object_kind = ObjectKind::RawObject;
    });
    assert_every_projection_changes(|spec| {
        spec.transitions[0].destination = State::Delayed;
    });
    assert_every_projection_changes(|spec| {
        spec.transitions[0].destination_object_kind = ObjectKind::FullReceipt;
    });
    assert_every_projection_changes(|spec| {
        spec.transitions[0].generation_change = GenerationChange::Increment;
    });
    assert_every_projection_changes(|spec| {
        spec.transitions[0].attempt_change = AttemptChange::Increment;
    });
    assert_every_projection_changes(|spec| {
        spec.transitions[0].token_change = TokenChange::New;
    });
    assert_every_projection_changes(|spec| {
        spec.transitions[0].reason_class = Nullable::Value(ReasonClass::Corruption);
    });
    assert_every_projection_changes(|spec| {
        spec.transitions[0].clock_requirement = ClockRequirement::None;
    });
    assert_every_projection_changes(|spec| {
        spec.transitions[0].required_syncs = vec![SyncStep::SourceDir];
    });
    assert_every_projection_changes(|spec| {
        spec.transitions[0].linearization = LinearizationPrimitive::RenameNoreplace;
    });
    assert_every_projection_changes(|spec| {
        spec.transitions[0].before_linearization_failure = FailureOutcome::OutcomeUnknown;
    });
    assert_every_projection_changes(|spec| {
        spec.transitions[0].after_linearization_failure = FailureOutcome::NotCommitted;
    });
    assert_every_projection_changes(|spec| {
        spec.transitions[0].resolver_probe_topology = ResolverProbeTopology::SourceAndDestination;
    });
    assert_every_projection_changes(|spec| {
        spec.transitions[0].qualification = TransitionQualification::AttemptsRemaining;
    });
}

#[test]
fn protocol_ir_identity_and_version_affect_every_projection() {
    assert_every_projection_changes(|spec| {
        spec.protocol = "different-protocol".into();
    });
    assert_every_projection_changes(|spec| {
        spec.version = PROTOCOL_IR_VERSION + 1;
    });
}

#[test]
fn every_exception_field_affects_every_projection() {
    assert_every_projection_changes(|spec| {
        spec.exceptions[0].name = ExceptionName::WallWatermarkAdvancement;
    });
    assert_every_projection_changes(|spec| {
        spec.exceptions[0].description = "different exception documentation".into();
    });
    assert_every_projection_changes(|spec| {
        spec.exceptions[0].source_object_kind = ObjectKind::CompactReceipt;
    });
    assert_every_projection_changes(|spec| {
        spec.exceptions[0].destination_object_kind = ObjectKind::FullReceipt;
    });
    assert_every_projection_changes(|spec| {
        spec.exceptions[0].clock_requirement = ClockRequirement::AuthenticatedWallFloor;
    });
    assert_every_projection_changes(|spec| {
        spec.exceptions[0].mutation_class = MutationClass::Unlink;
    });
    assert_every_projection_changes(|spec| {
        spec.exceptions[0].linearization = LinearizationPrimitive::RenameNoreplace;
    });
    assert_every_projection_changes(|spec| {
        spec.exceptions[0].required_syncs = vec![SyncStep::SourceDir];
    });
    assert_every_projection_changes(|spec| {
        spec.exceptions[0].before_linearization_failure = FailureOutcome::OutcomeUnknown;
    });
    assert_every_projection_changes(|spec| {
        spec.exceptions[0].after_linearization_failure = FailureOutcome::NotCommitted;
    });
}

#[test]
fn every_unlink_field_affects_every_projection() {
    assert_every_projection_changes(|spec| {
        spec.unlinks[0].name = UnlinkName::CompactReceiptRetentionDeletion;
    });
    assert_every_projection_changes(|spec| {
        spec.unlinks[0].description = "different unlink documentation".into();
    });
    assert_every_projection_changes(|spec| {
        spec.unlinks[0].source = State::Dead;
    });
    assert_every_projection_changes(|spec| {
        spec.unlinks[0].source_object_kind = ObjectKind::CompactReceipt;
    });
    assert_every_projection_changes(|spec| {
        spec.unlinks[0].source_authentication = SourceAuthentication::None;
    });
    assert_every_projection_changes(|spec| {
        spec.unlinks[0].clock_requirement = ClockRequirement::None;
    });
    assert_every_projection_changes(|spec| {
        spec.unlinks[0].qualification = TransitionQualification::None;
    });
    assert_every_projection_changes(|spec| {
        spec.unlinks[0].mutation_class = MutationClass::ReplacingMove;
    });
    assert_every_projection_changes(|spec| {
        spec.unlinks[0].linearization = LinearizationPrimitive::RenameReplace;
    });
    assert_every_projection_changes(|spec| {
        spec.unlinks[0].required_syncs = vec![SyncStep::DestinationDir];
    });
    assert_every_projection_changes(|spec| {
        spec.unlinks[0].before_linearization_failure = FailureOutcome::OutcomeUnknown;
    });
    assert_every_projection_changes(|spec| {
        spec.unlinks[0].after_linearization_failure = FailureOutcome::NotCommitted;
    });
    assert_every_projection_changes(|spec| {
        spec.unlinks[0].resolver_probe_topology = ResolverProbeTopology::SourceAndDestination;
    });
}

#[test]
fn every_reentry_field_affects_every_projection() {
    assert_every_projection_changes(|spec| {
        spec.reentry[0].name = ReentryName::RequeueQuarantine;
    });
    assert_every_projection_changes(|spec| {
        spec.reentry[0].source = State::Quarantine;
    });
    assert_every_projection_changes(|spec| {
        spec.reentry[0].description = "different reentry documentation".into();
    });
    assert_every_projection_changes(|spec| {
        spec.reentry[0].creates_new_identity = false;
    });
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
    for pointer in [
        "",
        "/transitions/0",
        "/exceptions/0",
        "/unlinks/0",
        "/reentry/0",
    ] {
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
        ("/transitions/0/source_object_kind", "invented_object_kind"),
        (
            "/transitions/0/destination_object_kind",
            "invented_object_kind",
        ),
        ("/transitions/0/reason_class", "invented_reason"),
        ("/transitions/0/clock_requirement", "invented_clock"),
        ("/transitions/0/qualification", "invented_qualification"),
        (
            "/transitions/0/resolver_probe_topology",
            "invented_probe_topology",
        ),
        ("/transitions/0/required_syncs/0", "invented_sync"),
        ("/transitions/0/linearization", "invented_linearization"),
        (
            "/transitions/0/before_linearization_failure",
            "invented_outcome",
        ),
        ("/exceptions/0/name", "invented_exception"),
        ("/exceptions/0/source_object_kind", "invented_object_kind"),
        (
            "/exceptions/0/destination_object_kind",
            "invented_object_kind",
        ),
        ("/exceptions/0/clock_requirement", "invented_clock"),
        ("/exceptions/0/mutation_class", "invented_mutation_class"),
        ("/exceptions/0/linearization", "invented_linearization"),
        ("/unlinks/0/name", "invented_unlink"),
        ("/unlinks/0/source", "invented_state"),
        ("/unlinks/0/source_object_kind", "invented_object_kind"),
        (
            "/unlinks/0/source_authentication",
            "invented_authentication",
        ),
        ("/unlinks/0/clock_requirement", "invented_clock"),
        ("/unlinks/0/qualification", "invented_qualification"),
        ("/unlinks/0/mutation_class", "invented_mutation_class"),
        ("/unlinks/0/linearization", "invented_linearization"),
        ("/unlinks/0/required_syncs/0", "invented_sync"),
        ("/unlinks/0/resolver_probe_topology", "invented_resolver"),
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
        "source_object_kind",
        "destination",
        "destination_object_kind",
        "generation_change",
        "attempt_change",
        "token_change",
        "reason_class",
        "clock_requirement",
        "required_syncs",
        "linearization",
        "before_linearization_failure",
        "after_linearization_failure",
        "resolver_probe_topology",
        "qualification",
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

    for field in [
        "name",
        "description",
        "source_object_kind",
        "destination_object_kind",
        "clock_requirement",
        "mutation_class",
        "linearization",
        "required_syncs",
        "before_linearization_failure",
        "after_linearization_failure",
    ] {
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

    for field in [
        "name",
        "description",
        "source",
        "source_object_kind",
        "source_authentication",
        "clock_requirement",
        "qualification",
        "mutation_class",
        "linearization",
        "required_syncs",
        "before_linearization_failure",
        "after_linearization_failure",
        "resolver_probe_topology",
    ] {
        let mut input = fixture_value();
        input["unlinks"][0].as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<StateMachineSpec>(input).is_err(),
            "accepted unlink without {field}"
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

    for field in [
        "protocol",
        "version",
        "transitions",
        "exceptions",
        "unlinks",
        "reentry",
    ] {
        let mut input = fixture_value();
        input.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<StateMachineSpec>(input).is_err(),
            "accepted root without {field}"
        );
    }
}

#[test]
fn rejects_non_u32_protocol_ir_versions() {
    for version in [
        Value::String("1".into()),
        serde_json::json!(-1),
        serde_json::json!(1.5),
    ] {
        let mut input = fixture_value();
        input["version"] = version;
        assert!(
            serde_json::from_value::<StateMachineSpec>(input).is_err(),
            "accepted a noninteger protocol IR version"
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
        (
            "/transitions/3/source_object_kind",
            serde_json::json!("raw_object"),
        ),
        ("/transitions/3/destination", serde_json::json!("receipt")),
        (
            "/transitions/3/destination_object_kind",
            serde_json::json!("full_receipt"),
        ),
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
            "/transitions/3/clock_requirement",
            serde_json::json!("authenticated_wall_floor"),
        ),
        (
            "/transitions/3/qualification",
            serde_json::json!("attempts_remaining"),
        ),
        (
            "/transitions/3/resolver_probe_topology",
            serde_json::json!("destination_only"),
        ),
        (
            "/transitions/3/reason_class",
            serde_json::json!("attempts_exhausted"),
        ),
        (
            "/transitions/3/required_syncs",
            serde_json::json!(["source_dir_fsync", "destination_dir_fsync"]),
        ),
        (
            "/transitions/3/linearization",
            serde_json::json!("publish_noreplace"),
        ),
        (
            "/transitions/3/before_linearization_failure",
            serde_json::json!("outcome_unknown"),
        ),
        (
            "/transitions/3/after_linearization_failure",
            serde_json::json!("not_committed"),
        ),
    ] {
        let mut input = fixture_value();
        *input.pointer_mut(pointer).unwrap() = value;
        let spec: StateMachineSpec = serde_json::from_value(input).unwrap();
        let error = validate_spec(&spec).unwrap_err();
        assert!(
            error.starts_with("transition claim "),
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
    spec.unlinks
        .retain(|unlink| unlink.name != UnlinkName::CompactReceiptRetentionDeletion);
    assert_eq!(
        validate_spec(&spec).unwrap_err(),
        "state-machine spec is missing unlink: compact_receipt_retention_deletion"
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
        serde_json::json!([
            "protocol",
            "version",
            "transitions",
            "exceptions",
            "unlinks",
            "reentry"
        ])
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
