use super::*;
use std::fmt::Write as _;

pub(super) const GENERATED_RUST: &str = "generated/state-machine.rs";
pub(super) const GENERATED_GO: &str = "generated/state-machine.go";
pub(super) const GENERATED_MARKDOWN: &str = "generated/state-machine.md";
pub(super) const CORE_RUST: &str = "crates/steadq-core/src/state_machine.rs";

pub(crate) fn check_generated(root: &Path) -> Result<(), String> {
    let (spec, digest) = load_spec(root)?;
    let expected = generated_outputs(&spec, &digest);
    for (artifact, expected_contents) in &expected {
        let actual = fs::read_to_string(root.join(artifact))
            .map_err(|error| format!("cannot read {artifact}: {error}"))?;
        if actual != *expected_contents {
            return Err(format!(
                "{artifact} differs from {SPEC}; run cargo xtask generate"
            ));
        }
    }

    Ok(())
}

pub(crate) fn generate(root: &Path) -> Result<(), String> {
    let (spec, digest) = load_spec(root)?;
    for (artifact, contents) in generated_outputs(&spec, &digest) {
        fs::write(root.join(artifact), contents)
            .map_err(|error| format!("cannot write {artifact}: {error}"))?;
    }
    Ok(())
}

pub(super) fn generated_outputs(
    spec: &StateMachineSpec,
    digest: &str,
) -> Vec<(&'static str, String)> {
    let rust = render_rust(spec, digest);
    vec![
        (GENERATED_RUST, rust.clone()),
        (CORE_RUST, rust),
        (GENERATED_GO, render_go(spec, digest)),
        (GENERATED_MARKDOWN, render_markdown(spec, digest)),
    ]
}

pub(super) fn render_rust(spec: &StateMachineSpec, digest: &str) -> String {
    let mut output = format!(
        "// Auto-generated from spec/state-machine.json. Do not edit by hand.\n// Source SHA-256: {digest}\n\n"
    );
    write_rust_enum(
        &mut output,
        "Operation",
        &Operation::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_rust_enum(
        &mut output,
        "State",
        &State::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_rust_enum(
        &mut output,
        "GenerationChange",
        &GenerationChange::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_rust_enum(
        &mut output,
        "AttemptChange",
        &AttemptChange::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_rust_enum(
        &mut output,
        "TokenChange",
        &TokenChange::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_rust_enum(
        &mut output,
        "ReasonClass",
        &ReasonClass::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_rust_enum(
        &mut output,
        "ClockRequirement",
        &ClockRequirement::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_rust_enum(
        &mut output,
        "TransitionQualification",
        &TransitionQualification::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_rust_enum(
        &mut output,
        "SyncStep",
        &SyncStep::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_rust_enum(
        &mut output,
        "LinearizationPrimitive",
        &LinearizationPrimitive::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_rust_enum(
        &mut output,
        "FailureOutcome",
        &FailureOutcome::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_rust_enum(
        &mut output,
        "MutationClass",
        &MutationClass::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_rust_enum(
        &mut output,
        "ExceptionName",
        &ExceptionName::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_rust_enum(
        &mut output,
        "ReentryName",
        &ReentryName::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    output.push_str(
        r#"#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransitionDef {
    pub operation: Operation,
    pub source: State,
    pub destination: State,
    pub generation_change: GenerationChange,
    pub attempt_change: AttemptChange,
    pub token_change: TokenChange,
    pub reason_class: Option<ReasonClass>,
    pub clock_requirement: ClockRequirement,
    pub required_syncs: &'static [SyncStep],
    pub linearization: LinearizationPrimitive,
    pub before_linearization_failure: FailureOutcome,
    pub after_linearization_failure: FailureOutcome,
    /// Human-readable resolver documentation, not an executable rule.
    pub resolution_behavior: &'static str,
    pub qualification: TransitionQualification,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExceptionDef {
    pub name: ExceptionName,
    pub description: &'static str,
    pub clock_requirement: ClockRequirement,
    pub mutation_class: MutationClass,
    pub linearization: LinearizationPrimitive,
    pub required_syncs: &'static [SyncStep],
    pub before_linearization_failure: FailureOutcome,
    pub after_linearization_failure: FailureOutcome,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReentryDef {
    pub name: ReentryName,
    pub source: State,
    pub description: &'static str,
    pub creates_new_identity: bool,
}

pub const TRANSITIONS: &[TransitionDef] = &[
"#,
    );
    for transition in &spec.transitions {
        let required_syncs = transition
            .required_syncs
            .iter()
            .map(|sync| format!("SyncStep::{}", sync.rust_name()))
            .collect::<Vec<_>>()
            .join(", ");
        let reason_class = match &transition.reason_class {
            Nullable::Value(reason) => format!("Some(ReasonClass::{})", reason.rust_name()),
            Nullable::Null => "None".into(),
        };
        let resolution_behavior = rust_literal(&transition.resolution_behavior);
        let resolution_field =
            if 8 + "resolution_behavior: ".len() + resolution_behavior.len() < 100 {
                format!("resolution_behavior: {resolution_behavior}")
            } else {
                format!("resolution_behavior:\n            {resolution_behavior}")
            };
        writeln!(
            output,
            "    TransitionDef {{\n        operation: Operation::{},\n        source: State::{},\n        destination: State::{},\n        generation_change: GenerationChange::{},\n        attempt_change: AttemptChange::{},\n        token_change: TokenChange::{},\n        reason_class: {},\n        clock_requirement: ClockRequirement::{},\n        required_syncs: &[{}],\n        linearization: LinearizationPrimitive::{},\n        before_linearization_failure: FailureOutcome::{},\n        after_linearization_failure: FailureOutcome::{},\n        {},\n        qualification: TransitionQualification::{},\n    }},",
            transition.operation.rust_name(),
            transition.source.rust_name(),
            transition.destination.rust_name(),
            transition.generation_change.rust_name(),
            transition.attempt_change.rust_name(),
            transition.token_change.rust_name(),
            reason_class,
            transition.clock_requirement.rust_name(),
            required_syncs,
            transition.linearization.rust_name(),
            transition.before_linearization_failure.rust_name(),
            transition.after_linearization_failure.rust_name(),
            resolution_field,
            transition.qualification.rust_name(),
        )
        .expect("writing to String cannot fail");
    }
    output.push_str("];\n\npub const EXCEPTIONS: &[ExceptionDef] = &[\n");
    for exception in &spec.exceptions {
        let required_syncs = exception
            .required_syncs
            .iter()
            .map(|sync| format!("SyncStep::{}", sync.rust_name()))
            .collect::<Vec<_>>()
            .join(", ");
        writeln!(
            output,
            "    ExceptionDef {{\n        name: ExceptionName::{},\n        description: {},\n        clock_requirement: ClockRequirement::{},\n        mutation_class: MutationClass::{},\n        linearization: LinearizationPrimitive::{},\n        required_syncs: &[{}],\n        before_linearization_failure: FailureOutcome::{},\n        after_linearization_failure: FailureOutcome::{},\n    }},",
            exception.name.rust_name(),
            rust_literal(&exception.description),
            exception.clock_requirement.rust_name(),
            exception.mutation_class.rust_name(),
            exception.linearization.rust_name(),
            required_syncs,
            exception.before_linearization_failure.rust_name(),
            exception.after_linearization_failure.rust_name(),
        )
        .expect("writing to String cannot fail");
    }
    output.push_str("];\n\npub const REENTRY: &[ReentryDef] = &[\n");
    for reentry in &spec.reentry {
        writeln!(
            output,
            "    ReentryDef {{\n        name: ReentryName::{},\n        source: State::{},\n        description: {},\n        creates_new_identity: {},\n    }},",
            reentry.name.rust_name(),
            reentry.source.rust_name(),
            rust_literal(&reentry.description),
            reentry.creates_new_identity,
        )
        .expect("writing to String cannot fail");
    }
    output.push_str(
        r#"];

/// Check if a transition from source to destination is legal.
pub fn is_legal_transition(source: State, destination: State) -> bool {
    TRANSITIONS
        .iter()
        .any(|transition| transition.source == source && transition.destination == destination)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legal_transitions() {
"#,
    );
    for transition in &spec.transitions {
        writeln!(
            output,
            "        assert!(is_legal_transition(State::{}, State::{}));",
            transition.source.rust_name(),
            transition.destination.rust_name(),
        )
        .expect("writing to String cannot fail");
    }
    writeln!(
        output,
        "    }}\n\n    #[test]\n    fn illegal_transitions() {{\n        for (source, destination) in [\n            (State::Receipt, State::Ready),\n            (State::Dead, State::Ready),\n            (State::Quarantine, State::Ready),\n            (State::Ready, State::Ready),\n            (State::Hidden, State::Leased),\n            (State::Ready, State::Receipt),\n        ] {{\n            assert!(!is_legal_transition(source, destination));\n        }}\n    }}\n\n    #[test]\n    fn generated_collections_are_complete() {{\n        assert_eq!(TRANSITIONS.len(), {});\n        assert_eq!(EXCEPTIONS.len(), {});\n        assert_eq!(REENTRY.len(), {});\n        assert!(TRANSITIONS\n            .iter()\n            .all(|transition| !transition.required_syncs.is_empty()));\n        assert!(TRANSITIONS\n            .iter()\n            .all(|transition| !transition.resolution_behavior.is_empty()));\n        assert!(TRANSITIONS.iter().all(|transition| {{\n            transition.before_linearization_failure == FailureOutcome::NotCommitted\n                && transition.after_linearization_failure == FailureOutcome::OutcomeUnknown\n        }}));\n        assert!(EXCEPTIONS.iter().all(|exception| exception.mutation_class\n            == MutationClass::ReplacingMove\n            && exception.linearization == LinearizationPrimitive::RenameReplace\n            && exception.required_syncs == [SyncStep::File, SyncStep::SameOrDestinationDirectory]\n            && exception.before_linearization_failure == FailureOutcome::NotCommitted\n            && exception.after_linearization_failure == FailureOutcome::OutcomeUnknown));\n        assert!(REENTRY.iter().all(|reentry| reentry.creates_new_identity));\n    }}\n\n    #[test]\n    fn claim_projects_complete_semantics() {{\n        let claim = TRANSITIONS\n            .iter()\n            .find(|transition| transition.operation == Operation::Claim)\n            .unwrap();\n        assert_eq!(claim.source, State::Ready);\n        assert_eq!(claim.destination, State::Leased);\n        assert_eq!(claim.attempt_change, AttemptChange::Increment);\n        assert_eq!(claim.generation_change, GenerationChange::Increment);\n        assert_eq!(claim.token_change, TokenChange::New);\n        assert_eq!(claim.reason_class, None);\n        assert_eq!(\n            claim.clock_requirement,\n            ClockRequirement::BoottimeAndAuthenticatedWallFloor\n        );\n        assert_eq!(\n            claim.required_syncs,\n            &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory]\n        );\n        assert_eq!(claim.linearization, LinearizationPrimitive::RenameNoreplace);\n        assert_eq!(\n            claim.before_linearization_failure,\n            FailureOutcome::NotCommitted\n        );\n        assert_eq!(\n            claim.after_linearization_failure,\n            FailureOutcome::OutcomeUnknown\n        );\n        assert!(claim.resolution_behavior.contains(\"both\"));\n        assert_eq!(claim.qualification, TransitionQualification::None);\n    }}\n\n    #[test]\n    fn enqueue_uses_no_overwrite_publication() {{\n        let enqueue = TRANSITIONS\n            .iter()\n            .find(|transition| transition.operation == Operation::EnqueueImmediate)\n            .unwrap();\n        assert_eq!(\n            enqueue.linearization,\n            LinearizationPrimitive::PublishNoreplace\n        );\n    }}\n\n    #[test]\n    fn terminal_and_exception_metadata_are_projected() {{\n        let reap = TRANSITIONS\n            .iter()\n            .find(|transition| transition.operation == Operation::ReapExpiredToDead)\n            .unwrap();\n        assert_eq!(reap.reason_class, Some(ReasonClass::AttemptsExhausted));\n        assert_eq!(\n            reap.qualification,\n            TransitionQualification::AttemptsExhausted\n        );\n        assert_eq!(EXCEPTIONS[0].name, ExceptionName::ReceiptCompaction);\n        assert_eq!(EXCEPTIONS[0].clock_requirement, ClockRequirement::None);\n        assert_eq!(\n            EXCEPTIONS[1].clock_requirement,\n            ClockRequirement::AuthenticatedWallFloor\n        );\n        assert_eq!(REENTRY[0].name, ReentryName::RequeueDead);\n        assert_eq!(REENTRY[0].source, State::Dead);\n    }}\n}}",
        spec.transitions.len(),
        spec.exceptions.len(),
        spec.reentry.len(),
    )
    .expect("writing to String cannot fail");
    output
}

fn write_rust_enum(output: &mut String, name: &str, variants: &[(&str, &str)]) {
    writeln!(
        output,
        "#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]\npub enum {name} {{"
    )
    .expect("writing to String cannot fail");
    for (variant, _) in variants {
        writeln!(output, "    {variant},").expect("writing to String cannot fail");
    }
    output.push_str("}\n\n");
}

fn render_go(spec: &StateMachineSpec, digest: &str) -> String {
    let mut output = format!(
        "// Auto-generated from spec/state-machine.json. Do not edit by hand.\n// Source SHA-256: {digest}\n\n"
    );
    output.push_str(
        "package steadq\n\n\
type OptionalString struct {\n\
\tValue   string\n\
\tPresent bool\n\
}\n\n\
type TransitionDef struct {\n\
\tOperation                  string\n\
\tSource                     string\n\
\tDestination                string\n\
\tGenerationChange           string\n\
\tAttemptChange              string\n\
\tTokenChange                string\n\
\tReasonClass                OptionalString\n\
\tClockRequirement           string\n\
\tRequiredSyncs              []string\n\
\tLinearization              string\n\
\tBeforeLinearizationFailure string\n\
\tAfterLinearizationFailure  string\n\
\tResolutionBehavior         string\n\
\tQualification              string\n\
}\n\n\
type ExceptionDef struct {\n\
\tName                       string\n\
\tDescription                string\n\
\tClockRequirement           string\n\
\tMutationClass              string\n\
\tLinearization              string\n\
\tRequiredSyncs              []string\n\
\tBeforeLinearizationFailure string\n\
\tAfterLinearizationFailure  string\n\
}\n\n\
type ReentryDef struct {\n\
\tName               string\n\
\tSource             string\n\
\tDescription        string\n\
\tCreatesNewIdentity bool\n\
}\n\n\
var Transitions = []TransitionDef{\n",
    );
    for transition in &spec.transitions {
        let reason_class = match &transition.reason_class {
            Nullable::Value(reason) => format!(
                "OptionalString{{Value: {}, Present: true}}",
                json_string(reason.as_str())
            ),
            Nullable::Null => "OptionalString{}".into(),
        };
        let required_syncs = transition
            .required_syncs
            .iter()
            .map(|sync| json_string(sync.as_str()))
            .collect::<Vec<_>>()
            .join(", ");
        writeln!(
            output,
            "\t{{Operation: {}, Source: {}, Destination: {}, GenerationChange: {}, AttemptChange: {}, TokenChange: {}, ReasonClass: {}, ClockRequirement: {}, RequiredSyncs: []string{{{}}}, Linearization: {}, BeforeLinearizationFailure: {}, AfterLinearizationFailure: {}, ResolutionBehavior: {}, Qualification: {}}},",
            json_string(transition.operation.as_str()),
            json_string(transition.source.as_str()),
            json_string(transition.destination.as_str()),
            json_string(transition.generation_change.as_str()),
            json_string(transition.attempt_change.as_str()),
            json_string(transition.token_change.as_str()),
            reason_class,
            json_string(transition.clock_requirement.as_str()),
            required_syncs,
            json_string(transition.linearization.as_str()),
            json_string(transition.before_linearization_failure.as_str()),
            json_string(transition.after_linearization_failure.as_str()),
            json_string(&transition.resolution_behavior),
            json_string(transition.qualification.as_str()),
        )
        .expect("writing to String cannot fail");
    }
    output.push_str("}\n\nvar Exceptions = []ExceptionDef{\n");
    for exception in &spec.exceptions {
        let required_syncs = exception
            .required_syncs
            .iter()
            .map(|sync| json_string(sync.as_str()))
            .collect::<Vec<_>>()
            .join(", ");
        writeln!(
            output,
            "\t{{Name: {}, Description: {}, ClockRequirement: {}, MutationClass: {}, Linearization: {}, RequiredSyncs: []string{{{}}}, BeforeLinearizationFailure: {}, AfterLinearizationFailure: {}}},",
            json_string(exception.name.as_str()),
            json_string(&exception.description),
            json_string(exception.clock_requirement.as_str()),
            json_string(exception.mutation_class.as_str()),
            json_string(exception.linearization.as_str()),
            required_syncs,
            json_string(exception.before_linearization_failure.as_str()),
            json_string(exception.after_linearization_failure.as_str()),
        )
        .expect("writing to String cannot fail");
    }
    output.push_str("}\n\nvar Reentry = []ReentryDef{\n");
    for reentry in &spec.reentry {
        writeln!(
            output,
            "\t{{Name: {}, Source: {}, Description: {}, CreatesNewIdentity: {}}},",
            json_string(reentry.name.as_str()),
            json_string(reentry.source.as_str()),
            json_string(&reentry.description),
            reentry.creates_new_identity,
        )
        .expect("writing to String cannot fail");
    }
    output.push_str(
        "}\n\n\
func IsLegalTransition(source, destination string) bool {\n\
\tfor _, t := range Transitions {\n\
\t\tif t.Source == source && t.Destination == destination {\n\
\t\t\treturn true\n\
\t\t}\n\
\t}\n\
\treturn false\n\
}\n",
    );
    output
}

fn render_markdown(spec: &StateMachineSpec, digest: &str) -> String {
    let mut output = format!(
        "<!-- Source: spec/state-machine.json; SHA-256: {digest} -->\n\n\
# SteadQ/1 State Machine (Generated)\n\n\
## Transitions\n\n\
| Operation | Source | Destination | Gen | Attempt | Token | Reason | Clock requirement | Required syncs | Linearization | Before failure | After failure | Resolution | Qualification |\n\
|-----------|--------|-------------|-----|---------|-------|--------|-------------------|----------------|---------------|----------------|---------------|------------|-------|\n",
    );
    for transition in &spec.transitions {
        let required_syncs = transition
            .required_syncs
            .iter()
            .map(|sync| sync.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        writeln!(
            output,
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            markdown(transition.operation.as_str()),
            markdown(transition.source.as_str()),
            markdown(transition.destination.as_str()),
            transition.generation_change.as_str(),
            transition.attempt_change.as_str(),
            transition.token_change.as_str(),
            match &transition.reason_class {
                Nullable::Value(reason) => (*reason).as_str(),
                Nullable::Null => "none",
            },
            transition.clock_requirement.as_str(),
            required_syncs,
            transition.linearization.as_str(),
            transition.before_linearization_failure.as_str(),
            transition.after_linearization_failure.as_str(),
            markdown(&transition.resolution_behavior),
            transition.qualification.as_str(),
        )
        .expect("writing to String cannot fail");
    }
    output.push_str(
        "\n## Exceptional mutations\n\n\
| Operation | Clock requirement | Class | Linearization | Required syncs | Before failure | After failure | Description |\n\
|-----------|-------------------|-------|---------------|----------------|----------------|---------------|-------------|\n",
    );
    for exception in &spec.exceptions {
        let required_syncs = exception
            .required_syncs
            .iter()
            .map(|sync| sync.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        writeln!(
            output,
            "| {} | {} | {} | {} | {} | {} | {} | {} |",
            markdown(exception.name.as_str()),
            exception.clock_requirement.as_str(),
            exception.mutation_class.as_str(),
            exception.linearization.as_str(),
            required_syncs,
            exception.before_linearization_failure.as_str(),
            exception.after_linearization_failure.as_str(),
            markdown(&exception.description),
        )
        .expect("writing to String cannot fail");
    }
    output.push_str("\n## Administrative re-entry (creates new identity)\n\n");
    for reentry in &spec.reentry {
        writeln!(
            output,
            "- **{}** (from {}): {} (creates new identity: {})",
            markdown(reentry.name.as_str()),
            markdown(reentry.source.as_str()),
            markdown(&reentry.description),
            reentry.creates_new_identity,
        )
        .expect("writing to String cannot fail");
    }
    output
}

fn rust_literal(value: &str) -> String {
    format!("{value:?}")
}

fn json_string(value: &str) -> String {
    serde_json::to_string(value).expect("serializing a string cannot fail")
}

fn markdown(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', " ")
}
