use super::*;
use std::fmt::Write as _;

pub(super) const GENERATED_RUST: &str = "generated/state-machine.rs";
pub(super) const GENERATED_GO: &str = "generated/state-machine.go";
pub(super) const GENERATED_MARKDOWN: &str = "generated/state-machine.md";
pub(super) const GENERATED_TLA: &str = "model/SteadQProtocol.tla";
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
        (GENERATED_TLA, render_tla(spec, digest)),
    ]
}

pub(super) fn render_rust(spec: &StateMachineSpec, digest: &str) -> String {
    let mut output = format!(
        "// Auto-generated from spec/state-machine.json. Do not edit by hand.\n// Source SHA-256: {digest}\n\n\
pub const PROTOCOL_IR_IDENTITY: &str = {};\n\
pub const PROTOCOL_IR_VERSION: u32 = {};\n\n",
        rust_literal(&spec.protocol),
        spec.version,
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
        "ObjectKind",
        &ObjectKind::ALL.map(|value| (value.rust_name(), value.as_str())),
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
        "ResolverProbeTopology",
        &ResolverProbeTopology::ALL.map(|value| (value.rust_name(), value.as_str())),
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
    pub source_object_kind: ObjectKind,
    pub destination: State,
    pub destination_object_kind: ObjectKind,
    pub generation_change: GenerationChange,
    pub attempt_change: AttemptChange,
    pub token_change: TokenChange,
    pub reason_class: Option<ReasonClass>,
    pub clock_requirement: ClockRequirement,
    pub required_syncs: &'static [SyncStep],
    pub linearization: LinearizationPrimitive,
    pub before_linearization_failure: FailureOutcome,
    pub after_linearization_failure: FailureOutcome,
    pub resolver_probe_topology: ResolverProbeTopology,
    pub qualification: TransitionQualification,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExceptionDef {
    pub name: ExceptionName,
    pub description: &'static str,
    pub source_object_kind: ObjectKind,
    pub destination_object_kind: ObjectKind,
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
        writeln!(
            output,
            "    TransitionDef {{\n        operation: Operation::{},\n        source: State::{},\n        source_object_kind: ObjectKind::{},\n        destination: State::{},\n        destination_object_kind: ObjectKind::{},\n        generation_change: GenerationChange::{},\n        attempt_change: AttemptChange::{},\n        token_change: TokenChange::{},\n        reason_class: {},\n        clock_requirement: ClockRequirement::{},\n        required_syncs: &[{}],\n        linearization: LinearizationPrimitive::{},\n        before_linearization_failure: FailureOutcome::{},\n        after_linearization_failure: FailureOutcome::{},\n        resolver_probe_topology: ResolverProbeTopology::{},\n        qualification: TransitionQualification::{},\n    }},",
            transition.operation.rust_name(),
            transition.source.rust_name(),
            transition.source_object_kind.rust_name(),
            transition.destination.rust_name(),
            transition.destination_object_kind.rust_name(),
            transition.generation_change.rust_name(),
            transition.attempt_change.rust_name(),
            transition.token_change.rust_name(),
            reason_class,
            transition.clock_requirement.rust_name(),
            required_syncs,
            transition.linearization.rust_name(),
            transition.before_linearization_failure.rust_name(),
            transition.after_linearization_failure.rust_name(),
            transition.resolver_probe_topology.rust_name(),
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
            "    ExceptionDef {{\n        name: ExceptionName::{},\n        description: {},\n        source_object_kind: ObjectKind::{},\n        destination_object_kind: ObjectKind::{},\n        clock_requirement: ClockRequirement::{},\n        mutation_class: MutationClass::{},\n        linearization: LinearizationPrimitive::{},\n        required_syncs: &[{}],\n        before_linearization_failure: FailureOutcome::{},\n        after_linearization_failure: FailureOutcome::{},\n    }},",
            exception.name.rust_name(),
            rust_literal(&exception.description),
            exception.source_object_kind.rust_name(),
            exception.destination_object_kind.rust_name(),
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
    output.push_str("];\n\n/// Return the complete protocol definition for an operation.\npub fn transition(operation: Operation) -> &'static TransitionDef {\n    match operation {\n");
    for (index, transition) in spec.transitions.iter().enumerate() {
        writeln!(
            output,
            "        Operation::{} => &TRANSITIONS[{index}],",
            transition.operation.rust_name(),
        )
        .expect("writing to String cannot fail");
    }
    output.push_str(
        r#"    }
}

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
        "    }}\n\n    #[test]\n    fn illegal_transitions() {{\n        for (source, destination) in [\n            (State::Receipt, State::Ready),\n            (State::Dead, State::Ready),\n            (State::Quarantine, State::Ready),\n            (State::Ready, State::Ready),\n            (State::Hidden, State::Leased),\n            (State::Ready, State::Receipt),\n        ] {{\n            assert!(!is_legal_transition(source, destination));\n        }}\n    }}\n\n    #[test]\n    fn generated_collections_are_complete() {{\n        assert_eq!(TRANSITIONS.len(), {});\n        assert_eq!(EXCEPTIONS.len(), {});\n        assert_eq!(REENTRY.len(), {});\n        assert!(TRANSITIONS\n            .iter()\n            .all(|transition| !transition.required_syncs.is_empty()));\n        assert!(TRANSITIONS.iter().all(|transition| {{\n            transition.before_linearization_failure == FailureOutcome::NotCommitted\n                && transition.after_linearization_failure == FailureOutcome::OutcomeUnknown\n        }}));\n        assert!(EXCEPTIONS.iter().all(|exception| exception.mutation_class\n            == MutationClass::ReplacingMove\n            && exception.linearization == LinearizationPrimitive::RenameReplace\n            && exception.required_syncs == [SyncStep::File, SyncStep::SameOrDestinationDirectory]\n            && exception.before_linearization_failure == FailureOutcome::NotCommitted\n            && exception.after_linearization_failure == FailureOutcome::OutcomeUnknown));\n        assert!(REENTRY.iter().all(|reentry| reentry.creates_new_identity));\n    }}\n\n    #[test]\n    fn claim_projects_complete_semantics() {{\n        let claim = TRANSITIONS\n            .iter()\n            .find(|transition| transition.operation == Operation::Claim)\n            .unwrap();\n        assert_eq!(claim.source, State::Ready);\n        assert_eq!(claim.source_object_kind, ObjectKind::FullJob);\n        assert_eq!(claim.destination, State::Leased);\n        assert_eq!(claim.destination_object_kind, ObjectKind::FullJob);\n        assert_eq!(claim.attempt_change, AttemptChange::Increment);\n        assert_eq!(claim.generation_change, GenerationChange::Increment);\n        assert_eq!(claim.token_change, TokenChange::New);\n        assert_eq!(claim.reason_class, None);\n        assert_eq!(\n            claim.clock_requirement,\n            ClockRequirement::BoottimeAndAuthenticatedWallFloor\n        );\n        assert_eq!(\n            claim.required_syncs,\n            &[SyncStep::DestinationDirectory, SyncStep::SourceDirectory]\n        );\n        assert_eq!(claim.linearization, LinearizationPrimitive::RenameNoreplace);\n        assert_eq!(\n            claim.before_linearization_failure,\n            FailureOutcome::NotCommitted\n        );\n        assert_eq!(\n            claim.after_linearization_failure,\n            FailureOutcome::OutcomeUnknown\n        );\n        assert_eq!(\n            claim.resolver_probe_topology,\n            ResolverProbeTopology::SourceAndDestination\n        );\n        assert_eq!(claim.qualification, TransitionQualification::None);\n    }}\n\n    #[test]\n    fn enqueue_uses_no_overwrite_publication() {{\n        let enqueue = TRANSITIONS\n            .iter()\n            .find(|transition| transition.operation == Operation::EnqueueImmediate)\n            .unwrap();\n        assert_eq!(\n            enqueue.linearization,\n            LinearizationPrimitive::PublishNoreplace\n        );\n    }}\n\n    #[test]\n    fn terminal_and_exception_metadata_are_projected() {{\n        let reap = TRANSITIONS\n            .iter()\n            .find(|transition| transition.operation == Operation::ReapExpiredToDead)\n            .unwrap();\n        assert_eq!(reap.reason_class, Some(ReasonClass::AttemptsExhausted));\n        assert_eq!(\n            reap.qualification,\n            TransitionQualification::AttemptsExhausted\n        );\n        let acknowledge = transition(Operation::Acknowledge);\n        assert_eq!(acknowledge.source_object_kind, ObjectKind::FullJob);\n        assert_eq!(acknowledge.destination_object_kind, ObjectKind::FullReceipt);\n        let quarantine = transition(Operation::Quarantine);\n        assert_eq!(quarantine.source_object_kind, ObjectKind::RawObject);\n        assert_eq!(quarantine.destination_object_kind, ObjectKind::RawObject);\n        assert_eq!(EXCEPTIONS[0].name, ExceptionName::ReceiptCompaction);\n        assert_eq!(EXCEPTIONS[0].source_object_kind, ObjectKind::FullReceipt);\n        assert_eq!(\n            EXCEPTIONS[0].destination_object_kind,\n            ObjectKind::CompactReceipt\n        );\n        assert_eq!(EXCEPTIONS[0].clock_requirement, ClockRequirement::None);\n        assert_eq!(\n            EXCEPTIONS[1].source_object_kind,\n            ObjectKind::WatermarkRecord\n        );\n        assert_eq!(\n            EXCEPTIONS[1].destination_object_kind,\n            ObjectKind::WatermarkRecord\n        );\n        assert_eq!(\n            EXCEPTIONS[1].clock_requirement,\n            ClockRequirement::AuthenticatedWallFloor\n        );\n        assert_eq!(REENTRY[0].name, ReentryName::RequeueDead);\n        assert_eq!(REENTRY[0].source, State::Dead);\n    }}\n}}",
        spec.transitions.len(),
        spec.exceptions.len(),
        spec.reentry.len(),
    )
    .expect("writing to String cannot fail");
    output.push_str(
        r#"
#[cfg(test)]
mod resolver_probe_tests {
    use super::*;

    #[test]
    fn transition_lookup_is_total() {
        for definition in TRANSITIONS {
            assert_eq!(transition(definition.operation), definition);
        }
    }

    #[test]
    fn resolver_probe_topology_matrix() {
        assert_eq!(
            transition(Operation::EnqueueImmediate).resolver_probe_topology,
            ResolverProbeTopology::DestinationOnly
        );
        assert_eq!(
            transition(Operation::Claim).resolver_probe_topology,
            ResolverProbeTopology::SourceAndDestination
        );
        assert_eq!(
            transition(Operation::Acknowledge).resolver_probe_topology,
            ResolverProbeTopology::ReceiptCandidatesAndSource
        );
    }
}
"#,
    );
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
        "// Auto-generated from spec/state-machine.json. Do not edit by hand.\n// Source SHA-256: {digest}\n\n\
package steadq\n\n\
const ProtocolIRIdentity = {}\n\
const ProtocolIRVersion uint32 = {}\n\n",
        json_string(&spec.protocol),
        spec.version,
    );
    output.push_str(
        "type OptionalString struct {\n\
\tValue   string\n\
\tPresent bool\n\
}\n\n\
type TransitionDef struct {\n\
\tOperation                  string\n\
\tSource                     string\n\
\tSourceObjectKind           string\n\
\tDestination                string\n\
\tDestinationObjectKind      string\n\
\tGenerationChange           string\n\
\tAttemptChange              string\n\
\tTokenChange                string\n\
\tReasonClass                OptionalString\n\
\tClockRequirement           string\n\
\tRequiredSyncs              []string\n\
\tLinearization              string\n\
\tBeforeLinearizationFailure string\n\
\tAfterLinearizationFailure  string\n\
\tResolverProbeTopology      string\n\
\tQualification              string\n\
}\n\n\
type ExceptionDef struct {\n\
\tName                       string\n\
\tDescription                string\n\
\tSourceObjectKind           string\n\
\tDestinationObjectKind      string\n\
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
            "\t{{Operation: {}, Source: {}, SourceObjectKind: {}, Destination: {}, DestinationObjectKind: {}, GenerationChange: {}, AttemptChange: {}, TokenChange: {}, ReasonClass: {}, ClockRequirement: {}, RequiredSyncs: []string{{{}}}, Linearization: {}, BeforeLinearizationFailure: {}, AfterLinearizationFailure: {}, ResolverProbeTopology: {}, Qualification: {}}},",
            json_string(transition.operation.as_str()),
            json_string(transition.source.as_str()),
            json_string(transition.source_object_kind.as_str()),
            json_string(transition.destination.as_str()),
            json_string(transition.destination_object_kind.as_str()),
            json_string(transition.generation_change.as_str()),
            json_string(transition.attempt_change.as_str()),
            json_string(transition.token_change.as_str()),
            reason_class,
            json_string(transition.clock_requirement.as_str()),
            required_syncs,
            json_string(transition.linearization.as_str()),
            json_string(transition.before_linearization_failure.as_str()),
            json_string(transition.after_linearization_failure.as_str()),
            json_string(transition.resolver_probe_topology.as_str()),
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
            "\t{{Name: {}, Description: {}, SourceObjectKind: {}, DestinationObjectKind: {}, ClockRequirement: {}, MutationClass: {}, Linearization: {}, RequiredSyncs: []string{{{}}}, BeforeLinearizationFailure: {}, AfterLinearizationFailure: {}}},",
            json_string(exception.name.as_str()),
            json_string(&exception.description),
            json_string(exception.source_object_kind.as_str()),
            json_string(exception.destination_object_kind.as_str()),
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
Protocol IR: `{}`, version `{}`.\n\n\
## Transitions\n\n\
| Operation | Source | Source kind | Destination | Destination kind | Gen | Attempt | Token | Reason | Clock requirement | Required syncs | Linearization | Before failure | After failure | Resolver probes | Qualification |\n\
|-----------|--------|-------------|-------------|------------------|-----|---------|-------|--------|-------------------|----------------|---------------|----------------|---------------|-----------------|---------------|\n",
        markdown(&spec.protocol),
        spec.version,
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
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            markdown(transition.operation.as_str()),
            markdown(transition.source.as_str()),
            transition.source_object_kind.as_str(),
            markdown(transition.destination.as_str()),
            transition.destination_object_kind.as_str(),
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
            transition.resolver_probe_topology.as_str(),
            transition.qualification.as_str(),
        )
        .expect("writing to String cannot fail");
    }
    output.push_str(
        "\n## Exceptional mutations\n\n\
| Operation | Source kind | Destination kind | Clock requirement | Class | Linearization | Required syncs | Before failure | After failure | Description |\n\
|-----------|-------------|------------------|-------------------|-------|---------------|----------------|----------------|---------------|-------------|\n",
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
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            markdown(exception.name.as_str()),
            exception.source_object_kind.as_str(),
            exception.destination_object_kind.as_str(),
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

pub(super) fn render_tla(spec: &StateMachineSpec, digest: &str) -> String {
    let mut output = format!(
        "-------------------------- MODULE SteadQProtocol --------------------------\n\
(* Auto-generated from spec/state-machine.json. Do not edit by hand. *)\n\
(* Source SHA-256: {digest} *)\n\n\
ProtocolIRIdentity == {}\n\
ProtocolIRVersion == {}\n\n",
        tla_string(&spec.protocol),
        spec.version,
    );

    write_tla_enum(
        &mut output,
        "ProtocolOperations",
        "Operation",
        &Operation::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_tla_enum(
        &mut output,
        "ProtocolStates",
        "State",
        &State::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_tla_enum(
        &mut output,
        "ProtocolObjectKinds",
        "ObjectKind",
        &ObjectKind::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_tla_enum(
        &mut output,
        "ProtocolGenerationChanges",
        "GenerationChange",
        &GenerationChange::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_tla_enum(
        &mut output,
        "ProtocolAttemptChanges",
        "AttemptChange",
        &AttemptChange::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_tla_enum(
        &mut output,
        "ProtocolTokenChanges",
        "TokenChange",
        &TokenChange::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_tla_enum(
        &mut output,
        "ProtocolReasonClasses",
        "ReasonClass",
        &ReasonClass::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    output.push_str("NoReasonClass == \"none\"\n\n");
    write_tla_enum(
        &mut output,
        "ProtocolClockRequirements",
        "ClockRequirement",
        &ClockRequirement::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_tla_enum(
        &mut output,
        "ProtocolSyncSteps",
        "SyncStep",
        &SyncStep::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_tla_enum(
        &mut output,
        "ProtocolLinearizationPrimitives",
        "Linearization",
        &LinearizationPrimitive::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_tla_enum(
        &mut output,
        "ProtocolFailureOutcomes",
        "FailureOutcome",
        &FailureOutcome::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_tla_enum(
        &mut output,
        "ProtocolResolverProbeTopologies",
        "ResolverProbeTopology",
        &ResolverProbeTopology::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_tla_enum(
        &mut output,
        "ProtocolTransitionQualifications",
        "TransitionQualification",
        &TransitionQualification::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_tla_enum(
        &mut output,
        "ProtocolMutationClasses",
        "MutationClass",
        &MutationClass::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_tla_enum(
        &mut output,
        "ProtocolExceptionNames",
        "Exception",
        &ExceptionName::ALL.map(|value| (value.rust_name(), value.as_str())),
    );
    write_tla_enum(
        &mut output,
        "ProtocolReentryNames",
        "Reentry",
        &ReentryName::ALL.map(|value| (value.rust_name(), value.as_str())),
    );

    output.push_str("ProtocolTransitions == <<\n");
    for (index, transition) in spec.transitions.iter().enumerate() {
        let required_syncs = transition
            .required_syncs
            .iter()
            .map(|sync| format!("SyncStep{}", sync.rust_name()))
            .collect::<Vec<_>>()
            .join(", ");
        let reason_class = match &transition.reason_class {
            Nullable::Value(reason) => format!("ReasonClass{}", reason.rust_name()),
            Nullable::Null => "NoReasonClass".into(),
        };
        writeln!(
            output,
            "    [operation |-> Operation{},\n     source |-> State{},\n     sourceObjectKind |-> ObjectKind{},\n     destination |-> State{},\n     destinationObjectKind |-> ObjectKind{},\n     generationChange |-> GenerationChange{},\n     attemptChange |-> AttemptChange{},\n     tokenChange |-> TokenChange{},\n     reasonClass |-> {},\n     clockRequirement |-> ClockRequirement{},\n     requiredSyncs |-> <<{}>>,\n     linearization |-> Linearization{},\n     beforeLinearizationFailure |-> FailureOutcome{},\n     afterLinearizationFailure |-> FailureOutcome{},\n     resolverProbeTopology |-> ResolverProbeTopology{},\n     qualification |-> TransitionQualification{}]{}",
            transition.operation.rust_name(),
            transition.source.rust_name(),
            transition.source_object_kind.rust_name(),
            transition.destination.rust_name(),
            transition.destination_object_kind.rust_name(),
            transition.generation_change.rust_name(),
            transition.attempt_change.rust_name(),
            transition.token_change.rust_name(),
            reason_class,
            transition.clock_requirement.rust_name(),
            required_syncs,
            transition.linearization.rust_name(),
            transition.before_linearization_failure.rust_name(),
            transition.after_linearization_failure.rust_name(),
            transition.resolver_probe_topology.rust_name(),
            transition.qualification.rust_name(),
            if index + 1 == spec.transitions.len() {
                ""
            } else {
                ","
            },
        )
        .expect("writing to String cannot fail");
    }
    output.push_str(
        ">>\n\n(* descriptionUtf8Hex stores exact UTF-8 bytes as lowercase hex. *)\n\
ProtocolExceptions == <<\n",
    );
    for (index, exception) in spec.exceptions.iter().enumerate() {
        let required_syncs = exception
            .required_syncs
            .iter()
            .map(|sync| format!("SyncStep{}", sync.rust_name()))
            .collect::<Vec<_>>()
            .join(", ");
        writeln!(
            output,
            "    [name |-> Exception{},\n     descriptionUtf8Hex |-> {},\n     sourceObjectKind |-> ObjectKind{},\n     destinationObjectKind |-> ObjectKind{},\n     clockRequirement |-> ClockRequirement{},\n     mutationClass |-> MutationClass{},\n     linearization |-> Linearization{},\n     requiredSyncs |-> <<{}>>,\n     beforeLinearizationFailure |-> FailureOutcome{},\n     afterLinearizationFailure |-> FailureOutcome{}]{}",
            exception.name.rust_name(),
            tla_string(&utf8_hex(&exception.description)),
            exception.source_object_kind.rust_name(),
            exception.destination_object_kind.rust_name(),
            exception.clock_requirement.rust_name(),
            exception.mutation_class.rust_name(),
            exception.linearization.rust_name(),
            required_syncs,
            exception.before_linearization_failure.rust_name(),
            exception.after_linearization_failure.rust_name(),
            if index + 1 == spec.exceptions.len() {
                ""
            } else {
                ","
            },
        )
        .expect("writing to String cannot fail");
    }
    output.push_str(">>\n\nProtocolReentry == <<\n");
    for (index, reentry) in spec.reentry.iter().enumerate() {
        writeln!(
            output,
            "    [name |-> Reentry{},\n     source |-> State{},\n     descriptionUtf8Hex |-> {},\n     createsNewIdentity |-> {}]{}",
            reentry.name.rust_name(),
            reentry.source.rust_name(),
            tla_string(&utf8_hex(&reentry.description)),
            if reentry.creates_new_identity {
                "TRUE"
            } else {
                "FALSE"
            },
            if index + 1 == spec.reentry.len() {
                ""
            } else {
                ","
            },
        )
        .expect("writing to String cannot fail");
    }
    output.push_str(
        ">>\n\n=============================================================================\n",
    );
    output
}

fn write_tla_enum(
    output: &mut String,
    set_name: &str,
    value_prefix: &str,
    variants: &[(&str, &str)],
) {
    for (variant, value) in variants {
        writeln!(output, "{value_prefix}{variant} == {}", tla_string(value))
            .expect("writing to String cannot fail");
    }
    let members = variants
        .iter()
        .map(|(variant, _)| format!("{value_prefix}{variant}"))
        .collect::<Vec<_>>()
        .join(", ");
    writeln!(output, "{set_name} == {{{members}}}\n").expect("writing to String cannot fail");
}

fn utf8_hex(value: &str) -> String {
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn tla_string(value: &str) -> String {
    json_string(value)
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
