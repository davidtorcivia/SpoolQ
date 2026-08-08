use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

const SPEC: &str = "spec/state-machine.json";
const GENERATED_RUST: &str = "generated/state-machine.rs";
const GENERATED_GO: &str = "generated/state-machine.go";
const GENERATED_MARKDOWN: &str = "generated/state-machine.md";
const CORE_RUST: &str = "crates/steadq-core/src/state_machine.rs";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateMachineSpec {
    transitions: Vec<Transition>,
    exceptions: Vec<Exception>,
    reentry: Vec<Reentry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Transition {
    operation: String,
    source: String,
    destination: String,
    generation_change: GenerationChange,
    attempt_change: AttemptChange,
    token_change: TokenChange,
    reason_class: Option<String>,
    required_syncs: Vec<SyncStep>,
    no_overwrite: bool,
    resolution_behavior: String,
    notes: Option<String>,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum GenerationChange {
    Zero,
    Increment,
    IncrementOrSame,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AttemptChange {
    Zero,
    Increment,
    Unchanged,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TokenChange {
    None,
    New,
    Same,
}

#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "snake_case")]
enum SyncStep {
    #[serde(rename = "file_fsync")]
    File,
    #[serde(rename = "destination_dir_fsync")]
    DestinationDir,
    #[serde(rename = "source_dir_fsync")]
    SourceDir,
    #[serde(rename = "same_or_destination_dir_fsync")]
    SameOrDestinationDir,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Exception {
    name: String,
    description: String,
    uses_replacing_rename: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reentry {
    name: String,
    source: String,
    description: String,
    creates_new_identity: bool,
}

fn main() -> ExitCode {
    let result = match env::args().nth(1).as_deref() {
        Some("check") => check_all(),
        Some("check-generated") => check_generated(),
        Some("generate") => generate(),
        _ => Err("usage: cargo xtask <check|check-generated|generate>".into()),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("xtask: {error}");
            ExitCode::FAILURE
        }
    }
}

fn workspace_root() -> Result<PathBuf, String> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| "xtask manifest has no workspace parent".into())
}

fn check_all() -> Result<(), String> {
    check_target()?;
    check_generated()?;

    let root = workspace_root()?;
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
        if !root.join(required).is_file() {
            return Err(format!("required baseline artifact is missing: {required}"));
        }
    }
    Ok(())
}

fn check_target() -> Result<(), String> {
    if !cfg!(all(
        target_os = "linux",
        target_arch = "x86_64",
        target_env = "gnu",
        target_pointer_width = "64"
    )) {
        return Err(format!(
            "strict prototype target is x86_64-unknown-linux-gnu; got {} {}",
            env::consts::ARCH,
            env::consts::OS
        ));
    }
    Ok(())
}

fn load_spec(root: &Path) -> Result<(StateMachineSpec, String), String> {
    let bytes =
        fs::read(root.join(SPEC)).map_err(|error| format!("cannot read {SPEC}: {error}"))?;
    let spec: StateMachineSpec =
        serde_json::from_slice(&bytes).map_err(|error| format!("cannot parse {SPEC}: {error}"))?;
    validate_spec(&spec)?;
    let digest = format!("{:x}", Sha256::digest(bytes));
    Ok((spec, digest))
}

fn validate_spec(spec: &StateMachineSpec) -> Result<(), String> {
    if spec.transitions.is_empty() {
        return Err("state-machine spec has no transitions".into());
    }

    let states = [
        "hidden",
        "ready",
        "leased",
        "delayed",
        "dead",
        "receipt",
        "quarantine",
        "active",
    ];
    let destinations = [
        "ready",
        "leased",
        "delayed",
        "dead",
        "receipt",
        "quarantine",
    ];
    let mut operation_names = HashSet::new();
    for transition in &spec.transitions {
        validate_identifier("operation", &transition.operation)?;
        if !operation_names.insert(transition.operation.as_str()) {
            return Err(format!(
                "duplicate transition operation: {}",
                transition.operation
            ));
        }
        if !states.contains(&transition.source.as_str()) {
            return Err(format!(
                "transition {} has unknown source state {}",
                transition.operation, transition.source
            ));
        }
        if !destinations.contains(&transition.destination.as_str()) {
            return Err(format!(
                "transition {} has unknown destination state {}",
                transition.operation, transition.destination
            ));
        }
        if transition.required_syncs.is_empty() {
            return Err(format!(
                "transition {} has no required syncs",
                transition.operation
            ));
        }
        if !transition.no_overwrite {
            return Err(format!(
                "transition {} must prohibit overwrite",
                transition.operation
            ));
        }
        let mut syncs = HashSet::new();
        for sync in &transition.required_syncs {
            if !syncs.insert(*sync) {
                return Err(format!(
                    "transition {} contains duplicate sync {}",
                    transition.operation,
                    sync.as_str()
                ));
            }
        }
        if transition.resolution_behavior.trim().is_empty() {
            return Err(format!(
                "transition {} has empty resolution behavior",
                transition.operation
            ));
        }
        if transition
            .reason_class
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(format!(
                "transition {} has an empty reason class",
                transition.operation
            ));
        }
        if transition
            .notes
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(format!(
                "transition {} has empty notes",
                transition.operation
            ));
        }
    }

    let mut exception_names = HashSet::new();
    for exception in &spec.exceptions {
        validate_identifier("exception", &exception.name)?;
        if !exception_names.insert(exception.name.as_str()) {
            return Err(format!("duplicate exception: {}", exception.name));
        }
        if exception.description.trim().is_empty() {
            return Err(format!("exception {} has no description", exception.name));
        }
        if !exception.uses_replacing_rename {
            return Err(format!(
                "exception {} must declare replacing rename behavior",
                exception.name
            ));
        }
    }

    let mut reentry_names = HashSet::new();
    for reentry in &spec.reentry {
        validate_identifier("reentry", &reentry.name)?;
        if !reentry_names.insert(reentry.name.as_str()) {
            return Err(format!("duplicate reentry: {}", reentry.name));
        }
        if !states.contains(&reentry.source.as_str()) {
            return Err(format!(
                "reentry {} has unknown source state {}",
                reentry.name, reentry.source
            ));
        }
        if reentry.description.trim().is_empty() {
            return Err(format!("reentry {} has no description", reentry.name));
        }
        if !reentry.creates_new_identity {
            return Err(format!(
                "reentry {} must create a new identity",
                reentry.name
            ));
        }
    }
    Ok(())
}

fn validate_identifier(kind: &str, value: &str) -> Result<(), String> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(format!("invalid {kind} identifier: {value:?}"));
    }
    Ok(())
}

fn check_generated() -> Result<(), String> {
    let root = workspace_root()?;
    let (spec, digest) = load_spec(&root)?;
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

fn generate() -> Result<(), String> {
    let root = workspace_root()?;
    let (spec, digest) = load_spec(&root)?;
    for (artifact, contents) in generated_outputs(&spec, &digest) {
        fs::write(root.join(artifact), contents)
            .map_err(|error| format!("cannot write {artifact}: {error}"))?;
    }
    Ok(())
}

fn generated_outputs(spec: &StateMachineSpec, digest: &str) -> Vec<(&'static str, String)> {
    let rust = render_rust(spec, digest);
    vec![
        (GENERATED_RUST, rust.clone()),
        (CORE_RUST, rust),
        (GENERATED_GO, render_go(spec, digest)),
        (GENERATED_MARKDOWN, render_markdown(spec, digest)),
    ]
}

fn render_rust(spec: &StateMachineSpec, digest: &str) -> String {
    let mut output = format!(
        "// Auto-generated from spec/state-machine.json. Do not edit by hand.\n// Source SHA-256: {digest}\n\n"
    );
    output.push_str(
        "pub struct TransitionDef {\n    pub operation: &'static str,\n    pub source: &'static str,\n    pub destination: &'static str,\n    pub generation_change: GenerationChange,\n    pub attempt_change: AttemptChange,\n    pub token_change: TokenChange,\n    pub no_overwrite: bool,\n}\n\n#[derive(Clone, Copy, Debug, PartialEq, Eq)]\npub enum GenerationChange {\n    Zero,\n    Increment,\n    IncrementOrSame,\n}\n\n#[derive(Clone, Copy, Debug, PartialEq, Eq)]\npub enum AttemptChange {\n    Zero,\n    Increment,\n    Unchanged,\n}\n\n#[derive(Clone, Copy, Debug, PartialEq, Eq)]\npub enum TokenChange {\n    None,\n    New,\n    Same,\n}\n\npub const TRANSITIONS: &[TransitionDef] = &[\n",
    );
    for transition in &spec.transitions {
        writeln!(
            output,
            "    TransitionDef {{\n        operation: {},\n        source: {},\n        destination: {},\n        generation_change: GenerationChange::{},\n        attempt_change: AttemptChange::{},\n        token_change: TokenChange::{},\n        no_overwrite: {},\n    }},",
            rust_string(&transition.operation),
            rust_string(&transition.source),
            rust_string(&transition.destination),
            transition.generation_change.rust_name(),
            transition.attempt_change.rust_name(),
            transition.token_change.rust_name(),
            transition.no_overwrite,
        )
        .expect("writing to String cannot fail");
    }
    output.push_str(
        "];\n\n/// Check if a transition from source to destination is legal.\npub fn is_legal_transition(source: &str, destination: &str) -> bool {\n    TRANSITIONS\n        .iter()\n        .any(|transition| transition.source == source && transition.destination == destination)\n}\n\n#[cfg(test)]\nmod tests {\n    use super::*;\n\n    #[test]\n    fn legal_transitions() {\n",
    );
    for transition in &spec.transitions {
        writeln!(
            output,
            "        assert!(is_legal_transition({}, {}));",
            rust_string(&transition.source),
            rust_string(&transition.destination),
        )
        .expect("writing to String cannot fail");
    }
    writeln!(
        output,
        "    }}\n\n    #[test]\n    fn illegal_transitions() {{\n        for (source, destination) in [\n            (\"receipt\", \"ready\"),\n            (\"dead\", \"ready\"),\n            (\"quarantine\", \"ready\"),\n            (\"ready\", \"ready\"),\n            (\"hidden\", \"leased\"),\n            (\"ready\", \"receipt\"),\n        ] {{\n            assert!(!is_legal_transition(source, destination));\n        }}\n    }}\n\n    #[test]\n    fn transition_count() {{\n        assert_eq!(TRANSITIONS.len(), {});\n    }}\n\n    #[test]\n    fn all_transitions_use_no_overwrite() {{\n        for transition in TRANSITIONS {{\n            assert!(\n                transition.no_overwrite,\n                \"transition {{}} must use no-overwrite\",\n                transition.operation\n            );\n        }}\n    }}\n\n    #[test]\n    fn claim_increments_attempt() {{\n        let claim = TRANSITIONS\n            .iter()\n            .find(|transition| transition.operation == \"claim\")\n            .unwrap();\n        assert_eq!(claim.attempt_change, AttemptChange::Increment);\n        assert_eq!(claim.generation_change, GenerationChange::Increment);\n        assert_eq!(claim.token_change, TokenChange::New);\n    }}\n\n    #[test]\n    fn ack_does_not_change_attempt() {{\n        let ack = TRANSITIONS\n            .iter()\n            .find(|transition| transition.operation == \"acknowledge\")\n            .unwrap();\n        assert_eq!(ack.attempt_change, AttemptChange::Unchanged);\n    }}\n\n    #[test]\n    fn renew_preserves_token() {{\n        let renew = TRANSITIONS\n            .iter()\n            .find(|transition| transition.operation == \"renew\")\n            .unwrap();\n        assert_eq!(renew.token_change, TokenChange::Same);\n        assert_eq!(renew.attempt_change, AttemptChange::Unchanged);\n    }}\n}}",
        spec.transitions.len()
    )
    .expect("writing to String cannot fail");
    output
}

fn render_go(spec: &StateMachineSpec, digest: &str) -> String {
    let mut output = format!(
        "// Auto-generated from spec/state-machine.json. Do not edit by hand.\n// Source SHA-256: {digest}\n\n"
    );
    output.push_str(
        "package steadq\n\n\
type TransitionDef struct {\n\
\tOperation       string\n\
\tSource           string\n\
\tDestination      string\n\
\tGenerationChange string\n\
\tAttemptChange    string\n\
\tTokenChange      string\n\
\tNoOverwrite      bool\n\
}\n\n\
var Transitions = []TransitionDef{\n",
    );
    for transition in &spec.transitions {
        writeln!(
            output,
            "\t{{Operation: {}, Source: {}, Destination: {}, GenerationChange: {}, AttemptChange: {}, TokenChange: {}, NoOverwrite: {}}},",
            rust_string(&transition.operation),
            rust_string(&transition.source),
            rust_string(&transition.destination),
            rust_string(transition.generation_change.as_str()),
            rust_string(transition.attempt_change.as_str()),
            rust_string(transition.token_change.as_str()),
            transition.no_overwrite,
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
| Operation | Source | Destination | Gen | Attempt | Token | No-overwrite |\n\
|-----------|--------|-------------|-----|---------|-------|--------------|\n",
    );
    for transition in &spec.transitions {
        writeln!(
            output,
            "| {} | {} | {} | {} | {} | {} | {} |",
            markdown(&transition.operation),
            markdown(&transition.source),
            markdown(&transition.destination),
            transition.generation_change.as_str(),
            transition.attempt_change.as_str(),
            transition.token_change.as_str(),
            if transition.no_overwrite {
                "True"
            } else {
                "False"
            },
        )
        .expect("writing to String cannot fail");
    }
    output.push_str("\n## Replacing-rename exceptions\n\n");
    for exception in &spec.exceptions {
        writeln!(
            output,
            "**{}**: {}",
            markdown(&exception.name),
            markdown(&exception.description)
        )
        .expect("writing to String cannot fail");
    }
    output.push_str("\n## Administrative re-entry (creates new identity)\n\n");
    for reentry in &spec.reentry {
        writeln!(
            output,
            "**{}** (from {}): {}",
            markdown(&reentry.name),
            markdown(&reentry.source),
            markdown(&reentry.description)
        )
        .expect("writing to String cannot fail");
    }
    output
}

fn rust_string(value: &str) -> String {
    serde_json::to_string(value).expect("serializing a string cannot fail")
}

fn markdown(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', " ")
}

impl GenerationChange {
    fn as_str(self) -> &'static str {
        match self {
            Self::Zero => "zero",
            Self::Increment => "increment",
            Self::IncrementOrSame => "increment_or_same",
        }
    }

    fn rust_name(self) -> &'static str {
        match self {
            Self::Zero => "Zero",
            Self::Increment => "Increment",
            Self::IncrementOrSame => "IncrementOrSame",
        }
    }
}

impl AttemptChange {
    fn as_str(self) -> &'static str {
        match self {
            Self::Zero => "zero",
            Self::Increment => "increment",
            Self::Unchanged => "unchanged",
        }
    }

    fn rust_name(self) -> &'static str {
        match self {
            Self::Zero => "Zero",
            Self::Increment => "Increment",
            Self::Unchanged => "Unchanged",
        }
    }
}

impl TokenChange {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::New => "new",
            Self::Same => "same",
        }
    }

    fn rust_name(self) -> &'static str {
        match self {
            Self::None => "None",
            Self::New => "New",
            Self::Same => "Same",
        }
    }
}

impl SyncStep {
    fn as_str(self) -> &'static str {
        match self {
            Self::File => "file_fsync",
            Self::DestinationDir => "destination_dir_fsync",
            Self::SourceDir => "source_dir_fsync",
            Self::SameOrDestinationDir => "same_or_destination_dir_fsync",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> StateMachineSpec {
        serde_json::from_str(
            r#"{
                "transitions": [{
                    "operation": "claim",
                    "source": "ready",
                    "destination": "leased",
                    "generation_change": "increment",
                    "attempt_change": "increment",
                    "token_change": "new",
                    "reason_class": null,
                    "required_syncs": ["destination_dir_fsync", "source_dir_fsync"],
                    "no_overwrite": true,
                    "resolution_behavior": "probe both",
                    "notes": null
                }],
                "exceptions": [{
                    "name": "receipt_compaction",
                    "description": "replace receipt",
                    "uses_replacing_rename": true
                }],
                "reentry": [{
                    "name": "requeue_dead",
                    "source": "dead",
                    "description": "new job",
                    "creates_new_identity": true
                }]
            }"#,
        )
        .unwrap()
    }

    #[test]
    fn validates_complete_fixture() {
        validate_spec(&fixture()).unwrap();
    }

    #[test]
    fn rejects_duplicate_operation() {
        let mut spec = fixture();
        spec.transitions.push(Transition {
            operation: "claim".into(),
            source: "ready".into(),
            destination: "leased".into(),
            generation_change: GenerationChange::Increment,
            attempt_change: AttemptChange::Increment,
            token_change: TokenChange::New,
            reason_class: None,
            required_syncs: vec![SyncStep::DestinationDir],
            no_overwrite: true,
            resolution_behavior: "probe both".into(),
            notes: None,
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
            "transition claim contains duplicate sync destination_dir_fsync"
        );
    }

    #[test]
    fn rejects_overwriting_transition() {
        let mut spec = fixture();
        spec.transitions[0].no_overwrite = false;
        assert_eq!(
            validate_spec(&spec).unwrap_err(),
            "transition claim must prohibit overwrite"
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
        let input = r#"{
            "transitions": [],
            "exceptions": [],
            "reentry": [],
            "extra": true
        }"#;
        assert!(serde_json::from_str::<StateMachineSpec>(input).is_err());
    }
}
