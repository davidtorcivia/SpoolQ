use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

const SPEC: &str = "spec/state-machine.json";
const SCHEMA: &str = "spec/state-machine.schema.json";
const SCHEMA_CONTRACT_SHA256: &str =
    "2d60a9f0d851f7587d63b1180f7dbec8345bb9194864d7bbf5c5a21e025564f6";
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
    operation: Operation,
    source: State,
    destination: State,
    generation_change: GenerationChange,
    attempt_change: AttemptChange,
    token_change: TokenChange,
    reason_class: Nullable<ReasonClass>,
    required_syncs: Vec<SyncStep>,
    no_overwrite: bool,
    resolution_behavior: String,
    notes: Nullable<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Nullable<T> {
    Value(T),
    Null,
}

#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Operation {
    EnqueueImmediate,
    EnqueueDelayed,
    Promote,
    Claim,
    ExhaustedReadyCleanup,
    Renew,
    Acknowledge,
    RetryNow,
    RetryLater,
    Bury,
    ReapExpiredToReady,
    ReapExpiredToDead,
    Quarantine,
}

#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "snake_case")]
enum State {
    Hidden,
    Ready,
    Leased,
    Delayed,
    Dead,
    Receipt,
    Quarantine,
    Active,
}

#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "snake_case")]
enum GenerationChange {
    Zero,
    Increment,
    IncrementOrSame,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum AttemptChange {
    Zero,
    Increment,
    Unchanged,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum TokenChange {
    None,
    New,
    Same,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum ReasonClass {
    AttemptsExhausted,
    ApplicationDefined,
    Corruption,
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
    name: ExceptionName,
    description: String,
    uses_replacing_rename: bool,
}

#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "snake_case")]
enum ExceptionName {
    ReceiptCompaction,
    WallWatermarkAdvancement,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reentry {
    name: ReentryName,
    source: State,
    description: String,
    creates_new_identity: bool,
}

#[derive(Clone, Copy, Deserialize, Eq, Hash, PartialEq)]
#[serde(rename_all = "snake_case")]
enum ReentryName {
    RequeueDead,
    RequeueQuarantine,
}

struct TransitionInvariant {
    source: State,
    destination: State,
    generation_change: GenerationChange,
    attempt_change: AttemptChange,
    token_change: TokenChange,
    reason_class: Option<ReasonClass>,
    required_syncs: &'static [SyncStep],
}

fn main() -> ExitCode {
    let result = run_command(env::args().nth(1).as_deref());

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("xtask: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run_command(command: Option<&str>) -> Result<(), String> {
    let root = workspace_root()?;
    dispatch_command(command, &root)
}

fn dispatch_command(command: Option<&str>, root: &Path) -> Result<(), String> {
    match command {
        Some("check") => check_all(root),
        Some("check-generated") => check_generated(root),
        Some("generate") => generate(root),
        _ => Err("usage: cargo xtask <check|check-generated|generate>".into()),
    }
}

fn workspace_root() -> Result<PathBuf, String> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| "xtask manifest has no workspace parent".into())
}

fn check_all(root: &Path) -> Result<(), String> {
    check_target(
        env::consts::ARCH,
        env::consts::OS,
        if cfg!(target_env = "gnu") { "gnu" } else { "" },
        usize::BITS,
    )?;
    check_generated(root)?;
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

fn check_target(arch: &str, os: &str, target_env: &str, pointer_width: u32) -> Result<(), String> {
    if (arch, os, target_env, pointer_width) != ("x86_64", "linux", "gnu", 64) {
        return Err(format!(
            "strict prototype target is x86_64-unknown-linux-gnu; got {arch}-{os}-{target_env} ({pointer_width}-bit)"
        ));
    }
    Ok(())
}

fn load_spec(root: &Path) -> Result<(StateMachineSpec, String), String> {
    validate_schema(root)?;
    let bytes =
        fs::read(root.join(SPEC)).map_err(|error| format!("cannot read {SPEC}: {error}"))?;
    let spec: StateMachineSpec =
        serde_json::from_slice(&bytes).map_err(|error| format!("cannot parse {SPEC}: {error}"))?;
    validate_spec(&spec)?;
    let digest = format!("{:x}", Sha256::digest(bytes));
    Ok((spec, digest))
}

fn validate_schema(root: &Path) -> Result<(), String> {
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
        "/properties/transitions/items/properties/destination/enum",
        &State::DESTINATIONS.map(State::as_str),
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
        "/properties/transitions/items/properties/required_syncs/items/enum",
        &SyncStep::ALL.map(SyncStep::as_str),
    )?;
    validate_schema_domain(
        &schema,
        "/properties/exceptions/items/properties/name/enum",
        &ExceptionName::ALL.map(ExceptionName::as_str),
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

fn validate_schema_domain(
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

fn validate_schema_value(value: &serde_json::Value, path: &str) -> Result<(), String> {
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

fn validate_spec(spec: &StateMachineSpec) -> Result<(), String> {
    if spec.transitions.is_empty() {
        return Err("state-machine spec has no transitions".into());
    }

    let mut operation_names = HashSet::new();
    for transition in &spec.transitions {
        if !operation_names.insert(transition.operation) {
            return Err(format!(
                "duplicate transition operation: {}",
                transition.operation.as_str()
            ));
        }
        if !transition.destination.is_destination() {
            return Err(format!(
                "transition {} has unknown destination state {}",
                transition.operation.as_str(),
                transition.destination.as_str()
            ));
        }
        if transition.required_syncs.is_empty() {
            return Err(format!(
                "transition {} has no required syncs",
                transition.operation.as_str()
            ));
        }
        if !transition.no_overwrite {
            return Err(format!(
                "transition {} must prohibit overwrite",
                transition.operation.as_str()
            ));
        }
        let mut syncs = HashSet::new();
        for sync in &transition.required_syncs {
            if !syncs.insert(*sync) {
                return Err(format!(
                    "transition {} contains duplicate sync {}",
                    transition.operation.as_str(),
                    sync.as_str()
                ));
            }
        }
        if transition.resolution_behavior.trim().is_empty() {
            return Err(format!(
                "transition {} has empty resolution behavior",
                transition.operation.as_str()
            ));
        }
        if matches!(
            &transition.notes,
            Nullable::Value(value) if value.trim().is_empty()
        ) {
            return Err(format!(
                "transition {} has empty notes",
                transition.operation.as_str()
            ));
        }
        validate_transition_invariant(transition)?;
    }
    for operation in Operation::ALL {
        if !operation_names.contains(&operation) {
            return Err(format!(
                "state-machine spec is missing transition operation: {}",
                operation.as_str()
            ));
        }
    }

    let mut exception_names = HashSet::new();
    for exception in &spec.exceptions {
        if !exception_names.insert(exception.name) {
            return Err(format!("duplicate exception: {}", exception.name.as_str()));
        }
        if exception.description.trim().is_empty() {
            return Err(format!(
                "exception {} has no description",
                exception.name.as_str()
            ));
        }
        if !exception.uses_replacing_rename {
            return Err(format!(
                "exception {} must declare replacing rename behavior",
                exception.name.as_str()
            ));
        }
    }
    for exception in ExceptionName::ALL {
        if !exception_names.contains(&exception) {
            return Err(format!(
                "state-machine spec is missing exception: {}",
                exception.as_str()
            ));
        }
    }

    let mut reentry_names = HashSet::new();
    for reentry in &spec.reentry {
        if !reentry_names.insert(reentry.name) {
            return Err(format!("duplicate reentry: {}", reentry.name.as_str()));
        }
        if reentry.source != reentry.name.expected_source() {
            return Err(format!(
                "reentry {} has source state {}; expected {}",
                reentry.name.as_str(),
                reentry.source.as_str(),
                reentry.name.expected_source().as_str()
            ));
        }
        if reentry.description.trim().is_empty() {
            return Err(format!(
                "reentry {} has no description",
                reentry.name.as_str()
            ));
        }
        if !reentry.creates_new_identity {
            return Err(format!(
                "reentry {} must create a new identity",
                reentry.name.as_str()
            ));
        }
    }
    for reentry in ReentryName::ALL {
        if !reentry_names.contains(&reentry) {
            return Err(format!(
                "state-machine spec is missing reentry: {}",
                reentry.as_str()
            ));
        }
    }
    Ok(())
}

fn validate_transition_invariant(transition: &Transition) -> Result<(), String> {
    let expected = transition.operation.invariant();
    if transition.source != expected.source {
        return Err(format!(
            "transition {} has source {}; expected {}",
            transition.operation.as_str(),
            transition.source.as_str(),
            expected.source.as_str()
        ));
    }
    if transition.destination != expected.destination {
        return Err(format!(
            "transition {} has destination {}; expected {}",
            transition.operation.as_str(),
            transition.destination.as_str(),
            expected.destination.as_str()
        ));
    }
    if transition.generation_change != expected.generation_change {
        return Err(format!(
            "transition {} has generation change {}; expected {}",
            transition.operation.as_str(),
            transition.generation_change.as_str(),
            expected.generation_change.as_str()
        ));
    }
    if transition.attempt_change != expected.attempt_change {
        return Err(format!(
            "transition {} has attempt change {}; expected {}",
            transition.operation.as_str(),
            transition.attempt_change.as_str(),
            expected.attempt_change.as_str()
        ));
    }
    if transition.token_change != expected.token_change {
        return Err(format!(
            "transition {} has token change {}; expected {}",
            transition.operation.as_str(),
            transition.token_change.as_str(),
            expected.token_change.as_str()
        ));
    }
    let reason_class = match &transition.reason_class {
        Nullable::Value(reason) => Some(*reason),
        Nullable::Null => None,
    };
    if reason_class != expected.reason_class {
        return Err(format!(
            "transition {} has reason class {}; expected {}",
            transition.operation.as_str(),
            reason_class.map_or("none", ReasonClass::as_str),
            expected.reason_class.map_or("none", ReasonClass::as_str)
        ));
    }
    if transition.required_syncs != expected.required_syncs {
        let actual = transition
            .required_syncs
            .iter()
            .copied()
            .map(SyncStep::as_str)
            .collect::<Vec<_>>();
        let expected = expected
            .required_syncs
            .iter()
            .copied()
            .map(SyncStep::as_str)
            .collect::<Vec<_>>();
        return Err(format!(
            "transition {} has syncs {actual:?}; expected {expected:?}",
            transition.operation.as_str()
        ));
    }
    Ok(())
}

fn check_generated(root: &Path) -> Result<(), String> {
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

fn generate(root: &Path) -> Result<(), String> {
    let (spec, digest) = load_spec(root)?;
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
            rust_string(transition.operation.as_str()),
            rust_string(transition.source.as_str()),
            rust_string(transition.destination.as_str()),
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
            rust_string(transition.source.as_str()),
            rust_string(transition.destination.as_str()),
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
            rust_string(transition.operation.as_str()),
            rust_string(transition.source.as_str()),
            rust_string(transition.destination.as_str()),
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
| Operation | Source | Destination | Gen | Attempt | Token | Reason | No-overwrite |\n\
|-----------|--------|-------------|-----|---------|-------|--------|--------------|\n",
    );
    for transition in &spec.transitions {
        writeln!(
            output,
            "| {} | {} | {} | {} | {} | {} | {} | {} |",
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
            markdown(exception.name.as_str()),
            markdown(&exception.description)
        )
        .expect("writing to String cannot fail");
    }
    output.push_str("\n## Administrative re-entry (creates new identity)\n\n");
    for reentry in &spec.reentry {
        writeln!(
            output,
            "**{}** (from {}): {}",
            markdown(reentry.name.as_str()),
            markdown(reentry.source.as_str()),
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
    const ALL: [Self; 3] = [Self::Zero, Self::Increment, Self::IncrementOrSame];

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

impl Operation {
    const ALL: [Self; 13] = [
        Self::EnqueueImmediate,
        Self::EnqueueDelayed,
        Self::Promote,
        Self::Claim,
        Self::ExhaustedReadyCleanup,
        Self::Renew,
        Self::Acknowledge,
        Self::RetryNow,
        Self::RetryLater,
        Self::Bury,
        Self::ReapExpiredToReady,
        Self::ReapExpiredToDead,
        Self::Quarantine,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::EnqueueImmediate => "enqueue_immediate",
            Self::EnqueueDelayed => "enqueue_delayed",
            Self::Promote => "promote",
            Self::Claim => "claim",
            Self::ExhaustedReadyCleanup => "exhausted_ready_cleanup",
            Self::Renew => "renew",
            Self::Acknowledge => "acknowledge",
            Self::RetryNow => "retry_now",
            Self::RetryLater => "retry_later",
            Self::Bury => "bury",
            Self::ReapExpiredToReady => "reap_expired_to_ready",
            Self::ReapExpiredToDead => "reap_expired_to_dead",
            Self::Quarantine => "quarantine",
        }
    }

    fn invariant(self) -> TransitionInvariant {
        use AttemptChange::{Increment as AttemptIncrement, Unchanged, Zero as AttemptZero};
        use GenerationChange::{Increment as GenerationIncrement, Zero as GenerationZero};
        use ReasonClass::{ApplicationDefined, AttemptsExhausted, Corruption};
        use State::{Active, Dead, Delayed, Hidden, Leased, Quarantine, Ready, Receipt};
        use SyncStep::{DestinationDir, File, SameOrDestinationDir, SourceDir};
        use TokenChange::{New, None as NoToken, Same};

        match self {
            Self::EnqueueImmediate => TransitionInvariant {
                source: Hidden,
                destination: Ready,
                generation_change: GenerationZero,
                attempt_change: AttemptZero,
                token_change: NoToken,
                reason_class: None,
                required_syncs: &[File, DestinationDir],
            },
            Self::EnqueueDelayed => TransitionInvariant {
                source: Hidden,
                destination: Delayed,
                generation_change: GenerationZero,
                attempt_change: AttemptZero,
                token_change: NoToken,
                reason_class: None,
                required_syncs: &[File, DestinationDir],
            },
            Self::Promote => TransitionInvariant {
                source: Delayed,
                destination: Ready,
                generation_change: GenerationIncrement,
                attempt_change: Unchanged,
                token_change: NoToken,
                reason_class: None,
                required_syncs: &[DestinationDir, SourceDir],
            },
            Self::Claim => TransitionInvariant {
                source: Ready,
                destination: Leased,
                generation_change: GenerationIncrement,
                attempt_change: AttemptIncrement,
                token_change: New,
                reason_class: None,
                required_syncs: &[DestinationDir, SourceDir],
            },
            Self::ExhaustedReadyCleanup => TransitionInvariant {
                source: Ready,
                destination: Dead,
                generation_change: GenerationIncrement,
                attempt_change: Unchanged,
                token_change: NoToken,
                reason_class: Some(AttemptsExhausted),
                required_syncs: &[DestinationDir, SourceDir],
            },
            Self::Renew => TransitionInvariant {
                source: Leased,
                destination: Leased,
                generation_change: GenerationIncrement,
                attempt_change: Unchanged,
                token_change: Same,
                reason_class: None,
                required_syncs: &[SameOrDestinationDir],
            },
            Self::Acknowledge => TransitionInvariant {
                source: Leased,
                destination: Receipt,
                generation_change: GenerationIncrement,
                attempt_change: Unchanged,
                token_change: Same,
                reason_class: None,
                required_syncs: &[DestinationDir, SourceDir],
            },
            Self::RetryNow => TransitionInvariant {
                source: Leased,
                destination: Ready,
                generation_change: GenerationIncrement,
                attempt_change: Unchanged,
                token_change: NoToken,
                reason_class: None,
                required_syncs: &[DestinationDir, SourceDir],
            },
            Self::RetryLater => TransitionInvariant {
                source: Leased,
                destination: Delayed,
                generation_change: GenerationIncrement,
                attempt_change: Unchanged,
                token_change: NoToken,
                reason_class: None,
                required_syncs: &[DestinationDir, SourceDir],
            },
            Self::Bury => TransitionInvariant {
                source: Leased,
                destination: Dead,
                generation_change: GenerationIncrement,
                attempt_change: Unchanged,
                token_change: NoToken,
                reason_class: Some(ApplicationDefined),
                required_syncs: &[DestinationDir, SourceDir],
            },
            Self::ReapExpiredToReady => TransitionInvariant {
                source: Leased,
                destination: Ready,
                generation_change: GenerationIncrement,
                attempt_change: Unchanged,
                token_change: NoToken,
                reason_class: None,
                required_syncs: &[DestinationDir, SourceDir],
            },
            Self::ReapExpiredToDead => TransitionInvariant {
                source: Leased,
                destination: Dead,
                generation_change: GenerationIncrement,
                attempt_change: Unchanged,
                token_change: NoToken,
                reason_class: Some(AttemptsExhausted),
                required_syncs: &[DestinationDir, SourceDir],
            },
            Self::Quarantine => TransitionInvariant {
                source: Active,
                destination: Quarantine,
                generation_change: GenerationIncrement,
                attempt_change: Unchanged,
                token_change: NoToken,
                reason_class: Some(Corruption),
                required_syncs: &[DestinationDir, SourceDir],
            },
        }
    }
}

impl State {
    const ALL: [Self; 8] = [
        Self::Hidden,
        Self::Ready,
        Self::Leased,
        Self::Delayed,
        Self::Dead,
        Self::Receipt,
        Self::Quarantine,
        Self::Active,
    ];
    const DESTINATIONS: [Self; 6] = [
        Self::Ready,
        Self::Leased,
        Self::Delayed,
        Self::Dead,
        Self::Receipt,
        Self::Quarantine,
    ];
    const REENTRY_SOURCES: [Self; 2] = [Self::Dead, Self::Quarantine];

    fn as_str(self) -> &'static str {
        match self {
            Self::Hidden => "hidden",
            Self::Ready => "ready",
            Self::Leased => "leased",
            Self::Delayed => "delayed",
            Self::Dead => "dead",
            Self::Receipt => "receipt",
            Self::Quarantine => "quarantine",
            Self::Active => "active",
        }
    }

    fn is_destination(self) -> bool {
        matches!(
            self,
            Self::Ready
                | Self::Leased
                | Self::Delayed
                | Self::Dead
                | Self::Receipt
                | Self::Quarantine
        )
    }
}

impl AttemptChange {
    const ALL: [Self; 3] = [Self::Zero, Self::Increment, Self::Unchanged];

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
    const ALL: [Self; 3] = [Self::None, Self::New, Self::Same];

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

impl ReasonClass {
    const ALL: [Self; 3] = [
        Self::AttemptsExhausted,
        Self::ApplicationDefined,
        Self::Corruption,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::AttemptsExhausted => "attempts_exhausted",
            Self::ApplicationDefined => "application_defined",
            Self::Corruption => "corruption",
        }
    }
}

impl SyncStep {
    const ALL: [Self; 4] = [
        Self::File,
        Self::DestinationDir,
        Self::SourceDir,
        Self::SameOrDestinationDir,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::File => "file_fsync",
            Self::DestinationDir => "destination_dir_fsync",
            Self::SourceDir => "source_dir_fsync",
            Self::SameOrDestinationDir => "same_or_destination_dir_fsync",
        }
    }
}

impl ExceptionName {
    const ALL: [Self; 2] = [Self::ReceiptCompaction, Self::WallWatermarkAdvancement];

    fn as_str(self) -> &'static str {
        match self {
            Self::ReceiptCompaction => "receipt_compaction",
            Self::WallWatermarkAdvancement => "wall_watermark_advancement",
        }
    }
}

impl ReentryName {
    const ALL: [Self; 2] = [Self::RequeueDead, Self::RequeueQuarantine];

    fn as_str(self) -> &'static str {
        match self {
            Self::RequeueDead => "requeue_dead",
            Self::RequeueQuarantine => "requeue_quarantine",
        }
    }

    fn expected_source(self) -> State {
        match self {
            Self::RequeueDead => State::Dead,
            Self::RequeueQuarantine => State::Quarantine,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::path::Path;
    use tempfile::TempDir;

    fn fixture() -> StateMachineSpec {
        serde_json::from_value(fixture_value()).unwrap()
    }

    fn fixture_value() -> Value {
        serde_json::from_str(include_str!("../../spec/state-machine.json")).unwrap()
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
            (SPEC, include_str!("../../spec/state-machine.json")),
            (SCHEMA, include_str!("../../spec/state-machine.schema.json")),
            (
                GENERATED_RUST,
                include_str!("../../generated/state-machine.rs"),
            ),
            (
                GENERATED_GO,
                include_str!("../../generated/state-machine.go"),
            ),
            (
                GENERATED_MARKDOWN,
                include_str!("../../generated/state-machine.md"),
            ),
            (
                CORE_RUST,
                include_str!("../../crates/steadq-core/src/state_machine.rs"),
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
                include_str!("../../generated/state-machine.rs"),
            ),
            (
                GENERATED_GO,
                include_str!("../../generated/state-machine.go"),
            ),
            (
                GENERATED_MARKDOWN,
                include_str!("../../generated/state-machine.md"),
            ),
            (
                CORE_RUST,
                include_str!("../../crates/steadq-core/src/state_machine.rs"),
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
            serde_json::from_str(include_str!("../../spec/state-machine.schema.json")).unwrap();
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
            serde_json::from_str(include_str!("../../spec/state-machine.schema.json")).unwrap();
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
            serde_json::from_str(include_str!("../../spec/state-machine.schema.json")).unwrap();
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
}
