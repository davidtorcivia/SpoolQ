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
