use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

const GENERATED_RUST: &str = "generated/state-machine.rs";
const CORE_RUST: &str = "crates/steadq-core/src/state_machine.rs";

fn main() -> ExitCode {
    let result = match env::args().nth(1).as_deref() {
        Some("check") => check_all(),
        Some("check-generated") => check_generated(),
        Some("generate") => Err(
            "generation is intentionally unavailable until A-007 closes the protocol IR; use check-generated"
                .into(),
        ),
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
    if env::consts::OS != "linux" || env::consts::ARCH != "x86_64" {
        return Err(format!(
            "strict prototype target is x86_64 Linux; got {} {}",
            env::consts::ARCH,
            env::consts::OS
        ));
    }
    Ok(())
}

fn check_generated() -> Result<(), String> {
    let root = workspace_root()?;
    let generated = fs::read(root.join(GENERATED_RUST))
        .map_err(|error| format!("cannot read {GENERATED_RUST}: {error}"))?;
    let core = fs::read(root.join(CORE_RUST))
        .map_err(|error| format!("cannot read {CORE_RUST}: {error}"))?;

    let generated_text = String::from_utf8(generated)
        .map_err(|error| format!("{GENERATED_RUST} is not UTF-8: {error}"))?;
    let core_text =
        String::from_utf8(core).map_err(|error| format!("{CORE_RUST} is not UTF-8: {error}"))?;

    if normalized_transition_table(&generated_text)? != normalized_transition_table(&core_text)? {
        return Err(format!(
            "the transition table in {CORE_RUST} differs from {GENERATED_RUST}; A-007 will replace this mirror check with deterministic generation"
        ));
    }
    if !generated_text.starts_with("// Auto-generated from spec/state-machine.json.") {
        return Err(format!("{GENERATED_RUST} is missing its source marker"));
    }
    for (artifact, marker) in [
        (
            "generated/state-machine.go",
            "Auto-generated from spec/state-machine.json",
        ),
        ("generated/state-machine.md", "State Machine (Generated)"),
    ] {
        let contents = fs::read_to_string(root.join(artifact))
            .map_err(|error| format!("cannot read {artifact}: {error}"))?;
        if !contents.contains(marker) {
            return Err(format!("{artifact} is missing its source marker"));
        }
    }
    Ok(())
}

fn normalized_transition_table(contents: &str) -> Result<String, String> {
    let start = contents
        .find("pub const TRANSITIONS")
        .ok_or_else(|| "missing TRANSITIONS table".to_owned())?;
    let table = &contents[start..];
    let end = table
        .find("];\n")
        .ok_or_else(|| "unterminated TRANSITIONS table".to_owned())?
        + 2;
    let normalized: String = table[..end]
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect();
    Ok(normalized.replace(",}", "}"))
}
