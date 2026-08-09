use std::collections::HashSet;
use std::fs;
use std::path::Path;

const MODEL: &str = "model/SteadQ.tla";
const CONFIG: &str = "model/SteadQ.cfg";
const README: &str = "model/README.md";
const INVARIANT_START: &str = "(* ---- Invariants ---- *)";
const INVARIANT_END: &str = "(* ---- End invariants ---- *)";
const README_INVARIANT_START: &str = "## Invariants checked";

pub(crate) fn check_invariant_evidence(root: &Path) -> Result<(), String> {
    let model = read(root, MODEL)?;
    let config = read(root, CONFIG)?;
    let readme = read(root, README)?;

    let claimed = claimed_invariants(&model)?;
    let configured = configured_invariants(&config)?;
    if configured != claimed {
        return Err(format!(
            "{CONFIG} invariant list differs from {MODEL}: expected {claimed:?}, got {configured:?}"
        ));
    }
    let documented = documented_invariants(&readme)?;
    if documented != claimed {
        return Err(format!(
            "{README} invariant list differs from {MODEL}: expected {claimed:?}, got {documented:?}"
        ));
    }
    Ok(())
}

fn read(root: &Path, relative: &str) -> Result<String, String> {
    fs::read_to_string(root.join(relative))
        .map_err(|error| format!("cannot read {relative}: {error}"))
}

fn claimed_invariants(model: &str) -> Result<Vec<String>, String> {
    let type_invariant = model
        .lines()
        .filter_map(top_level_operator_name)
        .find(|name| *name == "TypeInvariant")
        .ok_or_else(|| format!("{MODEL} does not define TypeInvariant"))?;
    let start = model
        .find(INVARIANT_START)
        .ok_or_else(|| format!("{MODEL} is missing {INVARIANT_START}"))?;
    let body_start = start + INVARIANT_START.len();
    let relative_end = model[body_start..]
        .find(INVARIANT_END)
        .ok_or_else(|| format!("{MODEL} is missing {INVARIANT_END}"))?;
    let body = &model[body_start..body_start + relative_end];

    let mut invariants = vec![type_invariant.to_owned()];
    invariants.extend(
        body.lines()
            .filter_map(top_level_operator_name)
            .map(str::to_owned),
    );
    if invariants.len() == 1 {
        return Err(format!("{MODEL} invariant section is empty"));
    }
    reject_duplicates(MODEL, &invariants)?;
    Ok(invariants)
}

fn top_level_operator_name(line: &str) -> Option<&str> {
    let trimmed = line.trim();
    if line != trimmed {
        return None;
    }
    let (name, _) = trimmed.split_once("==")?;
    let name = name.trim();
    if !is_ascii_identifier(name) {
        return None;
    }
    Some(name)
}

fn is_ascii_identifier(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn configured_invariants(config: &str) -> Result<Vec<String>, String> {
    let mut invariants = Vec::new();
    let mut collecting = false;
    let mut found = false;
    for line in config.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("\\*") {
            continue;
        }
        let mut words = trimmed.split_whitespace();
        let first = words.next().expect("nonempty line has a first word");
        if matches!(first, "INVARIANT" | "INVARIANTS") {
            if found {
                return Err(format!("{CONFIG} contains multiple invariant sections"));
            }
            found = true;
            collecting = true;
            invariants.extend(words.map(str::to_owned));
            continue;
        }
        if collecting {
            if is_config_keyword(first) {
                collecting = false;
            } else {
                invariants.extend(trimmed.split_whitespace().map(str::to_owned));
            }
        }
    }
    if !found {
        return Err(format!("{CONFIG} has no INVARIANT section"));
    }
    if invariants.is_empty() {
        return Err(format!("{CONFIG} INVARIANT section is empty"));
    }
    reject_duplicates(CONFIG, &invariants)?;
    Ok(invariants)
}

fn documented_invariants(readme: &str) -> Result<Vec<String>, String> {
    let start = readme
        .find(README_INVARIANT_START)
        .ok_or_else(|| format!("{README} is missing {README_INVARIANT_START}"))?;
    let body_start = start + README_INVARIANT_START.len();
    let body = &readme[body_start..];
    let body_end = body.find("\n## ").unwrap_or(body.len());
    let mut invariants = Vec::new();
    for line in body[..body_end].lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with("- ") {
            continue;
        }
        let remainder = trimmed
            .strip_prefix("- `")
            .ok_or_else(|| format!("{README} has a malformed invariant bullet: {trimmed}"))?;
        let (name, _) = remainder
            .split_once("`:")
            .ok_or_else(|| format!("{README} has a malformed invariant bullet: {trimmed}"))?;
        if !is_ascii_identifier(name) {
            return Err(format!(
                "{README} has an invalid invariant name in bullet: {trimmed}"
            ));
        }
        invariants.push(name.to_owned());
    }
    if invariants.is_empty() {
        return Err(format!("{README} invariant section is empty"));
    }
    reject_duplicates(README, &invariants)?;
    Ok(invariants)
}

fn is_config_keyword(word: &str) -> bool {
    matches!(
        word,
        "CONSTANT"
            | "CONSTANTS"
            | "CONSTRAINT"
            | "CONSTRAINTS"
            | "INIT"
            | "NEXT"
            | "PROPERTY"
            | "PROPERTIES"
            | "SPECIFICATION"
            | "SYMMETRY"
            | "VIEW"
    )
}

fn reject_duplicates(source: &str, values: &[String]) -> Result<(), String> {
    let mut unique = HashSet::new();
    for value in values {
        if !unique.insert(value) {
            return Err(format!("{source} contains duplicate invariant {value}"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_MODEL: &str = r#"
TypeInvariant == TRUE
(* ---- Invariants ---- *)
FirstInvariant == TRUE
SecondInvariant == TRUE
(* ---- End invariants ---- *)
"#;
    const VALID_CONFIG: &str = r#"
INIT Init
NEXT Next
INVARIANT
  TypeInvariant
  FirstInvariant
  SecondInvariant

CONSTANTS
  Nil = Nil
"#;
    const VALID_README: &str = r#"
## Invariants checked

- `TypeInvariant`: types.
- `FirstInvariant`: first.
- `SecondInvariant`: second.
"#;

    #[test]
    fn parses_exact_claimed_and_configured_invariants() {
        assert_eq!(
            claimed_invariants(VALID_MODEL).unwrap(),
            ["TypeInvariant", "FirstInvariant", "SecondInvariant"]
        );
        assert_eq!(
            configured_invariants(VALID_CONFIG).unwrap(),
            ["TypeInvariant", "FirstInvariant", "SecondInvariant"]
        );
        assert_eq!(
            documented_invariants(VALID_README).unwrap(),
            ["TypeInvariant", "FirstInvariant", "SecondInvariant"]
        );
    }

    #[test]
    fn top_level_operator_names_are_nonempty_ascii_identifiers() {
        for (line, expected) in [
            ("Invariant == TRUE", Some("Invariant")),
            ("Invariant_2 == TRUE", Some("Invariant_2")),
            ("== TRUE", None),
            ("Bad-Name == TRUE", None),
            ("Bad:Name == TRUE", None),
            ("  Nested == TRUE", None),
            ("NoDefinition", None),
        ] {
            assert_eq!(top_level_operator_name(line), expected, "{line}");
        }
    }

    #[test]
    fn rejects_missing_empty_and_duplicate_invariant_sections() {
        assert!(claimed_invariants("TypeInvariant == TRUE").is_err());
        assert!(claimed_invariants(&format!(
            "TypeInvariant == TRUE\n{INVARIANT_START}\n{INVARIANT_END}"
        ))
        .is_err());
        assert!(claimed_invariants(&format!(
            "TypeInvariant == TRUE\n{INVARIANT_START}\nTypeInvariant == TRUE\n{INVARIANT_END}"
        ))
        .is_err());
        assert!(configured_invariants("INIT Init").is_err());
        assert!(configured_invariants("INVARIANT\nCONSTANTS").is_err());
        assert!(configured_invariants("INVARIANT Same Same\nCONSTANTS").is_err());
        assert!(documented_invariants("# Missing section").is_err());
        assert!(documented_invariants("## Invariants checked\n\n## Next").is_err());
        assert!(documented_invariants("## Invariants checked\n\n- malformed\n").is_err());
        assert!(documented_invariants(
            "## Invariants checked\n\n- `Duplicate`: one.\n- `Duplicate`: two.\n"
        )
        .is_err());
    }

    #[test]
    fn repository_evidence_check_rejects_config_and_readme_drift() {
        let temp = tempfile::tempdir().unwrap();
        fs::create_dir(temp.path().join("model")).unwrap();
        for (path, contents) in [
            (MODEL, VALID_MODEL),
            (CONFIG, VALID_CONFIG),
            (README, VALID_README),
        ] {
            fs::write(temp.path().join(path), contents).unwrap();
        }
        check_invariant_evidence(temp.path()).unwrap();

        fs::write(
            temp.path().join(CONFIG),
            VALID_CONFIG.replace("  SecondInvariant\n", ""),
        )
        .unwrap();
        assert_eq!(
            check_invariant_evidence(temp.path()).unwrap_err(),
            "model/SteadQ.cfg invariant list differs from model/SteadQ.tla: expected [\"TypeInvariant\", \"FirstInvariant\", \"SecondInvariant\"], got [\"TypeInvariant\", \"FirstInvariant\"]"
        );

        fs::write(temp.path().join(CONFIG), VALID_CONFIG).unwrap();
        fs::write(
            temp.path().join(README),
            VALID_README.replace("- `SecondInvariant`: second.\n", ""),
        )
        .unwrap();
        assert_eq!(
            check_invariant_evidence(temp.path()).unwrap_err(),
            "model/README.md invariant list differs from model/SteadQ.tla: expected [\"TypeInvariant\", \"FirstInvariant\", \"SecondInvariant\"], got [\"TypeInvariant\", \"FirstInvariant\"]"
        );

        fs::write(
            temp.path().join(README),
            format!("{VALID_README}- `ExtraInvariant`: extra.\n"),
        )
        .unwrap();
        assert_eq!(
            check_invariant_evidence(temp.path()).unwrap_err(),
            "model/README.md invariant list differs from model/SteadQ.tla: expected [\"TypeInvariant\", \"FirstInvariant\", \"SecondInvariant\"], got [\"TypeInvariant\", \"FirstInvariant\", \"SecondInvariant\", \"ExtraInvariant\"]"
        );
    }
}
