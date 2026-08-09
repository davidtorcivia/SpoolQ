use std::collections::HashSet;
use std::fs;
use std::path::Path;

struct ModelEvidence {
    model: &'static str,
    configs: &'static [&'static str],
    readme: &'static str,
}

const MODEL: &str = "model/SteadQ.tla";
const CONFIG: &str = "model/SteadQ.cfg";
const README: &str = "model/README.md";
const MODEL_EVIDENCE: &[ModelEvidence] = &[
    ModelEvidence {
        model: MODEL,
        configs: &[CONFIG],
        readme: README,
    },
    ModelEvidence {
        model: "model/SteadQNamespace.tla",
        configs: &[
            "model/namespace/SteadQNamespaceOrdered.cfg",
            "model/namespace/SteadQNamespaceWeak.cfg",
        ],
        readme: "model/namespace/README.md",
    },
    ModelEvidence {
        model: "model/SteadQScheduling.tla",
        configs: &["model/scheduling/SteadQScheduling.cfg"],
        readme: "model/scheduling/README.md",
    },
    ModelEvidence {
        model: "model/SteadQReceipts.tla",
        configs: &["model/receipts/SteadQReceipts.cfg"],
        readme: "model/receipts/README.md",
    },
    ModelEvidence {
        model: "model/SteadQMaintenance.tla",
        configs: &["model/maintenance/SteadQMaintenance.cfg"],
        readme: "model/maintenance/README.md",
    },
];
const INVARIANT_START: &str = "(* ---- Invariants ---- *)";
const INVARIANT_END: &str = "(* ---- End invariants ---- *)";
const PROPERTY_START: &str = "(* ---- Properties ---- *)";
const PROPERTY_END: &str = "(* ---- End properties ---- *)";
const README_INVARIANT_START: &str = "## Invariants checked";
const README_PROPERTY_START: &str = "## Temporal properties checked";

pub(crate) fn check_model_evidence(root: &Path) -> Result<(), String> {
    for evidence in MODEL_EVIDENCE {
        let model = read(root, evidence.model)?;
        let claimed = claimed_invariants(&model, evidence.model)?;
        for config_path in evidence.configs {
            let config = read(root, config_path)?;
            let configured = configured_invariants(&config, config_path)?;
            if configured != claimed {
                return Err(format!(
                    "{config_path} invariant list differs from {}: expected {claimed:?}, got {configured:?}",
                    evidence.model
                ));
            }
        }
        let readme = read(root, evidence.readme)?;
        let documented = documented_invariants(&readme, evidence.readme)?;
        if documented != claimed {
            return Err(format!(
                "{} invariant list differs from {}: expected {claimed:?}, got {documented:?}",
                evidence.readme, evidence.model
            ));
        }

        let claimed_properties = claimed_properties(&model, evidence.model)?;
        for config_path in evidence.configs {
            let config = read(root, config_path)?;
            let configured = configured_properties(&config, config_path)?;
            if configured != claimed_properties {
                return Err(format!(
                    "{config_path} property list differs from {}: expected {claimed_properties:?}, got {configured:?}",
                    evidence.model
                ));
            }
        }
        let documented_properties = documented_properties(&readme, evidence.readme)?;
        if documented_properties != claimed_properties {
            return Err(format!(
                "{} property list differs from {}: expected {claimed_properties:?}, got {documented_properties:?}",
                evidence.readme, evidence.model
            ));
        }
    }
    Ok(())
}

fn read(root: &Path, relative: &str) -> Result<String, String> {
    fs::read_to_string(root.join(relative))
        .map_err(|error| format!("cannot read {relative}: {error}"))
}

fn claimed_invariants(model: &str, source: &str) -> Result<Vec<String>, String> {
    let type_invariant = model
        .lines()
        .filter_map(top_level_operator_name)
        .find(|name| *name == "TypeInvariant")
        .ok_or_else(|| format!("{source} does not define TypeInvariant"))?;
    let start = model
        .find(INVARIANT_START)
        .ok_or_else(|| format!("{source} is missing {INVARIANT_START}"))?;
    let body_start = start + INVARIANT_START.len();
    let relative_end = model[body_start..]
        .find(INVARIANT_END)
        .ok_or_else(|| format!("{source} is missing {INVARIANT_END}"))?;
    let body = &model[body_start..body_start + relative_end];

    let mut invariants = vec![type_invariant.to_owned()];
    invariants.extend(
        body.lines()
            .filter_map(top_level_operator_name)
            .map(str::to_owned),
    );
    if invariants.len() == 1 {
        return Err(format!("{source} invariant section is empty"));
    }
    reject_duplicates(source, &invariants)?;
    Ok(invariants)
}

fn claimed_properties(model: &str, source: &str) -> Result<Vec<String>, String> {
    let Some(start) = model.find(PROPERTY_START) else {
        if model.contains(PROPERTY_END) {
            return Err(format!("{source} is missing {PROPERTY_START}"));
        }
        return Ok(Vec::new());
    };
    let body_start = start + PROPERTY_START.len();
    let relative_end = model[body_start..]
        .find(PROPERTY_END)
        .ok_or_else(|| format!("{source} is missing {PROPERTY_END}"))?;
    let properties = model[body_start..body_start + relative_end]
        .lines()
        .filter_map(top_level_operator_name)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if properties.is_empty() {
        return Err(format!("{source} property section is empty"));
    }
    reject_duplicates(source, &properties)?;
    Ok(properties)
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

fn configured_invariants(config: &str, source: &str) -> Result<Vec<String>, String> {
    configured_claims(
        config,
        source,
        &["INVARIANT", "INVARIANTS"],
        "INVARIANT",
        true,
    )
}

fn configured_properties(config: &str, source: &str) -> Result<Vec<String>, String> {
    configured_claims(
        config,
        source,
        &["PROPERTY", "PROPERTIES"],
        "PROPERTY",
        false,
    )
}

fn configured_claims(
    config: &str,
    source: &str,
    keywords: &[&str],
    label: &str,
    required: bool,
) -> Result<Vec<String>, String> {
    let mut values = Vec::new();
    let mut collecting = false;
    let mut found = false;
    for line in config.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("\\*") {
            continue;
        }
        let mut words = trimmed.split_whitespace();
        let first = words.next().expect("nonempty line has a first word");
        if keywords.contains(&first) {
            if found {
                return Err(format!("{source} contains multiple {label} sections"));
            }
            found = true;
            collecting = true;
            values.extend(words.map(str::to_owned));
            continue;
        }
        if collecting {
            if is_config_keyword(first) {
                collecting = false;
            } else {
                values.extend(trimmed.split_whitespace().map(str::to_owned));
            }
        }
    }
    if !found {
        return if required {
            Err(format!("{source} has no {label} section"))
        } else {
            Ok(Vec::new())
        };
    }
    if values.is_empty() {
        return Err(format!("{source} {label} section is empty"));
    }
    reject_duplicates(source, &values)?;
    Ok(values)
}

fn documented_invariants(readme: &str, source: &str) -> Result<Vec<String>, String> {
    documented_claims(readme, source, README_INVARIANT_START, true)
}

fn documented_properties(readme: &str, source: &str) -> Result<Vec<String>, String> {
    documented_claims(readme, source, README_PROPERTY_START, false)
}

fn documented_claims(
    readme: &str,
    source: &str,
    heading: &str,
    required: bool,
) -> Result<Vec<String>, String> {
    let Some(start) = readme.find(heading) else {
        return if required {
            Err(format!("{source} is missing {heading}"))
        } else {
            Ok(Vec::new())
        };
    };
    let body_start = start + heading.len();
    let body = &readme[body_start..];
    let body_end = body.find("\n## ").unwrap_or(body.len());
    let mut values = Vec::new();
    for line in body[..body_end].lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with("- ") {
            continue;
        }
        let remainder = trimmed
            .strip_prefix("- `")
            .ok_or_else(|| format!("{source} has a malformed claim bullet: {trimmed}"))?;
        let (name, _) = remainder
            .split_once("`:")
            .ok_or_else(|| format!("{source} has a malformed claim bullet: {trimmed}"))?;
        if !is_ascii_identifier(name) {
            return Err(format!(
                "{source} has an invalid claim name in bullet: {trimmed}"
            ));
        }
        values.push(name.to_owned());
    }
    if values.is_empty() {
        return Err(format!("{source} {heading} section is empty"));
    }
    reject_duplicates(source, &values)?;
    Ok(values)
}

fn is_config_keyword(word: &str) -> bool {
    matches!(
        word,
        "CONSTANT"
            | "CONSTANTS"
            | "CONSTRAINT"
            | "CONSTRAINTS"
            | "INIT"
            | "INVARIANT"
            | "INVARIANTS"
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
            return Err(format!("{source} contains duplicate claim {value}"));
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
    const VALID_PROPERTY_MODEL: &str = r#"
(* ---- Properties ---- *)
FirstProperty == TRUE
SecondProperty == TRUE
(* ---- End properties ---- *)
"#;
    const VALID_PROPERTY_CONFIG: &str = r#"
SPECIFICATION Spec
PROPERTY
  FirstProperty
  SecondProperty
"#;
    const VALID_PROPERTY_README: &str = r#"
## Temporal properties checked

- `FirstProperty`: first.
- `SecondProperty`: second.
"#;

    #[test]
    fn parses_exact_claimed_and_configured_invariants() {
        assert_eq!(
            claimed_invariants(VALID_MODEL, MODEL).unwrap(),
            ["TypeInvariant", "FirstInvariant", "SecondInvariant"]
        );
        assert_eq!(
            configured_invariants(VALID_CONFIG, CONFIG).unwrap(),
            ["TypeInvariant", "FirstInvariant", "SecondInvariant"]
        );
        assert_eq!(
            documented_invariants(VALID_README, README).unwrap(),
            ["TypeInvariant", "FirstInvariant", "SecondInvariant"]
        );
    }

    #[test]
    fn parses_exact_claimed_and_configured_properties() {
        assert_eq!(
            claimed_properties(VALID_PROPERTY_MODEL, MODEL).unwrap(),
            ["FirstProperty", "SecondProperty"]
        );
        assert_eq!(
            configured_properties(VALID_PROPERTY_CONFIG, CONFIG).unwrap(),
            ["FirstProperty", "SecondProperty"]
        );
        assert_eq!(
            documented_properties(VALID_PROPERTY_README, README).unwrap(),
            ["FirstProperty", "SecondProperty"]
        );
        assert_eq!(
            claimed_properties(VALID_MODEL, MODEL).unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(
            configured_properties(VALID_CONFIG, CONFIG).unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(
            documented_properties(VALID_README, README).unwrap(),
            Vec::<String>::new()
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
        assert!(claimed_invariants("TypeInvariant == TRUE", MODEL).is_err());
        assert!(claimed_invariants(
            &format!("TypeInvariant == TRUE\n{INVARIANT_START}\n{INVARIANT_END}"),
            MODEL
        )
        .is_err());
        assert!(claimed_invariants(
            &format!(
                "TypeInvariant == TRUE\n{INVARIANT_START}\nTypeInvariant == TRUE\n{INVARIANT_END}"
            ),
            MODEL
        )
        .is_err());
        assert!(configured_invariants("INIT Init", CONFIG).is_err());
        assert!(configured_invariants("INVARIANT\nCONSTANTS", CONFIG).is_err());
        assert!(configured_invariants("INVARIANT Same Same\nCONSTANTS", CONFIG).is_err());
        assert!(documented_invariants("# Missing section", README).is_err());
        assert!(documented_invariants("## Invariants checked\n\n## Next", README).is_err());
        assert!(documented_invariants("## Invariants checked\n\n- malformed\n", README).is_err());
        assert!(documented_invariants(
            "## Invariants checked\n\n- `Duplicate`: one.\n- `Duplicate`: two.\n",
            README,
        )
        .is_err());
    }

    #[test]
    fn rejects_malformed_property_evidence() {
        assert!(claimed_properties(PROPERTY_END, MODEL).is_err());
        assert!(claimed_properties(&format!("{PROPERTY_START}\n{PROPERTY_END}"), MODEL).is_err());
        assert!(claimed_properties(
            &format!("{PROPERTY_START}\nSame == TRUE\nSame == TRUE\n{PROPERTY_END}"),
            MODEL
        )
        .is_err());
        assert!(configured_properties("PROPERTY\nCONSTANTS", CONFIG).is_err());
        assert!(configured_properties("PROPERTY Same Same", CONFIG).is_err());
        assert!(
            documented_properties("## Temporal properties checked\n\n## Next", README).is_err()
        );
        assert!(
            documented_properties("## Temporal properties checked\n\n- malformed", README).is_err()
        );
    }

    #[test]
    fn repository_evidence_check_rejects_config_and_readme_drift() {
        let temp = tempfile::tempdir().unwrap();
        for evidence in MODEL_EVIDENCE {
            let model = temp.path().join(evidence.model);
            fs::create_dir_all(model.parent().unwrap()).unwrap();
            fs::write(model, VALID_MODEL).unwrap();
            for config in evidence.configs {
                let config = temp.path().join(config);
                fs::create_dir_all(config.parent().unwrap()).unwrap();
                fs::write(config, VALID_CONFIG).unwrap();
            }
            let readme = temp.path().join(evidence.readme);
            fs::create_dir_all(readme.parent().unwrap()).unwrap();
            fs::write(readme, VALID_README).unwrap();
        }
        check_model_evidence(temp.path()).unwrap();

        let namespace_weak = "model/namespace/SteadQNamespaceWeak.cfg";
        fs::write(
            temp.path().join(namespace_weak),
            VALID_CONFIG.replace("  SecondInvariant\n", ""),
        )
        .unwrap();
        assert_eq!(
            check_model_evidence(temp.path()).unwrap_err(),
            "model/namespace/SteadQNamespaceWeak.cfg invariant list differs from model/SteadQNamespace.tla: expected [\"TypeInvariant\", \"FirstInvariant\", \"SecondInvariant\"], got [\"TypeInvariant\", \"FirstInvariant\"]"
        );
        fs::write(temp.path().join(namespace_weak), VALID_CONFIG).unwrap();

        fs::write(
            temp.path().join(CONFIG),
            VALID_CONFIG.replace("  SecondInvariant\n", ""),
        )
        .unwrap();
        assert_eq!(
            check_model_evidence(temp.path()).unwrap_err(),
            "model/SteadQ.cfg invariant list differs from model/SteadQ.tla: expected [\"TypeInvariant\", \"FirstInvariant\", \"SecondInvariant\"], got [\"TypeInvariant\", \"FirstInvariant\"]"
        );

        fs::write(temp.path().join(CONFIG), VALID_CONFIG).unwrap();
        fs::write(
            temp.path().join(README),
            VALID_README.replace("- `SecondInvariant`: second.\n", ""),
        )
        .unwrap();
        assert_eq!(
            check_model_evidence(temp.path()).unwrap_err(),
            "model/README.md invariant list differs from model/SteadQ.tla: expected [\"TypeInvariant\", \"FirstInvariant\", \"SecondInvariant\"], got [\"TypeInvariant\", \"FirstInvariant\"]"
        );

        fs::write(
            temp.path().join(README),
            format!("{VALID_README}- `ExtraInvariant`: extra.\n"),
        )
        .unwrap();
        assert_eq!(
            check_model_evidence(temp.path()).unwrap_err(),
            "model/README.md invariant list differs from model/SteadQ.tla: expected [\"TypeInvariant\", \"FirstInvariant\", \"SecondInvariant\"], got [\"TypeInvariant\", \"FirstInvariant\", \"SecondInvariant\", \"ExtraInvariant\"]"
        );
    }

    #[test]
    fn repository_evidence_check_rejects_property_drift() {
        let temp = tempfile::tempdir().unwrap();
        for evidence in MODEL_EVIDENCE {
            let model = temp.path().join(evidence.model);
            fs::create_dir_all(model.parent().unwrap()).unwrap();
            fs::write(model, VALID_MODEL).unwrap();
            for config in evidence.configs {
                let config = temp.path().join(config);
                fs::create_dir_all(config.parent().unwrap()).unwrap();
                fs::write(config, VALID_CONFIG).unwrap();
            }
            let readme = temp.path().join(evidence.readme);
            fs::create_dir_all(readme.parent().unwrap()).unwrap();
            fs::write(readme, VALID_README).unwrap();
        }

        let evidence = MODEL_EVIDENCE.last().unwrap();
        fs::write(
            temp.path().join(evidence.model),
            format!("{VALID_MODEL}\n{VALID_PROPERTY_MODEL}"),
        )
        .unwrap();
        fs::write(
            temp.path().join(evidence.configs[0]),
            format!("{VALID_CONFIG}\n{VALID_PROPERTY_CONFIG}"),
        )
        .unwrap();
        fs::write(
            temp.path().join(evidence.readme),
            format!("{VALID_README}\n{VALID_PROPERTY_README}"),
        )
        .unwrap();
        check_model_evidence(temp.path()).unwrap();

        fs::write(
            temp.path().join(evidence.configs[0]),
            format!(
                "{VALID_CONFIG}\n{}",
                VALID_PROPERTY_CONFIG.replace("  SecondProperty\n", "")
            ),
        )
        .unwrap();
        assert_eq!(
            check_model_evidence(temp.path()).unwrap_err(),
            "model/maintenance/SteadQMaintenance.cfg property list differs from model/SteadQMaintenance.tla: expected [\"FirstProperty\", \"SecondProperty\"], got [\"FirstProperty\"]"
        );
    }
}
