use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt, fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

const SUPPORTED_SPEC_VERSION: &str = "1.4";
const EXPECTED_RULE_COUNT: usize = 110;
const SPEC_PATH: &str = "docs/specs/fork-aware-header-chain-engine.md";
const MANIFEST_PATH: &str = "zakura-header-chain/conformance.toml";

pub fn run(repo_root: &Path, requested_rule: Option<&str>) -> Result<(), Box<dyn Error>> {
    let spec = fs::read_to_string(repo_root.join(SPEC_PATH))?;
    let manifest = fs::read_to_string(repo_root.join(MANIFEST_PATH))?;
    let report = validate(&spec, &manifest, &production_expectations())?;
    validate_repository_evidence(repo_root, &manifest)?;
    validate_acceptance_claims(&report, &spec, &manifest, &production_expectations())?;

    if let Some(rule_id) = requested_rule {
        let rule = report
            .rules
            .get(rule_id)
            .ok_or_else(|| ValidationError(format!("unknown conformance rule ID `{rule_id}`")))?;

        println!("{} — {}", rule.id, rule.name);
        println!("status: {}", rule.status);
        println!("specification: {}", rule.spec_text);
    } else {
        let unimplemented = report
            .rules
            .values()
            .filter(|rule| rule.status == RuleStatus::Unimplemented)
            .count();

        println!(
            "header conformance: {} rules validated for specification v{}; \
             {unimplemented} explicitly unimplemented; {} audit IDs unique",
            report.rules.len(),
            report.spec_version,
            report.audit_ids.len(),
        );
    }

    Ok(())
}

#[derive(Clone, Copy)]
struct Expectations<'a> {
    spec_version: &'a str,
    rule_count: usize,
    audit_ids: &'a [&'a str],
}

fn production_expectations() -> Expectations<'static> {
    const AUDIT_IDS: &[&str] = &[
        "AUD-01",
        "AUD-02",
        "AUD-03",
        "AUD-04",
        "AUD-05",
        "AUD-06",
        "AUD-07",
        "AUD-08",
        "AUD-09",
        "AUD-10",
        "AUD-11",
        "AUD-12",
        "AUD-13",
        "AUD-14",
        "AUD-15",
        "AUD-INCIDENT",
    ];

    Expectations {
        spec_version: SUPPORTED_SPEC_VERSION,
        rule_count: EXPECTED_RULE_COUNT,
        audit_ids: AUDIT_IDS,
    }
}

#[derive(Debug)]
struct Report {
    spec_version: String,
    rules: BTreeMap<String, RuleReport>,
    audit_ids: BTreeSet<String>,
}

#[derive(Debug)]
struct RuleReport {
    id: String,
    name: String,
    status: RuleStatus,
    spec_text: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum RuleStatus {
    Unimplemented,
    Implemented,
}

impl fmt::Display for RuleStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unimplemented => f.write_str("unimplemented"),
            Self::Implemented => f.write_str("implemented"),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    spec_version: String,
    rule: Vec<ManifestRule>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestRule {
    id: String,
    name: String,
    status: RuleStatus,
    #[serde(default)]
    owner: String,
    #[serde(default)]
    tests: Vec<String>,
    #[serde(default)]
    networks: Vec<String>,
}

#[derive(Debug)]
struct SpecRule {
    name: String,
    text: String,
}

fn validate(
    spec: &str,
    manifest_text: &str,
    expected: &Expectations<'_>,
) -> Result<Report, ValidationError> {
    let spec_version = parse_spec_version(spec)?;
    if spec_version != expected.spec_version {
        return Err(ValidationError(format!(
            "specification version is `{spec_version}`, expected `{}`",
            expected.spec_version
        )));
    }

    let spec_rules = parse_spec_rules(spec)?;
    if spec_rules.len() != expected.rule_count {
        return Err(ValidationError(format!(
            "specification defines {} normative rules, expected {}",
            spec_rules.len(),
            expected.rule_count
        )));
    }

    let audit_ids = parse_audit_ids(spec)?;
    let expected_audits = expected
        .audit_ids
        .iter()
        .map(|id| (*id).to_owned())
        .collect::<BTreeSet<_>>();
    check_set_equality("audit", &expected_audits, &audit_ids)?;

    let manifest: Manifest = toml::from_str(manifest_text)
        .map_err(|error| ValidationError(format!("invalid conformance manifest: {error}")))?;
    if manifest.spec_version != spec_version {
        return Err(ValidationError(format!(
            "manifest spec_version is `{}`, but the specification is `{spec_version}`",
            manifest.spec_version
        )));
    }

    let mut manifest_rules = BTreeMap::new();
    for rule in manifest.rule {
        validate_manifest_rule(&rule)?;
        let id = rule.id.clone();
        if manifest_rules.insert(id.clone(), rule).is_some() {
            return Err(ValidationError(format!(
                "duplicate conformance manifest rule ID `{id}`"
            )));
        }
    }

    let spec_ids = spec_rules.keys().cloned().collect::<BTreeSet<_>>();
    let matrix_ids = parse_rule_matrix_ids(spec)?;
    check_set_equality("rule-to-test matrix", &spec_ids, &matrix_ids)?;
    let manifest_ids = manifest_rules.keys().cloned().collect::<BTreeSet<_>>();
    check_set_equality("normative rule", &spec_ids, &manifest_ids)?;

    let mut rules = BTreeMap::new();
    for (id, spec_rule) in spec_rules {
        let manifest_rule = manifest_rules
            .remove(&id)
            .expect("rule ID sets are equal because they were checked above");
        if manifest_rule.name != spec_rule.name {
            return Err(ValidationError(format!(
                "manifest name for `{id}` is `{}`, expected `{}`",
                manifest_rule.name, spec_rule.name
            )));
        }

        rules.insert(
            id.clone(),
            RuleReport {
                id,
                name: spec_rule.name,
                status: manifest_rule.status,
                spec_text: spec_rule.text,
            },
        );
    }

    Ok(Report {
        spec_version,
        rules,
        audit_ids,
    })
}

fn parse_spec_version(spec: &str) -> Result<String, ValidationError> {
    let version = spec
        .lines()
        .find_map(|line| line.strip_prefix("Version: "))
        .ok_or_else(|| ValidationError("specification has no `Version:` field".to_owned()))?;

    Ok(version.trim_end_matches("<br>").trim().to_owned())
}

fn parse_spec_rules(spec: &str) -> Result<BTreeMap<String, SpecRule>, ValidationError> {
    let mut rules = BTreeMap::new();

    for line in spec.lines().filter(|line| line.starts_with("**LC-")) {
        let heading_end = line.find(".**").ok_or_else(|| {
            ValidationError(format!("malformed normative rule heading: `{line}`"))
        })?;
        let heading = &line[2..heading_end];
        let (id_and_authority, name) = heading.split_once(" — ").ok_or_else(|| {
            ValidationError(format!("normative rule heading has no name: `{heading}`"))
        })?;
        let (id, authority) = id_and_authority.split_once(" [").ok_or_else(|| {
            ValidationError(format!(
                "normative rule heading has no authority: `{heading}`"
            ))
        })?;

        if !authority.ends_with(']') || !valid_rule_id(id) || name.is_empty() {
            return Err(ValidationError(format!(
                "malformed normative rule heading: `{heading}`"
            )));
        }

        if rules
            .insert(
                id.to_owned(),
                SpecRule {
                    name: name.to_owned(),
                    text: line.to_owned(),
                },
            )
            .is_some()
        {
            return Err(ValidationError(format!(
                "duplicate specification rule ID `{id}`"
            )));
        }
    }

    Ok(rules)
}

fn parse_audit_ids(spec: &str) -> Result<BTreeSet<String>, ValidationError> {
    let mut audit_ids = BTreeSet::new();

    for line in spec.lines() {
        let Some(marker) = line.find("**AUD-") else {
            continue;
        };
        let definition = &line[marker + 2..];
        let Some(id) = definition.split_whitespace().next() else {
            continue;
        };
        if !definition[id.len()..].trim_start().starts_with('`') {
            continue;
        }

        if !audit_ids.insert(id.to_owned()) {
            return Err(ValidationError(format!(
                "duplicate specification audit ID `{id}`"
            )));
        }
    }

    Ok(audit_ids)
}

fn parse_rule_matrix_ids(spec: &str) -> Result<BTreeSet<String>, ValidationError> {
    let matrix = spec
        .split_once("### 7.4 Normative rule-to-test mapping")
        .map(|(_, rest)| rest)
        .and_then(|rest| {
            rest.split_once("### 7.5 Acceptance criteria")
                .map(|(matrix, _)| matrix)
        })
        .ok_or_else(|| {
            ValidationError(
                "specification has no bounded section 7.4 rule-to-test matrix".to_owned(),
            )
        })?;
    let mut ids = BTreeSet::new();

    for line in matrix.lines() {
        let Some(line) = line.strip_prefix("| LC-") else {
            continue;
        };
        let cell = format!(
            "LC-{}",
            line.split_once('|')
                .map(|(cell, _)| cell.trim())
                .ok_or_else(|| ValidationError(format!("malformed rule-to-test row: `{line}`")))?
        );
        for item in cell.split(',').map(str::trim) {
            if let Some((start, end)) = item.split_once("..") {
                let separator = start
                    .rfind('-')
                    .ok_or_else(|| ValidationError(format!("malformed rule range `{item}`")))?;
                let prefix = &start[..=separator];
                let start_digits = &start[separator + 1..];
                let start_number = start_digits
                    .parse::<u32>()
                    .map_err(|_| ValidationError(format!("malformed rule range start `{item}`")))?;
                let end_digits = end.rsplit_once('-').map_or(end, |(_, digits)| digits);
                let end_number = end_digits
                    .parse::<u32>()
                    .map_err(|_| ValidationError(format!("malformed rule range end `{item}`")))?;
                if end_number < start_number {
                    return Err(ValidationError(format!("descending rule range `{item}`")));
                }
                for number in start_number..=end_number {
                    ids.insert(format!(
                        "{prefix}{number:0width$}",
                        width = start_digits.len()
                    ));
                }
            } else if valid_rule_id(item) {
                ids.insert(item.to_owned());
            } else {
                return Err(ValidationError(format!(
                    "malformed rule-to-test matrix ID `{item}`"
                )));
            }
        }
    }

    Ok(ids)
}

fn validate_manifest_rule(rule: &ManifestRule) -> Result<(), ValidationError> {
    if !valid_rule_id(&rule.id) || rule.name.trim().is_empty() {
        return Err(ValidationError(format!(
            "manifest contains malformed rule ID or name: `{}`",
            rule.id
        )));
    }

    match rule.status {
        RuleStatus::Unimplemented => {
            if !rule.owner.is_empty() || !rule.tests.is_empty() || !rule.networks.is_empty() {
                return Err(ValidationError(format!(
                    "unimplemented rule `{}` must not claim owners, tests, or networks",
                    rule.id
                )));
            }
        }
        RuleStatus::Implemented => {
            if placeholder(&rule.owner) {
                return Err(ValidationError(format!(
                    "implemented rule `{}` has a missing or placeholder owner",
                    rule.id
                )));
            }
            if rule.tests.is_empty() || rule.tests.iter().any(|test| placeholder(test)) {
                return Err(ValidationError(format!(
                    "implemented rule `{}` has missing or placeholder tests",
                    rule.id
                )));
            }
            if rule.networks.is_empty() || rule.networks.iter().any(|network| placeholder(network))
            {
                return Err(ValidationError(format!(
                    "implemented rule `{}` has missing or placeholder networks",
                    rule.id
                )));
            }
            validate_network_matrix(rule)?;
        }
    }

    Ok(())
}

fn validate_network_matrix(rule: &ManifestRule) -> Result<(), ValidationError> {
    const NETWORKS: &[&str] = &["mainnet", "testnet", "custom"];

    let mut unique = BTreeSet::new();
    for network in &rule.networks {
        if !NETWORKS.contains(&network.as_str()) {
            return Err(ValidationError(format!(
                "implemented rule `{}` names unknown network `{network}`",
                rule.id
            )));
        }
        if !unique.insert(network.as_str()) {
            return Err(ValidationError(format!(
                "implemented rule `{}` names network `{network}` more than once",
                rule.id
            )));
        }
    }

    if unique.contains("mainnet") != unique.contains("testnet") {
        return Err(ValidationError(format!(
            "implemented rule `{}` must cover mainnet and testnet together",
            rule.id
        )));
    }

    Ok(())
}

fn validate_repository_evidence(
    repo_root: &Path,
    manifest_text: &str,
) -> Result<(), ValidationError> {
    let manifest: Manifest = toml::from_str(manifest_text)
        .map_err(|error| ValidationError(format!("invalid conformance manifest: {error}")))?;
    let test_functions = collect_test_functions(repo_root)?;

    for rule in manifest
        .rule
        .iter()
        .filter(|rule| rule.status == RuleStatus::Implemented)
    {
        let mut unique = BTreeSet::new();
        for test in &rule.tests {
            if !unique.insert(test) {
                return Err(ValidationError(format!(
                    "implemented rule `{}` names test `{test}` more than once",
                    rule.id
                )));
            }
            let Some((suite, symbol)) = test.rsplit_once("::") else {
                return Err(ValidationError(format!(
                    "implemented rule `{}` test `{test}` has no suite prefix",
                    rule.id
                )));
            };
            if suite.is_empty() || !valid_rust_identifier(symbol) {
                return Err(ValidationError(format!(
                    "implemented rule `{}` has malformed test reference `{test}`",
                    rule.id
                )));
            }

            match test_functions.get(symbol) {
                None => {
                    return Err(ValidationError(format!(
                        "implemented rule `{}` names missing Rust test `{test}`",
                        rule.id
                    )));
                }
                Some(paths) if paths.len() > 1 => {
                    return Err(ValidationError(format!(
                        "implemented rule `{}` test `{test}` is ambiguous across [{}]",
                        rule.id,
                        paths
                            .iter()
                            .map(|path| path.display().to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )));
                }
                Some(_) => {}
            }
        }
    }

    Ok(())
}

fn validate_acceptance_claims(
    report: &Report,
    spec: &str,
    manifest_text: &str,
    expected: &Expectations<'_>,
) -> Result<(), ValidationError> {
    let acceptance_rules = report
        .rules
        .values()
        .filter(|rule| rule.id.starts_with("LC-ACCEPT-"))
        .collect::<Vec<_>>();
    let implemented_acceptance = acceptance_rules
        .iter()
        .filter(|rule| rule.status == RuleStatus::Implemented)
        .count();
    if implemented_acceptance == 0 {
        return Ok(());
    }
    if implemented_acceptance != acceptance_rules.len() {
        return Err(ValidationError(
            "acceptance rules must become implemented together".to_owned(),
        ));
    }

    let unimplemented = report
        .rules
        .values()
        .filter(|rule| rule.status == RuleStatus::Unimplemented)
        .map(|rule| rule.id.as_str())
        .collect::<Vec<_>>();
    if !unimplemented.is_empty() {
        return Err(ValidationError(format!(
            "acceptance requires every normative rule to be implemented; remaining: [{}]",
            unimplemented.join(", ")
        )));
    }

    let manifest: Manifest = toml::from_str(manifest_text)
        .map_err(|error| ValidationError(format!("invalid conformance manifest: {error}")))?;
    let acceptance = manifest
        .rule
        .iter()
        .find(|rule| rule.id == "LC-ACCEPT-01")
        .ok_or_else(|| ValidationError("manifest has no LC-ACCEPT-01 rule".to_owned()))?;
    let covered_audits = acceptance
        .tests
        .iter()
        .filter_map(|test| test.split_once("::").map(|(suite, _)| suite.to_owned()))
        .filter(|suite| suite.starts_with("AUD-"))
        .collect::<BTreeSet<_>>();
    let required_audits = expected
        .audit_ids
        .iter()
        .map(|id| (*id).to_owned())
        .collect::<BTreeSet<_>>();
    check_set_equality("acceptance audit", &required_audits, &covered_audits)?;

    for (document, text) in [(SPEC_PATH, spec), (MANIFEST_PATH, manifest_text)] {
        let uppercase = text.to_ascii_uppercase();
        for marker in ["TODO", "TBD", "FIXME", "???"] {
            if uppercase.contains(marker) {
                return Err(ValidationError(format!(
                    "acceptance document `{document}` contains unresolved marker `{marker}`"
                )));
            }
        }
    }

    Ok(())
}

fn collect_test_functions(
    repo_root: &Path,
) -> Result<BTreeMap<String, Vec<PathBuf>>, ValidationError> {
    fn visit(
        path: &Path,
        tests: &mut BTreeMap<String, Vec<PathBuf>>,
    ) -> Result<(), ValidationError> {
        let entries = fs::read_dir(path).map_err(|error| {
            ValidationError(format!("failed to read `{}`: {error}", path.display()))
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                ValidationError(format!(
                    "failed to read an entry under `{}`: {error}",
                    path.display()
                ))
            })?;
            let entry_path = entry.path();
            let file_type = entry.file_type().map_err(|error| {
                ValidationError(format!(
                    "failed to inspect `{}`: {error}",
                    entry_path.display()
                ))
            })?;
            if file_type.is_dir() {
                let name = entry.file_name();
                if name != ".git" && name != "target" {
                    visit(&entry_path, tests)?;
                }
            } else if file_type.is_file()
                && entry_path
                    .extension()
                    .and_then(|extension| extension.to_str())
                    == Some("rs")
            {
                let source = fs::read_to_string(&entry_path).map_err(|error| {
                    ValidationError(format!(
                        "failed to read `{}`: {error}",
                        entry_path.display()
                    ))
                })?;
                for symbol in parse_test_function_names(&source) {
                    tests.entry(symbol).or_default().push(entry_path.clone());
                }
            }
        }

        Ok(())
    }

    let mut tests = BTreeMap::new();
    visit(repo_root, &mut tests)?;
    Ok(tests)
}

fn parse_test_function_names(source: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut has_test_attribute = false;
    let mut is_ignored = false;

    for line in source.lines() {
        let line = line.trim();
        if line.starts_with("#[") {
            has_test_attribute |= line == "#[test]" || line.starts_with("#[tokio::test");
            is_ignored |= line.starts_with("#[ignore");
            continue;
        }
        if line.is_empty() || line.starts_with("//") {
            continue;
        }
        if !has_test_attribute {
            is_ignored = false;
            continue;
        }

        if !is_ignored {
            if let Some(symbol) = rust_function_name(line) {
                names.push(symbol.to_owned());
            }
        }
        has_test_attribute = false;
        is_ignored = false;
    }

    names
}

fn rust_function_name(line: &str) -> Option<&str> {
    let (_, suffix) = line.split_once("fn ")?;
    let symbol_len = suffix
        .bytes()
        .take_while(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        .count();
    let symbol = &suffix[..symbol_len];
    valid_rust_identifier(symbol).then_some(symbol)
}

fn valid_rust_identifier(identifier: &str) -> bool {
    let mut bytes = identifier.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn valid_rule_id(id: &str) -> bool {
    id.starts_with("LC-")
        && id.len() > 3
        && id
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'-')
}

fn placeholder(value: &str) -> bool {
    let value = value.trim().to_ascii_lowercase();
    value.is_empty()
        || value == "?"
        || value.contains("todo")
        || value.contains("tbd")
        || value.contains("placeholder")
        || value.contains("unimplemented")
}

fn check_set_equality(
    kind: &str,
    expected: &BTreeSet<String>,
    actual: &BTreeSet<String>,
) -> Result<(), ValidationError> {
    let missing = expected.difference(actual).cloned().collect::<Vec<_>>();
    let unknown = actual.difference(expected).cloned().collect::<Vec<_>>();

    if missing.is_empty() && unknown.is_empty() {
        Ok(())
    } else {
        Err(ValidationError(format!(
            "{kind} ID mismatch; missing: [{}]; unknown: [{}]",
            missing.join(", "),
            unknown.join(", ")
        )))
    }
}

#[derive(Debug)]
struct ValidationError(String);

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for ValidationError {}

#[cfg(test)]
mod tests {
    use super::*;

    const SPEC: &str = r#"# Spec
Version: 1.3<br>

**LC-ONE-01 [LS] — First rule.** First text.
**LC-TWO-02 [ZW] — Second `rule`.** Second text.

1. **AUD-01 `first`:** text
**AUD-INCIDENT `incident`:** text

### 7.4 Normative rule-to-test mapping

| Normative rule IDs | Required test IDs |
| --- | --- |
| LC-ONE-01, LC-TWO-02 | TEST-01 |

### 7.5 Acceptance criteria
"#;

    const MANIFEST: &str = r#"spec_version = "1.3"

[[rule]]
id = "LC-ONE-01"
name = "First rule"
status = "unimplemented"

[[rule]]
id = "LC-TWO-02"
name = "Second `rule`"
status = "unimplemented"
"#;

    const EXPECTED: Expectations<'static> = Expectations {
        spec_version: "1.3",
        rule_count: 2,
        audit_ids: &["AUD-01", "AUD-INCIDENT"],
    };

    #[test]
    fn matching_documents_validate() {
        let report = validate(SPEC, MANIFEST, &EXPECTED).expect("fixture should validate");
        assert_eq!(report.rules.len(), 2);
        assert_eq!(report.audit_ids.len(), 2);
    }

    #[test]
    fn checked_in_documents_validate() {
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xtask should live directly under the repository root");
        let spec = fs::read_to_string(repo_root.join(SPEC_PATH))
            .expect("checked-in specification should be readable");
        let manifest = fs::read_to_string(repo_root.join(MANIFEST_PATH))
            .expect("checked-in conformance manifest should be readable");

        let report = validate(&spec, &manifest, &production_expectations())
            .expect("checked-in conformance documents should validate");
        assert_eq!(report.rules.len(), EXPECTED_RULE_COUNT);
        assert_eq!(
            report.audit_ids.len(),
            production_expectations().audit_ids.len()
        );
    }

    #[test]
    fn implemented_test_symbols_exist() {
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xtask should live directly under the repository root");
        let manifest = fs::read_to_string(repo_root.join(MANIFEST_PATH))
            .expect("checked-in conformance manifest should be readable");

        validate_repository_evidence(repo_root, &manifest)
            .expect("every claimed test should resolve to one Rust test function");
    }

    #[test]
    fn acceptance_is_all_or_nothing_and_requires_every_named_audit() {
        fn report(second_status: RuleStatus) -> Report {
            Report {
                spec_version: "1.3".to_owned(),
                rules: [
                    (
                        "LC-ACCEPT-01".to_owned(),
                        RuleReport {
                            id: "LC-ACCEPT-01".to_owned(),
                            name: "Complete".to_owned(),
                            status: RuleStatus::Implemented,
                            spec_text: "complete".to_owned(),
                        },
                    ),
                    (
                        "LC-ACCEPT-02".to_owned(),
                        RuleReport {
                            id: "LC-ACCEPT-02".to_owned(),
                            name: "Durable".to_owned(),
                            status: second_status,
                            spec_text: "durable".to_owned(),
                        },
                    ),
                ]
                .into_iter()
                .collect(),
                audit_ids: ["AUD-01".to_owned(), "AUD-INCIDENT".to_owned()]
                    .into_iter()
                    .collect(),
            }
        }

        let partial_manifest = r#"spec_version = "1.3"
[[rule]]
id = "LC-ACCEPT-01"
name = "Complete"
status = "implemented"
owner = "gate"
tests = ["AUD-01::matching_documents_validate"]
networks = ["mainnet", "testnet", "custom"]

[[rule]]
id = "LC-ACCEPT-02"
name = "Durable"
status = "unimplemented"
"#;
        let expectations = Expectations {
            spec_version: "1.3",
            rule_count: 2,
            audit_ids: &["AUD-01", "AUD-INCIDENT"],
        };
        let error = validate_acceptance_claims(
            &report(RuleStatus::Unimplemented),
            "complete specification",
            partial_manifest,
            &expectations,
        )
        .expect_err("acceptance rules cannot be promoted piecemeal");
        assert!(error.to_string().contains("implemented together"));

        let complete_manifest = partial_manifest
            .replace("status = \"unimplemented\"", "status = \"implemented\"")
            .replace(
                "tests = [\"AUD-01::matching_documents_validate\"]",
                "tests = [\"AUD-01::matching_documents_validate\", \
                 \"AUD-INCIDENT::matching_documents_validate\"]",
            );
        validate_acceptance_claims(
            &report(RuleStatus::Implemented),
            "complete specification",
            &complete_manifest,
            &expectations,
        )
        .expect("all rules and named audits close acceptance together");

        let missing_audit =
            complete_manifest.replace(", \"AUD-INCIDENT::matching_documents_validate\"", "");
        let error = validate_acceptance_claims(
            &report(RuleStatus::Implemented),
            "complete specification",
            &missing_audit,
            &expectations,
        )
        .expect_err("every named audit must be explicit");
        assert!(error.to_string().contains("AUD-INCIDENT"));
    }

    #[test]
    fn test_symbol_parser_excludes_ignored_tests() {
        let source = r#"
#[test]
fn included() {}

#[tokio::test]
async fn included_async() {}

#[test]
#[ignore = "not part of required CI"]
fn excluded() {}

fn helper() {}
"#;
        assert_eq!(
            parse_test_function_names(source),
            ["included", "included_async"]
        );
    }

    #[test]
    fn network_matrix_complete() {
        let paired = ManifestRule {
            id: "LC-ONE-01".to_owned(),
            name: "First rule".to_owned(),
            status: RuleStatus::Implemented,
            owner: "crate::owner".to_owned(),
            tests: vec!["HV-01::rule".to_owned()],
            networks: vec!["mainnet".to_owned(), "testnet".to_owned()],
        };
        validate_network_matrix(&paired).expect("the production networks are paired");

        let mut unpaired = paired;
        unpaired.networks.pop();
        let error = validate_network_matrix(&unpaired)
            .expect_err("a single production network must not claim complete coverage");
        assert!(error.to_string().contains("mainnet and testnet together"));
    }

    #[test]
    fn version_rule_and_name_mismatches_fail() {
        let wrong_version = MANIFEST.replacen("1.3", "1.2", 1);
        assert!(validate(SPEC, &wrong_version, &EXPECTED).is_err());

        let missing_rule = MANIFEST.replace(
            "\n[[rule]]\nid = \"LC-TWO-02\"\nname = \"Second `rule`\"\nstatus = \"unimplemented\"\n",
            "\n",
        );
        assert!(validate(SPEC, &missing_rule, &EXPECTED).is_err());

        let wrong_name = MANIFEST.replace("Second `rule`", "Renamed rule");
        assert!(validate(SPEC, &wrong_name, &EXPECTED).is_err());
    }

    #[test]
    fn duplicate_rule_and_audit_ids_fail() {
        let duplicate_rule = format!("{SPEC}\n**LC-ONE-01 [LS] — First rule.** duplicate");
        assert!(validate(&duplicate_rule, MANIFEST, &EXPECTED).is_err());

        let duplicate_audit = format!("{SPEC}\n**AUD-01 `again`:** duplicate");
        assert!(validate(&duplicate_audit, MANIFEST, &EXPECTED).is_err());

        let duplicate_manifest = format!(
            "{MANIFEST}\n[[rule]]\nid = \"LC-ONE-01\"\nname = \"First rule\"\nstatus = \"unimplemented\"\n"
        );
        assert!(validate(SPEC, &duplicate_manifest, &EXPECTED).is_err());
    }

    #[test]
    fn rule_to_test_matrix_must_cover_every_exact_rule_once_as_a_set() {
        let missing = SPEC.replace("LC-ONE-01, LC-TWO-02", "LC-ONE-01");
        let error =
            validate(&missing, MANIFEST, &EXPECTED).expect_err("a missing matrix rule must fail");
        assert!(error.to_string().contains("LC-TWO-02"));

        let unknown = SPEC.replace("LC-ONE-01, LC-TWO-02", "LC-ONE-01, LC-TWO-02, LC-THREE-03");
        let error =
            validate(&unknown, MANIFEST, &EXPECTED).expect_err("an unknown matrix rule must fail");
        assert!(error.to_string().contains("LC-THREE-03"));

        let ranged = SPEC
            .replace("**LC-TWO-02", "**LC-ONE-02")
            .replace("LC-ONE-01, LC-TWO-02", "LC-ONE-01..02");
        let ranged_manifest = MANIFEST.replace("LC-TWO-02", "LC-ONE-02");
        validate(&ranged, &ranged_manifest, &EXPECTED)
            .expect("an inclusive same-prefix range expands to both rules");
    }

    #[test]
    fn implementation_claims_require_real_owner_tests_and_networks() {
        let incomplete = MANIFEST.replacen("unimplemented", "implemented", 1);
        let error = validate(SPEC, &incomplete, &EXPECTED)
            .expect_err("an incomplete implementation claim must fail");
        assert!(error.to_string().contains("owner"));

        let placeholder = incomplete.replace(
            "status = \"implemented\"",
            "status = \"implemented\"\nowner = \"TODO\"\ntests = [\"placeholder\"]\nnetworks = [\"mainnet\"]",
        );
        assert!(validate(SPEC, &placeholder, &EXPECTED).is_err());

        let complete = incomplete.replace(
            "status = \"implemented\"",
            "status = \"implemented\"\nowner = \"crate::owner\"\ntests = [\"HV-01::rule\"]\nnetworks = [\"mainnet\", \"testnet\"]",
        );
        validate(SPEC, &complete, &EXPECTED).expect("a complete implementation claim should pass");

        let unknown_network = complete.replace("\"testnet\"", "\"production\"");
        assert!(validate(SPEC, &unknown_network, &EXPECTED).is_err());
    }
}
