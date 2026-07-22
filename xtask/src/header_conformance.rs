use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt, fs,
    path::Path,
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
        }
    }

    Ok(())
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
            "status = \"implemented\"\nowner = \"crate::owner\"\ntests = [\"HV-01::rule\"]\nnetworks = [\"mainnet\"]",
        );
        validate(SPEC, &complete, &EXPECTED).expect("a complete implementation claim should pass");
    }
}
