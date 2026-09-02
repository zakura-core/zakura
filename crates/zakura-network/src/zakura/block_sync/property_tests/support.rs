//! Shared reporting types for executable message contracts.

use std::{collections::BTreeSet, ops::AddAssign};

use proptest::test_runner::Config as ProptestConfig;

/// Counts deterministic cases by the kind of evidence they provide.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct CaseCensus {
    pub(super) legal: usize,
    pub(super) rule_invalid: usize,
    pub(super) compound_invalid: usize,
    pub(super) divergences: usize,
}

impl CaseCensus {
    pub(super) const fn new(
        legal: usize,
        rule_invalid: usize,
        compound_invalid: usize,
        divergences: usize,
    ) -> Self {
        Self {
            legal,
            rule_invalid,
            compound_invalid,
            divergences,
        }
    }

    pub(super) const fn legal(cases: usize) -> Self {
        Self::new(cases, 0, 0, 0)
    }

    pub(super) const fn compound(cases: usize) -> Self {
        Self::new(0, 0, cases, 0)
    }

    pub(super) const fn divergence(cases: usize) -> Self {
        Self::new(0, 0, 0, cases)
    }

    fn total(self) -> usize {
        self.legal
            .saturating_add(self.rule_invalid)
            .saturating_add(self.compound_invalid)
            .saturating_add(self.divergences)
    }
}

impl AddAssign for CaseCensus {
    fn add_assign(&mut self, other: Self) {
        self.legal = self.legal.saturating_add(other.legal);
        self.rule_invalid = self.rule_invalid.saturating_add(other.rule_invalid);
        self.compound_invalid = self.compound_invalid.saturating_add(other.compound_invalid);
        self.divergences = self.divergences.saturating_add(other.divergences);
    }
}

/// Current relationship between the candidate contract and production code.
#[derive(Clone, Copy)]
pub(super) enum RuleStatus {
    Conformant,
    CandidateContractDivergence {
        current: &'static str,
        target: &'static str,
    },
}

/// One independently testable rule and the deterministic evidence for it.
pub(super) struct ContractRule {
    pub(super) id: &'static str,
    pub(super) requirement: &'static str,
    pub(super) status: RuleStatus,
    pub(super) evidence: fn() -> CaseCensus,
}

/// Execute every rule, validate the ledger, and print a case census.
#[allow(clippy::print_stdout)] // This function intentionally emits the local human report.
pub(super) fn run_contract_report(
    message: &str,
    rule_prefix: &str,
    rules: &[ContractRule],
    compound_evidence: fn() -> CaseCensus,
    generated_property_count: u32,
) {
    let mut ids = BTreeSet::new();
    let mut total = CaseCensus::default();

    println!("{message} candidate-contract evidence");
    println!("ID     STATUS       LEGAL  INVALID  DIVERGENCE  REQUIREMENT");
    for (index, rule) in rules.iter().enumerate() {
        let expected_id = format!("{rule_prefix}-{:02}", index + 1);
        assert_eq!(rule.id, expected_id, "contract rule IDs must be contiguous");
        assert!(ids.insert(rule.id), "duplicate contract rule {}", rule.id);

        let evidence = (rule.evidence)();
        assert!(
            evidence.total() > 0,
            "{} has no deterministic cases",
            rule.id
        );
        match rule.status {
            RuleStatus::Conformant => assert_eq!(
                evidence.divergences, 0,
                "{} is marked conformant but reports a divergence",
                rule.id
            ),
            RuleStatus::CandidateContractDivergence { .. } => assert!(
                evidence.divergences > 0,
                "{} must execute a divergence witness",
                rule.id
            ),
        }
        total += evidence;

        let status = match rule.status {
            RuleStatus::Conformant => "conformant",
            RuleStatus::CandidateContractDivergence { .. } => "DIVERGENCE",
        };
        println!(
            "{:<6} {:<12} {:>5}  {:>7}  {:>10}  {}",
            rule.id,
            status,
            evidence.legal,
            evidence.rule_invalid,
            evidence.divergences,
            rule.requirement
        );
        if let RuleStatus::CandidateContractDivergence { current, target } = rule.status {
            println!("       current: {current}");
            println!("       target:  {target}");
        }
    }

    let compound = compound_evidence();
    assert!(
        compound.compound_invalid > 0,
        "the deterministic compound-invalid matrix is empty"
    );
    total += compound;

    let cases_per_property = ProptestConfig::default().cases;
    let configured_generated_cases =
        u64::from(cases_per_property).saturating_mul(u64::from(generated_property_count));

    println!("Deterministic evidence executions (categories may reuse an input)");
    println!("  legal:            {}", total.legal);
    println!("  rule-invalid:     {}", total.rule_invalid);
    println!("  compound-invalid: {}", total.compound_invalid);
    println!("  divergences:      {}", total.divergences);
    println!("  total:            {}", total.total());
    println!("Generated exploration");
    println!("  properties:       {generated_property_count}");
    println!("  cases/property:   {cases_per_property}");
    println!(
        "  configured cases: {configured_generated_cases} (excludes regressions and shrinking)"
    );
}
