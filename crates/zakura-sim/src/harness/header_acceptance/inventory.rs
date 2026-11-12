//! The acceptance inventories' pin (V12 F-637 and F-639).
//!
//! The manifest's inventories define their own obligations: deleting a
//! required contract with its row, a narrowing entry with its expectation,
//! or a composition check with its test removes the obligation along with
//! its evidence, and every check built from the same list still passes.
//! [`INVENTORY_FINGERPRINT`] pins them apart from the lists, the way C20
//! pins `NotApplicable`'s producers: a canonical text of the required
//! contract IDs, each narrowing entry's name, contracts and test, the
//! composition checks, each native mutation's full classification, and
//! every approved exception. [`inventory_errors`], run on the gate's own
//! path ([`super::tool::evaluate_artifacts`]), fails the record when the
//! text and the pin disagree, or when the required keys don't hold an
//! obligation per entry, so no list edit or test deletion can remove the
//! check. Any change to these lists moves the pin, which needs a reread and
//! Adam's approval. Each `ToMove` mutation that hdr-9 to 13 moves changes
//! its class, so two such PRs conflict on the pin: that's the review point.
//!
//! Narrowing entries are named by their metadata: their `releases`
//! closures are function pointers, which no comparison can pin. The static
//! check `the_narrowing_rules_match_their_pin` fingerprints their source
//! instead, and it's a composition check this pin names.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    contracts::{ContractState, NarrowingTest, REQUIRED_CONTRACTS},
    evaluate::Evaluation,
    required::{Permit, GATE_OBLIGATIONS},
    rows::{NativeClass, NativeMutation, StandingExclusion, STANDING_EXCLUSIONS},
    Approval, Expected, Gate, Key, Manifest, COMPOSITION_CHECKS, NATIVE_MUTATIONS,
};

/// The fingerprint of the committed inventories' canonical text
/// ([`canonical`]): the first 16 hex digits of its SHA-256. Moving it
/// needs a reread of every changed line and Adam's approval.
pub const INVENTORY_FINGERPRINT: &str = "06b1e5d312796b83";

/// What the inventories hold that the manifest doesn't carry.
pub struct Lists<'a> {
    /// The composition checks.
    pub composition: &'a [&'a str],
    /// The native mutations' classifications.
    pub native: &'a [NativeMutation],
    /// The standing exclusions of passing, catch and missing-source keys.
    pub exclusions: &'a [StandingExclusion],
    /// The gate obligations no row owns.
    pub obligations: &'a [super::required::GateObligation],
}

impl Lists<'static> {
    /// The committed lists.
    pub fn committed() -> Self {
        Lists {
            composition: COMPOSITION_CHECKS,
            native: NATIVE_MUTATIONS,
            exclusions: STANDING_EXCLUSIONS,
            obligations: GATE_OBLIGATIONS,
        }
    }
}

/// Every reason the committed inventories, as `manifest`, `required` and
/// [`Lists::committed`] hold them, don't match [`INVENTORY_FINGERPRINT`]:
/// see [`errors_against`].
pub fn inventory_errors(
    manifest: &Manifest,
    required: &BTreeMap<Key, BTreeSet<Gate>>,
) -> Vec<String> {
    errors_against(
        manifest,
        required,
        &Lists::committed(),
        INVENTORY_FINGERPRINT,
    )
}

/// Every reason `manifest`, `required` and `lists` don't match `pin`: the
/// manifest's required contracts aren't [`REQUIRED_CONTRACTS`], a line of
/// the canonical text appears twice, the text's fingerprint isn't `pin`
/// (the error then holds the text and its fingerprint), or `required`
/// lacks a composition key per check, a narrowing key per entry, or a
/// native key on exactly gate 6 per mutation that hasn't moved.
pub fn errors_against(
    manifest: &Manifest,
    required: &BTreeMap<Key, BTreeSet<Gate>>,
    lists: &Lists<'_>,
    pin: &str,
) -> Vec<String> {
    let mut errors = Vec::new();
    if manifest.required_contracts != REQUIRED_CONTRACTS {
        errors.push(format!(
            "the manifest requires contracts {:?}, not REQUIRED_CONTRACTS",
            manifest.required_contracts
        ));
    }
    let lines = canonical(manifest, lists);
    let mut seen = BTreeSet::new();
    for line in &lines {
        if !seen.insert(line) {
            errors.push(format!("the inventory names twice: {line}"));
        }
    }
    let text = lines.join("\n");
    let print = fingerprint(&text);
    if print != pin {
        errors.push(format!(
            "the acceptance inventories changed: reread every changed line and get Adam's approval, \
             then update INVENTORY_FINGERPRINT from {pin} to {print}. The inventory now reads:\n{text}"
        ));
    }
    for &check in lists.composition {
        let key = Key::Composition {
            test: check.to_owned(),
        };
        if !required.contains_key(&key) {
            errors.push(format!("composition check {check} has no required key"));
        }
    }
    for entry in &manifest.narrowing {
        let key = Key::NarrowingTest {
            entry: entry.entry.to_owned(),
        };
        if !required.contains_key(&key) {
            errors.push(format!(
                "narrowing entry {} has no required key",
                entry.entry
            ));
        }
    }
    for mutation in lists.native {
        if matches!(mutation.class, NativeClass::Moved { .. }) {
            continue;
        }
        let key = Key::Native {
            patch: mutation.patch.to_owned(),
        };
        if required.get(&key) != Some(&BTreeSet::from([Gate::G6])) {
            errors.push(format!(
                "native mutation {} isn't a required key on gate 6 alone",
                mutation.patch
            ));
        }
    }
    errors
}

/// The inventories' canonical text, one sorted line per entry with
/// tab-separated fields: each required contract ID; each narrowing entry's
/// name, contracts and test; each composition check; each native
/// mutation's patch and full class; and each approved exception. An
/// approved `Open` is one line per distinct template (its key's kind,
/// reason, owner, record and `until`), so another key under an existing
/// template, such as a new machine history's cleanup, doesn't move the
/// pin, but a copied approval on a new template does. Each standing
/// exclusion, standing gate permit, excluded contract and approved
/// `StaysNotApplicable` is a line of its own. Pending exceptions aren't
/// lines: they block anyway.
fn canonical(manifest: &Manifest, lists: &Lists<'_>) -> Vec<String> {
    let mut lines = Vec::new();
    for id in &manifest.required_contracts {
        lines.push(format!("contract\t{id:?}"));
    }
    for entry in &manifest.narrowing {
        let test = match entry.test {
            NarrowingTest::Test(test) => format!("test {test:?}"),
            NarrowingTest::Pending { until } => format!("pending until {until}"),
        };
        lines.push(format!(
            "narrowing\t{}\t{:?}\t{test}",
            entry.entry, entry.contracts
        ));
    }
    for check in lists.composition {
        lines.push(format!("composition\t{check}"));
    }
    for mutation in lists.native {
        lines.push(format!("native\t{}\t{:?}", mutation.patch, mutation.class));
    }
    let mut templates = BTreeSet::new();
    for result in &manifest.expected {
        match &result.expected {
            Expected::Open {
                reason,
                owner,
                approval: Approval::Adam { record },
                until,
            } => {
                let kind = result.keys.first().map_or_else(String::new, key_kind);
                templates.insert(format!(
                    "approved-open\t{kind}\t{reason}\t{owner}\t{record}\t{until:?}"
                ));
            }
            Expected::StaysNotApplicable {
                property,
                history,
                reason,
                approval: Approval::Adam { record },
                ..
            } => lines.push(format!(
                "approved-stays-not-applicable\t{property}\t{history:?}\t{reason}\t{record}"
            )),
            _ => {}
        }
    }
    lines.extend(templates);
    for exclusion in lists.exclusions {
        lines.push(format!(
            "standing-exclusion\t{:?}\t{}\t{}\t{:?}",
            exclusion.key, exclusion.owner, exclusion.reason, exclusion.approval
        ));
    }
    for obligation in lists.obligations {
        if let Permit::Standing {
            owner,
            reason,
            approval,
        } = obligation.permit
        {
            lines.push(format!(
                "standing-permit\t{}\t{owner}\t{reason}\t{approval:?}",
                obligation.name
            ));
        }
    }
    for contract in &manifest.contracts {
        if let ContractState::Excluded { reason, approval } = contract.state {
            lines.push(format!(
                "excluded-contract\t{:?}\t{reason}\t{approval:?}",
                contract.id
            ));
        }
    }
    lines.sort();
    lines
}

/// `key`'s variant name, such as `Native`.
fn key_kind(key: &Key) -> String {
    format!("{key:?}")
        .split(|c: char| !c.is_alphanumeric())
        .next()
        .unwrap_or_default()
        .to_owned()
}

/// The first 16 hex digits of `text`'s SHA-256.
fn fingerprint(text: &str) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(text.as_bytes())
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

impl Evaluation {
    /// Fails this evaluation for `errors`, the gate path's own checks such
    /// as [`inventory_errors`]: each becomes a stray and every gate fails,
    /// so a record can't show a passing gate while acceptance fails.
    pub fn invalidate(&mut self, errors: Vec<String>) {
        if errors.is_empty() {
            return;
        }
        self.stray.extend(errors);
        for verdict in self.gates.values_mut() {
            *verdict = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::harness::header_acceptance::{
        contracts::ContractId, evaluate::ObligationState, tool::committed_required, PLAN,
    };

    /// The committed manifest and its required keys.
    fn committed() -> (Manifest, BTreeMap<Key, BTreeSet<Gate>>) {
        let manifest = Manifest::committed();
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let required = committed_required(&repo, &manifest);
        (manifest, required)
    }

    /// The canonical text's fingerprint for `manifest` and `lists`.
    fn print(manifest: &Manifest, lists: &Lists<'_>) -> String {
        fingerprint(&canonical(manifest, lists).join("\n"))
    }

    /// The committed inventories match their pin, and their required keys
    /// hold every obligation.
    #[test]
    fn the_committed_inventories_match_their_pin() {
        let (manifest, required) = committed();
        assert_eq!(inventory_errors(&manifest, &required), Vec::<String>::new());
    }

    /// Each single inventory change is an error that names it, whatever
    /// else the run's results say: the pin moves, or a required key is
    /// missing.
    #[test]
    fn each_inventory_change_is_an_inventory_error() {
        let (manifest, required) = committed();
        let lists = Lists::committed();
        let pin = print(&manifest, &lists);
        assert_eq!(
            errors_against(&manifest, &required, &lists, &pin),
            Vec::<String>::new()
        );
        let moved = |manifest: &Manifest, lists: &Lists<'_>| print(manifest, lists) != pin;

        // A contract dropped from the required list and the table together.
        let mut dropped = manifest.clone();
        dropped
            .required_contracts
            .retain(|&id| id != ContractId::C22);
        dropped
            .contracts
            .retain(|contract| contract.id != ContractId::C22);
        assert!(moved(&dropped, &lists));
        assert!(!errors_against(&dropped, &required, &lists, &pin).is_empty());

        // A narrowing entry renamed, added, or widened to another contract.
        let mut renamed = manifest.clone();
        renamed.narrowing[0].entry = "N9";
        assert!(moved(&renamed, &lists));
        let mut added = manifest.clone();
        let mut extra = added.narrowing[0];
        extra.entry = "N6";
        added.narrowing.push(extra);
        assert!(moved(&added, &lists));
        assert!(errors_against(&added, &required, &lists, &pin)
            .iter()
            .any(|e| e.contains("N6 has no required key")));
        let mut widened = manifest.clone();
        widened.narrowing[0].contracts = &[ContractId::C11, ContractId::C12];
        assert!(moved(&widened, &lists));

        // A composition check dropped with its key.
        let composition: Vec<&str> = lists.composition[1..].to_vec();
        let fewer = Lists {
            composition: &composition,
            ..Lists::committed()
        };
        assert!(moved(&manifest, &fewer));
        let mut unkeyed = required.clone();
        unkeyed.remove(&Key::Composition {
            test: lists.composition[0].to_owned(),
        });
        assert!(errors_against(&manifest, &unkeyed, &lists, &pin)
            .iter()
            .any(|e| e.contains("has no required key")));

        // A native mutation reclassified, or its key dropped or moved to
        // another gate.
        let mut native = NATIVE_MUTATIONS.to_vec();
        let at = native
            .iter()
            .position(|m| m.patch == "readmission-unchecked")
            .expect("the retired mutation is classified");
        native[at].class = NativeClass::Retired {
            reason: "another reason",
        };
        let reclassified = Lists {
            native: &native,
            ..Lists::committed()
        };
        assert!(moved(&manifest, &reclassified));
        let key = Key::Native {
            patch: "readmission-unchecked".into(),
        };
        let mut gone = required.clone();
        gone.remove(&key);
        assert!(!errors_against(&manifest, &gone, &lists, &pin).is_empty());
        let mut regated = required.clone();
        regated.insert(key, BTreeSet::from([Gate::G5]));
        assert!(!errors_against(&manifest, &regated, &lists, &pin).is_empty());

        // An approval copied onto a pending Open, or an approved reason
        // reworded, moves the pin.
        let pending = manifest
            .expected
            .iter()
            .position(|result| {
                matches!(
                    result.expected,
                    Expected::Open {
                        approval: Approval::Pending,
                        ..
                    }
                )
            })
            .expect("the manifest holds a pending Open");
        let mut copied = manifest.clone();
        if let Expected::Open { approval, .. } = &mut copied.expected[pending].expected {
            *approval = PLAN;
        }
        assert!(moved(&copied, &lists));
        let approved = manifest
            .expected
            .iter()
            .position(|result| {
                matches!(
                    result.expected,
                    Expected::Open {
                        approval: Approval::Adam { .. },
                        ..
                    }
                )
            })
            .expect("the manifest holds an approved Open");
        let mut reworded = manifest.clone();
        if let Expected::Open { reason, .. } = &mut reworded.expected[approved].expected {
            *reason = "a reworded reason";
        }
        assert!(moved(&reworded, &lists));

        // Another result under an existing approved template doesn't.
        let mut another = manifest.clone();
        another.expected.push(another.expected[approved].clone());
        assert!(!moved(&another, &lists));
    }

    /// An inventory error fails an otherwise passing evaluation: it's a
    /// stray, and every gate fails, so the record can't pass a gate.
    #[test]
    fn an_inventory_error_fails_every_gate() {
        let mut evaluation = Evaluation {
            keys: BTreeMap::from([(
                Key::Composition {
                    test: "a_check".into(),
                },
                ObligationState::Verified,
            )]),
            gates: [Gate::G1, Gate::G2, Gate::G3, Gate::G4, Gate::G5, Gate::G6]
                .into_iter()
                .map(|gate| (gate, true))
                .collect(),
            stray: Vec::new(),
        };
        assert!(evaluation.accepted() && evaluation.fully_accepted());
        evaluation.invalidate(vec!["the acceptance inventories changed".into()]);
        assert!(!evaluation.accepted());
        assert!(!evaluation.fully_accepted());
        assert!(evaluation.gates.values().all(|verdict| !verdict));
        assert_eq!(evaluation.stray, vec!["the acceptance inventories changed"]);
    }
}
