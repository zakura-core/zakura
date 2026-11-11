//! Header sync's acceptance manifest (hdr-14, item 0).
//!
//! [`COVERAGE`](super::coverage::COVERAGE) names one witness and one bypass
//! per property. This manifest adds what header-sync acceptance needs on top:
//! for every header ID, the results a frozen-head acceptance run must give,
//! each a typed [`Expected`], and the contracts those results rely on.
//! [`evaluate`] judges a run's result file against both.
//!
//! The pieces, each with one job:
//!
//! * [`rows`]: one [`Row`] per header ID, with its expected results, and
//!   the [`GATE_REQUIREMENTS`] that no single ID owns.
//! * [`contracts`]: the relied-on contracts, [`REQUIRED_CONTRACTS`], their
//!   states and the [`NARROWING`] entries that release a result from one.
//! * [`required`]: the obligation keys the manifest must cover, derived
//!   from inventories the manifest doesn't control.
//! * [`evaluate`]: the gate evaluator.
//! * [`inventory`]: the pin on the lists that define obligations, which
//!   the gate's own path checks.
//!
//! Everything here is static data and pure functions. The acceptance tool
//! (hdr-14.7) produces the result file, and `tests/header_acceptance.rs`
//! holds the static checks and the evaluator's controls.
//!
//! # Identity
//!
//! A result counts for an expected result only when its whole
//! [`ExpectedId`] and the subject commit are equal, and each execution
//! inside it is joined to its expected execution by its whole [`RunId`].
//! Nothing here matches on a subset of those fields, such as a test's name
//! without its file and package, or a case's name without its property. The evaluator's controls change one field at a time and require
//! the result to stop counting.

pub mod contracts;
pub mod evaluate;
pub mod inventory;
pub mod reasons;
pub mod required;
pub mod rows;
pub mod tool;

use serde::{Deserialize, Serialize};

pub use contracts::{
    contract_table, Contract, ContractId, ContractState, Narrowing, ResultCtx, NARROWING,
    REQUIRED_CONTRACTS,
};
pub use evaluate::{evaluate, Evaluation, ObligationState, Subject};
pub use required::{required_keys, Inventories, Registered};
pub use rows::{
    harness_matrix, rows, GateRequirement, NativeClass, NativeMutation, COMPOSITION_CHECKS,
    GATE_REQUIREMENTS, NATIVE_MUTATIONS, NESTS,
};

use crate::property::{Source, Status};

/// One of sim-6.2's six gates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Gate {
    /// Order, attribution, freshness, consumption and cleanup.
    G1,
    /// Consequences, no unjustified punishment, effects despite a close.
    G2,
    /// Exchange rules, quantities and crossed updates.
    G3,
    /// Cadence.
    G4,
    /// Resources.
    G5,
    /// Missing evidence, inconclusive ends and tiers.
    G6,
}

/// Every gate, in order.
pub const GATES: [Gate; 6] = [Gate::G1, Gate::G2, Gate::G3, Gate::G4, Gate::G5, Gate::G6];

/// Who approved an open row, an unreachable reason or an exclusion (Rule
/// 19). Only [`Approval::Adam`] counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Approval {
    /// Not approved yet. A row with this approval blocks its key.
    Pending,
    /// Adam approved it, in the named record.
    Adam {
        /// Where the approval is written down, such as a plan decision.
        record: &'static str,
    },
}

impl Approval {
    /// Whether the approval counts.
    pub fn approved(self) -> bool {
        matches!(self, Approval::Adam { .. })
    }
}

/// The plan's frozen text, which Adam froze on 2026-11-08, as the record of
/// an approval it states.
pub const PLAN: Approval = Approval::Adam {
    record: "hdr-14 plan, frozen 2026-11-08",
};

/// A tier from properties.md's Tiers. A row's own `tier` is `Sim` or
/// `Machine`, and every other tier gets a [`Key::Tier`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tier {
    /// The owning crate's table suite.
    Table,
    /// A pure machine with no transport.
    Machine,
    /// A node harness under the simulator.
    Sim,
    /// A real network.
    Real,
}

/// Every tier properties.md names, in its order.
pub const TIERS: [Tier; 4] = [Tier::Table, Tier::Machine, Tier::Sim, Tier::Real];

/// A test, by its package, its file and its exact name. A result names the
/// same three.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TestRef {
    /// The Cargo package the test is in, such as `zakura-sim`.
    pub package: &'static str,
    /// The test's file, relative to its package.
    pub file: &'static str,
    /// The test's name.
    pub test: &'static str,
}

/// A `zakura-sim` test.
pub const fn sim_test(file: &'static str, test: &'static str) -> TestRef {
    TestRef {
        package: "zakura-sim",
        file,
        test,
    }
}

/// One acceptance history: a run of a registered harness on one committed
/// scenario and input, named by the witness test that builds it and, where
/// that test runs several, the case it names. hdr-14.7b binds each to the
/// digest of its recorded scenario and input (C18).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HistoryRef {
    /// The registered harness it runs on.
    pub harness: &'static str,
    /// The witness test that builds it.
    pub witness: TestRef,
    /// Which of the test's histories it is, never empty, such as `both
    /// ways` or `plan 0 of inputs(32, 256)`. A test that runs one history
    /// still names it.
    pub case: &'static str,
}

/// A planted case, `qa/planted/<property>/<name>.patch` and its `.toml`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CaseRef {
    /// The property directory it's under.
    pub property: &'static str,
    /// The case's name, without an extension.
    pub name: &'static str,
}

/// Which of a planted case's runs an execution is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CaseRun {
    /// The unpatched baseline.
    Baseline,
    /// The patched run, with the case's set `Active`.
    Patched,
    /// The patched run with the case's set as `Draft` (item 8).
    Draft,
    /// A native mutation's unpatched history.
    Unpatched,
    /// A native mutation's history on the restored checkout.
    Restored,
}

/// Where a patched run's violation is found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Found {
    /// At a step: the run ends `Outcome::Violation`, stopped by a member of
    /// the set.
    AtStep,
    /// At the end: an obligation the drain concludes or an end finding, with
    /// no stopping violation.
    AtEnd,
}

/// A property that a history's row expects to stay `NotApplicable`, with
/// the reason it must give.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExpectedNa {
    /// The property.
    pub property: &'static str,
    /// The reason its report must carry.
    pub reason: &'static str,
}

/// The harness planted case `name` of `property` runs on, under the
/// repository `repo`, from its `.toml` as the planted runner reads it: a
/// fuzz-input case's committed `input`, whose sidecar names its `harness`,
/// or a typed case's `[typed]` table's `harness`. A case names exactly one
/// of the two. An error says what can't be read.
pub fn planted_harness(
    repo: &std::path::Path,
    property: &str,
    name: &str,
) -> Result<String, String> {
    let read = |path: &std::path::Path| -> Result<toml::Table, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))
    };
    let path = repo
        .join("qa/planted")
        .join(property)
        .join(format!("{name}.toml"));
    let case = read(&path)?;
    let harness = |table: &toml::Table, what: &str| {
        table
            .get("harness")
            .and_then(toml::Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| format!("{what} names no harness"))
    };
    match (case.get("input"), case.get("typed")) {
        (Some(input), None) => {
            let input = input
                .as_str()
                .ok_or_else(|| format!("{}: `input` isn't a path", path.display()))?;
            let sidecar = read(&repo.join(input).with_extension("toml"))?;
            harness(&sidecar, &format!("{input}'s sidecar"))
        }
        (None, Some(typed)) => {
            let typed = typed
                .as_table()
                .ok_or_else(|| format!("{}: `typed` isn't a table", path.display()))?;
            harness(typed, &format!("{}'s [typed] table", path.display()))
        }
        _ => Err(format!(
            "{}: a case names either `input` or `[typed]`",
            path.display()
        )),
    }
}

/// One result a row expects (Rule 20).
#[derive(Clone, Debug)]
pub enum Expected {
    /// The history's whole report passes and its target holds.
    Holds {
        /// The target, the property whose passing key it covers.
        property: &'static str,
        /// The history.
        history: HistoryRef,
        /// Other properties of the harness expected `NotApplicable`.
        not_applicable: &'static [ExpectedNa],
    },
    /// The history's whole report passes and its target stays
    /// `NotApplicable` with `reason`.
    StaysNotApplicable {
        /// The target.
        property: &'static str,
        /// The history.
        history: HistoryRef,
        /// The reason the target's report must carry.
        reason: &'static str,
        /// The reason's approval (Rule 19).
        approval: Approval,
        /// Other properties of the harness expected `NotApplicable`.
        not_applicable: &'static [ExpectedNa],
    },
    /// A planted case's baseline holds, its patched run violates exactly
    /// `set`, and its `Draft` run completes with every member violated.
    Caught {
        /// The case.
        case: CaseRef,
        /// The harness its input runs on.
        harness: &'static str,
        /// Every ID it must violate: its own, plus any [`NESTS`] allows.
        set: &'static [&'static str],
        /// Where the patched run's violation is found.
        found: Found,
        /// Assertions judged on the case's own `Draft` trace, by name.
        companions: &'static [&'static str],
        /// Other properties of the harness expected `NotApplicable`.
        not_applicable: &'static [ExpectedNa],
    },
    /// A machine harness's planted case: the baseline holds and the patched
    /// run violates exactly the machine's ID.
    MachineCaught {
        /// The case.
        case: CaseRef,
        /// The machine harness.
        harness: &'static str,
    },
    /// A retained integrity mutation: the patched history fails the run as a
    /// harness failure carrying exactly `diagnostic`.
    IntegrityCaught {
        /// The directory under `qa/native-mutations/header-sync/`.
        patch: &'static str,
        /// The history it runs.
        history: HistoryRef,
        /// The integrity diagnostic, such as `OwnedMismatch`.
        diagnostic: &'static str,
        /// Properties of the history's harness expected `NotApplicable` on
        /// its clean runs.
        not_applicable: &'static [ExpectedNa],
    },
    /// A fresh-process Exact replay of one execution, with its reports and
    /// store snapshots equal.
    Replays {
        /// The execution.
        run: RunId,
    },
    /// A test's own pass, executed by its exact name at the subject commit.
    Passes {
        /// The test.
        test: TestRef,
    },
    /// Evidence that isn't built here (Rules 14 and 19).
    Open {
        /// Why it's open.
        reason: &'static str,
        /// Who owns the missing evidence.
        owner: &'static str,
        /// Its approval. A transitional row needs none, and still blocks.
        approval: Approval,
        /// The PR that closes a transitional row, or `None` for a standing
        /// one.
        until: Option<&'static str>,
    },
}

/// One execution, by the identity a result names it with.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum RunId {
    /// An acceptance history.
    History {
        /// Its harness.
        harness: String,
        /// Its witness test's package.
        package: String,
        /// Its witness test's file.
        file: String,
        /// Its witness test's name.
        test: String,
        /// Its case within that test.
        case: String,
    },
    /// One run of a planted case or a native mutation.
    Case {
        /// The property directory, or `native-mutations/header-sync` for a
        /// native mutation.
        property: String,
        /// The case's or mutation's name.
        name: String,
        /// Which run.
        run: CaseRun,
    },
}

impl From<HistoryRef> for RunId {
    /// The history's identity.
    fn from(history: HistoryRef) -> Self {
        RunId::History {
            harness: history.harness.to_owned(),
            package: history.witness.package.to_owned(),
            file: history.witness.file.to_owned(),
            test: history.witness.test.to_owned(),
            case: history.case.to_owned(),
        }
    }
}

/// The property directory a native mutation's [`RunId::Case`] names.
pub const NATIVE_DIR: &str = "native-mutations/header-sync";

impl RunId {
    /// Run `run` of planted case `case`.
    pub fn case(case: CaseRef, run: CaseRun) -> Self {
        RunId::Case {
            property: case.property.to_owned(),
            name: case.name.to_owned(),
            run,
        }
    }

    /// Run `run` of native mutation `patch`.
    pub fn native(patch: &str, run: CaseRun) -> Self {
        RunId::Case {
            property: NATIVE_DIR.to_owned(),
            name: patch.to_owned(),
            run,
        }
    }
}

/// An obligation key: one thing acceptance requires, derived apart from the
/// manifest (Rule 21).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum Key {
    /// A header ID's passing result: `Holds` or an approved
    /// `StaysNotApplicable`.
    Passing {
        /// The property.
        property: String,
    },
    /// An `Active` header ID's catch: `Caught`, `MachineCaught` or an
    /// approved `Open`.
    Catch {
        /// The property.
        property: String,
    },
    /// A planted case's own catch, so each case is named by a row.
    Case {
        /// The case's property directory.
        property: String,
        /// The case's name.
        name: String,
    },
    /// One execution's fresh replay.
    Replay {
        /// The execution.
        run: RunId,
    },
    /// One node-harness history's cleanup comparison (item 1).
    Cleanup {
        /// The history.
        run: RunId,
    },
    /// A named witness or bypass test from `COVERAGE`, executed by name.
    Witness {
        /// The test's package.
        package: String,
        /// The test's file.
        file: String,
        /// The test's name.
        test: String,
    },
    /// A source a header property needs, removed on one harness (item 6).
    MissingSource {
        /// The harness.
        harness: String,
        /// The source, as its `Debug` name.
        source: String,
    },
    /// A native mutation's classification result.
    Native {
        /// Its directory name.
        patch: String,
    },
    /// A [`GATE_REQUIREMENTS`] entry.
    Requirement {
        /// Its name.
        name: String,
    },
    /// A `Satisfied` contract's proving test.
    ContractTest {
        /// The contract, such as `C3`.
        contract: String,
        /// The test's package.
        package: String,
        /// The test's file.
        file: String,
        /// The test's name.
        test: String,
    },
    /// A [`NARROWING`] entry's test.
    NarrowingTest {
        /// The entry, such as `N1`.
        entry: String,
    },
    /// A counted history's whole-report pass: every execution a counted
    /// witness test runs owes one, whether or not a row's `Holds` names it.
    History {
        /// The history.
        run: RunId,
        /// The property its witness runs it for, as
        /// [`history_targets`](rows::history_targets) names it.
        target: String,
    },
    /// A node history's expected `NotApplicable` list (C1).
    NaList {
        /// The history.
        run: RunId,
    },
    /// A [`COMPOSITION_CHECKS`] test.
    Composition {
        /// The test's name, in `tests/header_acceptance.rs`.
        test: String,
    },
    /// One `NotApplicable` reason in [`reasons::NA_REASONS`], shown at its
    /// trigger.
    NaReason {
        /// The producer's file.
        file: String,
        /// The producer's function.
        function: String,
        /// The construction's position in the function.
        index: usize,
    },
    /// A header ID's entry for a tier other than its own (item 6).
    Tier {
        /// The property.
        property: String,
        /// The tier, as its `Debug` name.
        tier: String,
    },
}

impl Key {
    /// [`Key::Passing`] for `property`.
    pub fn passing(property: &str) -> Self {
        Key::Passing {
            property: property.to_owned(),
        }
    }

    /// [`Key::Catch`] for `property`.
    pub fn catch(property: &str) -> Self {
        Key::Catch {
            property: property.to_owned(),
        }
    }
}

/// An expected result's full identity, as a result names the one it claims
/// to satisfy.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum ExpectedId {
    /// [`Expected::Holds`] of this property on this history.
    Holds {
        /// The target.
        property: String,
        /// The history.
        run: RunId,
    },
    /// [`Expected::StaysNotApplicable`] of this property on this history.
    StaysNotApplicable {
        /// The target.
        property: String,
        /// The history.
        run: RunId,
    },
    /// [`Expected::Caught`] for this case's patched run.
    Caught(RunId),
    /// [`Expected::MachineCaught`] for this case's patched run.
    MachineCaught(RunId),
    /// [`Expected::IntegrityCaught`] for this mutation's patched run.
    IntegrityCaught(RunId),
    /// [`Expected::Replays`] of this execution.
    Replays(RunId),
    /// [`Expected::Passes`] of this test.
    Passes {
        /// The test's package.
        package: String,
        /// The test's file.
        file: String,
        /// The test's name.
        test: String,
    },
}

impl Expected {
    /// The identity a result must name, or `None` for an `Open`, which no
    /// result satisfies.
    pub fn id(&self) -> Option<ExpectedId> {
        Some(match self.clone() {
            Expected::Holds {
                property, history, ..
            } => ExpectedId::Holds {
                property: property.to_owned(),
                run: history.into(),
            },
            Expected::StaysNotApplicable {
                property, history, ..
            } => ExpectedId::StaysNotApplicable {
                property: property.to_owned(),
                run: history.into(),
            },
            Expected::Caught { case, .. } => {
                ExpectedId::Caught(RunId::case(case, CaseRun::Patched))
            }
            Expected::MachineCaught { case, .. } => {
                ExpectedId::MachineCaught(RunId::case(case, CaseRun::Patched))
            }
            Expected::IntegrityCaught { patch, .. } => {
                ExpectedId::IntegrityCaught(RunId::native(patch, CaseRun::Patched))
            }
            Expected::Replays { run } => ExpectedId::Replays(run),
            Expected::Passes { test } => ExpectedId::Passes {
                package: test.package.to_owned(),
                file: test.file.to_owned(),
                test: test.test.to_owned(),
            },
            Expected::Open { .. } => return None,
        })
    }

    /// The result kind, for [`NARROWING`] and for the static checks.
    pub fn kind(&self) -> ResultKind {
        match self {
            Expected::Holds { .. } => ResultKind::Holds,
            Expected::StaysNotApplicable { .. } => ResultKind::StaysNotApplicable,
            Expected::Caught { .. } => ResultKind::Caught,
            Expected::MachineCaught { .. } => ResultKind::MachineCaught,
            Expected::IntegrityCaught { .. } => ResultKind::IntegrityCaught,
            Expected::Replays { .. } => ResultKind::Replays,
            Expected::Passes { .. } => ResultKind::Passes,
            Expected::Open { .. } => ResultKind::Open,
        }
    }
}

impl Key {
    /// Whether `expected` is a result that can satisfy this key: the same
    /// kind of obligation, naming the same item in every identity field.
    /// The static checks and the evaluator both join keys to results only
    /// through this.
    ///
    /// A passing key takes a `Holds` or `StaysNotApplicable` of its own
    /// property, a catch key a `Caught` whose case is under its property and
    /// whose set contains it, or a `MachineCaught` of its property, a case
    /// key the catch of exactly that case, a replay key the replay of exactly
    /// that execution, a test key the `Passes` of exactly that test, and a
    /// requirement key what its [`GATE_OBLIGATIONS`](required::GATE_OBLIGATIONS)
    /// entry permits. Any other key also takes an `Open`, which blocks until
    /// approved. A narrowing test key is matched against `narrowing`,
    /// the manifest's entries.
    pub fn covers(&self, expected: &Expected, narrowing: &[contracts::Narrowing]) -> bool {
        let test_is = |package: &str, file: &str, name: &str, test: &TestRef| {
            test.package == package && test.file == file && test.test == name
        };
        match (self, expected) {
            (Key::Passing { property }, Expected::Holds { property: p, .. })
            | (Key::Passing { property }, Expected::StaysNotApplicable { property: p, .. }) => {
                property == p
            }
            (Key::Catch { property }, Expected::Caught { case, set, .. }) => {
                case.property == property && set.contains(&property.as_str())
            }
            (Key::Catch { property }, Expected::MachineCaught { case, .. }) => {
                case.property == property
            }
            (
                Key::Case { property, name },
                Expected::Caught { case, .. } | Expected::MachineCaught { case, .. },
            ) => case.property == property && case.name == name,
            (Key::Replay { run }, Expected::Replays { run: replayed }) => run == replayed,
            (
                Key::History { run, target },
                Expected::Holds {
                    history, property, ..
                }
                | Expected::StaysNotApplicable {
                    history, property, ..
                },
            ) => RunId::from(*history) == *run && property == target,
            // A transitional Open on a passing or catch key must be exactly
            // the one `rows::owed_open` writes for that key: its reason,
            // owner, approval and PR.
            (Key::Passing { .. } | Key::Catch { .. }, Expected::Open { until: Some(_), .. }) => {
                rows::owed_open(self).is_some_and(|open| same_open(expected, &open))
            }
            (
                Key::Native { patch },
                Expected::IntegrityCaught {
                    patch: p,
                    history,
                    diagnostic,
                    not_applicable,
                },
            ) => {
                patch == p
                    && NATIVE_MUTATIONS.iter().any(|mutation| {
                        mutation.patch == patch
                            && mutation.class
                                == NativeClass::Integrity {
                                    history: *history,
                                    diagnostic,
                                    not_applicable,
                                }
                    })
            }
            (Key::Cleanup { run }, Expected::Open { .. }) => {
                let machine =
                    matches!(run, RunId::History { harness, .. } if rows::is_machine(harness));
                let canonical = if machine {
                    rows::MACHINE_CLEANUP
                } else {
                    rows::NODE_CLEANUP
                };
                same_open(expected, &canonical)
            }
            (Key::NaList { .. }, Expected::Open { .. }) => same_open(expected, &rows::NA_LIST),
            (Key::NaReason { .. }, Expected::Open { .. }) => {
                same_open(expected, &rows::REASON_TEST)
            }
            (Key::Tier { property, tier }, Expected::Open { .. }) => TIERS
                .into_iter()
                .find(|t| format!("{t:?}") == *tier)
                .is_some_and(|t| same_open(expected, &rows::tier_exclusion(property, t))),
            (Key::Tier { property, tier }, Expected::Passes { test }) => {
                rows::TIER_TESTS.iter().any(|(p, t, covering)| {
                    p == property && format!("{t:?}") == *tier && covering == test
                })
            }
            // An approved standing Open on a passing, catch or
            // missing-source key counts only as the exact exclusion
            // `STANDING_EXCLUSIONS` grants that key.
            (
                Key::Passing { .. } | Key::Catch { .. } | Key::MissingSource { .. },
                Expected::Open {
                    until: None,
                    approval,
                    ..
                },
            ) if approval.approved() => {
                rows::exclusion_granted(rows::STANDING_EXCLUSIONS, self, expected)
            }
            (
                Key::Witness {
                    package,
                    file,
                    test,
                },
                Expected::Passes { test: t },
            ) => test_is(package, file, test, t),
            (
                Key::ContractTest {
                    package,
                    file,
                    test,
                    ..
                },
                Expected::Passes { test: t },
            ) => test_is(package, file, test, t),
            (Key::Composition { test }, Expected::Passes { test: t }) => {
                test_is("zakura-sim", rows::ACCEPTANCE_TESTS, test, t)
            }
            (Key::NarrowingTest { entry }, Expected::Passes { test: t }) => {
                narrowing.iter().any(|narrowing| {
                    narrowing.entry == entry
                        && matches!(narrowing.test, contracts::NarrowingTest::Test(n) if n == *t)
                })
            }
            // A native mutation's or a narrowing entry's transitional Open
            // must be exactly the one its classification or entry writes:
            // reason, owner, approval and PR.
            (Key::Native { patch }, Expected::Open { .. }) => {
                NATIVE_MUTATIONS.iter().any(|mutation| {
                    mutation.patch == patch
                        && match mutation.class {
                            NativeClass::ToMove { until, .. } => {
                                same_open(expected, &rows::native_open(until))
                            }
                            NativeClass::Retired { .. } => {
                                same_open(expected, &rows::retired_open())
                            }
                            _ => false,
                        }
                })
            }
            (Key::NarrowingTest { entry }, Expected::Open { .. }) => {
                narrowing.iter().any(|narrowing| {
                    narrowing.entry == entry
                        && matches!(narrowing.test, contracts::NarrowingTest::Pending { until }
                            if same_open(expected, &contracts::narrowing_open(until)))
                })
            }
            (Key::MissingSource { harness, .. }, Expected::Passes { test: t }) => {
                rows::missing_source_test(harness) == Some(*t)
            }
            (
                Key::Case { .. } | Key::Replay { .. } | Key::Witness { .. } | Key::History { .. },
                _,
            )
            | (Key::ContractTest { .. } | Key::Composition { .. }, _) => false,
            (Key::Requirement { name }, expected) => required::GATE_OBLIGATIONS
                .iter()
                .any(|obligation| obligation.name == name && obligation.permit.allows(expected)),
            // A standing Open still awaiting approval: it blocks.
            (
                Key::Passing { .. } | Key::Catch { .. } | Key::MissingSource { .. },
                Expected::Open { until: None, .. },
            ) => true,
            _ => false,
        }
    }
}

/// Whether `a` and `b` are the same `Open`: reason, owner, approval and
/// `until`.
fn same_open(a: &Expected, b: &Expected) -> bool {
    match (a, b) {
        (
            Expected::Open {
                reason,
                owner,
                approval,
                until,
            },
            Expected::Open {
                reason: r,
                owner: o,
                approval: p,
                until: u,
            },
        ) => reason == r && owner == o && approval == p && until == u,
        _ => false,
    }
}

/// An [`Expected`]'s variant, without its data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ResultKind {
    /// [`Expected::Holds`].
    Holds,
    /// [`Expected::StaysNotApplicable`].
    StaysNotApplicable,
    /// [`Expected::Caught`].
    Caught,
    /// [`Expected::MachineCaught`].
    MachineCaught,
    /// [`Expected::IntegrityCaught`].
    IntegrityCaught,
    /// [`Expected::Replays`].
    Replays,
    /// [`Expected::Passes`].
    Passes,
    /// [`Expected::Open`].
    Open,
}

/// One expected result with the key it covers and the gates it serves.
#[derive(Clone, Debug)]
pub struct ExpectedResult {
    /// The obligation keys it covers, each exactly once.
    pub keys: Vec<Key>,
    /// The gates it serves. They must equal each of its keys' (Rule 21).
    pub gates: Vec<Gate>,
    /// What must happen.
    pub expected: Expected,
}

/// A `Draft` row's promotion: the condition and owner that make it
/// `Active`.
#[derive(Clone, Copy, Debug)]
pub struct Promotion {
    /// What must hold first.
    pub condition: &'static str,
    /// Who promotes it.
    pub owner: &'static str,
}

/// One header ID's acceptance row.
#[derive(Clone, Debug)]
pub struct Row {
    /// The property's ID.
    pub property: &'static str,
    /// The tier its evidence runs in.
    pub tier: Tier,
    /// The harnesses that register it.
    pub harnesses: &'static [&'static str],
    /// Its registry status.
    pub status: Status,
    /// For a `Draft`, what promotes it.
    pub promotion: Option<Promotion>,
    /// Its expected results.
    pub expected: Vec<ExpectedResult>,
}

/// The manifest the evaluator judges: every row, every gate requirement and
/// native mutation's expected results, the contracts table and the
/// narrowing entries, as values a test can change one at a time.
#[derive(Clone, Debug)]
pub struct Manifest {
    /// Every expected result, from rows, requirements, native mutations,
    /// contract tests, narrowing tests and composition checks.
    pub expected: Vec<ExpectedResult>,
    /// The contracts table.
    pub contracts: Vec<Contract>,
    /// The contract IDs that must each have one row.
    pub required_contracts: Vec<ContractId>,
    /// The narrowing entries.
    pub narrowing: Vec<Narrowing>,
    /// The machine harnesses, by [`HarnessEntry::machine`](super::HarnessEntry::machine).
    pub machines: Vec<&'static str>,
    /// Every `(harness, ID, status)` the header harnesses register, as
    /// [`harness_matrix`] writes it. A run must report exactly its
    /// harness's IDs.
    pub matrix: Vec<(&'static str, &'static str, Status)>,
}

impl Manifest {
    /// The committed manifest: [`rows`], [`GATE_REQUIREMENTS`],
    /// [`NATIVE_MUTATIONS`], the contracts' and narrowing entries' tests and
    /// [`COMPOSITION_CHECKS`].
    pub fn committed() -> Self {
        let rows = rows();
        let mut expected: Vec<ExpectedResult> =
            rows.iter().flat_map(|row| row.expected.clone()).collect();
        rows::attach_history_passes(&mut expected);
        expected.extend(rows::counted_results(&expected.clone()));
        expected.extend(rows::requirement_results());
        expected.extend(rows::witness_results(&rows));
        expected.extend(rows::native_results());
        let contracts = contract_table();
        expected.extend(contracts::contract_test_results(&contracts));
        expected.extend(contracts::narrowing_test_results(NARROWING));
        expected.extend(rows::composition_results());
        expected.extend(rows::missing_source_results(&needs()));
        expected.extend(rows::reason_results());
        expected.extend(rows::tier_results(&registered()));
        Manifest {
            expected,
            contracts,
            required_contracts: REQUIRED_CONTRACTS.to_vec(),
            narrowing: NARROWING.to_vec(),
            machines: super::HARNESSES
                .iter()
                .filter(|entry| entry.machine)
                .map(|entry| entry.name)
                .collect(),
            matrix: harness_matrix(),
        }
    }
}

/// Every registration of the header harnesses, read from
/// [`HARNESSES`](super::HARNESSES).
pub fn registered() -> Vec<Registered> {
    super::HARNESSES
        .iter()
        .filter(|entry| rows::HEADER_HARNESSES.contains(&entry.name))
        .flat_map(|entry| {
            (entry.properties)()
                .into_iter()
                .map(move |property| Registered {
                    harness: entry.name.to_owned(),
                    property: property.id,
                    status: property.status,
                    machine: entry.machine,
                })
        })
        .collect()
}

/// Every `(harness, source)` a header harness's properties need, read
/// from each harness's registry, by the source's `Debug` name.
pub fn needs() -> Vec<(String, String)> {
    needs_of(rows::HEADER_HARNESSES)
        .expect("every header harness has its needs read, which tests/header_acceptance.rs checks")
}

/// [`needs`] for `harnesses`, or the first harness this can't read the
/// needs of, so a header harness added to the list without its registry
/// here fails instead of owing no missing-source key.
pub fn needs_of(harnesses: &[&str]) -> Result<Vec<(String, String)>, String> {
    use super::header_sync::{
        Flood, HeaderWire, Liveness, ReleaseWake, RepairHarness, Smoke, Storage,
    };
    use super::Subsystem;
    /// `S`'s needed sources, under its name.
    fn of<S: Subsystem>() -> Vec<(String, String)> {
        let sources: std::collections::BTreeSet<Source> = super::registry::<S>()
            .properties()
            .iter()
            .flat_map(|property| property.needs.iter().copied())
            .collect();
        sources
            .into_iter()
            .map(|source| (S::NAME.to_owned(), format!("{source:?}")))
            .collect()
    }
    let mut needs = Vec::new();
    for &harness in harnesses {
        let machine = [super::header_work::REACTOR, super::header_work::PUBLISHER]
            .into_iter()
            .find(|machine| machine.name == harness);
        needs.extend(match harness {
            _ if harness == Smoke::NAME => of::<Smoke>(),
            _ if harness == Storage::NAME => of::<Storage>(),
            _ if harness == ReleaseWake::NAME => of::<ReleaseWake>(),
            _ if harness == HeaderWire::NAME => of::<HeaderWire>(),
            _ if harness == Flood::NAME => of::<Flood>(),
            _ if harness == RepairHarness::NAME => of::<RepairHarness>(),
            _ if harness == Liveness::NAME => of::<Liveness>(),
            _ => match machine {
                Some(machine) => super::machine::needs(&machine)
                    .into_iter()
                    .map(|source| (machine.name.to_owned(), format!("{source:?}")))
                    .collect(),
                None => return Err(format!("{harness}'s needs aren't read")),
            },
        });
    }
    Ok(needs)
}
