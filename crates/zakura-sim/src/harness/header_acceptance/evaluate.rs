//! The gate evaluator (item 0).
//!
//! It takes the manifest, the required keys and an acceptance run's
//! [`ResultFile`], and gives every key one [`ObligationState`] and every
//! gate a verdict. A gate reads only its row and the run (Rule 15): the
//! expectation comes from the manifest, and a result supplies only what
//! happened.
//!
//! Precedence, per expected result:
//!
//! 1. **Blocked** if any contract that applies to it is blocked, missing
//!    from the table, or `Satisfied` without every proving test passing at
//!    the subject commit. That always wins over an exclusion.
//! 2. **Excluded** if it's an approved standing `Open`, or an approved
//!    `Excluded` contract applies to it.
//! 3. **Verified** if its own result matches and every contract that
//!    applies to it is `Satisfied`.
//! 4. **Blocked** otherwise.
//!
//! Every contract applies to every result unless a [`Narrowing`] entry
//! whose own key verified releases it (Rule 24). What an entry decides on
//! comes from the manifest's expected execution, never from what a result
//! claims about itself.
//!
//! The tests and proving catches that decide a contract's or an entry's
//! state depend only on C5, the binary's provenance: letting them depend on the contracts they
//! decide would make the graph cyclic, so a contract could never be
//! satisfied and an entry could never release. C5's own proving tests can't
//! depend on C5, so each must have run from a binary that embeds the
//! subject's source digest, which the result file records. Each of those
//! keys gets the same coverage, naming and gate checks as every other key
//! before it decides anything.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::{
    contracts::{
        contract_catch_key, contract_test_key, ContractId, ContractState, Narrowing, NarrowingTest,
        RunBy,
    },
    Approval, CaseRun, Expected, ExpectedId, ExpectedNa, ExpectedResult, Found, Gate, Key,
    Manifest, ResultCtx, RunId, TestRef, GATES,
};

/// An acceptance run's results, as the acceptance tool writes them.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultFile {
    /// The subject commit S every result must come from.
    pub subject: String,
    /// S's source digest, as `zakura-sim`'s build script computes it. C5's
    /// own proving tests must have run from a binary embedding it.
    pub subject_source: String,
    /// One record per result.
    pub results: Vec<ResultRecord>,
}

/// One result: the expected result it claims to satisfy, the commit it ran
/// at, and what happened.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultRecord {
    /// The expected result's full identity.
    pub expected: ExpectedId,
    /// The commit it ran at.
    pub commit: String,
    /// What happened.
    pub actual: Actual,
}

/// What happened, per result kind.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum Actual {
    /// A `Holds` or `StaysNotApplicable` history's run.
    Run(Execution),
    /// A node planted case's three runs and its companions.
    Caught {
        /// The unpatched baseline.
        baseline: Execution,
        /// The patched run.
        patched: Execution,
        /// The patched run with the set as `Draft`.
        draft: Execution,
        /// Each companion assertion, with the execution it was judged on.
        companions: Vec<Companion>,
    },
    /// A machine planted case's two runs.
    MachineCaught {
        /// The unpatched baseline.
        baseline: Execution,
        /// The patched run.
        patched: Execution,
    },
    /// A harness-caught planted case's two runs.
    HarnessCaught {
        /// The unpatched baseline.
        baseline: Execution,
        /// The patched run, which ends at its harness failure.
        patched: Execution,
    },
    /// A native integrity mutation's three runs, each naming the history
    /// it ran in [`ExecMeta::input`].
    IntegrityCaught {
        /// The unpatched history.
        unpatched: Execution,
        /// The patched history.
        patched: Execution,
        /// The history on the restored checkout.
        restored: Execution,
    },
    /// A fresh-process replay of one execution.
    Replay {
        /// The replayed execution.
        run: RunId,
        /// The digest of the trace it replayed, which must be the counted
        /// execution's own.
        of: String,
        /// The replayed execution's metadata.
        meta: ExecMeta,
        /// Whether the replay matched Exact.
        exact: bool,
        /// Whether the original and replayed full reports are equal.
        reports_equal: bool,
        /// Whether the original and replayed store snapshots are equal.
        stores_equal: bool,
        /// Whether the replay ran in a process of its own, not the one that
        /// recorded the execution.
        fresh: bool,
    },
    /// A test run by its exact name.
    Test {
        /// Every test the filter executed, by its own identity.
        ran: Vec<TestRan>,
        /// The source digest the test binary embeds, or `None` if the tool
        /// couldn't read one, which blocks the test's key.
        binary_source: Option<String>,
        /// The source digest of the checkout the test ran in, which reads
        /// its files, or `None` if the tool couldn't read one, which blocks
        /// the test's key too.
        checkout_source: Option<String>,
    },
}

/// One companion assertion's verdict.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Companion {
    /// Its declared name.
    pub name: String,
    /// The digest of the trace it was judged on, which must be the `Draft`
    /// run's own.
    pub trace: String,
    /// The execution it was judged on, which must be the catch's `Draft`
    /// run.
    pub run: RunId,
    /// Whether it held.
    pub held: bool,
}

/// One test the exact-name filter executed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestRan {
    /// Its package.
    pub package: String,
    /// Its file, relative to the package.
    pub file: String,
    /// Its name.
    pub test: String,
    /// How it ended.
    pub outcome: TestOutcome,
}

/// How a test ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TestOutcome {
    /// It passed.
    Passed,
    /// It failed.
    Failed,
    /// It was skipped or ignored.
    Skipped,
}

/// What an execution ran on and how, for [`Narrowing`] entries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecMeta {
    /// Its harness.
    pub harness: String,
    /// The IDs its `Draft` override named.
    pub draft_override: Vec<String>,
    /// Who ran it.
    pub run_by: RunBy,
    /// For a native mutation's run, which its [`RunId`] names only by the
    /// mutation and role, the history whose input it ran. `None` for every
    /// other execution.
    #[serde(default)]
    pub input: Option<RunId>,
}

/// How a run ended.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum End {
    /// A completed run with no stopping violation.
    Completed,
    /// Stopped by this `Active` property's violation.
    Violation {
        /// The property.
        property: String,
    },
    /// A harness failure with this diagnostic.
    HarnessFailure {
        /// The diagnostic.
        diagnostic: String,
    },
    /// Killed at its deadline or by a signal.
    Killed,
    /// It didn't run: a patch that didn't apply or a failed build.
    Failed {
        /// Why.
        why: String,
    },
}

/// One property's reported outcome.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum Report {
    /// It held, applying this many times.
    Held {
        /// Its applicability count.
        applicable: u64,
    },
    /// It ended `NotApplicable`, for this reason, as
    /// [`NaReason::name`](crate::NaReason::name) writes it, such as
    /// `never-applied`.
    NotApplicable {
        /// The reason.
        reason: String,
    },
    /// It was violated.
    Violated,
}

/// Whether a run's declared actions were each picked and reached their
/// effect.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Actions {
    /// Every declared action ran.
    Complete,
    /// The run stopped, and its actions up to that step match its declared
    /// prefix.
    Prefix,
    /// A declared action didn't run.
    Incomplete,
}

/// One execution's result.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Execution {
    /// Which execution it is.
    pub run: RunId,
    /// The digest of its trace, which names this one execution among every
    /// run of the same [`RunId`].
    pub trace: String,
    /// What it ran on.
    pub meta: ExecMeta,
    /// How it ended.
    pub end: End,
    /// Every reported property's outcome.
    pub reports: BTreeMap<String, Report>,
    /// Whether the target's trigger history held.
    pub trigger: bool,
    /// The scheduler's leaked task count at cleanup.
    pub leaked: u64,
    /// Whether its declared actions ran.
    pub actions: Actions,
}

/// An obligation's state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObligationState {
    /// Its result matched, and every contract it depends on is satisfied.
    Verified,
    /// Excluded by an approved standing `Open` or an approved contract
    /// exclusion.
    Excluded {
        /// Why.
        why: String,
    },
    /// It can't count yet.
    Blocked {
        /// Why.
        why: String,
    },
}

impl ObligationState {
    /// A blocked state for `why`.
    fn blocked(why: impl Into<String>) -> Self {
        ObligationState::Blocked { why: why.into() }
    }
}

/// The evaluator's answer.
#[derive(Clone, Debug)]
pub struct Evaluation {
    /// Each required key's state.
    pub keys: BTreeMap<Key, ObligationState>,
    /// Each gate's verdict: whether no key it serves is blocked.
    pub gates: BTreeMap<Gate, bool>,
    /// Keys an expected result covers that no inventory requires, results
    /// that name no expected result, and contract rows no required ID
    /// names. Any one fails acceptance.
    pub stray: Vec<String>,
}

impl Evaluation {
    /// Whether every key is verified or excluded and nothing is stray.
    pub fn accepted(&self) -> bool {
        self.stray.is_empty()
            && self.keys.iter().all(|(key, state)| match key {
                // No exclusion waives a static check or an evaluator control.
                Key::Composition { .. } => *state == ObligationState::Verified,
                _ => !matches!(state, ObligationState::Blocked { .. }),
            })
    }

    /// Whether every key is verified and nothing is stray.
    pub fn fully_accepted(&self) -> bool {
        self.stray.is_empty()
            && self
                .keys
                .values()
                .all(|state| *state == ObligationState::Verified)
    }
}

/// A contract's evaluated state.
#[derive(Clone, Debug)]
enum Standing {
    /// It counts.
    Satisfied,
    /// Approved exclusion.
    Excluded(String),
    /// It blocks.
    Blocked(String),
}

/// The subject as the caller established it, apart from the result file:
/// its commit, and the source digest of its clean checkout.
#[derive(Clone, Copy, Debug)]
pub struct Subject<'a> {
    /// The commit.
    pub commit: &'a str,
    /// Its clean checkout's source digest, C5's bootstrap anchor.
    pub source: &'a str,
}

/// Evaluates `results` against `manifest` and `required` for `subject`. A
/// result file naming another subject or source digest is stray.
pub fn evaluate(
    manifest: &Manifest,
    required: &BTreeMap<Key, BTreeSet<Gate>>,
    results: &ResultFile,
    subject: Subject<'_>,
) -> Evaluation {
    let mut stray = Vec::new();
    if results.subject != subject.commit || results.subject_source != subject.source {
        stray.push(format!(
            "the result file is for {} with source {}, not {} with {}",
            results.subject, results.subject_source, subject.commit, subject.source
        ));
    }
    let mut by_id: BTreeMap<&ExpectedId, Vec<&ResultRecord>> = BTreeMap::new();
    for record in &results.results {
        by_id.entry(&record.expected).or_default().push(record);
    }
    let expected_ids: BTreeSet<ExpectedId> = manifest
        .expected
        .iter()
        .filter_map(|result| result.expected.id())
        .collect();
    for id in by_id.keys() {
        if !expected_ids.contains(*id) {
            stray.push(format!("a result names no expected result: {id:?}"));
        }
    }
    for result in &manifest.expected {
        if result.keys.is_empty() {
            stray.push(format!(
                "an expected result covers no key: {:?}",
                result.expected
            ));
        }
    }
    // Executions count toward a replay or companion only from a record at
    // the subject commit whose identity the manifest expects, named once,
    // and whose executions are exactly the ones that expected result owns,
    // each in its role. Then, of those, a replay may stand only on a record
    // whose own judgement passes, so an execution its result didn't count
    // (another harness, killed, a broken run) never backs one.
    let mut candidates: Vec<(&ResultRecord, &ExpectedResult)> = Vec::new();
    for record in &results.results {
        let unique = results
            .results
            .iter()
            .filter(|other| other.expected == record.expected)
            .count()
            == 1;
        let owner = manifest
            .expected
            .iter()
            .find(|result| result.expected.id().as_ref() == Some(&record.expected));
        let owned = owner.is_some_and(|owner| {
            let ran: Vec<&RunId> = executions(&record.actual)
                .into_iter()
                .map(|execution| &execution.run)
                .collect();
            ran == owned_runs(&owner.expected).iter().collect::<Vec<_>>()
        });
        if record.commit != subject.commit || !unique || !owned {
            continue;
        }
        if let Some(owner) = owner {
            candidates.push((record, owner));
        }
    }
    let index = |records: &mut dyn Iterator<Item = &&ResultRecord>| {
        let mut traces: BTreeMap<RunId, BTreeSet<String>> = BTreeMap::new();
        for record in records {
            for execution in executions(&record.actual) {
                traces
                    .entry(execution.run.clone())
                    .or_default()
                    .insert(execution.trace.clone());
            }
        }
        traces
    };
    let owned_traces = index(&mut candidates.iter().map(|(record, _)| record));
    let judged: Vec<&ResultRecord> = candidates
        .iter()
        .filter(|(record, owner)| {
            judge(manifest, &owned_traces, &owner.expected, &record.actual).is_ok()
        })
        .map(|(record, _)| *record)
        .collect();
    let traces = index(&mut judged.iter());
    let evidence = Evidence {
        manifest,
        by_id,
        traces,
        subject: subject.commit,
        subject_source: subject.source,
    };

    let mut covering: BTreeMap<&Key, Vec<&ExpectedResult>> = BTreeMap::new();
    for result in &manifest.expected {
        for key in &result.keys {
            covering.entry(key).or_default().push(result);
        }
    }
    for key in covering.keys() {
        if !required.contains_key(*key) {
            stray.push(format!(
                "an expected result covers an unrequired key: {key:?}"
            ));
        }
    }
    // The one check every key gets first: exactly one expected result
    // covers it, names it, and carries its gates.
    let covered = |key: &Key| -> Result<&ExpectedResult, ObligationState> {
        let Some(gates) = required.get(key) else {
            return Err(ObligationState::blocked("it isn't a required key"));
        };
        match covering.get(key).map(Vec::as_slice) {
            None | Some([]) => Err(ObligationState::blocked("no expected result covers it")),
            Some([result]) if !key.covers(&result.expected, &manifest.narrowing) => Err(
                ObligationState::blocked("its expected result doesn't name it (Key::covers)"),
            ),
            Some([result]) => {
                let tags: BTreeSet<Gate> = result.gates.iter().copied().collect();
                if &tags != gates {
                    Err(ObligationState::blocked(format!(
                        "its gate tags {tags:?} differ from its key's {gates:?}"
                    )))
                } else {
                    Ok(result)
                }
            }
            Some(_) => Err(ObligationState::blocked("two expected results cover it")),
        }
    };
    let mut keys: BTreeMap<Key, ObligationState> = BTreeMap::new();
    // C5's own proving tests come first and depend on nothing, but each
    // must come from a binary embedding the subject's source digest.
    let decide = |key: &Key, c5: Option<&Standing>, bootstrap: bool| -> ObligationState {
        match covered(key) {
            Err(state) => state,
            Ok(result) => evidence.deciding_state(result, c5, bootstrap),
        }
    };
    let proving_keys = |id: ContractId| -> Vec<Key> {
        manifest
            .contracts
            .iter()
            .filter(|contract| contract.id == id)
            .flat_map(|contract| {
                let tests = contract
                    .proving
                    .iter()
                    .map(move |&t| contract_test_key(id, t));
                let catches = contract
                    .proving_catches
                    .iter()
                    .map(move |&case| contract_catch_key(id, case));
                tests.chain(catches)
            })
            .collect()
    };
    for key in proving_keys(ContractId::C5) {
        let state = decide(&key, None, true);
        keys.insert(key, state);
    }
    let c5 = evidence.contract_standing(ContractId::C5, &keys, None);
    for &id in &manifest.required_contracts {
        if id != ContractId::C5 {
            for key in proving_keys(id) {
                let state = decide(&key, Some(&c5), false);
                keys.insert(key, state);
            }
        }
    }
    let mut standing = BTreeMap::new();
    for &id in &manifest.required_contracts {
        let gate = (id != ContractId::C5).then_some(&c5);
        standing.insert(id, evidence.contract_standing(id, &keys, gate));
    }
    for contract in &manifest.contracts {
        if !manifest.required_contracts.contains(&contract.id) {
            stray.push(format!("{:?} has a row but isn't required", contract.id));
        }
    }
    // A narrowing entry releases results only once its own key verified.
    for entry in &manifest.narrowing {
        let key = Key::NarrowingTest {
            entry: entry.entry.to_owned(),
        };
        let state = decide(&key, Some(&c5), false);
        keys.insert(key, state);
    }
    let names: Vec<&str> = manifest.narrowing.iter().map(|entry| entry.entry).collect();
    let narrowing: Vec<&Narrowing> = manifest
        .narrowing
        .iter()
        .filter(|entry| {
            // Two entries of one name share one key, so neither releases.
            names.iter().filter(|name| **name == entry.entry).count() == 1
                && matches!(entry.test, NarrowingTest::Test(_))
                && keys.get(&Key::NarrowingTest {
                    entry: entry.entry.to_owned(),
                }) == Some(&ObligationState::Verified)
        })
        .collect();
    for key in required.keys() {
        if keys.contains_key(key) {
            continue;
        }
        let state = match covered(key) {
            Err(state) => state,
            // A proving or narrowing test key no contract or entry names
            // depends on C5 alone.
            Ok(result) if decides_a_contract(key) => {
                evidence.deciding_state(result, Some(&c5), false)
            }
            // A composition check is a static test of the manifest and the
            // evaluator, which no contract exclusion can stand in for. So it
            // carries its own provenance anchor, as C5's own tests do: it
            // passed at S from a binary embedding the subject's source
            // digest, and then it's verified whatever C5's standing.
            Ok(result) if matches!(key, Key::Composition { .. }) => {
                evidence.deciding_state(result, None, true)
            }
            Ok(result) => evidence.key_state(result, &standing, &narrowing),
        };
        keys.insert(key.clone(), state);
    }
    keys.retain(|key, _| required.contains_key(key));

    let gates = GATES
        .iter()
        .map(|&gate| {
            // Every gate also needs the static checks and the evaluator's
            // own controls (Rule 16).
            // A stray result fails every gate, since it can't be attributed
            // to one.
            // So are the contracts' and narrowing entries' own tests,
            // which Rule 21 requires at S whether or not a gate's result
            // depends on them.
            let passes = stray.is_empty()
                && required.iter().all(|(key, gates)| match key {
                    Key::Composition { .. } => keys[key] == ObligationState::Verified,
                    Key::ContractTest { .. }
                    | Key::ContractCatch { .. }
                    | Key::NarrowingTest { .. } => {
                        !matches!(keys[key], ObligationState::Blocked { .. })
                    }
                    _ if gates.contains(&gate) => {
                        !matches!(keys[key], ObligationState::Blocked { .. })
                    }
                    _ => true,
                });
            (gate, passes)
        })
        .collect();
    Evaluation { keys, gates, stray }
}

/// The manifest and the result file, indexed for lookups.
struct Evidence<'a> {
    manifest: &'a Manifest,
    by_id: BTreeMap<&'a ExpectedId, Vec<&'a ResultRecord>>,
    /// Every trace digest each execution was counted from, across every
    /// result that counts it.
    traces: BTreeMap<RunId, BTreeSet<String>>,
    subject: &'a str,
    subject_source: &'a str,
}

/// Whether an exclusion with `reason` and `approval` counts: it says why,
/// and Adam's approval names where he gave it.
fn recorded(reason: &str, approval: Approval) -> bool {
    !reason.is_empty() && matches!(approval, Approval::Adam { record } if !record.is_empty())
}

/// Whether `key` decides a contract's or a narrowing entry's state.
fn decides_a_contract(key: &Key) -> bool {
    matches!(
        key,
        Key::ContractTest { .. } | Key::ContractCatch { .. } | Key::NarrowingTest { .. }
    )
}

impl<'a> Evidence<'a> {
    /// The one result named `id` at the subject commit, or why there isn't
    /// one.
    fn one_result(&self, id: &ExpectedId) -> Result<&'a ResultRecord, String> {
        match self.by_id.get(id).map(Vec::as_slice) {
            None | Some([]) => Err("its result is missing".into()),
            Some([record]) if record.commit != self.subject => Err(format!(
                "its result is from {}, not the subject {}",
                record.commit, self.subject
            )),
            // A test whose binary embeds another source, or that ran in a
            // checkout of another source, whose files it reads, or whose
            // digest couldn't be read, isn't shown to have run S, whatever C5
            // says of provenance in general.
            Some(
                [ResultRecord {
                    actual:
                        Actual::Test {
                            binary_source,
                            checkout_source,
                            ..
                        },
                    ..
                }],
            ) if binary_source.as_deref() != Some(self.subject_source)
                || checkout_source.as_deref() != Some(self.subject_source) =>
            {
                Err(format!(
                    "its binary embeds {binary_source:?} and it ran in a checkout of \
                     {checkout_source:?}, not the subject's source {}",
                    self.subject_source
                ))
            }
            Some([record]) => Ok(record),
            Some(_) => Err("its result is duplicated".into()),
        }
    }

    /// Whether `test` passed at the subject commit, and, when `bootstrap`,
    /// ran from a binary embedding the subject's source digest, which must
    /// be known. [`Evidence::one_result`] already requires that digest of
    /// its binary and its checkout alike.
    fn test_passed(&self, test: TestRef, bootstrap: bool) -> Result<(), String> {
        let id = Expected::Passes { test }
            .id()
            .expect("a Passes has an identity");
        let record = self.one_result(&id)?;
        judge_test(&record.actual, test)?;
        if bootstrap {
            let embedded = match &record.actual {
                Actual::Test { binary_source, .. } => binary_source.as_deref(),
                _ => None,
            };
            let known = !self.subject_source.is_empty() && self.subject_source != "unknown";
            if !known || embedded != Some(self.subject_source) {
                return Err(format!(
                    "its binary embeds {embedded:?}, not the subject's source digest"
                ));
            }
        }
        Ok(())
    }

    /// Contract `id`'s standing: its row's state, with a `Satisfied` one
    /// counting only once every proving test's own key verified, which for
    /// a contract other than C5 already needed C5. An excluded C5 excludes
    /// the contracts its proving tests decide.
    fn contract_standing(
        &self,
        id: ContractId,
        keys: &BTreeMap<Key, ObligationState>,
        c5: Option<&Standing>,
    ) -> Standing {
        let rows: Vec<_> = self
            .manifest
            .contracts
            .iter()
            .filter(|contract| contract.id == id)
            .collect();
        let contract = match rows.as_slice() {
            [] => return Standing::Blocked(format!("{id:?} has no row in the contracts table")),
            [contract] => contract,
            _ => return Standing::Blocked(format!("{id:?} has two rows")),
        };
        match contract.state {
            ContractState::Blocked { waiting_for } => {
                Standing::Blocked(format!("{id:?} waits for {waiting_for}"))
            }
            ContractState::Excluded { reason, approval } if recorded(reason, approval) => {
                Standing::Excluded(format!("{id:?} is excluded: {reason}"))
            }
            ContractState::Excluded { .. } => Standing::Blocked(format!(
                "{id:?}'s exclusion isn't approved with a reason and a record"
            )),
            ContractState::Satisfied => {
                if contract.proving.is_empty() {
                    return Standing::Blocked(format!("{id:?} is satisfied with no proving test"));
                }
                let mut excluded = None;
                let tests = contract
                    .proving
                    .iter()
                    .map(|&test| (contract_test_key(id, test), format!("test {}", test.test)));
                let catches = contract.proving_catches.iter().map(|&case| {
                    (
                        contract_catch_key(id, case),
                        format!("catch {}/{}", case.property, case.name),
                    )
                });
                for (key, what) in tests.chain(catches) {
                    match keys.get(&key) {
                        Some(ObligationState::Verified) => {}
                        Some(ObligationState::Excluded { why }) => {
                            excluded = excluded.or(Some(why.clone()));
                        }
                        Some(ObligationState::Blocked { why }) => {
                            return Standing::Blocked(format!("{id:?}'s proving {what}: {why}"))
                        }
                        None => {
                            return Standing::Blocked(format!(
                                "{id:?}'s proving {what} has no state"
                            ))
                        }
                    }
                }
                match (excluded, c5) {
                    (Some(why), _) => Standing::Excluded(why),
                    (None, Some(Standing::Excluded(why))) => Standing::Excluded(why.clone()),
                    _ => Standing::Satisfied,
                }
            }
        }
    }

    /// The state of a key that decides a contract or a narrowing entry:
    /// its test passed at the subject commit, or its proving catch's result
    /// at the subject commit passed its own judgment, with C5's standing
    /// applied when `c5` is given, and for `bootstrap`, from a binary
    /// embedding the subject's source digest. A catch never bootstraps C5.
    fn deciding_state(
        &self,
        result: &ExpectedResult,
        c5: Option<&Standing>,
        bootstrap: bool,
    ) -> ObligationState {
        let own = match result.expected {
            Expected::Passes { test } => self.test_passed(test, bootstrap),
            // A proving catch is its case's own judgment, which reads no
            // contract.
            Expected::Caught { .. } | Expected::HarnessCaught { .. } if !bootstrap => {
                let id = result.expected.id().expect("a catch has an identity");
                self.one_result(&id).and_then(|record| {
                    judge(
                        self.manifest,
                        &self.traces,
                        &result.expected,
                        &record.actual,
                    )
                })
            }
            Expected::Open { until, .. } => Err(match until {
                Some(until) => format!("transitional until {until}"),
                None => "an Open decides nothing".to_owned(),
            }),
            _ => Err("a contract or entry is decided only by a test or a catch".to_owned()),
        };
        // A blocked C5 blocks. A test that's missing or failed blocks too,
        // even under an excluded C5, so only a passing one is excluded by it.
        match (c5, own) {
            (Some(Standing::Blocked(why)), _) => ObligationState::blocked(format!("C5: {why}")),
            (_, Err(why)) => ObligationState::blocked(why),
            (Some(Standing::Excluded(why)), Ok(())) => {
                ObligationState::Excluded { why: why.clone() }
            }
            (_, Ok(())) => ObligationState::Verified,
        }
    }

    /// `key`'s state, from `result`'s own outcome and the contracts that
    /// apply to `key`.
    fn key_state(
        &self,
        result: &ExpectedResult,
        standing: &BTreeMap<ContractId, Standing>,
        narrowing: &[&Narrowing],
    ) -> ObligationState {
        let own = match result.expected {
            Expected::Open {
                approval,
                until,
                owner,
                reason,
            } => match until {
                Some(until) => Err(format!("transitional until {until}")),
                None if owner.is_empty() => Err("a standing Open that names no owner".into()),
                None if recorded(reason, approval) => {
                    Ok(Some("an approved standing Open".to_owned()))
                }
                None => Err("a standing Open without approval".into()),
            },
            _ => {
                let id = result.expected.id().expect("only an Open has no identity");
                self.one_result(&id)
                    .and_then(|record| {
                        judge(
                            self.manifest,
                            &self.traces,
                            &result.expected,
                            &record.actual,
                        )
                    })
                    .map(|()| None)
            }
        };
        // A key sharing a contract test's result still gets every contract
        // here; only the keys that decide a contract skip them.
        // Whether a contract applies is a fact of the expected result's
        // identity, which a verified narrowing entry decides, not of what
        // its record shows: a missing or failed result is still judged
        // under exactly the contracts that apply to it.
        let ctx = self.context(result);
        let applicable: Vec<(ContractId, &Standing)> = self
            .manifest
            .required_contracts
            .iter()
            .filter(|&&id| {
                !narrowing
                    .iter()
                    .any(|entry| entry.contracts.contains(&id) && (entry.releases)(&ctx))
            })
            .map(|id| (*id, &standing[id]))
            .collect();
        let mut excluded = None;
        for (id, contract) in applicable {
            match contract {
                Standing::Blocked(why) => {
                    return ObligationState::blocked(format!("{id:?}: {why}"))
                }
                Standing::Excluded(why) => excluded = excluded.or(Some(why.clone())),
                Standing::Satisfied => {}
            }
        }
        match own {
            Ok(Some(why)) => ObligationState::Excluded { why },
            Ok(None) => match excluded {
                Some(why) => ObligationState::Excluded { why },
                None => ObligationState::Verified,
            },
            // A transitional or unapproved Open always blocks. Any other
            // result an approved excluded contract applies to is excluded
            // whatever its own result, since that contract usually wasn't
            // delivered.
            Err(why) => match (&result.expected, excluded) {
                (Expected::Open { .. }, _) | (_, None) => ObligationState::blocked(why),
                (_, Some(why)) => ObligationState::Excluded { why },
            },
        }
    }

    /// What the narrowing entries decide on for `result`, all from the
    /// manifest's expected execution and never from what a result says
    /// about itself: its kind, its harness and machine flag, and each
    /// execution's role, whether its `Draft` override is empty and who runs
    /// it ([`roles`]).
    fn context(&self, result: &ExpectedResult) -> ResultCtx {
        result_context(self.manifest, &result.expected)
    }
}

/// What the narrowing entries decide on for `expected` in `manifest`, as
/// the evaluator gives it: its kind, its harness and machine flag, and each
/// execution's role ([`roles`]), all from the manifest.
pub fn result_context(manifest: &Manifest, expected: &Expected) -> ResultCtx {
    let harness = expected_harness(manifest, expected);
    ResultCtx {
        kind: expected.kind(),
        machine: harness
            .as_deref()
            .is_some_and(|harness| manifest.machines.contains(&harness)),
        harness,
        executions: roles(expected),
    }
}

/// Who runs execution `run`, by its role: a planted case's baseline and
/// patched runs, and their replays, are the planted runner's, and every
/// other execution the acceptance tool's own.
fn runner(run: &RunId) -> RunBy {
    match run {
        RunId::Case {
            run: CaseRun::Baseline | CaseRun::Patched,
            property,
            ..
        } if property != super::NATIVE_DIR => RunBy::Planted,
        _ => RunBy::Tool,
    }
}

/// Each execution `expected` counts, as whether its role's `Draft` override
/// is empty and who runs it.
fn roles(expected: &Expected) -> Vec<(bool, RunBy)> {
    let runs = match expected {
        Expected::Replays { run } => vec![run.clone()],
        _ => owned_runs(expected),
    };
    runs.into_iter()
        .map(|run| {
            let draft = matches!(
                run,
                RunId::Case {
                    run: CaseRun::Draft,
                    ..
                }
            );
            (!draft, runner(&run))
        })
        .collect()
}

/// The executions an `expected` result's record carries, in the order
/// [`executions`] reads them. A replay counts its run without carrying it.
fn owned_runs(expected: &Expected) -> Vec<RunId> {
    match *expected {
        Expected::Holds { history, .. } | Expected::StaysNotApplicable { history, .. } => {
            vec![history.into()]
        }
        Expected::Caught { case, .. } => [CaseRun::Baseline, CaseRun::Patched, CaseRun::Draft]
            .map(|run| RunId::case(case, run))
            .to_vec(),
        Expected::MachineCaught { case, .. } | Expected::HarnessCaught { case, .. } => {
            [CaseRun::Baseline, CaseRun::Patched]
                .map(|run| RunId::case(case, run))
                .to_vec()
        }
        Expected::IntegrityCaught { patch, .. } => {
            [CaseRun::Unpatched, CaseRun::Patched, CaseRun::Restored]
                .map(|run| RunId::native(patch, run))
                .to_vec()
        }
        Expected::Replays { .. } | Expected::Passes { .. } | Expected::Open { .. } => Vec::new(),
    }
}

/// Every execution `actual` carries.
fn executions(actual: &Actual) -> Vec<&Execution> {
    match actual {
        Actual::Run(run) => vec![run],
        Actual::Caught {
            baseline,
            patched,
            draft,
            ..
        } => vec![baseline, patched, draft],
        Actual::MachineCaught { baseline, patched }
        | Actual::HarnessCaught { baseline, patched } => vec![baseline, patched],
        Actual::IntegrityCaught {
            unpatched,
            patched,
            restored,
            ..
        } => vec![unpatched, patched, restored],
        Actual::Replay { .. } | Actual::Test { .. } => Vec::new(),
    }
}

/// The harness `expected`'s executions run on, from the manifest: its
/// history's or case's, or for a replay, the harness of the execution it
/// replays. `None` for a test or an `Open`.
fn expected_harness(manifest: &Manifest, expected: &Expected) -> Option<String> {
    match expected {
        Expected::Holds { history, .. }
        | Expected::StaysNotApplicable { history, .. }
        | Expected::IntegrityCaught { history, .. } => Some(history.harness.to_owned()),
        Expected::Caught { harness, .. }
        | Expected::MachineCaught { harness, .. }
        | Expected::HarnessCaught { harness, .. } => Some((*harness).to_owned()),
        Expected::Replays { run } => replayed_harness(manifest, run),
        Expected::Passes { .. } | Expected::Open { .. } => None,
    }
}

/// The harness of execution `run`: a history's own, or the harness of the
/// case whose run it is.
fn replayed_harness(manifest: &Manifest, run: &RunId) -> Option<String> {
    match run {
        RunId::History { harness, .. } => Some(harness.clone()),
        RunId::Case { property, name, .. } => {
            manifest
                .expected
                .iter()
                .find_map(|result| match result.expected {
                    Expected::Caught { case, harness, .. }
                    | Expected::MachineCaught { case, harness }
                    | Expected::HarnessCaught { case, harness, .. }
                        if case.property == property && case.name == name =>
                    {
                        Some(harness.to_owned())
                    }
                    _ => None,
                })
        }
    }
}

/// The `Draft` override `run` should carry: a case's set for its `Draft`
/// run, and nothing for every other execution.
fn expected_override(manifest: &Manifest, run: &RunId) -> BTreeSet<String> {
    match run {
        RunId::Case {
            property,
            name,
            run: CaseRun::Draft,
        } => manifest
            .expected
            .iter()
            .find_map(|result| match result.expected {
                Expected::Caught { case, set, .. }
                    if case.property == property && case.name == name =>
                {
                    Some(set.iter().map(|id| (*id).to_owned()).collect())
                }
                _ => None,
            })
            .unwrap_or_default(),
        _ => BTreeSet::new(),
    }
}

/// Checks `meta` carries exactly the override `run` should.
fn override_is(manifest: &Manifest, run: &RunId, meta: &ExecMeta) -> Result<(), String> {
    let carried: BTreeSet<String> = meta.draft_override.iter().cloned().collect();
    let expected = expected_override(manifest, run);
    if carried != expected {
        return Err(format!(
            "{run:?} carries the Draft override {carried:?}, not {expected:?}"
        ));
    }
    Ok(())
}

/// Judges `actual` against `expected`.
fn judge(
    manifest: &Manifest,
    traces: &BTreeMap<RunId, BTreeSet<String>>,
    expected: &Expected,
    actual: &Actual,
) -> Result<(), String> {
    match (expected.clone(), actual) {
        (
            Expected::Holds {
                property,
                history,
                not_applicable,
            },
            Actual::Run(run),
        ) => {
            whole_report(
                manifest,
                run,
                &RunId::from(history),
                history.harness,
                property,
                not_applicable,
            )?;
            one_trace(traces, &run.run)?;
            target_held(run, property)
        }
        (
            Expected::StaysNotApplicable {
                property,
                history,
                reason,
                approval,
                not_applicable,
            },
            Actual::Run(run),
        ) => {
            if !recorded(reason, approval) {
                return Err(
                    "its NotApplicable reason isn't approved with a reason and a record".into(),
                );
            }
            whole_report(
                manifest,
                run,
                &RunId::from(history),
                history.harness,
                property,
                not_applicable,
            )?;
            one_trace(traces, &run.run)?;
            if !run.trigger {
                return Err("its history lost its trigger history".into());
            }
            match run.reports.get(property) {
                Some(Report::NotApplicable { reason: given }) if given == reason => Ok(()),
                other => Err(format!(
                    "{property} is {other:?}, not NotApplicable({reason})"
                )),
            }
        }
        (
            Expected::Caught {
                case,
                harness,
                set,
                found,
                companions,
                not_applicable,
                patched_not_applicable,
            },
            Actual::Caught {
                baseline,
                patched,
                draft,
                companions: shown,
            },
        ) => {
            if manifest.machines.contains(&harness) {
                return Err(format!(
                    "{harness} is a machine, so its case is a MachineCaught"
                ));
            }
            whole_report(
                manifest,
                baseline,
                &RunId::case(case, CaseRun::Baseline),
                harness,
                case.property,
                not_applicable,
            )?;
            target_held(baseline, case.property)?;
            // The patched build's completed runs go on past the break.
            let past_break = patched_not_applicable.unwrap_or(not_applicable);
            caught_patched(manifest, patched, case, harness, set, found, past_break)?;
            caught_draft(manifest, draft, case, harness, set, past_break)?;
            companions_shown(companions, shown, draft)
        }
        (
            Expected::MachineCaught { case, harness },
            Actual::MachineCaught { baseline, patched },
        ) => {
            if !manifest.machines.contains(&harness) {
                return Err(format!(
                    "{harness} isn't a machine, so its case is a Caught"
                ));
            }
            same_run(
                manifest,
                baseline,
                &RunId::case(case, CaseRun::Baseline),
                harness,
            )?;
            same_run(
                manifest,
                patched,
                &RunId::case(case, CaseRun::Patched),
                harness,
            )?;
            inventory(manifest, baseline, harness)?;
            inventory(manifest, patched, harness)?;
            completed(baseline).map_err(|why| format!("its baseline: {why}"))?;
            if patched.leaked != 0 {
                return Err(format!("its patched run leaked {} tasks", patched.leaked));
            }
            target_held(baseline, case.property)?;
            if violated(baseline).next().is_some() {
                return Err("its baseline violates a property".into());
            }
            // A stopping violation checked a prefix of the declared
            // actions, and a completed run all of them.
            match (&patched.end, patched.actions) {
                (End::Violation { property }, Actions::Prefix | Actions::Complete)
                    if property == case.property => {}
                (End::Completed, Actions::Complete) => {}
                (end, actions) => {
                    return Err(format!(
                        "its patched run ended {end:?} with actions {actions:?}"
                    ))
                }
            }
            if !patched.trigger {
                return Err("its patched run lost the trigger history".into());
            }
            exactly_violated(patched, &[case.property])
        }
        (
            Expected::HarnessCaught {
                case,
                harness,
                detail,
                not_applicable,
            },
            Actual::HarnessCaught { baseline, patched },
        ) => {
            if manifest.machines.contains(&harness) {
                return Err(format!("{harness} is a machine, which runs no drain"));
            }
            whole_report(
                manifest,
                baseline,
                &RunId::case(case, CaseRun::Baseline),
                harness,
                case.property,
                not_applicable,
            )?;
            target_held(baseline, case.property)?;
            same_run(
                manifest,
                patched,
                &RunId::case(case, CaseRun::Patched),
                harness,
            )?;
            inventory(manifest, patched, harness)?;
            if patched.leaked != 0 {
                return Err(format!("its patched run leaked {} tasks", patched.leaked));
            }
            missing_evidence(&patched.end, case.property, detail)?;
            match violated(patched).next() {
                Some(property) => Err(format!("its patched run violated {property}")),
                None => Ok(()),
            }
        }
        (
            Expected::IntegrityCaught {
                patch,
                history,
                diagnostic,
                not_applicable,
            },
            Actual::IntegrityCaught {
                unpatched,
                patched,
                restored,
            },
        ) => {
            let history_run = RunId::from(history);
            for (run, which) in [
                (unpatched, CaseRun::Unpatched),
                (patched, CaseRun::Patched),
                (restored, CaseRun::Restored),
            ] {
                if run.meta.input.as_ref() != Some(&history_run) {
                    return Err(format!(
                        "its {which:?} run ran {:?}, not {history_run:?}",
                        run.meta.input
                    ));
                }
            }
            for (run, which) in [
                (unpatched, CaseRun::Unpatched),
                (restored, CaseRun::Restored),
            ] {
                same_run(manifest, run, &RunId::native(patch, which), history.harness)?;
                completed(run)?;
                inventory(manifest, run, history.harness)?;
                if !run.trigger {
                    return Err(format!("its {which:?} history lost its trigger history"));
                }
                others_pass(run, &[], not_applicable)
                    .map_err(|why| format!("its {which:?} history: {why}"))?;
            }
            same_run(
                manifest,
                patched,
                &RunId::native(patch, CaseRun::Patched),
                history.harness,
            )?;
            match &patched.end {
                End::HarnessFailure { diagnostic: given } if given == diagnostic => Ok(()),
                other => Err(format!(
                    "its patched history ended {other:?}, not {diagnostic}"
                )),
            }
        }
        (
            Expected::Replays { run },
            Actual::Replay {
                run: replayed,
                of,
                meta,
                exact,
                reports_equal,
                stores_equal,
                fresh,
            },
        ) => {
            if *replayed != run {
                return Err(format!("it replayed {replayed:?}, not {run:?}"));
            }
            if !fresh {
                return Err("it replayed in the recording process".into());
            }
            if meta.input.is_some() {
                return Err("a replay names no input apart from its RunId".into());
            }
            runner_is(&run, meta)?;
            match traces.get(&run) {
                Some(counted) if counted.len() == 1 && counted.contains(of) && usable(of) => {}
                Some(counted) => {
                    return Err(format!(
                        "it replayed trace {of}, and {run:?} was counted from {counted:?}"
                    ))
                }
                None => return Err(format!("no counted execution {run:?} to replay")),
            }
            let harness = replayed_harness(manifest, &run)
                .ok_or_else(|| format!("no expected execution {run:?}"))?;
            if meta.harness != harness {
                return Err(format!("its replay ran on {}, not {harness}", meta.harness));
            }
            override_is(manifest, &run, meta)?;
            match (exact, reports_equal, stores_equal) {
                (true, true, true) => Ok(()),
                _ => Err(format!(
                    "exact {exact}, reports equal {reports_equal}, stores equal {stores_equal}"
                )),
            }
        }
        (Expected::Passes { test }, actual) => judge_test(actual, test),
        (expected, actual) => Err(format!(
            "a {:?} result can't satisfy a {:?}",
            kind_of(actual),
            expected.kind()
        )),
    }
}

/// Checks every result that counts execution `run` counted the same trace,
/// and that it's a usable identity.
fn one_trace(traces: &BTreeMap<RunId, BTreeSet<String>>, run: &RunId) -> Result<(), String> {
    match traces.get(run) {
        Some(counted) if counted.len() == 1 && counted.iter().all(|t| usable(t)) => Ok(()),
        counted => Err(format!("{run:?} was counted from traces {counted:?}")),
    }
}

/// Whether `trace` is a digest that can name an execution: not empty and
/// not the tool's marker for an unreadable artifact.
fn usable(trace: &str) -> bool {
    !trace.is_empty() && trace != "missing"
}

/// Checks the companions `shown` are exactly the `declared` names, each
/// shown once, held, and judged on `draft`, the catch's own `Draft` run.
fn companions_shown(
    declared: &[&str],
    shown: &[Companion],
    draft: &Execution,
) -> Result<(), String> {
    let names: Vec<&str> = shown.iter().map(|c| c.name.as_str()).collect();
    let unique: BTreeSet<&str> = names.iter().copied().collect();
    let declared: BTreeSet<&str> = declared.iter().copied().collect();
    if unique.len() != names.len() || unique != declared {
        return Err(format!(
            "its companions {names:?} aren't the declared {declared:?}"
        ));
    }
    for companion in shown {
        if companion.run != draft.run || companion.trace != draft.trace || !usable(&companion.trace)
        {
            return Err(format!(
                "companion {} was judged on {:?} trace {}, not its Draft run's",
                companion.name, companion.run, companion.trace
            ));
        }
        if !companion.held {
            return Err(format!("companion {} didn't hold", companion.name));
        }
    }
    Ok(())
}

/// A test result's verdict: the filter executed exactly `test`, by its own
/// package, file and name, and it passed.
fn judge_test(actual: &Actual, test: TestRef) -> Result<(), String> {
    let Actual::Test { ran, .. } = actual else {
        return Err(format!("a {:?} result isn't a test's", kind_of(actual)));
    };
    match ran.as_slice() {
        [one] if one.package == test.package && one.file == test.file && one.test == test.test => {
            match one.outcome {
                TestOutcome::Passed => Ok(()),
                outcome => Err(format!("{} ended {outcome:?}", test.test)),
            }
        }
        [one] => Err(format!(
            "the filter ran {}/{}::{}, not {}/{}::{}",
            one.package, one.file, one.test, test.package, test.file, test.test
        )),
        ran => Err(format!("the filter ran {} tests, not one", ran.len())),
    }
}

/// The name of `actual`'s variant.
fn kind_of(actual: &Actual) -> &'static str {
    match actual {
        Actual::Run(_) => "run",
        Actual::Caught { .. } => "caught",
        Actual::MachineCaught { .. } => "machine-caught",
        Actual::HarnessCaught { .. } => "harness-caught",
        Actual::IntegrityCaught { .. } => "integrity-caught",
        Actual::Replay { .. } => "replay",
        Actual::Test { .. } => "test",
    }
}

/// Checks `run` is the execution `id` on `harness`, carrying the `Draft`
/// override `id` should.
fn same_run(manifest: &Manifest, run: &Execution, id: &RunId, harness: &str) -> Result<(), String> {
    if &run.run != id {
        return Err(format!("its execution is {:?}, not {id:?}", run.run));
    }
    let native = matches!(id, RunId::Case { property, .. } if property == super::NATIVE_DIR);
    if !native && run.meta.input.is_some() {
        return Err("only a native mutation's run names its input apart from its RunId".into());
    }
    if run.meta.harness != harness {
        return Err(format!("it ran on {}, not {harness}", run.meta.harness));
    }
    runner_is(id, &run.meta)?;
    override_is(manifest, id, &run.meta)
}

/// Checks `meta` was run by whoever `run`'s role says runs it.
fn runner_is(run: &RunId, meta: &ExecMeta) -> Result<(), String> {
    let expected = runner(run);
    if meta.run_by != expected {
        return Err(format!(
            "{run:?} was run by {:?}, not {expected:?}",
            meta.run_by
        ));
    }
    Ok(())
}

/// Checks `run` reports exactly the properties the manifest's matrix says
/// `harness` registers.
fn inventory(manifest: &Manifest, run: &Execution, harness: &str) -> Result<(), String> {
    let registered: BTreeSet<&str> = manifest
        .matrix
        .iter()
        .filter(|(on, _, _)| *on == harness)
        .map(|(_, id, _)| *id)
        .collect();
    let reported: BTreeSet<&str> = run.reports.keys().map(String::as_str).collect();
    if registered.is_empty() || registered != reported {
        return Err(format!(
            "it reports {reported:?}, not {harness}'s registered {registered:?}"
        ));
    }
    // The registry reports Held only once a check applied, so a Held with
    // no applicable step is an export error, never a pass.
    if let Some((id, _)) = run
        .reports
        .iter()
        .find(|(_, report)| matches!(report, Report::Held { applicable: 0 }))
    {
        return Err(format!("{id} is Held with no applicable step"));
    }
    Ok(())
}

/// The properties `run` violated.
fn violated(run: &Execution) -> impl Iterator<Item = &str> {
    run.reports
        .iter()
        .filter(|(_, report)| **report == Report::Violated)
        .map(|(id, _)| id.as_str())
}

/// Checks `run` violated exactly `set`.
fn exactly_violated(run: &Execution, set: &[&str]) -> Result<(), String> {
    let found: BTreeSet<&str> = violated(run).collect();
    let set: BTreeSet<&str> = set.iter().copied().collect();
    if found != set {
        return Err(format!("it violated {found:?}, not exactly {set:?}"));
    }
    Ok(())
}

/// Checks `target` held with nonzero applicability and its trigger history.
fn target_held(run: &Execution, target: &str) -> Result<(), String> {
    match run.reports.get(target) {
        Some(Report::Held { applicable }) if *applicable > 0 && run.trigger => Ok(()),
        Some(Report::Held { applicable }) if *applicable > 0 => {
            Err(format!("{target} held without its trigger history"))
        }
        other => Err(format!(
            "{target} is {other:?}, not Held with applicability"
        )),
    }
}

/// The whole-report rule for a completed counted run: the execution is the
/// expected one, it completed with every declared action, leaked no task,
/// reports exactly its harness's properties, and every one other than
/// `target` held, or stayed `NotApplicable` with the benign reason the row
/// lists for it ([`others_pass`]). The caller judges `target`.
fn whole_report(
    manifest: &Manifest,
    run: &Execution,
    id: &RunId,
    harness: &str,
    target: &str,
    not_applicable: &[ExpectedNa],
) -> Result<(), String> {
    same_run(manifest, run, id, harness)?;
    completed(run)?;
    inventory(manifest, run, harness)?;
    others_pass(run, &[target], not_applicable)
}

/// Checks every property of `run`, a completed counted run, outside `skip`
/// held, or stayed `NotApplicable` with the reason `not_applicable` lists
/// for it, which must be benign ([`benign`]). A listed property that held,
/// an unlisted one that didn't apply, a wrong reason and a violation each
/// fail. So does an incomplete reason, such as `unsettled:watch-open`,
/// whatever a list says: it's evidence the run left open, not a property
/// with nothing to check, so no list can accept it (C6).
fn others_pass(
    run: &Execution,
    skip: &[&str],
    not_applicable: &[ExpectedNa],
) -> Result<(), String> {
    for na in not_applicable {
        if !skip.contains(&na.property) && !run.reports.contains_key(na.property) {
            return Err(format!(
                "its row expects {} NotApplicable, which it doesn't report",
                na.property
            ));
        }
    }
    for (property, report) in &run.reports {
        if skip.contains(&property.as_str()) {
            continue;
        }
        let listed = not_applicable
            .iter()
            .find(|na| na.property == property.as_str());
        match (report, listed) {
            (Report::Held { .. }, None) => {}
            (Report::NotApplicable { reason }, _) if !benign(reason) => {
                return Err(format!(
                    "{property} is NotApplicable({reason}), whose evidence is incomplete"
                ))
            }
            (Report::NotApplicable { reason: given }, Some(na)) if given == na.reason => {}
            (report, listed) => {
                return Err(format!(
                    "{property} is {report:?}, where its row expects {}",
                    listed.map_or("Held".to_owned(), |na| format!(
                        "NotApplicable({})",
                        na.reason
                    ))
                ))
            }
        }
    }
    Ok(())
}

/// Whether `reason`, a `NotApplicable` report's reason by name, says there
/// was nothing to judge ([`NaReason::is_benign`](crate::NaReason::is_benign))
/// rather than that the run's observations were incomplete. A name no
/// reason a header ID can carry has isn't benign.
fn benign(reason: &str) -> bool {
    super::reasons::NA_REASONS
        .iter()
        .map(|entry| entry.reason)
        .chain(super::reasons::NOT_HEADER.iter().map(|&(reason, _)| reason))
        .any(|known| known.is_benign() && known.name() == reason)
}

/// Checks `run` completed: no stopping violation, no harness failure, every
/// declared action, and no leaked task.
fn completed(run: &Execution) -> Result<(), String> {
    if run.end != End::Completed {
        return Err(format!("it ended {:?}, not completed", run.end));
    }
    if run.actions != Actions::Complete {
        return Err(format!("its declared actions are {:?}", run.actions));
    }
    if run.leaked != 0 {
        return Err(format!("it leaked {} tasks", run.leaked));
    }
    Ok(())
}

/// A `Caught`'s patched run: exactly `set` violated where `found` says, or
/// the case's own ID alone for [`Found::AtStepSetOnDraft`], with its
/// trigger history, a complete inventory, no harness failure and no leaked
/// task. A run stopped at a step is judged as a prefix, so other
/// properties may still have work pending there.
fn caught_patched(
    manifest: &Manifest,
    run: &Execution,
    case: super::CaseRef,
    harness: &str,
    set: &[&str],
    found: Found,
    not_applicable: &[ExpectedNa],
) -> Result<(), String> {
    same_run(manifest, run, &RunId::case(case, CaseRun::Patched), harness)?;
    inventory(manifest, run, harness)?;
    if run.leaked != 0 {
        return Err(format!("its patched run leaked {} tasks", run.leaked));
    }
    if !run.trigger {
        return Err("its patched run lost the trigger history".into());
    }
    match (found, &run.end) {
        (Found::AtStep, End::Violation { property }) if set.contains(&property.as_str()) => {
            if run.actions == Actions::Incomplete {
                return Err("its patched run's actions don't match its declared prefix".into());
            }
        }
        // The set breaks only after the stop, so the patched run shows the
        // case's own ID alone, and its Draft run shows the set.
        (Found::AtStepSetOnDraft, End::Violation { property }) if *property == case.property => {
            if run.actions == Actions::Incomplete {
                return Err("its patched run's actions don't match its declared prefix".into());
            }
            return exactly_violated(run, &[case.property]);
        }
        // A completed run gets the whole-report rule outside its set.
        (Found::AtEnd, End::Completed) => {
            if run.actions != Actions::Complete {
                return Err(format!("its patched run's actions are {:?}", run.actions));
            }
            others_pass(run, set, not_applicable)?;
        }
        (found, end) => return Err(format!("found {found:?}, its patched run ended {end:?}")),
    }
    exactly_violated(run, set)
}

/// Checks `end` is a harness failure of `property`'s missing evidence,
/// `<property> lacks evidence after step <n>: <its detail>` as
/// `HarnessFailure::MissingEvidence` writes it, whose detail says `detail`.
fn missing_evidence(end: &End, property: &str, detail: &str) -> Result<(), String> {
    let End::HarnessFailure { diagnostic } = end else {
        return Err(format!(
            "its patched run ended {end:?}, not at a harness failure"
        ));
    };
    let shown = diagnostic
        .strip_prefix(property)
        .and_then(|rest| rest.strip_prefix(" lacks evidence after step "))
        .and_then(|rest| rest.split_once(": "))
        .filter(|(step, _)| !step.is_empty() && step.bytes().all(|b| b.is_ascii_digit()))
        .map(|(_, shown)| shown);
    match shown {
        Some(shown) if shown.contains(detail) => Ok(()),
        _ => Err(format!(
            "its patched run's harness failure {diagnostic:?} isn't {property}'s missing \
             evidence saying {detail:?}"
        )),
    }
}

/// A `Caught`'s `Draft` run: a completed counted run with the set as its
/// override, every member violated, and every other property held or
/// expected `NotApplicable`.
fn caught_draft(
    manifest: &Manifest,
    run: &Execution,
    case: super::CaseRef,
    harness: &str,
    set: &[&str],
    not_applicable: &[ExpectedNa],
) -> Result<(), String> {
    same_run(manifest, run, &RunId::case(case, CaseRun::Draft), harness)?;
    completed(run)?;
    inventory(manifest, run, harness)?;
    exactly_violated(run, set)?;
    others_pass(run, set, not_applicable)
}
