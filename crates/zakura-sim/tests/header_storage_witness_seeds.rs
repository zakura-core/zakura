//! fuzz-12 item 7: each action and overlap the fuzz-12 controls added, as a
//! committed generator-3 seed, `tests/fixtures/header-storage-w-*.bin`, so
//! the fuzzer can draw it. Each seed is a witness scenario's canonical
//! prefix and 1,024 Plan bytes, and its run shows the overlap it's named
//! for in the facts it logs, not only that the run ended.
#![cfg(zakura_sim)]

mod support;

use std::{fs, path::PathBuf};

use support::{byte_inputs, storage_witnesses};
use zakura_chain::work::difficulty::U256;
use zakura_sim::{
    check::PropertyOutcome,
    domain::header_sync_progress::BOUNDED_ADVERSARY_PROGRESS,
    explore::{classify, ExploreOutcome},
    harness::{
        self,
        header_node::policy::OpKind,
        header_sync::{grammar, Storage, StorageScenario},
        Subsystem,
    },
    observe::{StorageFact, StoragePoint, StorageRequest, StorageResult},
    seed, NullSink, Observation, RunReport,
};

/// The Plan bytes after each seed's prefix.
const PLAN_BYTES: usize = 1_024;

/// The witness a seed carries, by stem: a property its run holds, which a
/// planted case's patch on the same input violates.
const WITNESSES: [(&str, &str); 1] = [(
    "header-storage-w-stop-storage",
    "header-sync/teardown-returns-baseline",
)];

/// The overlap a witness seed's run shows.
#[derive(Clone, Copy, Debug)]
enum Shows {
    /// An operation of this kind was pending at a reset and completed after
    /// it.
    HeldAcrossReset(OpKind),
    /// An apply committed, then the node reset, then its reply was delivered.
    ResetBetweenCommitAndDelivery,
    /// An invalidation, then a reconsideration while a prepare was pending.
    ReconsiderMidRound,
    /// A read was pending at a release and completed after it.
    ReleasedRead,
    /// An apply committed, another operation completed, then its reply was
    /// delivered.
    ReplyOutlasted,
    /// An apply's reply was delivered after the step `action` named, to a
    /// waiter or not.
    DeliveredAfter {
        action: &'static str,
        delivered: bool,
    },
    /// A storage stop dropped a held apply.
    StopDroppedApply,
    /// An acquire was refused `CapacityBusy` for the peer class while an
    /// earlier read for the same peer still held its lease.
    PeerLeaseDenied,
    /// An operation of this kind completed with this outcome.
    Failed(OpKind, &'static str),
    /// Two prepares from the same ancestor with different first headers,
    /// the later one's work comparing so with the earlier's.
    ForkWork {
        /// The earlier prepared fork's header count.
        earlier: u32,
        /// The later prepared fork's header count.
        later: u32,
        /// How the later fork's work compares with the earlier's.
        work: std::cmp::Ordering,
    },
    /// The node reached recovery's conditions after a prefix of this class.
    Recovered(&'static str),
    /// [`Shows::Recovered`], and the recovery owed the node a successor,
    /// which it met: `header-sync/bounded-adversary-progress` applied once
    /// and held (hdr-13).
    Owed(&'static str),
    /// The first read, held until two other reads completed, completed
    /// through the engine with a page after them. That's `holds()`'s first
    /// hold succeeding, which the migrated seed's slice doesn't show: its
    /// held read ends `PageUnavailable`.
    SucceededAfterHold,
}

/// The storage facts `report` logged, with their steps.
fn facts(report: &RunReport) -> Vec<(u64, StorageFact)> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage { fact, .. } => Some((entry.step, fact.clone())),
            _ => None,
        })
        .collect()
}

/// `request`'s operation kind, as a hold's selection names it.
fn kind(request: &StorageRequest) -> Option<OpKind> {
    Some(match request {
        StorageRequest::ContinuationLocator => OpKind::Locator,
        StorageRequest::AcquireHeaderPath { .. } => OpKind::Acquire,
        StorageRequest::ReadHeaderPath { .. } => OpKind::Read,
        StorageRequest::ReleaseHeaderPath { .. } => OpKind::Release,
        StorageRequest::PrepareHeaderTarget { .. } => OpKind::Prepare,
        StorageRequest::ApplyHeaderTarget { .. } => OpKind::Apply,
        StorageRequest::SelectedChainContains { .. } => OpKind::Membership,
        _ => return None,
    })
}

/// Each operation's kind, submission step and completion step, if it
/// completed.
fn operations(facts: &[(u64, StorageFact)]) -> Vec<(u64, OpKind, u64, Option<u64>)> {
    let mut ops: Vec<(u64, OpKind, u64, Option<u64>)> = Vec::new();
    for (step, fact) in facts {
        match fact {
            StorageFact::Submitted { id, request, .. } => {
                if let Some(kind) = kind(request) {
                    ops.push((*id, kind, *step, None));
                }
            }
            StorageFact::Completed { id, .. } => {
                if let Some(op) = ops.iter_mut().find(|op| op.0 == *id && op.3.is_none()) {
                    op.3 = Some(*step);
                }
            }
            _ => {}
        }
    }
    ops
}

/// The steps that picked control `action`.
fn picked(report: &RunReport, action: &str) -> Vec<u64> {
    report
        .picks
        .iter()
        .filter(|pick| pick.identity.ends_with(&format!(":{action}")))
        .map(|pick| pick.step)
        .collect()
}

impl Shows {
    /// Whether `report`'s run shows it.
    fn in_run(self, report: &RunReport) -> bool {
        let facts = facts(report);
        let ops = operations(&facts);
        let resets: Vec<(u64, &str)> = facts
            .iter()
            .filter_map(|(step, fact)| match fact {
                StorageFact::Reset { kind, .. } => Some((*step, kind.as_str())),
                _ => None,
            })
            .collect();
        let committed: Vec<(u64, u64)> = facts
            .iter()
            .filter_map(|(step, fact)| match fact {
                StorageFact::Committed { id, .. } => Some((*step, *id)),
                _ => None,
            })
            .collect();
        let delivered = |id: u64| {
            facts.iter().find_map(|(step, fact)| match fact {
                StorageFact::Completed {
                    id: done,
                    delivered,
                    ..
                } if *done == id => Some((*step, *delivered)),
                _ => None,
            })
        };
        let pending_at = |kind: OpKind, at: u64| {
            ops.iter().any(|(_, k, submitted, done)| {
                *k == kind && *submitted < at && done.is_some_and(|done| done > at)
            })
        };
        match self {
            Shows::HeldAcrossReset(kind) => resets.iter().any(|(at, _)| pending_at(kind, *at)),
            Shows::ResetBetweenCommitAndDelivery => committed.iter().any(|(at, id)| {
                delivered(*id).is_some_and(|(done, _)| {
                    resets.iter().any(|(reset, _)| at < reset && *reset < done)
                })
            }),
            Shows::ReconsiderMidRound => {
                let invalidated = resets.iter().find(|(_, kind)| *kind == "invalidate");
                let reconsidered = resets.iter().find(|(_, kind)| *kind == "reconsider");
                matches!((invalidated, reconsidered), (Some(i), Some(r)) if i.0 < r.0)
                    && reconsidered.is_some_and(|(at, _)| pending_at(OpKind::Prepare, *at + 1))
            }
            Shows::ReleasedRead => picked(report, "release")
                .first()
                .is_some_and(|at| pending_at(OpKind::Read, *at)),
            Shows::ReplyOutlasted => committed.iter().any(|(at, id)| {
                delivered(*id).is_some_and(|(done, _)| {
                    ops.iter().any(|op| {
                        op.0 != *id && op.3.is_some_and(|other| *at < other && other < done)
                    })
                })
            }),
            Shows::DeliveredAfter {
                action,
                delivered: to_waiter,
            } => {
                let Some(&acted) = picked(report, action).first() else {
                    return false;
                };
                committed.iter().any(|(at, id)| {
                    *at < acted
                        && delivered(*id)
                            .is_some_and(|(done, reached)| done > acted && reached == to_waiter)
                })
            }
            Shows::StopDroppedApply => facts.iter().any(|(_, fact)| match fact {
                StorageFact::ShutDown { dropped, .. } => dropped.iter().any(|id| {
                    ops.iter()
                        .any(|op| op.0 == *id && op.1 == OpKind::Apply && op.3.is_none())
                }),
                _ => false,
            }),
            Shows::PeerLeaseDenied => facts.iter().any(|(at, fact)| {
                matches!(fact, StorageFact::Completed {
                    result: StorageResult::CapacityBusy { class },
                    id,
                    ..
                } if class == "peer"
                    && ops.iter().any(|op| op.0 == *id && op.1 == OpKind::Acquire))
                    && ops.iter().any(|(_, kind, submitted, done)| {
                        *kind == OpKind::Read
                            && submitted < at
                            && done.is_none_or(|done| done > *at)
                    })
            }),
            Shows::SucceededAfterHold => {
                let Some(&(first, _, _, Some(done))) = ops.iter().find(|op| op.1 == OpKind::Read)
                else {
                    return false;
                };
                let before = ops
                    .iter()
                    .filter(|op| {
                        op.1 == OpKind::Read && op.0 != first && op.3.is_some_and(|at| at < done)
                    })
                    .count();
                before >= 2
                    && facts.iter().any(|(_, fact)| {
                        matches!(fact, StorageFact::Completed {
                            id,
                            outcome,
                            result: StorageResult::Page { .. },
                            ..
                        } if *id == first && outcome == "engine")
                    })
            }
            Shows::Failed(kind, outcome) => facts.iter().any(|(_, fact)| match fact {
                StorageFact::Completed {
                    id, outcome: done, ..
                } => done == outcome && ops.iter().any(|op| op.0 == *id && op.1 == kind),
                _ => false,
            }),
            Shows::ForkWork {
                earlier,
                later,
                work: ordering,
            } => {
                // Each prepared fork once: its ancestor, first header, header
                // count and work.
                let mut prepares: Vec<(StoragePoint, String, u32, U256)> = Vec::new();
                for (_, fact) in &facts {
                    if let StorageFact::Submitted {
                        request:
                            StorageRequest::PrepareHeaderTarget {
                                common_ancestor,
                                headers,
                                first: Some(first),
                                work: Some(work),
                                ..
                            },
                        ..
                    } = fact
                    {
                        let work = U256::from_dec_str(work).expect("work is logged in decimal");
                        if !prepares.iter().any(|(_, seen, ..)| *seen == first.hash) {
                            prepares.push((
                                common_ancestor.clone(),
                                first.hash.clone(),
                                *headers,
                                work,
                            ));
                        }
                    }
                }
                // Both whole forks from one ancestor, so their lengths and
                // work compare as the scenario names them.
                matches!(&prepares[..], [a, b]
                    if a.0 == b.0 && (a.2, b.2) == (earlier, later) && b.3.cmp(&a.3) == ordering)
            }
            Shows::Owed(class) => {
                Shows::Recovered(class).in_run(report)
                    && report.properties.iter().any(|reported| {
                        reported.property == BOUNDED_ADVERSARY_PROGRESS
                            && reported.outcome == PropertyOutcome::Held { applicable: 1 }
                    })
            }
            Shows::Recovered(class) => {
                let (Some(&recover), Some(&done)) = (
                    picked(report, "recover").first(),
                    picked(report, "recovered").first(),
                ) else {
                    return false;
                };
                let before = |at: &u64| *at < recover;
                recover < done
                    && match class {
                        "closes" => {
                            picked(report, "close").first().is_some_and(before)
                                && picked(report, "recovery-relink")
                                    .first()
                                    .is_some_and(|relink| *relink > recover)
                        }
                        "holds" => pending_at(OpKind::Prepare, recover),
                        "faults" => facts.iter().any(|(at, fact)| {
                            before(at)
                                && matches!(fact, StorageFact::Completed { id, outcome, .. }
                                    if outcome == "local-resource"
                                        && ops.iter().any(|op| op.0 == *id && op.1 == OpKind::Prepare))
                        }),
                        "invalidations" => {
                            resets
                                .iter()
                                .any(|(at, kind)| before(at) && *kind == "invalidate")
                                && resets.contains(&(recover, "reconsider"))
                        }
                        "reply-holds" => committed.iter().any(|(at, id)| {
                            before(at) && delivered(*id).is_some_and(|(done, _)| done > recover)
                        }),
                        "departure" => {
                            picked(report, "close").first().is_some_and(before)
                                && picked(report, "recovery-relink").is_empty()
                        }
                        _ => false,
                    }
            }
        }
    }
}

/// What each witness seed's run shows, by stem, and which [`PLAN_BYTES`]
/// slice of the [`byte_inputs`] stream is its Plan. Most use slice 16.
/// The hold, the reconsider, the release and two forks use a slice whose
/// run orders their overlap that way.
fn shows(stem: &str) -> (Shows, usize) {
    use std::cmp::Ordering;
    let witness = stem
        .strip_prefix("header-storage-w-")
        .expect("witness stems share a prefix");
    let class = |class: &str| {
        [
            "closes",
            "holds",
            "faults",
            "invalidations",
            "reply-holds",
            "departure",
        ]
        .into_iter()
        .find(|known| *known == class)
        .unwrap_or_else(|| panic!("{stem} names no prefix class"))
    };
    if let Some(named) = witness.strip_prefix("recover-") {
        return (Shows::Recovered(class(named)), 16);
    }
    if let Some(named) = witness.strip_prefix("owed-") {
        return (Shows::Owed(class(named)), 16);
    }
    if let Some((kind, allow, outcome, _)) = storage_witnesses::fault_witnesses()
        .into_iter()
        .find(|(kind, _, outcome, _)| storage_witnesses::fault_stem(*kind, outcome) == stem)
    {
        let _ = allow;
        return (Shows::Failed(kind, outcome), 16);
    }
    match witness {
        "reset-locator" => (Shows::HeldAcrossReset(OpKind::Locator), 16),
        "reset-prepare" => (Shows::HeldAcrossReset(OpKind::Prepare), 16),
        "reset-apply" => (Shows::HeldAcrossReset(OpKind::Apply), 16),
        "reset-reply" => (Shows::ResetBetweenCommitAndDelivery, 16),
        "hold-succeeds" => (Shows::SucceededAfterHold, 2),
        "reconsider" => (Shows::ReconsiderMidRound, 20),
        "release" => (Shows::ReleasedRead, 17),
        "reply-hold" => (Shows::ReplyOutlasted, 16),
        "reply-after-shutdown" => (
            Shows::DeliveredAfter {
                action: "shutdown",
                delivered: false,
            },
            16,
        ),
        "reply-after-close" => (
            Shows::DeliveredAfter {
                action: "close",
                delivered: true,
            },
            16,
        ),
        "stop-storage" => (Shows::StopDroppedApply, 16),
        "peer-lease-denied" => (Shows::PeerLeaseDenied, 16),
        // The node prepares mirror-0's branch-1 fork first.
        "equal-forks" => (
            Shows::ForkWork {
                earlier: 4,
                later: 4,
                work: Ordering::Equal,
            },
            16,
        ),
        "longer-fork" => (
            Shows::ForkWork {
                earlier: 5,
                later: 3,
                work: Ordering::Less,
            },
            17,
        ),
        "weighted-fork" => (
            Shows::ForkWork {
                earlier: 5,
                later: 3,
                work: Ordering::Greater,
            },
            17,
        ),
        _ => panic!("{stem} has no overlap to show"),
    }
}

/// The committed fixture directory.
fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// A full fuzz input: `scenario`'s canonical prefix, then slice `index` of
/// [`byte_inputs`].
fn input(index: usize, scenario: &StorageScenario) -> Vec<u8> {
    let stream = byte_inputs(64, 1_024).concat();
    let plan = stream
        .get(index * PLAN_BYTES..(index + 1) * PLAN_BYTES)
        .expect("64 inputs of up to 1,024 bytes hold every seed's Plan");
    grammar::input(scenario, plan).unwrap_or_else(|error| panic!("{scenario:?}: {error}"))
}

/// Each committed witness seed is its scenario's input. Its run through
/// the registered harness ends clean, and shows the overlap it's named
/// for. Every witness has a seed.
#[test]
fn committed_witness_seeds_show_their_overlaps() {
    let entry = harness::find(Storage::NAME).unwrap();
    let seeds = storage_witnesses::witness_seeds();
    // 15 named witnesses, 10 faults, 5 recovery classes and hdr-13's two
    // owed recoveries.
    assert_eq!(seeds.len(), 32);
    for (stem, scenario) in seeds {
        let (shows, slice) = shows(&stem);
        let committed = fs::read(fixtures().join(format!("{stem}.bin")))
            .unwrap_or_else(|error| panic!("{stem}: {error}"));
        assert_eq!(
            committed,
            input(slice, &scenario),
            "{stem}: the committed seed changed; regenerate the seeds if that's intended"
        );
        assert_eq!(
            Storage::draw(&committed[..Storage::SCENARIO_BYTES]),
            scenario,
            "{stem}"
        );
        let run = (entry.run_bytes)(&committed, &mut NullSink, None);
        let report = run
            .report()
            .unwrap_or_else(|| panic!("{stem}: {:?}", run.result));
        assert_eq!(
            classify(entry, &run).outcome,
            ExploreOutcome::Clean,
            "{stem}"
        );
        assert_eq!(run.leaked(), 0, "{stem}");
        assert!(shows.in_run(report), "{stem} doesn't show {shows:?}");
    }
}

/// Rewrites every witness seed and its sidecar. Run it with
/// `RUSTFLAGS="--cfg zakura_sim" cargo test -p zakura-sim --test
/// header_storage_witness_seeds -- --ignored regenerate_witness_seeds`.
#[test]
#[ignore = "rewrites the committed seeds"]
fn regenerate_witness_seeds() {
    for (stem, scenario) in storage_witnesses::witness_seeds() {
        let (shows, slice) = shows(&stem);
        let input = input(slice, &scenario);
        let base = fixtures().join(&stem);
        fs::write(base.with_extension("bin"), &input).expect("the fixture directory exists");
        let mut sidecar = format!(
            "# The committed fuzz input `{stem}.bin`, a fuzz-12 item 7 witness whose run\n\
             # shows {shows:?}. Its 344-byte prefix is the scenario's canonical\n\
             # encoding, then 1,024 Plan bytes.\n\
             harness = \"{harness}\"\n\
             generator = {generator}\n\
             sha256 = \"{digest}\"\n\
             scenario_sha256 = \"{scenario}\"\n\
             source = \"crates/zakura-sim/tests/header_storage_witness_seeds.rs\"\n",
            harness = Storage::NAME,
            generator = Storage::GENERATOR,
            digest = seed::sha256(&input),
            scenario = seed::scenario_sha256(
                &serde_json::to_value(&scenario).expect("scenarios serialize")
            ),
        );
        for (_, property) in WITNESSES.iter().filter(|(seed, _)| *seed == stem) {
            sidecar.push_str(&format!(
                "\n[[witness]]\nproperty = \"{property}\"\nexpect = \"holds\"\n"
            ));
        }
        fs::write(base.with_extension("toml"), sidecar).expect("the fixture directory exists");
    }
}
