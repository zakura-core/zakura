//! E2b: the protocol-state collector's index layout, and the required-state
//! matrix. Each collected state has a deterministic witness, and each
//! witness records the same trace with and without the collector.
#![cfg(zakura_sim)]

mod support;

use support::storage_witnesses::{competing_mirrors, header_apply, reply_hold};

use std::{
    collections::{BTreeMap, BTreeSet},
    thread,
};

use sha2::{Digest, Sha256};
use zakura_network::zakura::{
    block_sync_streams, observe::HeaderRequestTerminal, v9::RegtestMessage, FrameRejection,
};
use zakura_sim::{
    explore::{
        feedback::{Counters, Feature, Feedback, FEATURES, FEEDBACK_VERSION},
        state::{
            collector, Coverage, Disposition, Family, Fork as ForkState, FrameLabel, Operation,
            RetireReason, Row, Service, StateFeature, Transition, REQUIRED, SPAN,
        },
    },
    harness::{
        block_node::{
            control::{ControlAction, ControlStep},
            policy::{BlockAllow, BlockKind, Hold, Select, Trigger},
            Policy as BlockPolicy,
        },
        block_sync::{BlockScenario, Storage as BlockStorage},
        header_node::{
            policy::{Allow, Hold as HeaderHold, Trigger as HeaderTrigger},
            Policy as HeaderPolicy,
        },
        header_sync::{HeaderWire, Storage as HeaderStorage, StorageScenario},
        run_harness,
        wire_plans::act_scenario,
        HarnessError, Subsystem,
    },
    model::Label,
    observe::{StorageFact, StorageRequest, StorageResult},
    peer::adversary::Act,
    Checker, MemorySink, Observation, Plan, PropertyReport, RecordSink, RunEnd, RunReport,
};

/// Fresh counters for one run.
fn counters() -> &'static Counters {
    Box::leak(Box::new(Counters::new()))
}

/// Every state feature, by its index.
fn decoder() -> BTreeMap<Feature, StateFeature> {
    StateFeature::all()
        .into_iter()
        .map(|state| (state.feature(), state))
        .collect()
}

#[test]
fn state_features_have_distinct_indices_inside_their_blocks() {
    let all = StateFeature::all();
    let decoder = decoder();
    assert_eq!(
        decoder.len(),
        all.len(),
        "two state features share an index"
    );
    for state in &all {
        let index = state.feature().index();
        let first = state.family().first();
        assert!(
            (first..first + SPAN).contains(&index),
            "{state:?} leaves its family's span"
        );
        // The probe's 256 features sit at the top of the index space.
        assert!(usize::from(index) < FEATURES - 256, "{state:?}");
        assert_eq!(StateFeature::decode(state.feature()), Some(*state));
    }
    // Each block ends before the next one in its family starts.
    let mut blocks: BTreeMap<(Family, u16), u16> = BTreeMap::new();
    for state in &all {
        let offset = state.feature().index() - state.family().first() - state.block();
        let size = blocks.entry((state.family(), state.block())).or_default();
        *size = (*size).max(offset + 1);
    }
    let starts: Vec<_> = blocks.keys().copied().collect();
    for pair in starts.windows(2) {
        let [(family, start), (next_family, next_start)] = pair else {
            unreachable!()
        };
        if family == next_family {
            assert!(
                start + blocks[&(*family, *start)] <= *next_start,
                "{pair:?}"
            );
        }
    }
    for ((family, start), size) in &blocks {
        assert!(start + size <= SPAN, "{family:?} block at {start}");
    }
}

/// [`FEEDBACK_VERSION`] 4's layout: one pinned index per block, and every
/// vocabulary's size. A change here changes what a corpus means, so it
/// bumps the version.
#[test]
fn the_layout_is_pinned_to_its_version() {
    use zakura_sim::explore::state::*;
    assert_eq!(FEEDBACK_VERSION, 4);
    let pinned = [
        (
            StateFeature::HeaderRetired {
                purpose: Purpose::AuxiliaryRepair,
                phase: RetiredPhase::Applying,
                reason: RetireReason::TimedOut,
            },
            (4 + 2) * 21 + 19,
        ),
        (
            StateFeature::BlockRequestStep {
                last: RequestProgress::Written,
                step: RequestEvent::Reset,
            },
            512 + 3 * 9,
        ),
        (
            StateFeature::BlockOpCompleted {
                op: EpochWork::Submit,
                stage: BlockStage::Admitted,
                epoch: Epoch::Stale,
            },
            1024 + (4 + 2) * 2 + 1,
        ),
        (
            StateFeature::Completion {
                service: Service::Block,
                op: Operation::SubmitBlock,
                timing: Timing::CallerWaiting,
            },
            4096 + (13 + 9) * 3,
        ),
        (
            StateFeature::Disposition {
                service: Service::Header,
                op: Operation::ApplyHeaderTarget,
                disposition: Disposition::Taken,
            },
            4096 + 512 + 6 * 8,
        ),
        (
            StateFeature::CallEnded {
                call: Call::Apply,
                end: CallEnd::Replied,
            },
            4096 + 1024 + 3 * 6,
        ),
        (StateFeature::Fork(Fork::VerifiedReset), 8192 + 4),
        (StateFeature::Fork(Fork::EqualWork), 8192 + 7),
        (
            StateFeature::Replacement {
                kind: ReplacementKind::Stream,
                service: StreamService::BlockSync,
                event: ReplacementEvent::OldTrafficAfter,
            },
            12288 + (3 + 1) * 4 + 3,
        ),
        (
            StateFeature::BlockBudget {
                change: BudgetChange::Charged,
                before: Level::AtLimit,
                after: Level::OverLimit,
            },
            16384 + (4 + 2) * 4 + 3,
        ),
        (
            StateFeature::HeaderCapacity {
                event: CapacityEvent::WaiterFired,
                class: CapacityClass::Fallback,
            },
            16384 + 512 + 4 + 2,
        ),
        (
            StateFeature::CadenceEmptied {
                row: Row::HeaderStatus,
                direction: Direction::FromNode,
            },
            16384 + 1024 + 6 * 2 + 1,
        ),
        (
            StateFeature::HeaderLease {
                class: CapacityClass::Fallback,
                level: Level::OverLimit,
            },
            16384 + 1536 + 2 * 4 + 3,
        ),
        (
            StateFeature::Frame {
                row: Row::BlockBlock,
                direction: Direction::ToNode,
                label: FrameLabel::Conformant,
                stage: FrameStage::Unpunished,
            },
            20480 + ((2 * 2) * 13 + 12) * 6 + 5,
        ),
        (
            StateFeature::Transition(Transition::RefusedUnlisted),
            20480 + 2048 + 17,
        ),
        (StateFeature::ConformantCrossing, 20480 + 3072),
        (
            StateFeature::Hold {
                op: Operation::ReadRange,
                part: HeldPart::Reply,
                stage: HoldStage::Resumed,
            },
            24576 + (12 * 2 + 1) * 3 + 2,
        ),
        (
            StateFeature::ReorgStage(ReorgStage::VerdictOther),
            28672 + 6,
        ),
        (
            StateFeature::Obligation {
                namespace: Namespace::HeaderSync,
                point: ObligationPoint::Expired,
            },
            32768 + 3 * 3 + 2,
        ),
        (StateFeature::ConditionsEstablished, 32768 + 512),
        (
            StateFeature::Retired(Retirement::ConnectionTornDown),
            36864 + 2,
        ),
        (
            StateFeature::Acknowledged {
                service: Service::Block,
                how: Acknowledgement::ShutDown,
                dropped: Dropped::Some,
            },
            36864 + 16 + 7,
        ),
        (StateFeature::Baseline(Baseline::Unsettled), 36864 + 32 + 1),
        (StateFeature::IncompleteDrain, 36864 + 48),
    ];
    for (state, index) in pinned {
        assert_eq!(state.feature().index(), index, "{state:?}");
    }
    let sizes = [
        Family::ALL.len(),
        Purpose::ALL.len(),
        RetiredPhase::ALL.len(),
        RetireReason::ALL.len(),
        RequestProgress::ALL.len(),
        RequestEvent::ALL.len(),
        EpochWork::ALL.len(),
        BlockStage::ALL.len(),
        Epoch::ALL.len(),
        Service::ALL.len(),
        Operation::ALL.len(),
        Timing::ALL.len(),
        Disposition::ALL.len(),
        Call::ALL.len(),
        CallEnd::ALL.len(),
        Fork::ALL.len(),
        ReplacementKind::ALL.len(),
        StreamService::ALL.len(),
        ReplacementEvent::ALL.len(),
        BudgetChange::ALL.len(),
        Level::ALL.len(),
        CapacityEvent::ALL.len(),
        CapacityClass::ALL.len(),
        Row::ALL.len(),
        Direction::ALL.len(),
        FrameLabel::ALL.len(),
        FrameStage::ALL.len(),
        Transition::ALL.len(),
        HeldPart::ALL.len(),
        HoldStage::ALL.len(),
        ReorgStage::ALL.len(),
        Namespace::ALL.len(),
        ObligationPoint::ALL.len(),
        Retirement::ALL.len(),
        Acknowledgement::ALL.len(),
        Dropped::ALL.len(),
        Baseline::ALL.len(),
    ];
    assert_eq!(
        sizes,
        [
            10, 2, 4, 21, 4, 9, 2, 4, 2, 2, 13, 3, 8, 4, 6, 8, 2, 3, 4, 4, 4, 2, 4, 12, 2, 13, 6,
            18, 2, 3, 7, 8, 3, 3, 2, 2, 2
        ]
    );
    assert_eq!(StateFeature::all().len(), 2_689);
    // Swapping two values inside a vocabulary keeps every size above, and
    // can keep every pinned index, but moves both values' indices. The
    // digest of every feature's index pins the whole mapping.
    let mapping: String = StateFeature::all()
        .into_iter()
        .map(|state| format!("{} {state:?}\n", state.feature().index()))
        .collect();
    let digest: String = Sha256::digest(mapping)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    assert_eq!(
        digest, "62dda724b9a4bc56960d601ac0bb3a9baff241866c1b52fd4dede0ef73b65df6",
        "the index mapping moved: bump FEEDBACK_VERSION and this digest together"
    );
}

/// The families are the feature table's rows, as the C1 suite manifest's
/// `feature/*` IDs name them.
#[test]
fn families_are_the_feature_table() {
    let ids: Vec<&str> = Family::ALL.iter().map(|family| family.name()).collect();
    assert_eq!(
        ids,
        [
            "feature/request-reset",
            "feature/completion-cancellation",
            "feature/competing-forks",
            "feature/replacement",
            "feature/budget",
            "feature/regulation",
            "feature/hold",
            "feature/reorg-stage",
            "feature/recovery",
            "feature/teardown",
        ]
    );
    for family in Family::ALL {
        assert!(
            REQUIRED.iter().any(|row| row.family == *family),
            "{family:?} has no required state"
        );
    }
}

/// Every vocabulary of production names lists each name production writes:
/// header sync's retire reasons, the precheck's refusals, the labels and
/// the tabled rows.
#[test]
fn vocabularies_list_every_production_name() {
    let reasons = [
        HeaderRequestTerminal::TargetNotRetained,
        HeaderRequestTerminal::NoLocatorIntersection,
        HeaderRequestTerminal::HistoryPruned,
        HeaderRequestTerminal::TargetNotSelected,
        HeaderRequestTerminal::SubscriptionSuperseded,
        HeaderRequestTerminal::SubscriptionClosed,
        HeaderRequestTerminal::Disconnected,
        HeaderRequestTerminal::LocalError,
        HeaderRequestTerminal::MalformedResponse,
        HeaderRequestTerminal::ExcludedAuxiliaryInput,
        HeaderRequestTerminal::RepairObsolete,
        HeaderRequestTerminal::ResourceStalled,
        HeaderRequestTerminal::SendError,
        HeaderRequestTerminal::SessionReplaced,
        HeaderRequestTerminal::Shutdown,
        HeaderRequestTerminal::SnapshotObsolete,
        HeaderRequestTerminal::StagingRefused,
        HeaderRequestTerminal::TargetAdmitted,
        HeaderRequestTerminal::TargetRejected,
        HeaderRequestTerminal::TimedOut,
    ];
    // An exhaustive match fails to build when header sync adds a reason.
    for reason in reasons {
        match reason {
            HeaderRequestTerminal::TargetNotRetained
            | HeaderRequestTerminal::NoLocatorIntersection
            | HeaderRequestTerminal::HistoryPruned
            | HeaderRequestTerminal::TargetNotSelected
            | HeaderRequestTerminal::SubscriptionSuperseded
            | HeaderRequestTerminal::SubscriptionClosed
            | HeaderRequestTerminal::Disconnected
            | HeaderRequestTerminal::LocalError
            | HeaderRequestTerminal::MalformedResponse
            | HeaderRequestTerminal::ExcludedAuxiliaryInput
            | HeaderRequestTerminal::RepairObsolete
            | HeaderRequestTerminal::ResourceStalled
            | HeaderRequestTerminal::SendError
            | HeaderRequestTerminal::SessionReplaced
            | HeaderRequestTerminal::Shutdown
            | HeaderRequestTerminal::SnapshotObsolete
            | HeaderRequestTerminal::StagingRefused
            | HeaderRequestTerminal::TargetAdmitted
            | HeaderRequestTerminal::TargetRejected
            | HeaderRequestTerminal::TimedOut => {}
        }
        assert_ne!(RetireReason::of(reason.label()), RetireReason::Unlisted);
    }
    assert_eq!(RetireReason::ALL.len(), reasons.len() + 1);
    let refusals = [
        FrameRejection::UnknownMessageType,
        FrameRejection::ReservedFlags,
        FrameRejection::PayloadTooShort { min: 1 },
        FrameRejection::Unsolicited,
        FrameRejection::AboveReservation { bytes: 1 },
        FrameRejection::AboveOwed {
            frames: 1,
            bytes: 1,
        },
    ];
    for refusal in refusals {
        assert_ne!(
            Transition::refused(refusal.label()),
            Transition::RefusedUnlisted
        );
    }
    let labels: Vec<&str> = Label::ALL.iter().map(|label| label.name()).collect();
    let listed: Vec<&str> = FrameLabel::ALL.iter().map(|label| label.name()).collect();
    assert_eq!(labels, listed);
    let mut rows = Vec::new();
    for streams in [block_sync_streams(), &RegtestMessage::LAYOUT[..]] {
        for stream in streams {
            let id = zakura_sim::observe::StreamId {
                kind: stream.kind,
                version: stream.version,
            };
            rows.extend(
                stream
                    .messages
                    .unwrap_or_default()
                    .iter()
                    .map(|rule| Row::of(id, Some(rule.name))),
            );
            rows.push(Row::of(id, None));
        }
    }
    let listed: Vec<Row> = Row::ALL
        .iter()
        .copied()
        .filter(|row| *row != Row::Unlisted)
        .collect();
    assert_eq!(
        rows, listed,
        "Row lists each tabled row once, in table order"
    );
}

/// What a run recorded and reported: every record, digests included, and
/// every property report.
#[derive(Debug, PartialEq)]
struct Recorded {
    records: Vec<String>,
    properties: Vec<PropertyReport>,
    end: RunEnd,
}

/// Runs a harness with a sink and an extra checker.
type Run = Box<dyn Fn(&mut dyn RecordSink, Option<&mut dyn Checker>) -> RunReport + Send + Sync>;

/// One deterministic witness run.
struct Witness {
    /// What it runs.
    name: String,
    /// The harness.
    harness: &'static str,
    /// True if the harness's fuzz decoder draws its scenario.
    drawn: bool,
    /// The run.
    run: Run,
}

/// The report of a run that ended, at a harness failure or not.
fn report(result: Result<RunReport, HarnessError>) -> RunReport {
    match result {
        Ok(report) => report,
        Err(HarnessError::Run { report, .. }) => *report,
        Err(error) => panic!("{error}"),
    }
}

/// A witness of fuzz `input` on `harness`.
fn drawn(harness: &'static str, name: &str, input: Vec<u8>) -> Witness {
    let entry = zakura_sim::harness::find(harness).unwrap();
    Witness {
        name: name.to_owned(),
        harness,
        drawn: true,
        run: Box::new(move |sink, extra| {
            let run = (entry.run_bytes)(&input, sink, extra);
            report(run.result)
        }),
    }
}

/// A witness of an explicit `S` scenario under Plan `plan`.
fn explicit<S: Subsystem>(name: &str, scenario: S::Scenario, plan: Vec<u8>) -> Witness
where
    S::Scenario: Send + Sync,
{
    Witness {
        name: name.to_owned(),
        harness: S::NAME,
        drawn: false,
        run: Box::new(move |sink, extra| {
            report(run_harness::<S>(
                &scenario,
                Plan::from_fuzz(&plan),
                sink,
                extra,
            ))
        }),
    }
}

/// The committed seed `stem`, a full fuzz input the harness draws its
/// scenario from.
fn seed(stem: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("{stem}.bin"));
    std::fs::read(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

/// Plan bytes after a scenario prefix, the same on every call.
fn plan() -> Vec<u8> {
    // Truncation to a byte is the point: a spread of byte values.
    (0..64u32).map(|i| (i * 37 % 251) as u8).collect()
}

/// Every witness.
fn witnesses() -> Vec<Witness> {
    let block = "block-sync/storage";
    let mut all = vec![
        drawn(block, "block default", [&[0, 0][..], &plan()].concat()),
        drawn(
            block,
            "block paused handoffs",
            [&[0xFF, 0x03][..], &plan()].concat(),
        ),
        drawn(
            block,
            "block peers leave",
            [&[0xFF, 0x05][..], &plan()].concat(),
        ),
        drawn(
            block,
            "block reset mid flight",
            [&[0xFF, 0x0C][..], &plan()].concat(),
        ),
        drawn(
            block,
            "block reset past the tip",
            [&[0xFF, 0x0E][..], &plan()].concat(),
        ),
        drawn(
            block,
            "block stale beside current",
            [&[0xFF, 0x06][..], &plan()].concat(),
        ),
        drawn(
            block,
            "block stalled reset",
            [&[0xFF, 0x0A][..], &plan()].concat(),
        ),
        drawn("header-sync/storage", "header storage default", plan()),
        explicit::<BlockStorage>("block teardown", teardown(), plan()),
        explicit::<BlockStorage>(
            "block relink while applying",
            relink_while_applying(),
            plan(),
        ),
        drawn(
            "header-sync/storage",
            "header storage locator dropped at its timeout",
            seed("header-storage-locator-dropped"),
        ),
        drawn(
            "header-sync/storage",
            "header storage apply pending at close",
            seed("header-storage-apply-at-close"),
        ),
        drawn(
            "header-sync/storage",
            "header storage competing mirrors",
            seed("header-storage-competing-mirrors"),
        ),
        explicit::<HeaderWire>(
            "header wire status burst",
            act_scenario(Act::StatusBurst),
            plan(),
        ),
    ];
    for tag in 1..=14u8 {
        all.push(drawn(
            "header-sync/wire",
            &format!("header wire saved plan {tag}"),
            [&[0xFE, tag][..], &plan()].concat(),
        ));
    }
    all.push(explicit::<HeaderStorage>(
        GROW_TO_DRAIN,
        StorageScenario::grow(),
        grow_to_drain_plan(),
    ));
    // fuzz-12 item 7's witness seeds.
    for (stem, _) in support::storage_witnesses::witness_seeds() {
        all.push(drawn("header-sync/storage", &stem, seed(&stem)));
    }
    // fuzz-13's saved Plans, each run from its committed seed.
    for stem in FUZZ_13_SEEDS {
        all.push(drawn("header-sync/wire", stem, seed(stem)));
    }
    // Competing mirrors under more plans, which also reach a reorg.
    for (index, plan) in support::byte_inputs(6, 128).into_iter().enumerate() {
        all.push(explicit::<HeaderStorage>(
            &format!("header storage competing mirrors {index}"),
            competing_mirrors(),
            plan,
        ));
    }
    all
}

/// fuzz-13's committed `header-sync/wire` seeds.
const FUZZ_13_SEEDS: [&str; 3] = [
    "header-wire-reopen",
    "header-wire-old-page-after-reopen",
    "header-wire-outcome-crossing",
];

/// The no-hold passing control: [`StorageScenario::grow`] under a Plan whose
/// run leaves an operation pending through its drain, and prepares one chain
/// to two targets from one ancestor.
const GROW_TO_DRAIN: &str = "header storage grow to its drain";

/// [`GROW_TO_DRAIN`]'s Plan.
fn grow_to_drain_plan() -> Vec<u8> {
    support::byte_inputs(1, 1_024).remove(0)
}

/// Eight blocks whose verdicts wait for the link to close, and a teardown
/// once two submissions wait, as `tests/block_properties.rs` case 6 runs.
fn teardown() -> BlockScenario {
    BlockScenario {
        blocks: 8,
        policy: BlockPolicy {
            holds: vec![Hold {
                select: Select {
                    kind: Some(BlockKind::Apply),
                    ..Select::default()
                },
                until: Trigger::Closed("mirror-0".to_owned()),
                then: vec![BlockAllow::Committed],
                deadline_ms: None,
            }],
            ..BlockPolicy::converging()
        },
        control: vec![ControlStep {
            action: ControlAction::Teardown,
            when: Some(Trigger::Count(
                2,
                Box::new(Trigger::Submitted(Select {
                    kind: Some(BlockKind::Apply),
                    ..Select::default()
                })),
            )),
        }],
        ..BlockScenario::default()
    }
}

/// Eight blocks, and a second link to `mirror-0`, its first still open, once
/// the node submits its first block.
fn relink_while_applying() -> BlockScenario {
    BlockScenario {
        blocks: 8,
        control: vec![ControlStep {
            action: ControlAction::Relink {
                peer: "mirror-0".to_owned(),
            },
            when: Some(Trigger::Submitted(Select {
                kind: Some(BlockKind::Apply),
                ..Select::default()
            })),
        }],
        ..BlockScenario::default()
    }
}

/// Runs `witness` without and with the collector, checks that the two
/// record the same trace, and returns the states the collector hit.
fn states(witness: &Witness) -> BTreeSet<StateFeature> {
    let recorded = |collect: bool| {
        let counters = counters();
        let mut sink = MemorySink::default();
        let mut collecting = collect.then(|| Feedback::new(counters, collector).start());
        let extra = collecting
            .as_mut()
            .map(|collecting| collecting as &mut dyn Checker);
        let report = (witness.run)(&mut sink, extra);
        let recorded = Recorded {
            records: sink.lines(),
            properties: report.properties,
            end: report.end,
        };
        (recorded, counters.hits())
    };
    let (without, _) = recorded(false);
    let (with, hits) = recorded(true);
    assert_eq!(
        with, without,
        "{}: the collector changed the run",
        witness.name
    );
    let decoder = decoder();
    hits.into_iter()
        .map(|feature| {
            *decoder
                .get(&feature)
                .unwrap_or_else(|| panic!("{}: {feature:?} is no state feature", witness.name))
        })
        .collect()
}

#[test]
fn every_collected_state_has_a_witness() {
    let witnesses = witnesses();
    let reached: Vec<BTreeSet<StateFeature>> = thread::scope(|scope| {
        let runs: Vec<_> = witnesses
            .iter()
            .map(|witness| scope.spawn(|| states(witness)))
            .collect();
        runs.into_iter()
            .map(|run| run.join().expect("the witness runs"))
            .collect()
    });
    // Block storage sends an action's reply to block sync's handle and a
    // record's to nobody, so only a range read can find its caller gone.
    for (witness, states) in witnesses.iter().zip(&reached) {
        let lost = states.iter().find(|state| {
            matches!(
                state,
                StateFeature::Disposition {
                    service: Service::Block,
                    op,
                    disposition: Disposition::Undelivered,
                } if *op != Operation::ReadRange
            )
        });
        assert_eq!(lost, None, "{}: no caller to lose", witness.name);
    }
    // Each drawn header storage seed shows the rows it's committed for on
    // its own run, so another witness can't stand in for it.
    let own = [
        (
            "header storage locator dropped at its timeout",
            &["completion after cancellation request"][..],
        ),
        (
            "header storage apply pending at close",
            &["completion after owner retirement"][..],
        ),
        (
            "header storage competing mirrors",
            &[
                "two distinct peer offers",
                "shared ancestor",
                "selected winner",
            ][..],
        ),
        (
            "header-storage-w-hold-succeeds",
            &["operation kind × blocked × release × resumed operation"][..],
        ),
        (
            "header-storage-w-equal-forks",
            &["stronger, weaker or equal work"][..],
        ),
        (
            "header-storage-w-longer-fork",
            &["stronger, weaker or equal work"][..],
        ),
        (
            "header-storage-w-weighted-fork",
            &["stronger, weaker or equal work"][..],
        ),
        (
            "header-storage-w-peer-lease-denied",
            &["exactly limit", "attempted over-limit and denied"][..],
        ),
        (
            "header-storage-w-recover-closes",
            &["conditions established"][..],
        ),
        (
            "header-storage-w-stop-storage",
            &["external acknowledgement"][..],
        ),
        ("header-wire-reopen", &["stream replacement"][..]),
        (
            "header-wire-old-page-after-reopen",
            &["old-session traffic delivered"][..],
        ),
        (
            "header-wire-outcome-crossing",
            &["conformant crossed updates"][..],
        ),
    ];
    for (name, rows) in own {
        let (witness, states) = witnesses
            .iter()
            .zip(&reached)
            .find(|(witness, _)| witness.name == name)
            .unwrap_or_else(|| panic!("no witness {name}"));
        assert!(witness.drawn, "{name}");
        for state in rows {
            let row = REQUIRED.iter().find(|row| row.state == *state).unwrap();
            let Coverage::Collected { shows, drawn, .. } = row.coverage else {
                panic!("{state} is collected");
            };
            assert!(drawn, "{state} is drawn");
            assert!(states.iter().any(shows), "{name} doesn't show {state}");
        }
    }
    for row in REQUIRED {
        let Coverage::Collected {
            harnesses,
            drawn,
            shows,
        } = row.coverage
        else {
            continue;
        };
        for harness in harnesses {
            let shown = witnesses
                .iter()
                .zip(&reached)
                .filter(|(witness, _)| witness.harness == *harness)
                .any(|(_, states)| states.iter().any(shows));
            assert!(
                shown,
                "{}: no {harness} witness shows {:?}",
                row.state, row.family
            );
        }
        let drawn_shown = witnesses
            .iter()
            .zip(&reached)
            .filter(|(witness, _)| witness.drawn && harnesses.contains(&witness.harness))
            .any(|(_, states)| states.iter().any(shows));
        assert_eq!(drawn_shown, drawn, "{}: drawn", row.state);
    }
}

/// The states `name`'s witness hits.
fn witness_states(name: &str) -> BTreeSet<StateFeature> {
    let witness = witnesses()
        .into_iter()
        .find(|witness| witness.name == name)
        .unwrap_or_else(|| panic!("no witness {name}"));
    states(&witness)
}

/// Each fork-work label has the witness built for it, and each hold stage
/// has one for a held completion and one for a held reply.
#[test]
fn fuzz_12_features_have_their_witnesses() {
    use zakura_sim::explore::state::{HeldPart, HoldStage};
    for (stem, label) in [
        ("header-storage-w-equal-forks", ForkState::EqualWork),
        // The five-header branch is prepared first, so the three-header
        // one is weaker unweighted and stronger at weight 2.
        ("header-storage-w-longer-fork", ForkState::WeakerWork),
        ("header-storage-w-weighted-fork", ForkState::StrongerWork),
    ] {
        let labels: Vec<ForkState> = witness_states(stem)
            .into_iter()
            .filter_map(|state| match state {
                StateFeature::Fork(
                    fork @ (ForkState::StrongerWork | ForkState::WeakerWork | ForkState::EqualWork),
                ) => Some(fork),
                _ => None,
            })
            .collect();
        assert_eq!(labels, [label], "{stem}");
    }
    for (stem, op, part) in [
        (
            "header-storage-w-hold-succeeds",
            Operation::ReadHeaderPath,
            HeldPart::Completion,
        ),
        (
            "header-storage-w-reply-hold",
            Operation::ApplyHeaderTarget,
            HeldPart::Reply,
        ),
    ] {
        let states = witness_states(stem);
        for &stage in HoldStage::ALL {
            let hold = StateFeature::Hold { op, part, stage };
            assert!(states.contains(&hold), "{stem}: {hold:?}");
        }
    }
}

/// The weighted-fork seed prepares the five-header branch again after the
/// three-header one, and that retry earns no reverse label: the seed hits
/// only [`ForkState::StrongerWork`], as the test above checks.
#[test]
fn a_retried_prepare_earns_no_fork_work() {
    let entry = zakura_sim::harness::find("header-sync/storage").unwrap();
    let run = (entry.run_bytes)(
        &seed("header-storage-w-weighted-fork"),
        &mut zakura_sim::NullSink,
        None,
    );
    let report = report(run.result);
    let prepares: Vec<_> = report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage {
                fact:
                    StorageFact::Submitted {
                        request:
                            StorageRequest::PrepareHeaderTarget {
                                common_ancestor,
                                first: Some(first),
                                target,
                                ..
                            },
                        ..
                    },
                ..
            } => Some((common_ancestor.clone(), first.hash.clone(), target.clone())),
            _ => None,
        })
        .collect();
    let retry_after_other = prepares.iter().enumerate().any(|(at, prepare)| {
        prepares[..at].contains(prepare)
            && prepares[..at]
                .iter()
                .any(|other| other.0 == prepare.0 && other.1 != prepare.1)
    });
    assert!(retry_after_other, "{prepares:?}");
}

/// [`reply_hold`], with its held apply's completion held too, until a
/// virtual second. The two parts resume at the one completion.
#[test]
fn both_held_parts_of_one_apply_resume() {
    use zakura_sim::explore::state::{HeldPart, HoldStage};
    let mut scenario = reply_hold();
    scenario.policy.holds = vec![HeaderHold {
        select: header_apply(1),
        until: HeaderTrigger::VirtualTime(1_000),
        then: vec![Allow::Engine],
        deadline_ms: None,
    }];
    let witness = explicit::<HeaderStorage>("apply held twice", scenario, plan());
    let states = states(&witness);
    for part in [HeldPart::Completion, HeldPart::Reply] {
        for &stage in HoldStage::ALL {
            let hold = StateFeature::Hold {
                op: Operation::ApplyHeaderTarget,
                part,
                stage,
            };
            assert!(states.contains(&hold), "{hold:?}");
        }
    }
}

/// A run with no hold hits no hold stage, and one chain prepared to two
/// targets from one ancestor hits no fork work.
#[test]
fn unheld_operations_and_one_chain_are_passing_controls() {
    let report = report(run_harness::<HeaderStorage>(
        &StorageScenario::grow(),
        Plan::from_fuzz(&grow_to_drain_plan()),
        &mut zakura_sim::NullSink,
        None,
    ));
    assert!(StorageScenario::grow().policy.holds.is_empty());
    assert!(StorageScenario::grow().policy.reply_holds.is_empty());
    let prepares: Vec<_> = report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage {
                fact:
                    StorageFact::Submitted {
                        request:
                            StorageRequest::PrepareHeaderTarget {
                                common_ancestor,
                                target,
                                first: Some(first),
                                ..
                            },
                        ..
                    },
                ..
            } => Some((common_ancestor.clone(), first.hash.clone(), target.clone())),
            _ => None,
        })
        .collect();
    let one_chain = prepares.iter().any(|(ancestor, first, target)| {
        prepares
            .iter()
            .any(|other| other.0 == *ancestor && other.1 == *first && other.2 != *target)
    });
    assert!(one_chain, "one chain is prepared to two targets");
    let states = witness_states(GROW_TO_DRAIN);
    let crossed = states.iter().find(|state| {
        matches!(
            state,
            StateFeature::Hold { .. }
                | StateFeature::Fork(
                    ForkState::StrongerWork | ForkState::WeakerWork | ForkState::EqualWork
                )
        )
    });
    assert_eq!(crossed, None);
}

/// [`competing_mirrors`], with `mirror-0`'s first apply held until
/// `mirror-1`'s heavier branch has applied. Storage then applies `mirror-0`'s
/// A5 as a weaker side branch, below the selected tip a locator reported.
fn weaker_branch_applied_last() -> StorageScenario {
    StorageScenario {
        policy: HeaderPolicy {
            holds: vec![HeaderHold {
                select: header_apply(1),
                until: HeaderTrigger::Completed(header_apply(2)),
                then: vec![Allow::Engine],
                deadline_ms: Some(20_000),
            }],
            ..HeaderPolicy::converging()
        },
        ..competing_mirrors()
    }
}

/// Whether the node's storage applied a target below the selected tip a
/// locator last reported: `mirror-0`'s A5 after `mirror-1`'s heavier branch,
/// whose tip is 7.
fn weaker_applied_below_tip(report: &RunReport) -> bool {
    let mut prepared = Vec::new();
    let mut applies = BTreeMap::new();
    let mut tip = 0;
    for entry in report.log.entries() {
        let Observation::Storage {
            instance: 0, fact, ..
        } = &entry.obs
        else {
            continue;
        };
        match fact {
            StorageFact::Submitted { request, id, .. } => match request {
                StorageRequest::PrepareHeaderTarget { owner, target, .. } => {
                    prepared.push((owner.clone(), target.height));
                }
                StorageRequest::ApplyHeaderTarget { owner, .. } => {
                    if let Some((_, height)) = prepared.iter().rev().find(|(by, _)| by == owner) {
                        applies.insert(*id, *height);
                    }
                }
                _ => {}
            },
            StorageFact::Completed { id, result, .. } => match result {
                StorageResult::Locator { tip: at, .. } => tip = at.height,
                StorageResult::Applied if applies.get(id) == Some(&5) && tip == 7 => {
                    return true;
                }
                _ => {}
            },
            _ => {}
        }
    }
    false
}

/// Storage returns `Applied` for a weaker side branch too, so an apply
/// below the reported tip is no reorg until a locator reports its target
/// as the selected tip.
#[test]
fn a_weaker_side_branch_applied_is_no_reorg() {
    for (index, plan) in support::byte_inputs(6, 128).into_iter().enumerate() {
        let witness = explicit::<HeaderStorage>(
            &format!("header storage weaker branch applied last {index}"),
            weaker_branch_applied_last(),
            plan,
        );
        let report = (witness.run)(&mut MemorySink::default(), None);
        assert!(
            weaker_applied_below_tip(&report),
            "{}: storage applied no weaker branch below the reported tip",
            witness.name
        );
        let states = states(&witness);
        assert!(
            !states.contains(&StateFeature::Fork(ForkState::HeaderReorgApplied)),
            "{}: {states:?}",
            witness.name
        );
    }
}
