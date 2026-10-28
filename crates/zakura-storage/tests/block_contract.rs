//! The block storage contract and its fixture blocks (storage.md ST4.1 and
//! ST4.2).

use std::{
    collections::BTreeSet,
    future::Future,
    num::{NonZeroU32, NonZeroU64},
    pin::pin,
    task::{Context, Poll, Waker},
};

use zakura_chain::{
    block,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
};
use zakura_header_chain::{
    BodyUnavailableSummary, BodyWorkAuthority, ConsensusBodyInvalid, EvidenceId, Frontier,
    OperatorBodyRetry, SourceId, StateVersion, TransientBodyFailure, TransientBodyFailureKind,
};
use zakura_network::zakura::{
    decode_block_payload_for_test, routine_accepts_body, BlockApplyResult, BlockSyncAction,
    BlockSyncMisbehavior, ZakuraPeerId, MSG_BS_BLOCK,
};
use zakura_node_services::{
    constants::MAX_CHECKPOINT_HEIGHT_GAP,
    header_chain::{ApplyHeaderTargetOutcome, Port},
};
use zakura_storage::{
    block::{
        fixture::{BlockFixture, BodySpec, FixtureBlock},
        ApplyClass, ApplyLimits, BlockOp, BlockOutcome, Commitment, Conditions, Gate,
        IllegalOutcome, RetryKind, Stage, Verdict,
    },
    header::adapter::EnginePort,
};

/// Polls `future` once. Every engine-port operation runs when first polled.
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("the engine port completes each operation on its first poll"),
    }
}

/// A body-work scope on the genesis anchor.
fn scope() -> BodyWorkAuthority {
    BodyWorkAuthority::for_snapshot(&EnginePort::new(BlockFixture::regtest().headers()).snapshot())
}

/// One action of each [`BlockSyncAction`] variant, in declaration order.
fn one_action_of_each() -> Vec<BlockSyncAction> {
    let fixture = BlockFixture::regtest();
    let block = fixture
        .block(fixture.anchor(), 0, BodySpec::exact(1_000))
        .block;
    let hash = block.hash();
    let owner = scope().bind(1, NonZeroU64::MIN);
    let source = SourceId::from_digest([1; 32]);
    let evidence = EvidenceId::from_digest([2; 32]);
    let version = StateVersion::new(1);
    vec![
        BlockSyncAction::QueryNeededBlocks {
            query_id: NonZeroU64::MIN,
            from: block::Height(1),
            limit: 16,
            best_header_tip: block::Height(1),
            scope: scope(),
        },
        BlockSyncAction::SubmitBlock {
            owner,
            source,
            token: 1,
            block,
        },
        BlockSyncAction::RecordBodyUnavailable {
            expected_version: version,
            failure: TransientBodyFailure {
                hash,
                evidence,
                kind: TransientBodyFailureKind::VerifierUnavailable,
                availability: BodyUnavailableSummary::default(),
            },
        },
        BlockSyncAction::RecordBodyInvalid {
            expected_version: version,
            invalid: ConsensusBodyInvalid {
                hash,
                evidence,
                rule: zakura_header_chain::BodyRuleId::new("block.subsidy"),
                source,
            },
        },
        BlockSyncAction::RestartBodyAvailability {
            expected_version: version,
            discovery: zakura_header_chain::BodySupplierDiscovered {
                hash,
                evidence,
                availability: BodyUnavailableSummary::default(),
            },
        },
        BlockSyncAction::RetryBodyAvailability {
            expected_version: version,
            retry: OperatorBodyRetry {
                hash,
                evidence,
                availability: BodyUnavailableSummary::default(),
            },
        },
        BlockSyncAction::Misbehavior {
            peer: ZakuraPeerId::new([3; 32]).expect("a 32-byte node ID is in bounds"),
            reason: BlockSyncMisbehavior::StatusSpam,
        },
    ]
}

/// ST4.1: every action variant becomes exactly one operation, distinct from
/// the others, and every operation but the range read is some action's.
#[test]
fn every_action_maps_to_one_operation() {
    let ops: Vec<BlockOp> = one_action_of_each().iter().map(BlockOp::of).collect();
    assert_eq!(ops, BlockOp::ALL[..7], "actions map to operations in order");
    for op in BlockOp::ALL {
        assert_eq!(
            op.contract().op,
            op,
            "{} indexes its own contract",
            op.name()
        );
    }
    let names: BTreeSet<_> = BlockOp::ALL.iter().map(|op| op.name()).collect();
    assert_eq!(
        names.len(),
        BlockOp::ALL.len(),
        "operation names are unique"
    );
}

const CLASSES: [ApplyClass; 2] = [ApplyClass::Checkpoint, ApplyClass::Full];

/// The verdict outcome for `verdict`.
const fn verdict(verdict: Verdict) -> BlockOutcome {
    BlockOutcome::Verdict(verdict)
}

/// The retryable outcome for `kind`.
const fn retry(kind: RetryKind) -> BlockOutcome {
    BlockOutcome::Verdict(Verdict::Retryable(kind))
}

/// The payload-mismatch outcomes, in canonical order.
fn mismatches() -> Vec<BlockOutcome> {
    Commitment::ALL
        .iter()
        .map(|commitment| verdict(Verdict::PayloadMismatch(*commitment)))
        .collect()
}

/// ST4.1: every operation's rows, outcome by outcome, with their gates.
#[test]
fn the_contract_pins_its_production_rows() {
    // Only a submission has two stages, and only a misbehavior report has
    // none.
    for op in BlockOp::ALL {
        let stages: Vec<_> = op.contract().stages.iter().map(|s| s.stage).collect();
        let expected = match op {
            BlockOp::SubmitBlock => vec![Stage::Queued, Stage::Admitted],
            BlockOp::Misbehavior => vec![],
            _ => vec![Stage::Pending],
        };
        assert_eq!(stages, expected, "{} stages", op.name());
    }
    let rows = |op: BlockOp, stage, class| op.outcomes(stage, class);
    for class in CLASSES {
        // The driver awaits a query or record alone, so neither needs a gate.
        assert_eq!(
            rows(BlockOp::QueryNeededBlocks, Stage::Pending, class),
            [
                (BlockOutcome::Answer, Gate::Always),
                (BlockOutcome::Fail, Gate::Always)
            ]
        );
        for op in [
            BlockOp::RecordBodyUnavailable,
            BlockOp::RestartBodyAvailability,
            BlockOp::RetryBodyAvailability,
        ] {
            assert_eq!(
                rows(op, Stage::Pending, class),
                [
                    (BlockOutcome::Apply, Gate::Always),
                    (BlockOutcome::Fail, Gate::Always)
                ],
                "{}",
                op.name()
            );
        }
        // A lost invalid-body record shuts the driver down, which is
        // excluded, so it has no state-error outcome.
        assert_eq!(
            rows(BlockOp::RecordBodyInvalid, Stage::Pending, class),
            [(BlockOutcome::Apply, Gate::Always)]
        );
        assert_eq!(
            rows(BlockOp::ReadRange, Stage::Pending, class),
            [
                (BlockOutcome::Serve, Gate::Always),
                (BlockOutcome::Error, Gate::Always)
            ]
        );
        // A queued body only moves on; it has no verdict.
        assert_eq!(
            rows(BlockOp::SubmitBlock, Stage::Queued, class),
            [(BlockOutcome::Admit, Gate::AdmitNext)]
        );
    }
    let mut full = vec![
        (verdict(Verdict::Committed), Gate::ParentCommitted),
        (verdict(Verdict::Duplicate), Gate::KnownToState),
    ];
    full.extend(
        mismatches()
            .into_iter()
            .map(|outcome| (outcome, Gate::ParentCommitted)),
    );
    full.extend([
        (verdict(Verdict::ConsensusInvalid), Gate::NewAtAdmission),
        (retry(RetryKind::MissingContext), Gate::Always),
        (retry(RetryKind::Canceled), Gate::Always),
        (retry(RetryKind::Storage), Gate::NotCommitted),
        (retry(RetryKind::VerifierUnavailable), Gate::Always),
        (verdict(Verdict::Ambiguous), Gate::ParentCommitted),
    ]);
    assert_eq!(
        rows(BlockOp::SubmitBlock, Stage::Admitted, ApplyClass::Full),
        full
    );
    // Only the checkpoint verifier exhausts its queue.
    let mut checkpoint = full.clone();
    checkpoint.insert(
        checkpoint.len() - 1,
        (retry(RetryKind::ResourceExhausted), Gate::Always),
    );
    assert_eq!(
        rows(
            BlockOp::SubmitBlock,
            Stage::Admitted,
            ApplyClass::Checkpoint
        ),
        checkpoint
    );
    // No verifier times out.
    for class in CLASSES {
        assert!(!class
            .retry_kinds()
            .iter()
            .any(|kind| kind.kind() == TransientBodyFailureKind::Timeout));
    }
}

/// ST4.1: each gate opens under exactly the conditions the module doc
/// names, shown on concrete conditions.
#[test]
fn each_gate_opens_under_its_conditions() {
    let none = Conditions::default();
    let room = Conditions {
        oldest_with_room: true,
        ..none
    };
    let parent = Conditions {
        parent_committed: true,
        ..none
    };
    let committed = Conditions {
        committed: true,
        ..parent
    };
    let cases = [
        (Gate::Always, none, true),
        (Gate::Always, committed, true),
        (Gate::AdmitNext, room, true),
        (Gate::AdmitNext, none, false),
        (Gate::ParentCommitted, parent, true),
        (Gate::ParentCommitted, none, false),
        (Gate::ParentCommitted, committed, false),
        (Gate::KnownToState, committed, true),
        (
            Gate::KnownToState,
            Conditions {
                replaced: true,
                ..none
            },
            true,
        ),
        (
            Gate::KnownToState,
            Conditions {
                older_in_flight: true,
                ..none
            },
            true,
        ),
        (Gate::KnownToState, parent, false),
        (Gate::NotCommitted, parent, true),
        (Gate::NotCommitted, committed, false),
        // A commit after admission leaves the verifier past its check.
        (Gate::NewAtAdmission, committed, true),
        (
            Gate::NewAtAdmission,
            Conditions {
                committed_at_admission: true,
                ..committed
            },
            false,
        ),
    ];
    for (gate, conditions, open) in cases {
        assert_eq!(
            gate.open(&conditions),
            open,
            "{gate:?} under {conditions:?}"
        );
    }
}

/// ST4.1: the outcomes legal for a submission in concrete situations, and
/// `check` refuses the rest, including another stage's outcomes.
#[test]
fn a_submission_has_the_outcomes_its_situation_allows() {
    let admitted = |class, conditions: Conditions| {
        BlockOp::SubmitBlock.enabled(Stage::Admitted, class, &conditions)
    };
    // An admitted body on a committed parent may commit or fail any way
    // but as a duplicate.
    let fresh = Conditions {
        parent_committed: true,
        ..Conditions::default()
    };
    let mut expected = vec![verdict(Verdict::Committed)];
    expected.extend(mismatches());
    expected.extend([
        verdict(Verdict::ConsensusInvalid),
        retry(RetryKind::MissingContext),
        retry(RetryKind::Canceled),
        retry(RetryKind::Storage),
        retry(RetryKind::VerifierUnavailable),
        verdict(Verdict::Ambiguous),
    ]);
    assert_eq!(admitted(ApplyClass::Full, fresh), expected);
    // Without its parent it can't commit, and state can't check its
    // commitments. A failed ancestor can still fail it with `Storage`.
    let orphan = vec![
        verdict(Verdict::ConsensusInvalid),
        retry(RetryKind::MissingContext),
        retry(RetryKind::Canceled),
        retry(RetryKind::Storage),
        retry(RetryKind::VerifierUnavailable),
    ];
    assert_eq!(admitted(ApplyClass::Full, Conditions::default()), orphan);
    // Once another submission commits it after admission, only the
    // duplicate and the verifier's failures remain.
    let committed = Conditions {
        committed: true,
        ..fresh
    };
    let known = vec![
        verdict(Verdict::Duplicate),
        verdict(Verdict::ConsensusInvalid),
        retry(RetryKind::MissingContext),
        retry(RetryKind::Canceled),
        retry(RetryKind::VerifierUnavailable),
    ];
    assert_eq!(admitted(ApplyClass::Full, committed), known);
    let mut known_checkpoint = known.clone();
    known_checkpoint.push(retry(RetryKind::ResourceExhausted));
    assert_eq!(
        admitted(ApplyClass::Checkpoint, committed),
        known_checkpoint
    );
    // A hash committed when the verifier received it never reaches a
    // consensus rule: both verifiers report the duplicate first.
    let committed_at_admission = Conditions {
        committed_at_admission: true,
        ..committed
    };
    for (class, known) in [
        (ApplyClass::Full, &known),
        (ApplyClass::Checkpoint, &known_checkpoint),
    ] {
        let mut expected = known.clone();
        expected.retain(|outcome| *outcome != verdict(Verdict::ConsensusInvalid));
        assert_eq!(
            admitted(class, committed_at_admission),
            expected,
            "{class:?}"
        );
    }
    // An older admission of the same hash makes a duplicate legal too.
    let mut both = expected.clone();
    both.insert(1, verdict(Verdict::Duplicate));
    assert_eq!(
        admitted(
            ApplyClass::Full,
            Conditions {
                older_in_flight: true,
                ..fresh
            }
        ),
        both
    );

    // The driver admits only while it awaits nothing.
    let next = Conditions {
        oldest_with_room: true,
        ..Conditions::default()
    };
    let queued = |conditions: Conditions| {
        BlockOp::SubmitBlock.enabled(Stage::Queued, ApplyClass::Full, &conditions)
    };
    assert_eq!(queued(next), [BlockOutcome::Admit]);
    assert_eq!(
        queued(Conditions {
            driver_awaiting: true,
            ..next
        }),
        []
    );
    assert_eq!(queued(Conditions::default()), []);
    // Nor does it take a verdict then.
    let parent = Conditions {
        parent_committed: true,
        ..Conditions::default()
    };
    let admitted = |conditions: Conditions| {
        BlockOp::SubmitBlock.enabled(Stage::Admitted, ApplyClass::Full, &conditions)
    };
    assert!(admitted(parent).contains(&verdict(Verdict::Committed)));
    assert_eq!(
        admitted(Conditions {
            driver_awaiting: true,
            ..parent
        }),
        []
    );
    assert!(Stage::Queued.needs_free_driver() && Stage::Admitted.needs_free_driver());
    assert!(!Stage::Pending.needs_free_driver());

    // `check` refuses an outcome the stage lacks or the gate closes.
    assert_eq!(
        BlockOp::SubmitBlock.check(
            Stage::Queued,
            ApplyClass::Full,
            &next,
            verdict(Verdict::Committed)
        ),
        Err(IllegalOutcome {
            op: BlockOp::SubmitBlock,
            stage: Stage::Queued,
            outcome: verdict(Verdict::Committed),
        })
    );
    assert!(BlockOp::SubmitBlock
        .check(
            Stage::Admitted,
            ApplyClass::Full,
            &fresh,
            BlockOutcome::Admit
        )
        .is_err());
    assert!(BlockOp::SubmitBlock
        .check(
            Stage::Admitted,
            ApplyClass::Full,
            &committed,
            verdict(Verdict::Committed)
        )
        .is_err());
    assert!(BlockOp::Misbehavior
        .check(
            Stage::Pending,
            ApplyClass::Full,
            &fresh,
            BlockOutcome::Apply
        )
        .is_err());
    assert!(BlockOp::QueryNeededBlocks
        .check(
            Stage::Pending,
            ApplyClass::Full,
            &fresh,
            BlockOutcome::Answer
        )
        .is_ok());
}

/// ST4.1: each verdict's outcome reports the result the driver reports for
/// it, attributes a rejection to the supplier, and carries the driver's
/// stable evidence for the same inputs.
#[test]
fn verdicts_report_the_drivers_results() {
    let owner = scope().bind(4, NonZeroU64::MIN);
    let source = SourceId::from_digest([9; 32]);
    let hash = block::Hash([5; 32]);
    let mut verdicts = vec![
        (Verdict::Committed, BlockApplyResult::Committed),
        (Verdict::Duplicate, BlockApplyResult::Duplicate),
        (
            Verdict::PayloadMismatch(Commitment::AuthDataRoot),
            BlockApplyResult::Rejected,
        ),
        (
            Verdict::PayloadMismatch(Commitment::ChainHistoryBlockTxAuthCommitment),
            BlockApplyResult::Rejected,
        ),
        (
            Verdict::PayloadMismatch(Commitment::FinalSaplingRoot),
            BlockApplyResult::Rejected,
        ),
        (Verdict::ConsensusInvalid, BlockApplyResult::Rejected),
        (Verdict::Ambiguous, BlockApplyResult::Unavailable),
    ];
    for kind in ApplyClass::Full.retry_kinds() {
        verdicts.push((Verdict::Retryable(*kind), BlockApplyResult::Unavailable));
    }
    let mut evidence = BTreeSet::new();
    for (verdict, result) in verdicts {
        let outcome = verdict.outcome(owner, source, hash, ApplyClass::Full);
        assert_eq!(outcome.result(), result, "{verdict:?}");
        // Ambiguous reports exactly what a verifier-unavailable retry does.
        if verdict != Verdict::Ambiguous {
            assert!(evidence.insert(outcome.evidence().digest()), "{verdict:?}");
        }
    }
    assert_eq!(
        Verdict::Ambiguous.outcome(owner, source, hash, ApplyClass::Full),
        Verdict::Retryable(RetryKind::VerifierUnavailable).outcome(
            owner,
            source,
            hash,
            ApplyClass::Full
        )
    );
    // Block sync disconnects a supplier only for a mismatch that proves
    // tampering.
    let tampering: Vec<_> = Commitment::ALL
        .iter()
        .map(|commitment| commitment.kind().proves_tampered_body())
        .collect();
    assert_eq!(tampering, [true, true, false]);
    assert!(Verdict::Committed.commits() && Verdict::Ambiguous.commits());
    assert!(!Verdict::Duplicate.commits());
}

/// ST4.1: the apply limits admit by class as `drain_pending_block_applies`
/// does, with room for two checkpoint ranges. The driver clamps the
/// checkpoint limit to exactly `MAX_CHECKPOINT_HEIGHT_GAP`, and the others
/// to at least 1 (zakurad's `components/sync.rs`).
#[test]
fn apply_limits_admit_by_class() {
    let limits = ApplyLimits::clamped(1, 0, 3, block::Height(10));
    assert_eq!(
        (limits.checkpoint, limits.full, limits.combined),
        (MAX_CHECKPOINT_HEIGHT_GAP, 1, 3)
    );
    let raised = ApplyLimits::clamped(10_000, 5, 0, block::Height(10));
    assert_eq!(
        (raised.checkpoint, raised.full, raised.combined),
        (MAX_CHECKPOINT_HEIGHT_GAP, 5, 1)
    );
    assert_eq!(
        ApplyClass::of(block::Height(10), limits.max_checkpoint_height),
        ApplyClass::Checkpoint
    );
    assert_eq!(
        ApplyClass::of(block::Height(11), limits.max_checkpoint_height),
        ApplyClass::Full
    );
    // Full: under the full and combined limits.
    assert!(limits.has_room(ApplyClass::Full, 0, 0));
    assert!(!limits.has_room(ApplyClass::Full, 0, 1));
    assert!(!limits.has_room(ApplyClass::Full, 3, 0));
    // Checkpoint: two ranges' worth, past the combined limit.
    let pipeline = 2 * MAX_CHECKPOINT_HEIGHT_GAP;
    assert!(limits.has_room(ApplyClass::Checkpoint, 3, 0));
    assert!(limits.has_room(ApplyClass::Checkpoint, pipeline - 1, 0));
    assert!(!limits.has_room(ApplyClass::Checkpoint, pipeline, 0));
    assert!(!limits.has_room(ApplyClass::Checkpoint, pipeline - 1, 1));
}

/// Decodes `block`'s bytes as a routine decodes a `Block` payload, and
/// whether the routine's own checks accept it from a frame authorized for
/// `height`, when the outstanding request expects `expected` there.
fn routine_accepts_at(
    block: &FixtureBlock,
    height: block::Height,
    expected: Option<block::Hash>,
) -> bool {
    let mut payload = vec![MSG_BS_BLOCK];
    payload.extend(block.bytes());
    let (decoded, _) = decode_block_payload_for_test(payload).expect("a fixture payload decodes");
    assert_eq!(*decoded, *block.block, "the payload decodes to the block");
    routine_accepts_body(&decoded, height, expected)
}

/// Whether the routine accepts `block` for a request that names its
/// header's hash at its height.
fn routine_accepts(block: &FixtureBlock) -> bool {
    let requested = block.block.header.hash();
    routine_accepts_at(block, block.height, Some(requested))
}

/// ST4.2: a fixture chain links parent to child, its bodies decode to
/// themselves at exactly their sizes, and every body passes the routine's
/// checks.
#[test]
fn fixture_bodies_link_decode_and_pass_the_routine_checks() {
    let fixture = BlockFixture::regtest();
    let anchor = fixture.anchor();
    let min = BlockFixture::min_body_size(block::Height(1));
    // Sizes either side of the padding script's length-prefix steps, and the
    // largest block.
    let sizes = [
        min,
        min + 1,
        min + 252,
        min + 253,
        min + 300,
        min + 65_540,
        u32::try_from(block::MAX_BLOCK_BYTES).expect("the block limit fits u32"),
    ];
    let specs: Vec<_> = sizes.iter().map(|size| BodySpec::exact(*size)).collect();
    let chain = fixture.extend(anchor, &specs, 0);
    let mut parent = anchor;
    for (block, size) in chain.iter().zip(sizes) {
        assert_eq!(
            block.height,
            parent.height.next().expect("heights stay small")
        );
        assert_eq!(block.block.header.previous_block_hash, parent.hash);
        assert_eq!(block.size(), size);
        let bytes = block.bytes();
        assert_eq!(bytes.len(), size as usize);
        let decoded: block::Block = bytes.zcash_deserialize_into().expect("a body decodes");
        assert_eq!(decoded, *block.block);
        assert_eq!(decoded.hash(), block.hash());
        assert!(routine_accepts(block));
        parent = block.frontier();
    }
}

/// ST4.2: bodies at heights that need multi-byte coinbase height pushes
/// still decode, two branches never share a block at a height, and the
/// routine rejects a body for another height or hash.
#[test]
fn fixture_heights_and_branches_stay_distinct() {
    let fixture = BlockFixture::regtest();
    let anchor = fixture.anchor();
    let spec = BodySpec::exact(BlockFixture::min_body_size(block::Height(70_000)) + 10);
    let main = fixture.uniform(anchor, 20, spec, 0);
    let fork = fixture.uniform(anchor, 20, spec, 1);
    for (left, right) in main.iter().zip(&fork) {
        assert_eq!(left.height, right.height);
        assert_ne!(left.hash(), right.hash());
    }
    // The routine rejects a body for another height, for a sibling's
    // hash, or where the request expects no body.
    let next = main[0].height.next().expect("heights stay small");
    assert!(!routine_accepts_at(&main[0], next, Some(main[0].hash())));
    assert!(!routine_accepts_at(
        &main[0],
        main[0].height,
        Some(fork[0].hash())
    ));
    assert!(!routine_accepts_at(&main[0], main[0].height, None));
    // A far parent exercises the 3-byte height push.
    let far = Frontier::new(block::Height(69_999), main[0].hash());
    let block = fixture.block(far, 0, spec);
    assert_eq!(block.height, block::Height(70_000));
    assert!(routine_accepts(&block));
    assert_eq!(block.block.zcash_serialized_size(), spec.size as usize);
}

/// Prepares and applies `blocks`' headers on `parent` through `port`.
fn insert(port: &EnginePort, parent: Frontier, blocks: &[FixtureBlock], request_id: u64) {
    let request =
        BlockFixture::regtest().target_request(&port.snapshot(), parent, blocks, request_id);
    let prepared = ready(port.prepare_header_target(request)).expect("fixture headers prepare");
    assert_eq!(
        ready(port.apply_header_target(prepared)).expect("fixture headers apply"),
        ApplyHeaderTargetOutcome::Applied
    );
}

/// ST4.2: the engine accepts a fixture chain's headers, with their hints,
/// and selects a longer fork.
#[test]
fn the_engine_accepts_fixture_headers() {
    let fixture = BlockFixture::regtest();
    let port = EnginePort::new(fixture.headers());
    let anchor = fixture.anchor();
    let main = fixture.uniform(
        anchor,
        8,
        BodySpec {
            size: 2_000,
            hint: 500,
        },
        0,
    );
    insert(&port, anchor, &main, 1);
    for block in &main {
        assert_eq!(
            port.advertised_body_size(block.hash()),
            NonZeroU32::new(500)
        );
    }
    assert_eq!(
        port.snapshot().frontiers.header_best,
        main.last().expect("eight blocks").frontier()
    );
    let fork_parent = main[1].frontier();
    let fork = fixture.uniform(fork_parent, 7, BodySpec::exact(1_500), 1);
    insert(&port, fork_parent, &fork, 2);
    assert_eq!(
        port.advertised_body_size(fork[0].hash()),
        NonZeroU32::new(1_500)
    );
    assert_eq!(
        port.snapshot().frontiers.header_best,
        fork.last().expect("seven blocks").frontier()
    );
}
