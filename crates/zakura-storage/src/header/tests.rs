use std::{num::NonZeroU64, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use zakura_chain::{parameters::Network, serialization::ZcashDeserializeInto};
use zakura_header_chain::{
    BodyWorkAuthority, BodyWorkEpoch, BranchId, ErrorCategory, Frontier, HeaderBatchInput,
    HeaderContextFact, HeaderGeneration, HeaderRules, HeaderWorkAuthority, InsertHeaders, SourceId,
    TargetCompletion, ValidationLease, VerifiedGeneration,
};
use zakura_node_services::header_chain::{AdapterKey, TargetEntries};
use zakura_test::vectors::{BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_GENESIS_BYTES};

use super::*;

/// Times to check each outcome at: submission, just before and at the adapter
/// deadline, and the VCT repair watchdog.
const TIMES: [Duration; 4] = [
    Duration::ZERO,
    ADAPTER_DEADLINE.saturating_sub(Duration::from_nanos(1)),
    ADAPTER_DEADLINE,
    VCT_REPAIR_WATCHDOG,
];

const FAULTS: [Fault; 3] = [Fault::Timeout, Fault::Unavailable, Fault::LocalResource];

/// A clock fixed at one instant, so fixture preparation never reads the host
/// clock.
struct FixedClock(DateTime<Utc>);

impl zakura_header_chain::Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}

/// A normal header-target owner on a made-up branch.
fn owner() -> HeaderSyncWorkOwner {
    HeaderWorkAuthority {
        header_generation: HeaderGeneration::new(2),
        branch: BranchId::new(block::Hash([1; 32]), block::Hash([2; 32])),
    }
    .bind(3, NonZeroU64::new(4).expect("four is nonzero"))
    .into()
}

/// Mainnet block 1 prepared on genesis, sealed with `key` for `owner`, so an
/// apply request holds a real sealed target.
fn prepared_target(key: &AdapterKey, owner: HeaderSyncWorkOwner) -> PreparedHeaderTarget {
    let genesis: Arc<block::Block> = BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into()
        .expect("the genesis vector deserializes");
    let block_1: Arc<block::Block> = BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .expect("the block 1 vector deserializes");
    let parent = Frontier::new(block::Height(0), genesis.hash());
    let lease = ValidationLease::new(
        parent,
        vec![HeaderContextFact {
            frontier: parent,
            header: genesis.header.clone(),
        }],
        Network::Mainnet,
        [0; 32],
    );
    let rules = HeaderRules::for_validation_lease(&lease).expect("Mainnet has a PoW policy");
    let batch = zakura_header_chain::prepare_headers(
        HeaderBatchInput::new(std::slice::from_ref(&block_1.header)),
        parent,
        &rules,
        &FixedClock(block_1.header.time),
    )
    .expect("Mainnet block 1 prepares on genesis");
    PreparedHeaderTarget::from_insert(
        key,
        Box::new(InsertHeaders {
            owner,
            source: SourceId::from_digest([5; 32]),
            parent_hash: parent.hash,
            target_tip_hash: block_1.hash(),
            completion: TargetCompletion::TargetComplete {
                common_ancestor: parent,
            },
            batch,
            aux: Vec::new(),
        }),
    )
}

/// One request per operation, in [`HeaderOp::ALL`] order, with path handles
/// that `key` issued.
fn requests(key: &AdapterKey) -> Vec<HeaderRequest> {
    let source = SourceId::from_digest([6; 32]);
    let ancestor = Frontier::new(block::Height(1), block::Hash([1; 32]));
    let target = Frontier::new(block::Height(2), block::Hash([2; 32]));
    let scope = owner().header_authority();
    let path = RetainedHeaderPath::from_adapter(key, 7, source, 8, ancestor, target, scope);
    let permit = Arc::new(zakura_rt::sync::Semaphore::new(1))
        .try_acquire_owned()
        .expect("a fresh semaphore has a permit");
    vec![
        HeaderRequest::ContinuationLocator {
            permit: LocatorReadPermit::new(permit),
        },
        HeaderRequest::VctRepairContext {
            owner: BodyWorkAuthority {
                header: scope,
                verified_generation: VerifiedGeneration::new(3),
                body_work_epoch: BodyWorkEpoch::default(),
            }
            .bind(8, NonZeroU64::new(9).expect("nine is nonzero")),
            height: target.height,
        },
        HeaderRequest::AcquireHeaderPath(AcquirePath {
            source,
            session_id: 8,
            scope,
            target_tip_hash: target.hash,
            locator_hashes: vec![ancestor.hash],
        }),
        HeaderRequest::ReadHeaderPath {
            path: path.clone(),
            request: ReadPath {
                after_hash: ancestor.hash,
                max_header_count: 1,
                want_tree_aux: false,
            },
        },
        HeaderRequest::ReleaseHeaderPath { path },
        HeaderRequest::PrepareHeaderTarget(PrepareHeaderTarget {
            source,
            owner: owner(),
            common_ancestor: ancestor,
            target,
            entries: TargetEntries::try_from(Vec::new()).expect("no entries fit the cap"),
            completion: TargetCompletion::TargetComplete {
                common_ancestor: ancestor,
            },
        }),
        HeaderRequest::ApplyHeaderTarget {
            target: prepared_target(key, owner()),
        },
        HeaderRequest::SelectedChainContains { hash: target.hash },
    ]
}

#[test]
fn requests_cover_every_operation_in_order() {
    let ops: Vec<_> = requests(&AdapterKey::new())
        .iter()
        .map(HeaderRequest::op)
        .collect();
    assert_eq!(ops, HeaderOp::ALL);
    for op in HeaderOp::ALL {
        assert_eq!(
            op.contract().op,
            op,
            "{} has another op's contract",
            op.name()
        );
    }
}

#[test]
fn enabled_starts_with_engine_and_repeats() {
    for op in HeaderOp::ALL {
        for t in TIMES {
            let enabled: Vec<_> = op.enabled(t).collect();
            assert_eq!(enabled.first(), Some(&Outcome::Engine), "{}", op.name());
            assert_eq!(enabled, op.enabled(t).collect::<Vec<_>>(), "{}", op.name());
            let mut unique = enabled.clone();
            unique.sort();
            unique.dedup();
            assert_eq!(
                unique.len(),
                enabled.len(),
                "{} repeats an outcome",
                op.name()
            );
        }
    }
}

/// Every legal fault builds a reply of the request's own operation, and every
/// other (operation, fault, time) is refused by both `check` and
/// `fault_reply`.
#[test]
fn fault_replies_match_the_contract() {
    let key = AdapterKey::new();
    for request in requests(&key) {
        let op = request.op();
        for t in TIMES {
            for fault in FAULTS {
                let enabled = request.enabled(&key, t);
                assert_eq!(enabled, op.enabled(t).collect::<Vec<_>>());
                let legal = enabled.contains(&Outcome::Fault(fault));
                assert_eq!(op.check(Outcome::Fault(fault), t).is_ok(), legal);
                assert_eq!(request.check(&key, Outcome::Fault(fault), t).is_ok(), legal);
                match request.fault_reply(&key, fault, t) {
                    Ok(reply) => {
                        assert!(legal, "{} built an illegal {fault:?} at {t:?}", op.name());
                        assert_eq!(reply.op(), op);
                    }
                    Err(illegal) => {
                        assert!(!legal, "{} refused a legal {fault:?} at {t:?}", op.name());
                        assert_eq!(
                            illegal,
                            IllegalOutcome {
                                op,
                                outcome: Outcome::Fault(fault),
                                since_submit: t,
                            }
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn timeout_waits_for_the_adapter_deadline() {
    let just_before = ADAPTER_DEADLINE - Duration::from_nanos(1);
    for op in HeaderOp::ALL {
        let gated = op.contract().faults.iter().any(|&(fault, gate)| {
            fault == Fault::Timeout && gate == FaultGate::AfterAdapterDeadline
        });
        let is_target = matches!(
            op,
            HeaderOp::PrepareHeaderTarget | HeaderOp::ApplyHeaderTarget
        );
        assert_eq!(gated, !is_target, "{}", op.name());
        let timeout = Outcome::Fault(Fault::Timeout);
        assert!(op.check(timeout, just_before).is_err(), "{}", op.name());
        assert_eq!(op.check(timeout, ADAPTER_DEADLINE).is_ok(), gated);
    }
}

#[test]
fn prepare_and_apply_fault_only_with_local_resource() {
    for op in [HeaderOp::PrepareHeaderTarget, HeaderOp::ApplyHeaderTarget] {
        for t in TIMES {
            assert_eq!(
                op.enabled(t).collect::<Vec<_>>(),
                [Outcome::Engine, Outcome::Fault(Fault::LocalResource)],
            );
        }
    }
    for op in HeaderOp::ALL {
        if !matches!(
            op,
            HeaderOp::PrepareHeaderTarget | HeaderOp::ApplyHeaderTarget
        ) {
            assert_eq!(
                op.enabled(Duration::ZERO).collect::<Vec<_>>(),
                [Outcome::Engine, Outcome::Fault(Fault::Unavailable)],
                "{}",
                op.name()
            );
        }
    }
}

/// The local-resource fault matches the production adapter's value: a local
/// resource failure on the request owner's branch, blamed on no peer.
#[test]
fn local_resource_names_the_owner_branch() {
    let key = AdapterKey::new();
    for request in requests(&key) {
        let Some(owner) = request.owner() else {
            continue;
        };
        let error = match request.fault_reply(&key, Fault::LocalResource, Duration::ZERO) {
            Ok(HeaderReply::PrepareHeaderTarget(Err(error)))
            | Ok(HeaderReply::ApplyHeaderTarget(Err(error))) => error,
            other => panic!("{} gave {other:?}", request.op().name()),
        };
        assert_eq!(error.category, ErrorCategory::LocalResourceOrStorage);
        assert_eq!(
            error.subject,
            ErrorSubject::Branch(owner.header_authority().branch)
        );
        assert!(!error.is_automatic_header_peer_fault());
    }
}

#[test]
fn only_prepare_and_apply_report_an_owner() {
    let key = AdapterKey::new();
    for request in requests(&key) {
        let has_owner = matches!(
            request.op(),
            HeaderOp::PrepareHeaderTarget | HeaderOp::ApplyHeaderTarget
        );
        assert_eq!(
            request.owner().is_some(),
            has_owner,
            "{}",
            request.op().name()
        );
        if has_owner {
            assert_eq!(request.owner(), Some(owner()));
        }
    }
}

/// The three deadline kinds land where the reactor and adapter put them. An
/// adapter deadline bounds one state read, so a page read lists each of its
/// five reads as its own stage with a fresh 30 s deadline.
#[test]
fn deadlines_follow_production() {
    use HeaderOp::*;
    for op in HeaderOp::ALL {
        let deadlines = op.contract().deadlines;
        let reactor = deadlines
            .iter()
            .any(|d| matches!(d, Deadline::Reactor { .. }));
        let watchdog = deadlines
            .iter()
            .any(|d| matches!(d, Deadline::FatalWatchdog { .. }));
        let stages: Vec<_> = deadlines
            .iter()
            .filter_map(|d| match d {
                Deadline::Adapter { stage, after } => {
                    assert_eq!(*after, ADAPTER_DEADLINE);
                    Some(*stage)
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            reactor,
            matches!(op, ContinuationLocator | VctRepairContext),
            "{}",
            op.name()
        );
        assert_eq!(
            watchdog,
            matches!(op, PrepareHeaderTarget | ApplyHeaderTarget),
            "{}",
            op.name()
        );
        let expected: &[AdapterStage] = match op {
            ReadHeaderPath => &[
                AdapterStage::StateRead,
                AdapterStage::FinalizedBodySizesRead,
                AdapterStage::FinalizedTipRead,
                AdapterStage::FinalizedHashRead,
                AdapterStage::FinalizedRootsRead,
            ],
            PrepareHeaderTarget => &[AdapterStage::ValidationLeaseRead],
            ApplyHeaderTarget => &[],
            _ => &[AdapterStage::StateRead],
        };
        assert_eq!(stages, expected, "{}", op.name());
        for (i, stage) in stages.iter().enumerate() {
            assert!(
                !stages[i + 1..].contains(stage),
                "{} repeats {stage:?}",
                op.name()
            );
        }
        // Only the body-size stage serves a degraded reply instead of failing.
        for stage in stages {
            assert_eq!(
                stage.degrades(),
                stage == AdapterStage::FinalizedBodySizesRead,
                "{stage:?}"
            );
        }
    }
}

/// Every operation that owns capacity names the events that end it, and
/// shutdown ends all of it.
#[test]
fn owned_capacity_has_release_events() {
    use HeaderOp::*;
    for op in HeaderOp::ALL {
        let releases = op.contract().releases;
        let expected: &[Release] = match op {
            ContinuationLocator => &[Release::LocatorPermitOnFinish, Release::Shutdown],
            AcquireHeaderPath => &[
                Release::LeaseAbandonedAcquire,
                Release::LeaseIdleExpired,
                Release::Shutdown,
            ],
            ReadHeaderPath => &[
                Release::LeaseConsumedByRead,
                Release::LeaseIdleExpired,
                Release::Shutdown,
            ],
            ReleaseHeaderPath => &[
                Release::LeaseReleased,
                Release::LeaseIdleExpired,
                Release::Shutdown,
            ],
            _ => &[],
        };
        assert_eq!(releases, expected, "{}", op.name());
    }
}

#[test]
fn outcome_names_are_distinct_identity_parts() {
    let names: Vec<_> = [
        Outcome::Engine,
        Outcome::Fault(Fault::Timeout),
        Outcome::Fault(Fault::Unavailable),
        Outcome::Fault(Fault::LocalResource),
        Outcome::Rejected,
    ]
    .map(Outcome::name)
    .into();
    assert_eq!(
        names,
        [
            "engine",
            "timeout",
            "unavailable",
            "local-resource",
            "rejected"
        ]
    );
    for name in names {
        assert!(!name.contains(':'), "{name} would split a storage identity");
    }
}

/// A second acquire by a peer that holds a lease waits on capacity, as
/// `capacity_signals_follow_reservations_and_capacity_classes` in
/// zakura-state shows. Only a stale scope or exhausted IDs give `Busy`.
#[test]
fn a_held_lease_refuses_with_capacity_busy() {
    let busy: Vec<_> = AcquireRefusal::ALL
        .into_iter()
        .filter(|refusal| !refusal.waits_for_capacity())
        .collect();
    assert!(AcquireRefusal::PeerHoldsLease.waits_for_capacity());
    assert_eq!(
        busy,
        [AcquireRefusal::IdsExhausted, AcquireRefusal::ScopeAhead]
    );
}

/// Idle expiry isn't per peer. Peer A's idle lease ends when peer B acquires
/// or reads, so B can take the slot, as the "expired slots are reclaimed"
/// step of `retained_path_leases_are_exact_bounded_session_scoped_and_expiring`
/// in zakura-state shows. Every operation that holds a lease can lose it this
/// way, and only acquire and read sweep.
#[test]
fn any_peers_acquire_or_read_sweeps_idle_leases() {
    use HeaderOp::*;
    for op in HeaderOp::ALL {
        let contract = op.contract();
        assert_eq!(
            contract.sweeps_idle_leases,
            matches!(op, AcquireHeaderPath | ReadHeaderPath),
            "{}",
            op.name()
        );
        assert_eq!(
            contract.releases.contains(&Release::LeaseIdleExpired),
            matches!(op, AcquireHeaderPath | ReadHeaderPath | ReleaseHeaderPath),
            "{}",
            op.name()
        );
    }
    // A's lease, held by its pending read, ends when B's acquire runs first.
    let a_holds = ReadHeaderPath.contract();
    let b_runs = AcquireHeaderPath.contract();
    assert!(b_runs.sweeps_idle_leases && a_holds.releases.contains(&Release::LeaseIdleExpired));
}

/// B's acquire with an empty locator fails `LocatorCount` before
/// `RetainedPathLeaseRegistry::reserve`, so it leaves A's expired lease and
/// its capacity signal untouched. Only a request that passes every input
/// check sweeps.
#[test]
fn a_failed_input_check_skips_the_sweep() {
    use HeaderOp::*;
    for op in HeaderOp::ALL {
        let contract = op.contract();
        let expected: &[InputCheck] = match op {
            AcquireHeaderPath => &[InputCheck::LocatorCount],
            ReadHeaderPath => &[InputCheck::AdapterHandle, InputCheck::PageCount],
            ReleaseHeaderPath => &[InputCheck::AdapterHandle],
            ApplyHeaderTarget => &[InputCheck::PreparedTargetSeal],
            _ => &[],
        };
        assert_eq!(contract.input_checks, expected, "{}", op.name());
        assert_eq!(contract.sweeps(None), contract.sweeps_idle_leases);
        for &check in contract.input_checks {
            assert!(!contract.sweeps(Some(check)), "{} {check:?}", op.name());
        }
    }
    let b_acquire = AcquireHeaderPath.contract();
    assert!(!b_acquire.sweeps(Some(InputCheck::LocatorCount)));
    assert!(b_acquire.sweeps(None));
}

/// Rule A: a read or release with another adapter's handle, or an apply of a
/// target another adapter sealed, gets the adapter's rejection before any
/// deadline starts. So at 30 s it still has no `timeout`, no fault and no
/// engine result. With the issuing adapter's key, nothing is rejected.
#[test]
fn foreign_adapter_inputs_have_only_the_rejection() {
    use HeaderOp::*;
    let ours = AdapterKey::new();
    let theirs = AdapterKey::new();
    for request in requests(&theirs) {
        let op = request.op();
        assert_eq!(request.rejected_by(&theirs), None, "{}", op.name());
        assert!(request.rejection_reply(&theirs).is_none());
        let expected = match op {
            ReadHeaderPath | ReleaseHeaderPath => InputCheck::AdapterHandle,
            ApplyHeaderTarget => InputCheck::PreparedTargetSeal,
            _ => {
                assert_eq!(request.rejected_by(&ours), None, "{}", op.name());
                continue;
            }
        };
        assert_eq!(request.rejected_by(&ours), Some(expected), "{}", op.name());
        assert!(!op.contract().sweeps(request.rejected_by(&ours)));
        for t in TIMES {
            assert_eq!(
                request.enabled(&ours, t),
                [Outcome::Rejected],
                "{}",
                op.name()
            );
            assert!(request.check(&ours, Outcome::Rejected, t).is_ok());
            assert!(request.check(&ours, Outcome::Engine, t).is_err());
            for fault in FAULTS {
                assert!(request.fault_reply(&ours, fault, t).is_err(), "{fault:?}");
            }
        }
        match (op, request.rejection_reply(&ours)) {
            (
                ReadHeaderPath,
                Some(HeaderReply::ReadHeaderPath(Err(PortError::Unavailable { source: None }))),
            )
            | (
                ReleaseHeaderPath,
                Some(HeaderReply::ReleaseHeaderPath(Err(PortError::Unavailable { source: None }))),
            ) => {}
            (ApplyHeaderTarget, Some(HeaderReply::ApplyHeaderTarget(Err(error)))) => {
                assert_eq!(error.category, ErrorCategory::StaleTargetOrGeneration);
                assert_eq!(
                    error.subject,
                    ErrorSubject::Branch(owner().header_authority().branch)
                );
            }
            (_, other) => panic!("{} rejected with {other:?}", op.name()),
        }
    }
    for check in [InputCheck::LocatorCount, InputCheck::PageCount] {
        assert!(
            !check.in_adapter(),
            "state runs {check:?} under the deadline"
        );
    }
    assert!(InputCheck::AdapterHandle.in_adapter());
    assert!(InputCheck::PreparedTargetSeal.in_adapter());
}

/// An apply of a target this adapter sealed passes the seal check, so it
/// keeps its engine result and its `local-resource` fault.
#[test]
fn an_own_target_seal_keeps_engine_and_fault() {
    let key = AdapterKey::new();
    let apply = HeaderRequest::ApplyHeaderTarget {
        target: prepared_target(&key, owner()),
    };
    assert_eq!(apply.rejected_by(&key), None);
    assert!(apply.rejection_reply(&key).is_none());
    for t in TIMES {
        assert_eq!(
            apply.enabled(&key, t),
            [Outcome::Engine, Outcome::Fault(Fault::LocalResource)]
        );
    }
    assert!(apply
        .fault_reply(&key, Fault::LocalResource, Duration::ZERO)
        .is_ok());
}
