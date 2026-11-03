//! The owner model's failures (storage.md ST3-B+2): each committed row of
//! the plan's mutation table, with the exact failure it must give.

use std::{
    future::Future,
    pin::pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use zakura_chain::block;
use zakura_header_chain::{Frontier, HeaderWorkAuthority, SourceId};
use zakura_node_services::header_chain::{
    AcquirePath, AcquirePathReply, ApplyHeaderTargetOutcome, LocatorReadPermit, Port, PortError,
    ReadPath, RetainedHeaderPath,
};
use zakura_rt::sync::Semaphore;
use zakura_storage::header::{
    controlled::{ControlledPort, Fact, FactKind, OpId},
    evidence::{LeaseFact, LeaseKey, ModelFailure, Owner, OwnerModel, OwnerState, StateField},
    fixture::HeaderFixture,
    Outcome,
};

/// Polls `future` once, which completes any engine-port call.
fn ready<F: Future>(future: F) -> F::Output {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("expected the future to be ready"),
    }
}

/// A pristine controlled port whose engine holds `count` headers on the
/// anchor, admitted through the engine port, which leaves no owner state.
/// Returns the port and the headers' frontiers.
fn chain(count: u32) -> (ControlledPort, Vec<Frontier>) {
    let fixture = HeaderFixture::regtest();
    let port = ControlledPort::new(&fixture);
    let anchor = fixture.anchor();
    let headers = fixture.extend(anchor, count, 0);
    let request = fixture.target_request(&port.engine().snapshot(), anchor, &headers, 1);
    let prepared = ready(port.engine().prepare_header_target(request)).expect("fixture prepares");
    assert_eq!(
        ready(port.engine().apply_header_target(prepared)).expect("fixture applies"),
        ApplyHeaderTargetOutcome::Applied
    );
    assert!(port.is_pristine());
    let frontiers = (1..=count)
        .zip(&headers)
        .map(|(height, header)| Frontier::new(block::Height(height), header.hash()))
        .collect();
    (port, frontiers)
}

/// Submits `call`, completes it with the engine outcome, and returns its
/// reply.
fn run<F: Future + Unpin>(port: &ControlledPort, mut call: F) -> F::Output {
    assert!(pin!(&mut call)
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    let id = port.pending().last().expect("the call is pending").id;
    port.complete(id, Outcome::Engine)
        .expect("the engine outcome is legal");
    ready(call)
}

/// Peer 1's acquire of the path from the anchor to `target`.
fn acquire(port: &ControlledPort, target: Frontier) -> AcquirePath {
    AcquirePath {
        source: SourceId::from_digest([1; 32]),
        session_id: 1,
        scope: HeaderWorkAuthority::for_target(&port.engine().snapshot(), target.hash),
        target_tip_hash: target.hash,
        locator_hashes: vec![HeaderFixture::regtest().anchor().hash],
    }
}

/// The leased path in `reply`.
fn acquired(reply: Result<AcquirePathReply, PortError>) -> RetainedHeaderPath {
    match reply {
        Ok(AcquirePathReply::Acquired(path)) => *path,
        other => panic!("expected a lease, got {other:?}"),
    }
}

/// A page of up to `count` headers after `after`.
fn page_after(after: Frontier, count: u32) -> ReadPath {
    ReadPath {
        after_hash: after.hash,
        max_header_count: count,
        want_tree_aux: false,
    }
}

/// Facts the model has applied, with `port`'s state matching it.
fn checked(port: &ControlledPort, model: &mut OwnerModel) {
    assert_eq!(
        model.check_boundary(&port.take_facts(), &port.owner_state()),
        Ok(())
    );
}

/// The facts of peer 1 leasing the path to `target` and releasing it.
fn lease_and_release(port: &ControlledPort, target: Frontier) -> Vec<Fact> {
    let path = acquired(run(port, port.acquire_header_path(acquire(port, target))));
    run(port, port.release_header_path(path)).expect("releases");
    port.take_facts()
}

/// `facts` renumbered gaplessly from the first one's sequence number.
fn renumbered(mut facts: Vec<Fact>) -> Vec<Fact> {
    let first = facts.first().map_or(1, |fact| fact.seq);
    for (seq, fact) in (first..).zip(&mut facts) {
        fact.seq = seq;
    }
    facts
}

/// A drain between two delivered batches stole the middle one, so the
/// next batch doesn't continue the facts the model applied.
#[test]
fn a_stolen_interior_batch_is_a_gap() {
    let (port, chain) = chain(1);
    let mut model = OwnerModel::new();
    run(&port, port.selected_chain_contains(chain[0].hash)).expect("membership reads");
    checked(&port, &mut model);
    run(&port, port.selected_chain_contains(chain[0].hash)).expect("membership reads");
    let stolen = port.take_facts();
    run(&port, port.selected_chain_contains(chain[0].hash)).expect("membership reads");
    let next = port.take_facts();
    assert_eq!(
        next[0].seq,
        stolen.last().expect("the stolen batch has facts").seq + 1
    );
    assert_eq!(
        model.check_boundary(&next, &port.owner_state()),
        Err(ModelFailure::Gap { seq: next[0].seq })
    );
}

/// A drain stole the last batch, so the actual state reflects facts the
/// model never applied.
#[test]
fn a_stolen_last_batch_is_stale() {
    let (port, chain) = chain(1);
    let mut model = OwnerModel::new();
    run(&port, port.selected_chain_contains(chain[0].hash)).expect("membership reads");
    checked(&port, &mut model);
    run(&port, port.selected_chain_contains(chain[0].hash)).expect("membership reads");
    let stolen = port.take_facts();
    let applied_seq = model.state().through_seq;
    assert_eq!(
        model.compare(&port.owner_state()),
        Err(ModelFailure::Stale {
            through_seq: stolen.last().expect("the stolen batch has facts").seq,
            applied_seq,
        })
    );
}

/// A required lease fact deleted and the later facts renumbered, so the
/// numbering alone can't tell, is the fact the acquire's transaction
/// lacks.
#[test]
fn a_deleted_fact_is_a_missing_effect() {
    let (port, chain) = chain(1);
    let mut facts = lease_and_release(&port, chain[0]);
    let leased = facts
        .iter()
        .position(|fact| matches!(fact.kind, FactKind::Lease(LeaseFact::Leased { .. })))
        .expect("the acquire records its lease");
    let removed = facts.remove(leased);
    assert_eq!(
        OwnerModel::new().apply(&renumbered(facts)),
        Err(ModelFailure::MissingEffect {
            txn: removed.txn,
            expected: removed.kind,
        })
    );
}

/// Deleting a transaction's head leaves it without one.
#[test]
fn a_deleted_head_is_a_malformed_transaction() {
    let (port, chain) = chain(1);
    let mut facts = lease_and_release(&port, chain[0]);
    let head = facts
        .iter()
        .position(|fact| matches!(fact.kind, FactKind::Completed(_)))
        .expect("the acquire completes");
    // The acquire's lease fact now ends its transaction.
    let txn = facts.remove(head).txn;
    assert_eq!(
        OwnerModel::new().apply(&renumbered(facts)),
        Err(ModelFailure::MalformedTxn { txn })
    );
}

/// Two facts of one transaction that keep their sequence numbers but swap
/// payloads hold the right facts in the wrong order.
#[test]
fn swapped_facts_are_an_order_violation() {
    let (port, chain) = chain(1);
    let mut facts = lease_and_release(&port, chain[0]);
    let ended = facts
        .iter()
        .position(|fact| {
            matches!(
                fact.kind,
                FactKind::Lease(LeaseFact::CapabilityEnded { .. })
            )
        })
        .expect("the release ends the capability");
    let txn = facts[ended].txn;
    assert_eq!(facts[ended + 1].txn, txn, "its retention ends with it");
    let kind = facts[ended].kind.clone();
    facts[ended].kind = facts[ended + 1].kind.clone();
    facts[ended + 1].kind = kind;
    assert_eq!(
        OwnerModel::new().apply(&facts),
        Err(ModelFailure::OrderViolation { txn })
    );
}

/// An acquire through the engine port after the baseline leases without
/// a fact, so the model doesn't know the lease the state holds.
#[test]
fn an_engine_acquire_is_an_unknown_owner() {
    let (port, chain) = chain(1);
    let mut model = OwnerModel::new();
    run(&port, port.selected_chain_contains(chain[0].hash)).expect("membership reads");
    checked(&port, &mut model);
    let path = acquired(ready(
        port.engine().acquire_header_path(acquire(&port, chain[0])),
    ));
    let lease = LeaseKey {
        peer: SourceId::from_digest([1; 32]),
        session_id: 1,
        lease_id: 1,
    };
    assert_eq!(path.target, chain[0]);
    assert_eq!(
        model.check_boundary(&port.take_facts(), &port.owner_state()),
        Err(ModelFailure::UnknownOwner {
            txn: None,
            owner: Owner::Lease(lease),
        })
    );
}

/// A read through the engine port of a lease's final page consumes it
/// without caching anything, and without a fact, so the live leases
/// differ from the model's.
#[test]
fn an_engine_read_of_the_final_page_is_a_lease_mismatch() {
    let (port, chain) = chain(1);
    let mut model = OwnerModel::new();
    let path = acquired(run(
        &port,
        port.acquire_header_path(acquire(&port, chain[0])),
    ));
    checked(&port, &mut model);
    let anchor = HeaderFixture::regtest().anchor();
    ready(port.engine().read_header_path(path, page_after(anchor, 1))).expect("the page reads");
    let actual = port.owner_state();
    assert!(actual.leases.is_empty() && actual.continuations.is_empty());
    assert_eq!(
        model.check_boundary(&port.take_facts(), &actual),
        Err(ModelFailure::Mismatch(StateField::Leases))
    );
}

/// The facts a checked port publishes keep every owner in step with the
/// model across a lease's whole life, the base case each row above breaks.
#[test]
fn a_lease_life_matches_the_model() {
    let (port, chain) = chain(3);
    let mut model = OwnerModel::new();
    let anchor = HeaderFixture::regtest().anchor();
    let path = acquired(run(
        &port,
        port.acquire_header_path(acquire(&port, chain[2])),
    ));
    checked(&port, &mut model);
    run(&port, port.read_header_path(path, page_after(anchor, 1))).expect("the page reads");
    checked(&port, &mut model);
    let mut continued = acquire(&port, chain[2]);
    continued.locator_hashes = vec![chain[0].hash];
    let path = acquired(run(&port, port.acquire_header_path(continued)));
    checked(&port, &mut model);
    run(&port, port.read_header_path(path, page_after(chain[0], 2))).expect("the page reads");
    checked(&port, &mut model);
    port.shut_down();
    checked(&port, &mut model);
    assert_eq!(model.state(), port.owner_state());
}

/// A shutdown that drops an operation whose submission the model never
/// saw names an unknown owner. Here the submission's transaction is
/// deleted and the later facts and transactions renumbered, as a producer
/// that kept the slot but skipped `Submitted` would publish them.
#[test]
fn a_shutdown_dropping_an_unseen_operation_is_an_unknown_owner() {
    let (port, chain) = chain(1);
    let mut call = port.selected_chain_contains(chain[0].hash);
    assert!(pin!(&mut call)
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    let id = port.pending()[0].id;
    port.shut_down();
    drop(call);
    let mut facts = port.take_facts();
    let submitted = facts
        .iter()
        .position(|fact| matches!(fact.kind, FactKind::Submitted { .. }))
        .expect("the call was submitted");
    facts.remove(submitted);
    for (n, fact) in (1..).zip(&mut facts) {
        (fact.seq, fact.txn) = (n, n);
    }
    assert!(matches!(
        facts.as_slice(),
        [Fact {
            kind: FactKind::ShutDown { .. },
            ..
        }]
    ));
    assert_eq!(
        OwnerModel::new().apply(&facts),
        Err(ModelFailure::UnknownOwner {
            txn: Some(1),
            owner: Owner::Operation(id),
        })
    );
}

/// A partial read through the engine port moves a known lease into the
/// continuation cache without a fact, so the state holds a continuation
/// the model never created, even though it knows the key as a lease.
#[test]
fn an_engine_partial_read_is_an_unknown_continuation() {
    let (port, chain) = chain(3);
    let mut model = OwnerModel::new();
    let path = acquired(run(
        &port,
        port.acquire_header_path(acquire(&port, chain[2])),
    ));
    checked(&port, &mut model);
    let lease = model.state().leases[0].lease;
    let anchor = HeaderFixture::regtest().anchor();
    ready(port.engine().read_header_path(path, page_after(anchor, 1))).expect("the page reads");
    assert_eq!(
        model.check_boundary(&port.take_facts(), &port.owner_state()),
        Err(ModelFailure::UnknownOwner {
            txn: None,
            owner: Owner::Continuation(lease),
        })
    );
}

/// D19: an unknown owner outranks an illegal head. The acquire's head
/// claims a delivery that didn't happen, and its transaction also drops a
/// permit storage never took.
#[test]
fn an_unknown_owner_outranks_an_illegal_head() {
    let (port, chain) = chain(1);
    let mut facts = lease_and_release(&port, chain[0]);
    let head = facts
        .iter()
        .position(|fact| matches!(fact.kind, FactKind::Completed(_)))
        .expect("the acquire completes");
    let FactKind::Completed(completion) = &mut facts[head].kind else {
        unreachable!("the head was matched above");
    };
    completion.delivered = !completion.delivered;
    let stray = Fact {
        kind: FactKind::PermitDropped { id: OpId(99) },
        ..facts[head].clone()
    };
    let txn = stray.txn;
    facts.insert(head, stray);
    assert_eq!(
        OwnerModel::new().apply(&renumbered(facts)),
        Err(ModelFailure::UnknownOwner {
            txn: Some(txn),
            owner: Owner::Permit(OpId(99)),
        })
    );
}

/// A release's head that omits the lease end its facts record disagrees
/// with them.
#[test]
fn a_head_without_its_lease_end_is_an_unexpected_effect() {
    let (port, chain) = chain(1);
    let mut facts = lease_and_release(&port, chain[0]);
    let head = facts
        .iter()
        .rposition(|fact| matches!(&fact.kind, FactKind::Completed(c) if !c.lease_ends.is_empty()))
        .expect("the release ends its lease");
    let FactKind::Completed(completion) = &mut facts[head].kind else {
        unreachable!("the head was matched above");
    };
    completion.lease_ends.clear();
    assert_eq!(
        OwnerModel::new().apply(&facts),
        Err(ModelFailure::UnexpectedEffect {
            txn: facts[head].txn,
            found: facts[head].kind.clone(),
        })
    );
}

/// A change to one field of an owner state, and the field it changes.
type FieldChange = (StateField, fn(&mut OwnerState));

/// Each field of a real owner state, changed alone without naming an
/// unknown owner, is a mismatch in that field. Dropping an operation is
/// the slot vanishing without a fact.
#[test]
fn each_changed_state_field_is_a_mismatch() {
    let (port, chain) = chain(3);
    let mut model = OwnerModel::new();
    let anchor = HeaderFixture::regtest().anchor();
    let path = acquired(run(
        &port,
        port.acquire_header_path(acquire(&port, chain[2])),
    ));
    run(&port, port.read_header_path(path, page_after(anchor, 1))).expect("the page reads");
    let mut other = acquire(&port, chain[2]);
    other.source = SourceId::from_digest([2; 32]);
    acquired(run(&port, port.acquire_header_path(other)));
    let semaphore = Arc::new(Semaphore::new(1));
    let mut locator = port.continuation_locator(LocatorReadPermit::new(
        Arc::clone(&semaphore)
            .try_acquire_owned()
            .expect("the semaphore has a permit"),
    ));
    assert!(pin!(&mut locator)
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    checked(&port, &mut model);
    let actual = port.owner_state();
    assert!(
        !actual.operations.is_empty()
            && !actual.permits.is_empty()
            && !actual.leases.is_empty()
            && !actual.continuations.is_empty(),
        "every owner kind is present: {actual:?}"
    );
    let changes: [FieldChange; 7] = [
        (StateField::Operations, |state| state.operations.clear()),
        (StateField::Permits, |state| state.permits.clear()),
        (StateField::Leases, |state| state.leases.clear()),
        (StateField::Continuations, |state| {
            state.continuations.clear()
        }),
        (StateField::ClassWaiters, |state| {
            state.class_waiters.general = !state.class_waiters.general
        }),
        (StateField::LastLeaseId, |state| state.last_lease_id += 1),
        (StateField::ContinuationHits, |state| {
            state.continuation_hits += 1
        }),
    ];
    for (field, change) in changes {
        let mut changed = actual.clone();
        change(&mut changed);
        assert_eq!(
            model.compare(&changed),
            Err(ModelFailure::Mismatch(field)),
            "{field:?}"
        );
    }
}
