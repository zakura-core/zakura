//! Session capacity: the five tests from #945, renamed, and an operation
//! sequence proptest.

use std::time::Duration;

use proptest::prelude::*;

use super::*;

const INBOUND: ServicePeerDirection = ServicePeerDirection::Inbound;
const OUTBOUND: ServicePeerDirection = ServicePeerDirection::Outbound;

fn capacity(inbound: usize, outbound: usize, setup: usize) -> SessionCapacity {
    SessionCapacity::new(
        "test",
        &ServicePeerLimits {
            max_inbound_peers: inbound,
            max_outbound_peers: outbound,
            max_pending_escalations: setup,
            ..ServicePeerLimits::default()
        },
    )
}

/// Every slot of the default limits.
fn all_free() -> FreeSlots {
    let limits = ServicePeerLimits::default();
    FreeSlots {
        inbound: limits.max_inbound_peers,
        outbound: limits.max_outbound_peers,
        setup: limits.max_pending_escalations,
        inbound_setup: limits.max_pending_escalations - 1,
    }
}

#[test]
fn inbound_setups_leave_an_outbound_setup_slot() {
    let capacity = SessionCapacity::new("test", &ServicePeerLimits::default());
    let setup = ServicePeerLimits::default().max_pending_escalations;
    let mut inbound: Vec<_> = (0..setup - 1)
        .map(|_| capacity.reserve(INBOUND).unwrap())
        .collect();
    for _ in 0..20 {
        assert!(!capacity.available(INBOUND));
        assert!(capacity.reserve(INBOUND).is_err());
        assert!(capacity.available(OUTBOUND));
        let outbound = capacity.reserve(OUTBOUND).unwrap();
        assert_eq!(capacity.free().setup, 0);
        assert!(capacity.reserve(OUTBOUND).is_err());
        outbound.admitted();
        assert_eq!(capacity.free().setup, 1);
        assert!(!capacity.available(INBOUND));
        drop(outbound);
        // Churning one inbound setup neither steals nor leaks the protected slot.
        drop(inbound.pop());
        inbound.push(capacity.reserve(INBOUND).unwrap());
    }
    drop(inbound);
    assert_eq!(capacity.free(), all_free());
}

#[test]
fn a_failed_setup_returns_inbound_capacity() {
    let capacity = SessionCapacity::new("test", &ServicePeerLimits::default());
    let setup = ServicePeerLimits::default().max_pending_escalations;
    let outbound: Vec<_> = (0..setup)
        .map(|_| capacity.reserve(OUTBOUND).unwrap())
        .collect();
    for _ in 0..64 {
        assert!(capacity.reserve(INBOUND).is_err());
    }
    drop(outbound);
    let inbound: Vec<_> = (0..setup - 1)
        .map(|_| capacity.reserve(INBOUND).unwrap())
        .collect();
    assert!(capacity.reserve(OUTBOUND).is_ok());
    drop(inbound);
    assert_eq!(capacity.free(), all_free());
}

#[test]
fn minimal_setup_limits_keep_outbound_and_inbound_only_modes() {
    for (setup, inbound, outbound, inbound_ok, outbound_ok) in [
        (0, 1, 1, false, false),
        (1, 1, 1, false, true),
        (1, 1, 0, true, false),
        (2, 0, 1, false, true),
        (2, 1, 1, true, true),
    ] {
        let capacity = capacity(inbound, outbound, setup);
        let before = capacity.free();
        for (direction, available) in [(INBOUND, inbound_ok), (OUTBOUND, outbound_ok)] {
            assert_eq!(capacity.available(direction), available);
            let reservation = capacity.reserve(direction);
            assert_eq!(reservation.is_ok(), available);
            drop(reservation);
            assert_eq!(capacity.free(), before);
        }
    }
}

#[tokio::test]
async fn setup_and_retirement_keep_their_session_slot() {
    let capacity = capacity(1, 1, 2);
    let mut changed = capacity.subscribe();
    let outbound = capacity.reserve(OUTBOUND).unwrap();
    let inbound = capacity.reserve(INBOUND).unwrap();
    assert!(
        capacity.reserve(INBOUND).is_err(),
        "the setup slots are shared across directions"
    );
    outbound.admitted();
    changed.changed().await.unwrap();
    inbound.admitted();
    outbound.admitted();
    assert_eq!(
        capacity.free().setup,
        2,
        "a second `admitted` releases nothing"
    );
    let (send, _recv) = crate::zakura::transport::worker_framed_channel(1);
    let send = send.with_session_resources(Some(outbound.clone()));
    let retiring_worker: Arc<dyn SessionResources> = outbound.clone();
    drop(outbound);
    assert!(capacity.reserve(OUTBOUND).is_err());
    drop(send);
    assert!(
        capacity.reserve(OUTBOUND).is_err(),
        "a retiring worker keeps the slot after the service drops its sender"
    );
    drop(retiring_worker);
    assert!(capacity.available(OUTBOUND));
    drop(inbound);
    assert!(capacity.available(INBOUND));
}

#[tokio::test]
async fn an_abandoned_setup_returns_capacity_and_wakes_demand() {
    let capacity = capacity(1, 1, 2);
    let setup = capacity.reserve(INBOUND).unwrap();
    assert!(!capacity.available(INBOUND));
    let SessionDemand::WaitForChange(wait) = capacity.demand(INBOUND) else {
        panic!("a full direction must wait");
    };
    assert!(matches!(capacity.demand(OUTBOUND), SessionDemand::OpenNow));
    // The release lands after `demand` checked and before anyone waits.
    drop(setup);
    tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .expect("a release after the check wakes the waiter");
    assert!(capacity.available(INBOUND));
    assert!(capacity.available(OUTBOUND));
}

/// One step of an operation sequence.
#[derive(Clone, Debug)]
enum Op {
    Reserve(bool),
    Admit(usize),
    CloneOwner(usize),
    DropOwner(usize),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => any::<bool>().prop_map(Op::Reserve),
        2 => (0usize..8).prop_map(Op::Admit),
        1 => (0usize..8).prop_map(Op::CloneOwner),
        2 => (0usize..8).prop_map(Op::DropOwner),
    ]
}

/// A live session: its owners and what the model expects it to hold.
struct Live {
    owners: Vec<Arc<dyn SessionResources>>,
    inbound: bool,
    admitted: bool,
}

proptest! {
    /// After every step, each free count equals its limit minus the live
    /// sessions that hold that slot, and `available` and `reserve` agree with
    /// the counts.
    #[test]
    fn operation_sequences_keep_every_count_in_bounds(
        inbound in 0usize..4,
        outbound in 0usize..4,
        setup in 0usize..5,
        ops in prop::collection::vec(op(), 1..96),
    ) {
        let capacity = capacity(inbound, outbound, setup);
        let inbound_setup = setup.saturating_sub(usize::from(outbound > 0));
        let mut live: Vec<Live> = Vec::new();
        for op in ops {
            match op {
                Op::Reserve(is_inbound) => {
                    let direction = if is_inbound { INBOUND } else { OUTBOUND };
                    let free = capacity.free();
                    let expected = if is_inbound {
                        free.inbound > 0 && free.setup > 0 && free.inbound_setup > 0
                    } else {
                        free.outbound > 0 && free.setup > 0
                    };
                    prop_assert_eq!(capacity.available(direction), expected);
                    match capacity.reserve(direction) {
                        Ok(reservation) => {
                            prop_assert!(expected);
                            live.push(Live { owners: vec![reservation], inbound: is_inbound, admitted: false });
                        }
                        Err(SessionFull) => prop_assert!(!expected),
                    }
                }
                Op::Admit(index) => {
                    if let Some(session) = live.get_mut(index) {
                        session.owners[0].admitted();
                        session.admitted = true;
                    }
                }
                Op::CloneOwner(index) => {
                    if let Some(session) = live.get_mut(index) {
                        let owner = session.owners[0].clone();
                        session.owners.push(owner);
                    }
                }
                Op::DropOwner(index) => {
                    if let Some(session) = live.get_mut(index) {
                        session.owners.pop();
                        if session.owners.is_empty() {
                            live.remove(index);
                        }
                    }
                }
            }
            let count = |filter: &dyn Fn(&Live) -> bool| live.iter().filter(|session| filter(session)).count();
            prop_assert_eq!(
                capacity.free(),
                FreeSlots {
                    inbound: inbound - count(&|session| session.inbound),
                    outbound: outbound - count(&|session| !session.inbound),
                    setup: setup - count(&|session| !session.admitted),
                    inbound_setup: inbound_setup
                        - count(&|session| session.inbound && !session.admitted),
                }
            );
        }
        drop(live);
        prop_assert_eq!(
            capacity.free(),
            FreeSlots { inbound, outbound, setup, inbound_setup }
        );
    }
}
