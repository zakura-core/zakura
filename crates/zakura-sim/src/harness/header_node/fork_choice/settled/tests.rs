//! The settled-convergence rule's conditions, fed by hand.

use std::time::Duration;

use zakura_network::zakura::{observe::ReactorSizes, ServicePeerDirection};

use super::*;
use crate::harness::header_node::Fork;

/// Two mirrors on forks from genesis: five unweighted headers on branch 1
/// and three at weight 2 on branch 2, which has more work.
fn fixture() -> (HeaderFixture, ForkFixture) {
    let headers = HeaderFixture::regtest().with_weight(2, 2);
    let fork = |branch, len| Chain {
        main: 0,
        fork: Some(Fork { branch, len }),
    };
    (
        headers,
        ForkFixture {
            mirrors: vec![fork(1, 5), fork(2, 3)],
        },
    )
}

/// Mirror `index`'s peer ID.
fn peer(index: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![index; 32]).expect("32 bytes is a valid peer ID")
}

/// `peer`'s session admission.
fn admitted(peer: ZakuraPeerId) -> ReactorEvent {
    ReactorEvent::SessionAdmitted {
        peer,
        session_id: 1,
        direction: ServicePeerDirection::Inbound,
        replaced: None,
        origin: None,
    }
}

/// Nothing outside the reactor busy, at `now`, with the host's clock,
/// which admits every fixture header.
fn idle(now: SimInstant) -> Outside {
    Outside {
        port_busy: false,
        environment_acted: false,
        now,
        clock: std::time::SystemTime::now().into(),
    }
}

/// A rule that has seen both mirrors admitted and quiet `Sizes`.
fn quiet() -> (Settled, block::Hash, block::Hash) {
    let (headers, fork) = fixture();
    let mut settled = Settled::new(&headers, &fork);
    settled.observe(&admitted(peer(1)), SimInstant::ZERO);
    settled.observe(&admitted(peer(2)), SimInstant::ZERO);
    settled.observe(
        &ReactorEvent::Sizes {
            sizes: ReactorSizes::default(),
        },
        SimInstant::ZERO,
    );
    let lighter = fork.mirrors[0].tip(&headers).hash;
    let heavier = fork.mirrors[1].tip(&headers).hash;
    (settled, lighter, heavier)
}

/// A quiet node on the heavier tip holds, and one on the lighter tip
/// breaks.
#[test]
fn a_settled_node_must_be_on_the_heaviest_tip() {
    let (mut settled, lighter, heavier) = quiet();
    assert_eq!(settled.judge(heavier, idle(SimInstant::ZERO)), Some(None));
    let broke = settled
        .judge(lighter, idle(SimInstant::ZERO))
        .expect("a settled point");
    assert!(broke.is_some_and(|detail| detail.contains("heaviest mirror tips")));
}

/// Each condition the tap reads keeps a point from being settled: a mirror
/// not yet admitted, a slot, a pending call, a busy port, a harness close
/// or relink, and a deferred pass whose refill hasn't come.
#[test]
fn every_condition_must_hold() {
    let (headers, fork) = fixture();
    let (_, lighter, _) = quiet();
    let mut one = Settled::new(&headers, &fork);
    one.observe(&admitted(peer(1)), SimInstant::ZERO);
    one.observe(
        &ReactorEvent::Sizes {
            sizes: ReactorSizes::default(),
        },
        SimInstant::ZERO,
    );
    assert_eq!(
        one.judge(lighter, idle(SimInstant::ZERO)),
        None,
        "one mirror"
    );

    let (mut busy, ..) = quiet();
    busy.observe(
        &ReactorEvent::Sizes {
            sizes: ReactorSizes {
                storage_calls: vec![7],
                ..ReactorSizes::default()
            },
        },
        SimInstant::ZERO,
    );
    assert_eq!(busy.judge(lighter, idle(SimInstant::ZERO)), None, "a call");

    let (mut settled, ..) = quiet();
    for outside in [
        Outside {
            port_busy: true,
            ..idle(SimInstant::ZERO)
        },
        Outside {
            environment_acted: true,
            ..idle(SimInstant::ZERO)
        },
    ] {
        assert_eq!(settled.judge(lighter, outside), None, "{outside:?}");
    }

    let (mut deferred, ..) = quiet();
    deferred.observe(
        &ReactorEvent::CachedPassDeferred {
            refill_in: Duration::from_secs(1),
        },
        SimInstant::ZERO,
    );
    assert_eq!(
        deferred.judge(lighter, idle(SimInstant::ZERO)),
        None,
        "deferred"
    );
    let later = SimInstant::ZERO
        .checked_add(Duration::from_secs(1))
        .expect("representable");
    assert!(deferred.judge(lighter, idle(later)).is_some(), "refilled");

    // No `Sizes` yet means the calls are unknown, so nothing is settled.
    let mut fresh = Settled::new(&headers, &fork);
    fresh.observe(&admitted(peer(1)), SimInstant::ZERO);
    fresh.observe(&admitted(peer(2)), SimInstant::ZERO);
    assert_eq!(
        fresh.judge(lighter, idle(SimInstant::ZERO)),
        None,
        "no sizes"
    );
}

/// A run clock that would defer a mirror's header keeps the rule from ever
/// finding a settled point: a correct node can't select a deferred header,
/// and nothing promotes it.
#[test]
fn a_clock_that_defers_a_mirror_header_settles_nothing() {
    let (headers, _) = fixture();
    let (mut settled, lighter, heavier) = quiet();
    let genesis = headers.genesis_header().time;
    let early = Outside {
        clock: genesis - chrono::Duration::hours(3),
        ..idle(SimInstant::ZERO)
    };
    assert_eq!(settled.judge(lighter, early), None);
    // The first judgment fixed it, whatever the clock later.
    assert_eq!(settled.judge(heavier, idle(SimInstant::ZERO)), None);
}
