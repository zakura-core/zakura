//! Machine tests for [`StatusSchedule`]: which peers a status flush sends to
//! and when the next flush wakes.
//!
//! The proptest records send outcomes the way `ReactorIO::send_status` does.
//! It compares every flush with which peers it admitted and which latest
//! status it queued to each, and checks that the flush's sends agree with its
//! wake. The delivery's deadline rules are pinned by named cases here and in
//! [`crate::zakura::block_sync::status`].

use std::time::Duration;

use proptest::prelude::*;

use super::*;
use crate::zakura::block_sync::MAX_BS_RESPONSE_BYTES;

const PEERS: u8 = 4;

fn peer(byte: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![byte; 32]).expect("the test peer id is within bounds")
}

fn status(high: u32) -> BlockSyncStatus {
    BlockSyncStatus {
        servable_low: block::Height(0),
        servable_high: block::Height(high),
        tip_hash: verified_hash(high),
        max_blocks_per_response: 1,
        max_inflight_requests: 1,
        max_response_bytes: MAX_BS_RESPONSE_BYTES,
    }
}

/// The flush peer list: admitted peers in `live`, with readiness from `ready`.
fn flush_peers(admitted: &[bool], live: u8, ready: u8) -> Vec<(ZakuraPeerId, bool)> {
    (0..PEERS)
        .filter(|id| admitted[usize::from(*id)] && live & (1 << id) != 0)
        .map(|id| (peer(id), ready & (1 << id) != 0))
        .collect()
}

#[test]
fn a_ready_peer_with_the_latest_status_needs_no_send() {
    let now = Instant::now();
    let mut schedule = StatusSchedule::default();
    schedule.admit(&peer(0), now);
    let peers = [(peer(0), true)];
    assert_eq!(
        schedule.due(
            status(5),
            peers.iter().map(|(peer, ready)| (peer, *ready)),
            now
        ),
        vec![peer(0)]
    );

    schedule
        .delivery_mut(&peer(0))
        .expect("the admitted peer has a delivery record")
        .queued(status(5), now);
    assert!(schedule
        .due(
            status(5),
            peers.iter().map(|(peer, ready)| (peer, *ready)),
            now
        )
        .is_empty());
    schedule.rearm(status(5), peers.iter().map(|(peer, ready)| (peer, *ready)));
    assert_eq!(schedule.refresh_at(), None);

    // A peer that has not sent its own status still needs ours.
    let unready = [(peer(0), false)];
    assert_eq!(
        schedule.due(
            status(5),
            unready.iter().map(|(peer, ready)| (peer, *ready)),
            now
        ),
        vec![peer(0)]
    );
}

#[test]
fn a_replacement_session_starts_a_fresh_delivery() {
    let now = Instant::now();
    let mut schedule = StatusSchedule::default();
    schedule.admit(&peer(0), now);
    schedule
        .delivery_mut(&peer(0))
        .expect("the admitted peer has a delivery record")
        .queued(status(5), now);
    schedule.admit(&peer(0), now);
    assert_eq!(
        schedule.due(status(5), [(&peer(0), true)], now),
        vec![peer(0)]
    );
}

/// A Status that the cadence deferred is due again at the cadence's next
/// send, unless a higher tip replaces it: that is due from the deferral at
/// once. Queuing a Status or backing off a full queue drops the deferral.
#[test]
fn a_new_tip_is_due_at_once_only_while_a_deferral_holds() {
    let start = Instant::now();
    let ms = |ms: u64| start + Duration::from_millis(ms);
    // (whether the queue took a Status at 1 ms or backed off as full, the
    // latest status, the expected wake)
    for (after_deferral, latest, wake) in [
        (None, 5, ms(10_000)),
        (None, 6, ms(0)),
        (Some(true), 6, ms(1)),
        (Some(false), 6, ms(101)),
    ] {
        let mut schedule = StatusSchedule::default();
        schedule.admit(&peer(0), start);
        let delivery = schedule
            .delivery_mut(&peer(0))
            .expect("the admitted peer has a delivery record");
        delivery.deferred_until(status(5), ms(0), ms(10_000));
        match after_deferral {
            Some(true) => delivery.queued(status(5), ms(1)),
            Some(false) => delivery.queue_full(ms(1)),
            None => {}
        }
        schedule.rearm(status(latest), [(&peer(0), false)]);
        assert_eq!(
            schedule.refresh_at(),
            Some(wake),
            "{after_deferral:?}, latest {latest}"
        );
    }
}

#[derive(Clone, Debug)]
enum Input {
    Admit(u8),
    Remove(u8),
    Queued(u8),
    Deferred(u8, u64),
    QueueFull(u8),
    StatusChange,
    Flush { live: u8, ready: u8 },
    Wait(u64),
}

fn input() -> impl Strategy<Value = Input> {
    prop_oneof![
        1 => (0..PEERS).prop_map(Input::Admit),
        1 => (0..PEERS).prop_map(Input::Remove),
        2 => (0..PEERS).prop_map(Input::Queued),
        1 => (0..PEERS, 0u64..300).prop_map(|(peer, ms)| Input::Deferred(peer, ms)),
        1 => (0..PEERS).prop_map(Input::QueueFull),
        1 => Just(Input::StatusChange),
        3 => (0u8..16, 0u8..16).prop_map(|(live, ready)| Input::Flush { live, ready }),
        1 => (0u64..250).prop_map(Input::Wait),
    ]
}

proptest! {
    /// Every flush sends to live admitted peers only, in peer-list order, and
    /// to exactly the peers whose deadline the wake has reached. A ready peer
    /// that holds the latest status needs no send, and any other live peer
    /// keeps a wake.
    #[test]
    fn status_flushes_agree_with_their_wake(inputs in prop::collection::vec(input(), 1..80)) {
        let base = Instant::now();
        let mut schedule = StatusSchedule::default();
        // Each admitted peer, with whether it holds the latest queued status.
        let mut admitted: [Option<bool>; PEERS as usize] = [None; PEERS as usize];
        let mut latest = 1u32;
        let mut now_ms = 0u64;
        for input in inputs {
            let now = base + Duration::from_millis(now_ms);
            match input {
                Input::Admit(id) => {
                    schedule.admit(&peer(id), now);
                    admitted[usize::from(id)] = Some(false);
                }
                Input::Remove(id) => {
                    schedule.remove(&peer(id));
                    admitted[usize::from(id)] = None;
                }
                Input::Queued(id) => {
                    let actual = schedule.delivery_mut(&peer(id)).map(|delivery| delivery.queued(status(latest), now));
                    prop_assert_eq!(actual.is_some(), admitted[usize::from(id)].is_some());
                    if let Some(holds_latest) = admitted[usize::from(id)].as_mut() {
                        *holds_latest = true;
                    }
                }
                Input::Deferred(id, ms) => {
                    let at = now + Duration::from_millis(ms);
                    let actual = schedule
                        .delivery_mut(&peer(id))
                        .map(|delivery| delivery.deferred_until(status(latest), now, at));
                    prop_assert_eq!(actual.is_some(), admitted[usize::from(id)].is_some());
                }
                Input::QueueFull(id) => {
                    let actual = schedule.delivery_mut(&peer(id)).map(|delivery| delivery.queue_full(now));
                    prop_assert_eq!(actual.is_some(), admitted[usize::from(id)].is_some());
                }
                Input::StatusChange => {
                    latest += 1;
                    for holds_latest in admitted.iter_mut().flatten() {
                        *holds_latest = false;
                    }
                }
                Input::Flush { live, ready } => {
                    let present: Vec<bool> = admitted.iter().map(Option::is_some).collect();
                    let peers = flush_peers(&present, live, ready);
                    let due = schedule.due(status(latest), peers.iter().map(|(peer, ready)| (peer, *ready)), now);
                    let mut listed = peers.iter().map(|(peer, _)| peer);
                    prop_assert!(
                        due.iter().all(|peer| listed.any(|listed| listed == peer)),
                        "a flush sends to listed peers, in list order"
                    );
                    schedule.rearm(status(latest), peers.iter().map(|(peer, ready)| (peer, *ready)));
                    let wake = schedule.refresh_at();
                    prop_assert_eq!(
                        !due.is_empty(),
                        wake.is_some_and(|wake| wake <= now),
                        "a flush sends exactly when its wake is due"
                    );
                    for (peer, ready) in &peers {
                        let holds_latest = admitted[usize::from(peer.as_bytes()[0])]
                            .expect("flushes list admitted peers");
                        if holds_latest && *ready {
                            prop_assert!(!due.contains(peer), "a ready peer with the latest status");
                        } else {
                            prop_assert!(wake.is_some(), "a peer that needs our status keeps a wake");
                        }
                    }
                    if peers.iter().all(|(peer, ready)| {
                        *ready && admitted[usize::from(peer.as_bytes()[0])] == Some(true)
                    }) {
                        prop_assert_eq!(wake, None, "no live peer needs our status");
                    }
                }
                Input::Wait(ms) => now_ms += ms,
            }
        }
    }
}
