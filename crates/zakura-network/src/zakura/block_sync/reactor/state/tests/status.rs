//! Machine tests for [`StatusSchedule`]: which peers a status flush sends to
//! and when the next flush wakes.
//!
//! The proptest records send outcomes the way `ReactorIO::send_status` does
//! and compares every flush with an independent model of each peer's last
//! queued status and retry time.

use std::time::Duration;

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
    assert_eq!(schedule.due(status(5), &peers, now), vec![peer(0)]);

    schedule
        .delivery_mut(&peer(0))
        .expect("the admitted peer has a delivery record")
        .queued(status(5));
    assert!(schedule.due(status(5), &peers, now).is_empty());
    schedule.rearm(status(5), &peers);
    assert_eq!(schedule.refresh_at(), None);

    // A peer that has not sent its own status still needs ours.
    let unready = [(peer(0), false)];
    assert_eq!(schedule.due(status(5), &unready, now), vec![peer(0)]);
}

#[test]
fn a_replacement_session_starts_a_fresh_delivery() {
    let now = Instant::now();
    let mut schedule = StatusSchedule::default();
    schedule.admit(&peer(0), now);
    schedule
        .delivery_mut(&peer(0))
        .expect("the admitted peer has a delivery record")
        .queued(status(5));
    schedule.admit(&peer(0), now);
    assert_eq!(
        schedule.due(status(5), &[(peer(0), true)], now),
        vec![peer(0)]
    );
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

/// One admitted peer: the status index it last queued and its retry time in
/// milliseconds after the test start.
#[derive(Clone, Copy, Debug)]
struct ModelDelivery {
    last_queued: Option<u32>,
    retry_ms: u64,
}

impl ModelDelivery {
    /// The retry time while the peer still needs `latest`.
    fn deadline(&self, latest: u32, ready: bool) -> Option<u64> {
        (self.last_queued != Some(latest) || !ready).then_some(self.retry_ms)
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Every flush sends to exactly the live peers the model finds due, in
    /// peer-list order, and wakes at the model's earliest pending retry.
    #[test]
    fn status_schedule_matches_model(inputs in prop::collection::vec(input(), 1..80)) {
        let base = Instant::now();
        let mut schedule = StatusSchedule::default();
        let mut model: [Option<ModelDelivery>; PEERS as usize] = [None; PEERS as usize];
        let mut latest = 1u32;
        let mut now_ms = 0u64;
        for input in inputs {
            let now = base + Duration::from_millis(now_ms);
            match input {
                Input::Admit(id) => {
                    schedule.admit(&peer(id), now);
                    model[usize::from(id)] = Some(ModelDelivery { last_queued: None, retry_ms: now_ms });
                }
                Input::Remove(id) => {
                    schedule.remove(&peer(id));
                    model[usize::from(id)] = None;
                }
                Input::Queued(id) => {
                    let actual = schedule.delivery_mut(&peer(id)).map(|delivery| delivery.queued(status(latest)));
                    prop_assert_eq!(actual.is_some(), model[usize::from(id)].is_some());
                    if let Some(delivery) = model[usize::from(id)].as_mut() {
                        delivery.last_queued = Some(latest);
                    }
                }
                Input::Deferred(id, ms) => {
                    let at = now + Duration::from_millis(ms);
                    let actual = schedule.delivery_mut(&peer(id)).map(|delivery| delivery.deferred_until(at));
                    prop_assert_eq!(actual.is_some(), model[usize::from(id)].is_some());
                    if let Some(delivery) = model[usize::from(id)].as_mut() {
                        delivery.retry_ms = now_ms + ms;
                    }
                }
                Input::QueueFull(id) => {
                    let actual = schedule.delivery_mut(&peer(id)).map(|delivery| delivery.queue_full(now));
                    prop_assert_eq!(actual.is_some(), model[usize::from(id)].is_some());
                    if let Some(delivery) = model[usize::from(id)].as_mut() {
                        delivery.retry_ms = now_ms + 100;
                    }
                }
                Input::StatusChange => latest += 1,
                Input::Flush { live, ready } => {
                    let admitted: Vec<bool> = model.iter().map(Option::is_some).collect();
                    let peers = flush_peers(&admitted, live, ready);
                    let due = schedule.due(status(latest), &peers, now);
                    let expected_due: Vec<_> = peers
                        .iter()
                        .filter(|(id, ready)| {
                            let index = usize::from(id.as_bytes()[0]);
                            model[index]
                                .and_then(|delivery| delivery.deadline(latest, *ready))
                                .is_some_and(|retry_ms| retry_ms <= now_ms)
                        })
                        .map(|(id, _)| id.clone())
                        .collect();
                    prop_assert_eq!(&due, &expected_due);

                    schedule.rearm(status(latest), &peers);
                    let expected_wake = peers
                        .iter()
                        .filter_map(|(id, ready)| {
                            model[usize::from(id.as_bytes()[0])]
                                .and_then(|delivery| delivery.deadline(latest, *ready))
                        })
                        .min()
                        .map(|ms| base + Duration::from_millis(ms));
                    prop_assert_eq!(schedule.refresh_at(), expected_wake);
                }
                Input::Wait(ms) => now_ms += ms,
            }
        }
    }
}
