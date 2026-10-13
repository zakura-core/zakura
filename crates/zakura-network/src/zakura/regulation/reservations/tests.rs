//! Reservations: each refusal, want, the precheck, the pool, and an
//! operation-sequence proptest against a ghost key set.

use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier,
    },
    task::{Context, Wake, Waker},
    time::Duration,
};

use proptest::prelude::*;

use super::*;
use crate::zakura::{
    regulation::test_family::{message_type, Probe, RULES},
    wire_codec::decode_frame,
    Frame,
};

/// One `Get`'s response: two parts of up to 64 bytes, then the ending.
const CAP: ResponseCap = ResponseCap {
    frames: 2,
    bytes: 2 * 65 + 4,
};

fn pool(entries: usize) -> ReservationPool {
    ReservationPool::new(entries).expect("a positive pool size is valid")
}

fn reservations(pool: &ReservationPool, keys: &[u32]) -> Reservations<u32> {
    let mut reservations = Reservations::new(RULES, 8);
    for &key in keys {
        let entry = pool.try_entry().expect("the test pool has room");
        reservations
            .reserve(key, message_type::GET, CAP, entry)
            .expect("the key is new and the map has room");
    }
    reservations
}

#[test]
fn a_response_claims_its_frames_then_its_ending() {
    let pool = pool(4);
    let mut map = reservations(&pool, &[1]);
    assert_eq!(
        map.claim_frame(&1, message_type::PART, 65),
        Ok(Claimed { abandoned: false })
    );
    assert_eq!(
        map.claim_end(&1, message_type::DONE, 4),
        Ok(Ended {
            frames: 1,
            bytes: 69,
            abandoned: false,
        })
    );
    assert!(map.is_empty());
    assert_eq!(pool.held(), 0, "the ending frees the pool entry");
}

#[test]
fn every_violation_refuses() {
    let pool = pool(8);
    let mut map = reservations(&pool, &[1]);
    assert_eq!(
        map.claim_frame(&2, message_type::PART, 1),
        Err(ClaimRefused::Unsolicited {
            message_type: message_type::PART
        })
    );
    assert_eq!(
        map.claim_end(&1, message_type::PONG, 4),
        Err(ClaimRefused::WrongRequest {
            message_type: message_type::PONG
        })
    );
    for (message_type, ends) in [
        (message_type::DONE, false),
        (message_type::PART, true),
        (message_type::STATUS, false),
        (message_type::GET, true),
    ] {
        let refused = if ends {
            map.claim_end(&1, message_type, 4).unwrap_err()
        } else {
            map.claim_frame(&1, message_type, 4)
                .map(|_| ())
                .unwrap_err()
        };
        assert_eq!(refused, ClaimRefused::WrongRole { message_type });
    }
    // Two parts, then a third over the frame budget.
    map.claim_frame(&1, message_type::PART, 2).unwrap();
    map.claim_frame(&1, message_type::PART, 2).unwrap();
    assert_eq!(
        map.claim_frame(&1, message_type::PART, 2),
        Err(ClaimRefused::OverFrames { frames: 2 })
    );
    // The ending of the remaining bytes fits exactly; one byte more does not.
    let mut exact = reservations(&pool, &[2, 3]);
    assert_eq!(
        exact.claim_end(&2, message_type::DONE, 135),
        Err(ClaimRefused::OverBytes { bytes: 134 })
    );
    exact.claim_end(&3, message_type::DONE, 134).unwrap();
    // After its ending, the key is unsolicited.
    assert_eq!(
        exact.claim_end(&3, message_type::DONE, 4),
        Err(ClaimRefused::Unsolicited {
            message_type: message_type::DONE
        })
    );
    // Refusals change nothing: the refused ending of key 2 left it live.
    exact.claim_end(&2, message_type::DONE, 4).unwrap();
}

#[test]
fn an_abandoned_reservation_still_delivers_and_never_refuses() {
    let pool = pool(4);
    let mut map = reservations(&pool, &[1]);
    assert!(map.abandon(&1));
    assert_eq!(
        map.claim_frame(&1, message_type::PART, 3),
        Ok(Claimed { abandoned: true })
    );
    assert_eq!(
        map.claim_end(&1, message_type::FAILED, 4),
        Ok(Ended {
            frames: 1,
            bytes: 7,
            abandoned: true,
        })
    );
    assert!(!map.abandon(&1), "an ended reservation cannot be abandoned");
}

#[test]
fn a_fenced_reservation_ends_its_exchange_with_its_ending() {
    use crate::zakura::{regulation::WriterFence, CloseCause};
    use tokio_util::sync::CancellationToken;

    for ended in [false, true] {
        let pool = pool(2);
        let connection = CancellationToken::new();
        let fence = WriterFence::new(connection.clone(), CloseCause::new());
        let mut map = Reservations::new(RULES, 8);
        for key in [1u32, 2] {
            let exchange = fence.open().unwrap();
            let writer = exchange.writer();
            map.reserve_fenced(
                key,
                message_type::GET,
                CAP,
                pool.try_entry().unwrap(),
                exchange,
            )
            .unwrap();
            // Key 1's request reaches its first byte; key 2's never does.
            if key == 1 {
                assert!(writer.publish(|| {}));
                assert!(writer.try_start(|| true));
            }
        }
        if ended {
            map.claim_end(&1, message_type::DONE, 4).unwrap();
        }
        drop(map);
        assert_eq!(
            connection.is_cancelled(),
            !ended,
            "only a started exchange left without its ending closes the connection"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn no_time_passing_removes_a_reservation() {
    let pool = pool(4);
    let mut map = reservations(&pool, &[1]);
    tokio::time::advance(Duration::from_secs(24 * 60 * 60)).await;
    assert_eq!(map.len(), 1);
    map.claim_end(&1, message_type::DONE, 4).unwrap();
}

#[test]
fn reserving_is_local_and_bounded() {
    let pool = pool(16);
    let mut map = Reservations::new(RULES, 2);
    let entry = || pool.try_entry().unwrap();
    map.reserve(1, message_type::GET, CAP, entry()).unwrap();
    assert_eq!(
        map.reserve(1, message_type::GET, CAP, entry()),
        Err(ReserveRefused::KeyLive)
    );
    assert_eq!(
        map.reserve(2, message_type::PART, CAP, entry()),
        Err(ReserveRefused::NotARequest {
            message_type: message_type::PART
        })
    );
    map.reserve(2, message_type::PING, CAP, entry()).unwrap();
    assert_eq!(
        map.reserve(3, message_type::GET, CAP, entry()),
        Err(ReserveRefused::AtCapacity)
    );
    assert_eq!(pool.held(), 2, "refused entries return to the pool");
    assert!(map.retract(&1));
    assert!(!map.retract(&1));
    assert_eq!(pool.held(), 1);
}

#[test]
fn the_precheck_needs_a_live_reservation_with_room() {
    let pool = pool(4);
    let mut map = reservations(&pool, &[]);
    assert_eq!(
        map.precheck(message_type::PART, 1),
        Err(ClaimRefused::Unsolicited {
            message_type: message_type::PART
        })
    );
    map.reserve(1, message_type::GET, CAP, pool.try_entry().unwrap())
        .unwrap();
    map.reserve(
        2,
        message_type::GET,
        ResponseCap {
            frames: 1,
            bytes: 10,
        },
        pool.try_entry().unwrap(),
    )
    .unwrap();
    assert_eq!(map.precheck(message_type::PART, 134), Ok(()));
    assert_eq!(
        map.precheck(message_type::PART, 135),
        Err(ClaimRefused::OverBytes { bytes: 134 })
    );
    // A response to a request with no live reservation is unsolicited.
    assert_eq!(
        map.precheck(message_type::PONG, 4),
        Err(ClaimRefused::Unsolicited {
            message_type: message_type::PONG
        })
    );
    assert_eq!(
        map.precheck(message_type::GET, 4),
        Err(ClaimRefused::WrongRole {
            message_type: message_type::GET
        })
    );
    // Charging key 1 lowers the largest remaining budget to key 1's rest.
    map.claim_frame(&1, message_type::PART, 65).unwrap();
    assert_eq!(
        map.precheck(message_type::PART, 70),
        Err(ClaimRefused::OverBytes { bytes: 69 })
    );
    map.claim_end(&1, message_type::DONE, 4).unwrap();
    assert_eq!(
        map.precheck(message_type::PART, 11),
        Err(ClaimRefused::OverBytes { bytes: 10 })
    );
}

/// Decode `frame` only if the precheck passes, as a reader does.
fn checked_decode(map: &Reservations<u32>, frame: &Frame) -> Option<Probe> {
    map.precheck(frame.message_type, frame.payload.len())
        .ok()
        .map(|()| decode_frame::<Probe>(frame).expect("the test frame is valid"))
}

#[test]
fn a_refused_precheck_allocates_no_decoded_body() {
    let part = crate::zakura::wire_codec::encode_frame(&Probe::Part(vec![1; 64])).unwrap();
    let pool = pool(4);
    let empty = reservations(&pool, &[]);
    let (decoded, refused) = zakura_test::allocations::measure(|| checked_decode(&empty, &part));
    assert!(decoded.is_none());
    assert_eq!(refused.requested_bytes, 0);
    // The control: an authorized frame decodes and allocates its body.
    let live = reservations(&pool, &[1]);
    let (decoded, allowed) = zakura_test::allocations::measure(|| checked_decode(&live, &part));
    assert!(decoded.is_some());
    assert!(allowed.requested_bytes >= 64);
}

#[test]
fn the_shared_precheck_maps_refusals_to_frame_rejections() {
    let pool = pool(4);
    let shared = SharedReservations::new(reservations(&pool, &[1]));
    let slot = PrecheckSlot::default();
    assert!(slot.get().is_none());
    assert!(slot.attach(Arc::new(shared.clone())));
    assert!(!slot.attach(Arc::new(shared.clone())));
    let precheck = slot.get().unwrap();
    assert_eq!(precheck.check(message_type::PART, 65), Ok(()));
    assert_eq!(
        precheck.check(message_type::PART, 135),
        Err(FrameRejection::AboveReservation { bytes: 134 })
    );
    shared.lock().claim_end(&1, message_type::DONE, 4).unwrap();
    assert_eq!(
        precheck.check(message_type::PART, 1),
        Err(FrameRejection::Unsolicited)
    );
}

/// Count scheduler wakeups, including ones that retry a still-full pool.
#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn a_full_pool_waits_without_lost_or_spurious_wakeups() {
    let pool = pool(1);
    let held = pool.try_entry().unwrap();
    let wakes = Arc::new(WakeCount::default());
    let waker = Waker::from(wakes.clone());
    let mut context = Context::from_waker(&waker);
    let mut first = Box::pin(pool.entry());
    let mut second = Box::pin(pool.entry());
    assert!(first.as_mut().poll(&mut context).is_pending());
    assert!(second.as_mut().poll(&mut context).is_pending());
    // Waiting allocates nothing.
    let (pending, waiting) =
        zakura_test::allocations::measure(|| first.as_mut().poll(&mut context).is_pending());
    assert!(pending);
    assert_eq!(waiting.requests, 0);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 0);
    drop(held);
    assert_eq!(
        wakes.0.load(Ordering::SeqCst),
        1,
        "one release wakes one waiter"
    );
    let entry = match first.as_mut().poll(&mut context) {
        std::task::Poll::Ready(entry) => entry,
        std::task::Poll::Pending => panic!("the oldest waiter receives the freed entry"),
    };
    assert!(second.as_mut().poll(&mut context).is_pending());
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    drop(entry);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 2);
    assert!(second.as_mut().poll(&mut context).is_ready());
}

#[test]
fn racing_threads_never_overcommit_the_pool() {
    const THREADS: usize = 8;
    const ENTRIES: usize = 3;
    for _ in 0..50 {
        let pool = pool(ENTRIES);
        let barrier = Arc::new(Barrier::new(THREADS));
        let taken: Vec<_> = (0..THREADS)
            .map(|_| {
                let pool = pool.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    pool.try_entry()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|thread| thread.join().expect("no racing thread panics"))
            .collect();
        assert_eq!(taken.iter().flatten().count(), ENTRIES);
        assert_eq!(pool.held(), ENTRIES);
    }
}

#[derive(Clone, Debug)]
enum Op {
    Reserve(u32),
    Part { key: u32, len: usize },
    End { key: u32, len: usize },
    Abandon(u32),
    Retract(u32),
}

fn op() -> impl Strategy<Value = Op> {
    let key = 0u32..6;
    prop_oneof![
        2 => key.clone().prop_map(Op::Reserve),
        3 => (key.clone(), 1usize..80).prop_map(|(key, len)| Op::Part { key, len }),
        2 => (key.clone(), 1usize..80).prop_map(|(key, len)| Op::End { key, len }),
        1 => key.clone().prop_map(Op::Abandon),
        1 => key.prop_map(Op::Retract),
    ]
}

/// What the ghost set knows about one live key.
#[derive(Clone, Copy, Debug, Default)]
struct Ghost {
    frames: u32,
    bytes: u64,
    abandoned: bool,
}

proptest! {
    #[test]
    fn operation_sequences_refuse_exactly_the_violations(ops in prop::collection::vec(op(), 1..96)) {
        const CAP_ENTRIES: usize = 4;
        let pool = pool(CAP_ENTRIES + 1);
        let mut map = Reservations::new(RULES, CAP_ENTRIES);
        let mut ghost: HashMap<u32, Ghost> = HashMap::new();
        for op in ops {
            match op {
                Op::Reserve(key) => {
                    let result = map.reserve(key, message_type::GET, CAP, pool.try_entry().unwrap());
                    let expected = if ghost.contains_key(&key) {
                        Err(ReserveRefused::KeyLive)
                    } else if ghost.len() >= CAP_ENTRIES {
                        Err(ReserveRefused::AtCapacity)
                    } else {
                        ghost.insert(key, Ghost::default());
                        Ok(())
                    };
                    prop_assert_eq!(result, expected);
                }
                Op::Part { key, len } => {
                    let result = map.claim_frame(&key, message_type::PART, len);
                    match ghost.get_mut(&key) {
                        None => prop_assert!(matches!(result, Err(ClaimRefused::Unsolicited { .. })), "{:?}", result),
                        Some(state) if state.frames >= CAP.frames => {
                            prop_assert_eq!(result, Err(ClaimRefused::OverFrames { frames: CAP.frames }));
                        }
                        Some(state) if state.bytes + len as u64 > CAP.bytes => {
                            prop_assert_eq!(result, Err(ClaimRefused::OverBytes { bytes: CAP.bytes }));
                        }
                        Some(state) => {
                            prop_assert_eq!(result, Ok(Claimed { abandoned: state.abandoned }));
                            state.frames += 1;
                            state.bytes += len as u64;
                        }
                    }
                }
                Op::End { key, len } => {
                    let result = map.claim_end(&key, message_type::DONE, len);
                    match ghost.get(&key).copied() {
                        None => prop_assert!(matches!(result, Err(ClaimRefused::Unsolicited { .. })), "{:?}", result),
                        Some(state) if state.bytes + len as u64 > CAP.bytes => {
                            prop_assert_eq!(result, Err(ClaimRefused::OverBytes { bytes: CAP.bytes }));
                        }
                        Some(state) => {
                            prop_assert_eq!(result, Ok(Ended {
                                frames: state.frames,
                                bytes: state.bytes + len as u64,
                                abandoned: state.abandoned,
                            }));
                            ghost.remove(&key);
                        }
                    }
                }
                Op::Abandon(key) => {
                    prop_assert_eq!(map.abandon(&key), ghost.contains_key(&key));
                    if let Some(state) = ghost.get_mut(&key) {
                        state.abandoned = true;
                    }
                }
                Op::Retract(key) => {
                    prop_assert_eq!(map.retract(&key), ghost.remove(&key).is_some());
                }
            }
            prop_assert_eq!(map.len(), ghost.len());
            prop_assert_eq!(pool.held(), ghost.len());
            // The precheck admits exactly what the roomiest reservation fits.
            let largest = ghost.values().map(|state| CAP.bytes - state.bytes).max();
            for len in [1usize, 64, 134, 135] {
                let expected = match largest {
                    None => Err(ClaimRefused::Unsolicited { message_type: message_type::PART }),
                    Some(largest) if len as u64 > largest => Err(ClaimRefused::OverBytes { bytes: largest }),
                    Some(_) => Ok(()),
                };
                prop_assert_eq!(map.precheck(message_type::PART, len), expected);
            }
        }
        drop(map);
        prop_assert_eq!(pool.held(), 0);
    }
}

#[tokio::test]
async fn a_reader_waits_for_the_services_precheck_choice() {
    let slot = PrecheckSlot::default();
    slot.pause();
    assert!(futures::poll!(std::pin::pin!(slot.ready())).is_pending());
    let shared = SharedReservations::new(Reservations::<u32>::new(RULES, 4));
    assert!(slot.attach(Arc::new(shared)));
    assert!(futures::poll!(std::pin::pin!(slot.ready())).is_ready());
    assert_eq!(
        slot.get().unwrap().check(message_type::PART, 1),
        Err(FrameRejection::Unsolicited)
    );

    let slot = PrecheckSlot::default();
    slot.pause();
    slot.start();
    assert!(futures::poll!(std::pin::pin!(slot.ready())).is_ready());
    assert!(slot.get().is_none());
    assert!(
        !slot.attach(Arc::new(SharedReservations::new(Reservations::<u32>::new(
            RULES, 4
        ))))
    );
}
