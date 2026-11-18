//! Decoder v1 for cadence hold and settlement histories.
//!
//! The replay drives the production sender's rows with a one-frame queue. It
//! records what it updated, when each row queued a frame, and which frames
//! wait in the queue. It checks each send result only against those records:
//! a result names a hold that the records show, and a queued frame carries the
//! row's latest value. It never derives the result itself; named tests in
//! `tests.rs` pin which hold a row reports first. Receiving a frame settles
//! its guard synchronously; scheduled transport writes belong in IO tests.

use std::{collections::VecDeque, time::Duration};

use zakura_rt::time::Instant;

use super::{cadence, send_slot, Bucket, RowSend, Slot};
use crate::zakura::{
    framed_channel,
    machine_test::MAX_HISTORY_BYTES,
    regulation::test_family::{decode, Probe, PING, STATUS},
};

const HOLD: &str = "cadence/hold";
const SETTLEMENT: &str = "cadence/settlement";
const INTERVAL: u64 = 30;
/// Frames the shared queue holds.
const QUEUE: usize = 1;

/// Applicable transitions in one decoder-v1 cadence history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct CadenceReplaySummary {
    /// Send attempts checked against the replay's records.
    pub attempts: usize,
    /// Attempts held by the row's interval.
    pub paced: usize,
    /// Attempts held by a queued frame's owner.
    pub unwritten: usize,
    /// Attempts refused by the shared queue.
    pub queue_full: usize,
    /// Frames whose guards settled at the receive boundary.
    pub settled: usize,
}

/// Replay at most [`MAX_HISTORY_BYTES`] bytes as two cadence rows sharing a
/// one-frame queue.
///
/// Each byte chooses update, advance, send or settle. The high bits choose the
/// row, value or elapsed seconds. A result the records contradict panics with
/// its property ID.
pub fn replay_cadence_bytes(data: &[u8]) -> CadenceReplaySummary {
    replay(data, |_| {})
}

fn replay(data: &[u8], mut before_send: impl FnMut(&mut Slot<Probe>)) -> CadenceReplaySummary {
    let origin = Instant::now();
    let mut now = 0;
    let mut slots = [STATUS, PING].map(|rule| Slot {
        message_type: rule.message_type,
        send_interval: Duration::from_secs(INTERVAL),
        latest: None,
        last_sent: None,
        unwritten: zakura_rt::sync::Weak::new(),
        mirror: Bucket::new(
            cadence(&rule).expect("the replayed rows declare a cadence"),
            origin,
        ),
    });
    let (send, mut recv) = framed_channel(QUEUE);
    // The latest value updated on each row since it last queued.
    let mut pending = [None, None];
    // When each row last queued a frame, in seconds after `origin`.
    let mut sent_at: [Option<u64>; 2] = [None, None];
    // The frames in the queue: their row and value, oldest first.
    let mut queue = VecDeque::new();
    let mut summary = CadenceReplaySummary::default();
    for &byte in &data[..data.len().min(MAX_HISTORY_BYTES)] {
        let row = usize::from((byte >> 2) & 1);
        match byte & 3 {
            0 => {
                let value = u32::from(byte >> 3);
                pending[row] = Some(value);
                slots[row].latest = Some(message(row, value));
            }
            1 => now += u64::from(byte >> 2),
            2 => {
                before_send(&mut slots[row]);
                let result = send_slot(&mut slots[row], origin + Duration::from_secs(now), &send)
                    .expect("the replay's queue stays open and its values encode");
                let due = sent_at[row].map(|at| at + INTERVAL);
                let paced = due.is_some_and(|due| now < due);
                let owns_a_frame = queue.iter().any(|&(owner, _)| owner == row);
                let context = format!("byte {byte}, time {now}, {result:?}");
                if result != RowSend::Idle {
                    assert!(pending[row].is_some(), "{HOLD}: no value waits, {context}");
                }
                match result {
                    RowSend::Idle => {
                        assert!(pending[row].is_none(), "{HOLD}: a value waits, {context}");
                    }
                    RowSend::DeferredUntil(at) => {
                        assert!(paced, "{HOLD}: the interval passed, {context}");
                        assert_eq!(
                            Some(at),
                            due.map(|due| origin + Duration::from_secs(due)),
                            "{HOLD}: a deferral ends one interval after the last send"
                        );
                        summary.paced += 1;
                    }
                    RowSend::Unwritten => {
                        assert!(owns_a_frame, "{HOLD}: no frame of the row waits, {context}");
                        summary.unwritten += 1;
                    }
                    RowSend::QueueFull => {
                        assert_eq!(queue.len(), QUEUE, "{HOLD}: the queue has room, {context}");
                        summary.queue_full += 1;
                    }
                    RowSend::Queued => {
                        assert!(!paced, "{HOLD}: the interval has not passed, {context}");
                        assert!(
                            !owns_a_frame,
                            "{HOLD}: the row's last frame is unwritten, {context}"
                        );
                        let value = pending[row]
                            .take()
                            .expect("checked above: a queued row had a value");
                        queue.push_back((row, value));
                        sent_at[row] = Some(now);
                    }
                }
                summary.attempts += 1;
            }
            _ => match queue.pop_front() {
                Some((owner, value)) => {
                    let frame = recv.try_recv().expect("the records name a queued frame");
                    assert_eq!(decode(&frame), message(owner, value), "{SETTLEMENT}");
                    summary.settled += 1;
                }
                None => assert!(recv.try_recv().is_err(), "{SETTLEMENT}: unexpected frame"),
            },
        }
        for (row, slot) in slots.iter().enumerate() {
            slot.assert_invariants();
            // The row holds a frame's guard exactly while that frame is in
            // the queue.
            assert_eq!(
                slot.unwritten.strong_count() > 0,
                queue.iter().any(|&(owner, _)| owner == row),
                "{SETTLEMENT}: frame ownership"
            );
        }
    }
    summary
}

fn message(row: usize, value: u32) -> Probe {
    if row == 0 {
        Probe::Status(value)
    } else {
        Probe::Ping(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zakura::machine_test::machine_target_tests;

    // Status queues; replacement waits for time and settlement. Ping then fills
    // the shared queue while Status waits. Both rows eventually settle.
    const WITNESS: &[u8] = &[0, 2, 8, 2, 121, 2, 3, 4, 6, 2, 3, 2, 3];

    machine_target_tests! {
        replay: replay_cadence_bytes,
        target: "cadence",
        decoder: 1,
        witness: WITNESS => CadenceReplaySummary {
            attempts: 6,
            paced: 1,
            unwritten: 1,
            queue_full: 1,
            settled: 3,
        },
    }

    #[test]
    #[should_panic(expected = "cadence/hold")]
    fn the_replay_rejects_a_shortened_interval() {
        // Status queues and settles, then sends a new value at once.
        replay(&[0, 2, 3, 8, 2], |slot| {
            slot.last_sent = slot
                .last_sent
                .and_then(|at| at.checked_sub(Duration::from_secs(INTERVAL)));
        });
    }

    #[test]
    #[should_panic(expected = "a row holds an unwritten frame it never sent")]
    fn the_invariants_reject_a_missing_interval() {
        replay(WITNESS, |slot| slot.last_sent = None);
    }

    #[test]
    #[should_panic(expected = "cadence/settlement")]
    fn the_replay_rejects_a_missing_frame_hold() {
        replay(WITNESS, |slot| {
            slot.unwritten = zakura_rt::sync::Weak::new()
        });
    }
}
