//! Decoder v1 for cadence hold and settlement histories.
//!
//! The driver calls the production sender with a one-frame queue. An independent
//! ledger tracks pending values, send times and queued owners. Receiving a frame
//! settles its guard synchronously; scheduled transport writes belong in IO tests.

use std::{collections::VecDeque, time::Duration};

use zakura_rt::time::Instant;

use super::{send_slot, RowSend, Slot};
use crate::zakura::{
    framed_channel,
    regulation::test_family::{decode, Probe, PING, STATUS},
};

const HOLD: &str = "cadence/hold";
const SETTLEMENT: &str = "cadence/settlement";
const INTERVAL: u64 = 30;

/// Applicable transitions in one decoder-v1 cadence history.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct CadenceReplaySummary {
    /// Send attempts checked against the independent ledger.
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

/// Replay at most 512 bytes as two cadence rows sharing a one-frame queue.
///
/// Each byte chooses update, advance, send or settle. The high bits choose the
/// row, value or elapsed seconds. A disagreement panics with its property ID.
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
    });
    let (send, mut recv) = framed_channel(1);
    let mut pending = [None, None];
    let mut sent_at: [Option<u64>; 2] = [None, None];
    let mut queue = VecDeque::new();
    let mut summary = CadenceReplaySummary::default();
    for &byte in data.iter().take(512) {
        let row = usize::from((byte >> 2) & 1);
        match byte & 3 {
            0 => {
                let value = u32::from(byte >> 3);
                pending[row] = Some(value);
                slots[row].latest = Some(message(row, value));
            }
            1 => now += u64::from(byte >> 2),
            2 => {
                let expected = if pending[row].is_none() {
                    RowSend::Idle
                } else if let Some(due) = sent_at[row].filter(|at| now < *at + INTERVAL) {
                    summary.paced += 1;
                    RowSend::DeferredUntil(origin + Duration::from_secs(due + INTERVAL))
                } else if queue.iter().any(|&(owner, _)| owner == row) {
                    summary.unwritten += 1;
                    RowSend::Unwritten
                } else if !queue.is_empty() {
                    summary.queue_full += 1;
                    RowSend::QueueFull
                } else {
                    RowSend::Queued
                };
                before_send(&mut slots[row]);
                let actual = send_slot(&mut slots[row], origin + Duration::from_secs(now), &send);
                assert_eq!(actual, Ok(expected), "{HOLD}: byte {byte}, time {now}");
                summary.attempts += 1;
                if expected == RowSend::Queued {
                    let value = pending[row]
                        .take()
                        .expect("the ledger admitted a pending value");
                    queue.push_back((row, value));
                    sent_at[row] = Some(now);
                }
            }
            _ => match queue.pop_front() {
                Some((owner, value)) => {
                    let frame = recv.try_recv().expect("the ledger names a queued frame");
                    assert_eq!(decode(&frame), message(owner, value), "{SETTLEMENT}");
                    summary.settled += 1;
                }
                None => assert!(recv.try_recv().is_err(), "{SETTLEMENT}: unexpected frame"),
            },
        }
        for row in 0..2 {
            assert_eq!(
                slots[row].latest,
                pending[row].map(|value| message(row, value)),
                "{HOLD}: pending value"
            );
            assert_eq!(
                slots[row].unwritten.strong_count() > 0,
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
    use proptest::prelude::*;

    // Status queues; replacement waits for time and settlement. Ping then fills
    // the shared queue while Status waits. Both rows eventually settle.
    const WITNESS: &[u8] = &[0, 2, 8, 2, 121, 2, 3, 4, 6, 2, 3, 2, 3];

    #[test]
    #[allow(
        clippy::print_stdout,
        reason = "CI retains these applicability counters"
    )]
    fn cadence_witness_exercises_every_hold_and_settlement() {
        let summary = replay_cadence_bytes(WITNESS);
        println!("tier=Machine target=cadence decoder=1 {summary:?}");
        assert_eq!(
            summary,
            CadenceReplaySummary {
                attempts: 6,
                paced: 1,
                unwritten: 1,
                queue_full: 1,
                settled: 3,
            }
        );
        assert_eq!(replay_cadence_bytes(&[]), CadenceReplaySummary::default());
    }

    #[test]
    #[should_panic(expected = "cadence/hold")]
    fn cadence_witness_rejects_missing_interval() {
        replay(WITNESS, |slot| slot.last_sent = None);
    }

    #[test]
    #[should_panic(expected = "cadence/settlement")]
    fn cadence_witness_rejects_missing_frame_hold() {
        replay(WITNESS, |slot| {
            slot.unwritten = zakura_rt::sync::Weak::new()
        });
    }

    proptest! {
        #[test]
        fn cadence_histories_match_the_ledger(bytes in prop::collection::vec(any::<u8>(), 0..=512)) {
            replay_cadence_bytes(&bytes);
        }
    }
}
