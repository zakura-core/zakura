//! Cadence: bucket arithmetic, exemptions, the conformant-sender guarantee,
//! and the sender's one-frame bound.

use std::time::Duration;

use proptest::prelude::*;

use super::*;
use crate::zakura::{
    framed_channel,
    regulation::test_family::{Probe, DONE, EVERY_30_SECONDS, GET, PART, PING, PONG, STATUS},
    testkit::TestClock,
};

const LAYOUT: u16 = 900;

fn buckets() -> (CadenceBuckets<TestClock>, TestClock) {
    let clock = TestClock::new();
    (CadenceBuckets::new(clock.clone()), clock)
}

fn secs(secs: u64) -> Duration {
    Duration::from_secs(secs)
}

#[test]
fn a_full_bucket_admits_its_capacity_back_to_back_then_exhausts() {
    let (mut buckets, _clock) = buckets();
    for _ in 0..EVERY_30_SECONDS.capacity {
        assert_eq!(buckets.charge(LAYOUT, &STATUS), CadenceCharge::Admit);
    }
    assert_eq!(buckets.charge(LAYOUT, &STATUS), CadenceCharge::Exhausted);
}

#[test]
fn one_token_returns_per_refill_interval_up_to_capacity() {
    let (mut buckets, clock) = buckets();
    for _ in 0..EVERY_30_SECONDS.capacity {
        buckets.charge(LAYOUT, &STATUS);
    }
    clock.advance(secs(14));
    assert_eq!(buckets.charge(LAYOUT, &STATUS), CadenceCharge::Exhausted);
    clock.advance(secs(1));
    assert_eq!(buckets.charge(LAYOUT, &STATUS), CadenceCharge::Admit);
    clock.advance(secs(24 * 60 * 60));
    buckets.charge(LAYOUT, &STATUS);
    assert_eq!(
        buckets.tokens(LAYOUT, STATUS.message_type),
        Some(EVERY_30_SECONDS.capacity - 1)
    );
}

#[test]
fn a_sender_at_exactly_the_refill_rate_never_drifts() {
    // Main's bucket discarded the fractional token on every refill. This row
    // refills once per 1.5 s and the sender matches it, charging at uneven
    // points within each interval.
    let exact = MessageRule {
        role: MessageRole::Announcement {
            cadence: Cadence {
                capacity: 2,
                refill_interval: Duration::from_millis(1500),
                send_interval: Duration::from_millis(1500),
            },
        },
        ..STATUS
    };
    let (mut buckets, clock) = buckets();
    buckets.charge(LAYOUT, &exact);
    for step in 0..10_000u64 {
        clock.advance(Duration::from_millis(if step % 2 == 0 {
            1000
        } else {
            2000
        }));
        assert_eq!(
            buckets.charge(LAYOUT, &exact),
            CadenceCharge::Admit,
            "step {step}"
        );
    }
}

#[test]
fn rows_without_a_cadence_charge_nothing() {
    let (mut buckets, _clock) = buckets();
    for rule in [GET, PART, DONE, PONG] {
        for _ in 0..4096 {
            assert_eq!(buckets.charge(LAYOUT, &rule), CadenceCharge::Exempt);
        }
        assert_eq!(buckets.tokens(LAYOUT, rule.message_type), None);
    }
    // A request with a cadence is charged.
    assert_eq!(buckets.charge(LAYOUT, &PING), CadenceCharge::Admit);
}

#[test]
fn layouts_that_reuse_a_message_type_have_separate_buckets() {
    let (mut buckets, _clock) = buckets();
    for _ in 0..EVERY_30_SECONDS.capacity {
        buckets.charge(LAYOUT, &STATUS);
    }
    assert_eq!(buckets.charge(LAYOUT, &STATUS), CadenceCharge::Exhausted);
    assert_eq!(buckets.charge(LAYOUT + 1, &STATUS), CadenceCharge::Admit);
}

#[test]
fn a_sender_faster_than_the_refill_exhausts_only_after_its_capacity() {
    // 0.9 × the refill interval: one token lost per ten messages.
    let (mut buckets, clock) = buckets();
    let mut admitted = 0u32;
    loop {
        match buckets.charge(LAYOUT, &STATUS) {
            CadenceCharge::Admit => admitted += 1,
            CadenceCharge::Exhausted => break,
            CadenceCharge::Exempt => unreachable!("STATUS declares a cadence"),
        }
        assert!(admitted < 1000, "a fast sender must exhaust the bucket");
        clock.advance(Duration::from_millis(13_500));
    }
    assert!(admitted >= EVERY_30_SECONDS.capacity);
}

#[test]
fn the_burst_after_the_longest_outage_is_admitted() {
    let (mut buckets, clock) = buckets();
    // Steady sending first.
    for _ in 0..100 {
        assert_eq!(buckets.charge(LAYOUT, &STATUS), CadenceCharge::Admit);
        clock.advance(EVERY_30_SECONDS.send_interval);
    }
    // The outage: every message sent during it arrives at its end, together
    // with the one sent as the connection recovers.
    clock.advance(Cadence::MAX_OUTAGE);
    let burst = Cadence::MAX_OUTAGE.as_secs() / EVERY_30_SECONDS.send_interval.as_secs() + 1;
    for _ in 0..burst {
        assert_eq!(buckets.charge(LAYOUT, &STATUS), CadenceCharge::Admit);
    }
}

#[test]
fn the_burst_after_a_local_pause_of_any_length_is_admitted() {
    for pause in [
        secs(1),
        secs(29),
        secs(31),
        secs(10 * 60),
        secs(24 * 60 * 60),
    ] {
        let (mut buckets, clock) = buckets();
        for _ in 0..10 {
            buckets.charge(LAYOUT, &STATUS);
            clock.advance(EVERY_30_SECONDS.send_interval);
        }
        clock.advance(pause);
        buckets.credit_pause(LAYOUT, &[STATUS, GET], pause);
        let burst = pause.as_secs() / EVERY_30_SECONDS.send_interval.as_secs() + 1;
        for sent in 0..burst {
            assert_eq!(
                buckets.charge(LAYOUT, &STATUS),
                CadenceCharge::Admit,
                "message {sent} after a {pause:?} pause"
            );
        }
    }
}

#[test]
fn pause_credit_carries_its_remainder() {
    let (mut buckets, _clock) = buckets();
    for _ in 0..EVERY_30_SECONDS.capacity {
        buckets.charge(LAYOUT, &STATUS);
    }
    // Many short pauses add up to exactly one refill interval.
    for _ in 0..15 {
        buckets.credit_pause(LAYOUT, &[STATUS], secs(1));
    }
    assert_eq!(buckets.tokens(LAYOUT, STATUS.message_type), Some(1));
}

/// A conformant sender's schedule and the network and reader around it.
#[derive(Clone, Debug)]
struct Schedule {
    /// The row's cadence: a minimal one that the layout validator accepts.
    cadence: Cadence,
    /// Extra gap after `send_interval` before each message, in seconds.
    gaps: Vec<u64>,
    /// Transport delay of each message, in seconds, below the outage bound.
    delays: Vec<u64>,
    /// Local read pauses: (message index, seconds).
    pauses: Vec<(usize, u64)>,
}

fn schedule() -> impl Strategy<Value = Schedule> {
    let max_delay = Cadence::MAX_OUTAGE.as_secs() - 1;
    let cadence = (2u64..=120).prop_flat_map(|send| {
        (1..send).prop_map(move |refill| Cadence {
            capacity: Cadence::min_capacity(secs(send)),
            refill_interval: secs(refill),
            send_interval: secs(send),
        })
    });
    (cadence, 1usize..400).prop_flat_map(move |(cadence, len)| {
        let send = cadence.send_interval.as_secs();
        (
            prop::collection::vec(prop_oneof![4 => Just(0u64), 1 => 0..20 * send], len),
            prop::collection::vec(
                prop_oneof![2 => Just(0u64), 1 => Just(max_delay), 1 => 0..=max_delay],
                len,
            ),
            prop::collection::vec((0..len, 0u64..7200), 0..4),
        )
            .prop_map(move |(gaps, delays, pauses)| Schedule {
                cadence,
                gaps,
                delays,
                pauses,
            })
    })
}

proptest! {
    /// For every cadence the layout validator accepts, any sender that obeys
    /// `send_interval`, over a transport that delivers in order and delays
    /// each message by less than the outage bound, to a reader that pauses at
    /// will, is never exhausted.
    #[test]
    fn a_conformant_sender_is_never_exhausted(schedule in schedule()) {
        let row = MessageRule {
            role: MessageRole::Announcement { cadence: schedule.cadence },
            ..STATUS
        };
        let send = schedule.cadence.send_interval.as_secs();
        // Sending times, then in-order delivery times.
        let mut sent_at = 0;
        let mut delivered_at = 0;
        let mut arrivals = Vec::new();
        for (gap, delay) in schedule.gaps.iter().zip(&schedule.delays) {
            delivered_at = (sent_at + delay).max(delivered_at);
            arrivals.push(delivered_at);
            sent_at += send + gap;
        }
        let (mut buckets, clock) = buckets();
        let mut now = 0;
        for (index, arrival) in arrivals.into_iter().enumerate() {
            // The reader reads at the arrival, or after its own pause.
            let mut read_at = arrival;
            for &(at, pause) in &schedule.pauses {
                if at == index {
                    read_at = read_at.max(now) + pause;
                    clock.advance(secs(read_at - now));
                    now = read_at;
                    buckets.credit_pause(LAYOUT, &[row], secs(pause));
                }
            }
            if read_at > now {
                clock.advance(secs(read_at - now));
                now = read_at;
            }
            prop_assert_eq!(buckets.charge(LAYOUT, &row), CadenceCharge::Admit, "message {}", index);
        }
    }
}

fn status_frames(output: &mut crate::zakura::FramedRecv) -> usize {
    std::iter::from_fn(|| output.try_recv().ok()).count()
}

#[test]
fn the_sender_waits_for_its_interval_and_keeps_only_the_latest_value() {
    let clock = TestClock::new();
    let mut sender = CadenceSender::<Probe, _>::with_clock(clock.clone());
    let (send, mut output) = framed_channel(8);
    assert_eq!(sender.next_due(), None);
    sender.update(Probe::Status(1)).unwrap();
    assert_eq!(sender.send_due(&send), Ok(1));
    sender.update(Probe::Status(2)).unwrap();
    sender.update(Probe::Status(3)).unwrap();
    assert_eq!(sender.send_due(&send), Ok(0), "the interval has not passed");
    clock.advance(EVERY_30_SECONDS.send_interval);
    // The first frame is still unwritten; the sender waits for it.
    assert_eq!(sender.send_due(&send), Ok(0));
    let first = output.try_recv().unwrap();
    assert_eq!(
        crate::zakura::wire_codec::decode_frame::<Probe>(&first),
        Ok(Probe::Status(1))
    );
    assert_eq!(sender.send_due(&send), Ok(1));
    let second = output.try_recv().unwrap();
    assert_eq!(
        crate::zakura::wire_codec::decode_frame::<Probe>(&second),
        Ok(Probe::Status(3)),
        "the latest value replaced the older one"
    );
    assert_eq!(
        sender.update(Probe::Get(1)),
        Err(CadenceSendError::NoCadence { message_type: 1 })
    );
}

#[test]
fn a_blocked_writer_holds_at_most_one_frame_per_row() {
    let clock = TestClock::new();
    let mut sender = CadenceSender::<Probe, _>::with_clock(clock.clone());
    let (send, mut output) = framed_channel(64);
    for value in 0..100 {
        sender.update(Probe::Status(value)).unwrap();
        sender.update(Probe::Ping(value)).unwrap();
        sender.send_due(&send).unwrap();
        clock.advance(EVERY_30_SECONDS.send_interval);
    }
    // Nobody read the output: one frame for each cadence row.
    assert_eq!(status_frames(&mut output), 2);
}
