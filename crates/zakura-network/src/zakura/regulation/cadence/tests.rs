//! Cadence: bucket arithmetic, exemptions, the conformant-sender guarantee,
//! and the sender's one-frame bound. Named tests pin each rule; one proptest
//! compares the ledger with a naive interval-stepping bucket.

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

/// A dispatch bucket that refills while full drops the partial interval, so
/// any window holds at most `capacity` plus its whole intervals of dispatches.
#[test]
fn a_capped_refill_keeps_no_partial_token_while_full() {
    let start = Instant::now();
    let interval = EVERY_30_SECONDS.refill_interval;
    let mut capped = Bucket::new(EVERY_30_SECONDS, start);
    let mut uncapped = Bucket::new(EVERY_30_SECONDS, start);
    let burst = start + interval.mul_f64(1.9);
    capped.refill_capped(burst);
    uncapped.refill(burst);
    for bucket in [&mut capped, &mut uncapped] {
        for _ in 0..EVERY_30_SECONDS.capacity {
            assert!(bucket.take());
        }
    }
    // The uncapped bucket earns a token 0.1 interval after the burst spent
    // its capacity.
    let soon = start + interval * 2;
    uncapped.refill(soon);
    assert!(uncapped.take());
    capped.refill_capped(soon);
    assert!(!capped.take());
    assert_eq!(capped.next_token_at(), burst + interval);
    capped.refill_capped(burst + interval);
    assert!(capped.take());
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
            CadenceCharge::Exempt | CadenceCharge::Overrun => {
                unreachable!("STATUS declares a cadence")
            }
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

#[test]
fn a_row_reports_why_it_did_not_send() {
    let clock = TestClock::new();
    let mut sender = CadenceSender::<Probe, _>::with_clock(clock.clone());
    let (send, mut output) = framed_channel(1);
    let status = STATUS.message_type;
    assert_eq!(sender.send_row(status, &send), Ok(RowSend::Idle));
    sender.update(Probe::Status(1)).unwrap();
    assert_eq!(sender.send_row(status, &send), Ok(RowSend::Queued));
    sender.update(Probe::Status(2)).unwrap();
    let due = clock.now() + EVERY_30_SECONDS.send_interval;
    assert_eq!(
        sender.send_row(status, &send),
        Ok(RowSend::DeferredUntil(due)),
        "pacing is not queue pressure"
    );
    clock.advance(EVERY_30_SECONDS.send_interval);
    assert_eq!(sender.send_row(status, &send), Ok(RowSend::Unwritten));
    output.try_recv().unwrap();
    // Another row's frame fills the one-slot queue.
    sender.update(Probe::Ping(1)).unwrap();
    assert_eq!(
        sender.send_row(PING.message_type, &send),
        Ok(RowSend::Queued)
    );
    assert_eq!(sender.send_row(status, &send), Ok(RowSend::QueueFull));
    output.try_recv().unwrap();
    assert_eq!(sender.send_row(status, &send), Ok(RowSend::Queued));
    assert_eq!(
        sender.send_row(GET.message_type, &send),
        Err(CadenceSendError::NoCadence {
            message_type: GET.message_type
        })
    );
}

#[test]
fn a_minimum_interval_only_slows_a_row() {
    let clock = TestClock::new();
    let slower = EVERY_30_SECONDS.send_interval * 2;
    let mut sender = CadenceSender::<Probe, _>::with_clock(clock.clone())
        .with_min_interval(STATUS.message_type, Duration::from_secs(1))
        .unwrap()
        .with_min_interval(PING.message_type, slower)
        .unwrap();
    let (send, mut output) = framed_channel(8);
    sender.update(Probe::Status(1)).unwrap();
    sender.update(Probe::Ping(1)).unwrap();
    assert_eq!(sender.send_due(&send), Ok(2));
    status_frames(&mut output);
    let start = clock.now();
    sender.update(Probe::Status(2)).unwrap();
    sender.update(Probe::Ping(2)).unwrap();
    assert_eq!(
        sender.send_row(STATUS.message_type, &send),
        Ok(RowSend::DeferredUntil(
            start + EVERY_30_SECONDS.send_interval
        ))
    );
    assert_eq!(
        sender.send_row(PING.message_type, &send),
        Ok(RowSend::DeferredUntil(start + slower))
    );
}

/// One input to a single cadence bucket.
#[derive(Clone, Debug)]
enum LedgerOp {
    Charge { advance: Duration },
    Pause { advance: Duration, paused: Duration },
}

fn ledger_op() -> impl Strategy<Value = LedgerOp> {
    prop_oneof![
        3 => (0..10_000u64).prop_map(|advance| LedgerOp::Charge {
            advance: Duration::from_millis(advance),
        }),
        1 => (0..10_000u64, 0..10_000u64).prop_map(|(advance, paused)| LedgerOp::Pause {
            advance: Duration::from_millis(advance),
            paused: Duration::from_millis(paused),
        }),
    ]
}

/// A naive bucket that steps one refill interval at a time, where the ledger
/// divides. It tracks only the token count; the low-bucket threshold is pinned
/// by `a_charge_reports_a_low_bucket_below_a_quarter_of_capacity`.
struct BucketModel {
    cadence: Cadence,
    tokens: u32,
    next_refill: Instant,
    paused: Duration,
}

impl BucketModel {
    fn refill(&mut self, now: Instant) {
        while self.next_refill <= now {
            if self.tokens < self.cadence.capacity {
                self.tokens += 1;
            }
            self.next_refill += self.cadence.refill_interval;
        }
    }
}

proptest! {
    /// The ledger, charged at explicit instants, matches a bucket that steps
    /// one refill interval at a time: refills cap at capacity, pause credit
    /// exceeds it, and a charge is admitted exactly when a token remains.
    #[test]
    fn the_ledger_matches_an_interval_stepping_bucket(
        capacity in 1u32..8,
        refill_ms in 100u64..2_000,
        ops in prop::collection::vec(ledger_op(), 1..64),
    ) {
        let cadence = Cadence {
            capacity,
            refill_interval: Duration::from_millis(refill_ms),
            send_interval: Duration::from_millis(refill_ms),
        };
        let key = CadenceKey { layout: LAYOUT, message_type: STATUS.message_type };
        let mut now = Instant::now();
        let mut ledger = CadenceLedger::default();
        let mut model: Option<BucketModel> = None;
        for op in ops {
            let advance = match op {
                LedgerOp::Charge { advance } | LedgerOp::Pause { advance, .. } => advance,
            };
            now += advance;
            let bucket = model.get_or_insert_with(|| BucketModel {
                cadence,
                tokens: capacity,
                next_refill: now + cadence.refill_interval,
                paused: Duration::ZERO,
            });
            bucket.refill(now);
            match op {
                LedgerOp::Charge { .. } => {
                    let admitted = matches!(ledger.charge(key, cadence, now, false), Charged::Admit { .. });
                    prop_assert_eq!(admitted, bucket.tokens > 0);
                    if admitted {
                        bucket.tokens -= 1;
                    }
                }
                LedgerOp::Pause { paused, .. } => {
                    ledger.credit_pause(key, cadence, paused, now);
                    bucket.paused += paused;
                    while bucket.paused >= cadence.refill_interval {
                        bucket.tokens += 1;
                        bucket.paused -= cadence.refill_interval;
                    }
                }
            }
            let actual = ledger.buckets.get(&key).expect("the first input created the bucket");
            actual.assert_invariants(now);
            prop_assert_eq!(actual.tokens, bucket.tokens);
        }
    }
}

/// A metered row: 10,000 bytes a second, a 50,000 byte opening allowance and
/// 20,000 bytes the sender may hold unwritten.
const METERED_RATE: ByteRate = ByteRate {
    bytes_per_second: 10_000,
    opening_allowance: 50_000,
    sender_allowance: 20_000,
};

const METERED: MessageRule = MessageRule {
    name: "metered",
    message_type: 40,
    payload: crate::zakura::PayloadLen::between(1, 10_000),
    role: MessageRole::Metered { rate: METERED_RATE },
};

/// The stream receive window the metered tests grant.
const WINDOW: u64 = 30_000;

#[test]
fn a_byte_bucket_pays_its_depth_then_refills_at_its_rate() {
    let start = Instant::now();
    let mut bucket = ByteBucket::sender(METERED_RATE, start);
    assert!(bucket.try_pay(50_000, start));
    assert!(!bucket.try_pay(1, start));
    assert_eq!(bucket.ready_at(1_000, start), Some(start + secs(1) / 10));
    assert_eq!(bucket.ready_at(50_001, start), None);

    // A tenth of a second earns 1,000 bytes, in ten steps or in one.
    let mut stepped = bucket.clone();
    for step in 1..=10 {
        stepped.refill(start + Duration::from_millis(10 * step));
    }
    bucket.refill(start + secs(1) / 10);
    assert_eq!(bucket.tokens(), 1_000);
    assert_eq!(stepped.tokens(), 1_000);

    // It refills to its depth and no further.
    assert!(!bucket.try_pay(50_001, start + secs(60)));
    assert!(bucket.try_pay(50_000, start + secs(60)));
}

/// A metered sender pays its connection's bucket before a frame goes, so
/// two senders on one connection share its allowance, and each keeps at
/// most one of its frames unwritten.
#[test]
fn a_metered_sender_pays_first_and_keeps_one_frame_unwritten() {
    let start = Instant::now();
    let bucket = Arc::new(Mutex::new(ByteBucket::sender(METERED_RATE, start)));
    let (mut first, mut second) = (
        MeteredSender::new(bucket.clone()),
        MeteredSender::new(bucket),
    );
    assert_eq!(first.ready(30_000, start), MeteredReady::Now);
    let written = first.pay(30_000, start).expect("the bucket holds it");
    assert_eq!(first.ready(1, start), MeteredReady::Unwritten);
    assert!(first.pay(1, start).is_none(), "one frame unwritten at most");

    // The other sender finds what the first left.
    let at = start + secs(1);
    assert_eq!(second.ready(30_000, start), MeteredReady::At(at));
    assert!(second.pay(30_000, start).is_none());
    assert!(second.pay(30_000, at).is_some());

    // Once the transport writes the first frame, the next may go.
    drop(written);
    assert_eq!(first.ready(1, at), MeteredReady::At(at + secs(1) / 10_000));
}

#[test]
fn a_metered_row_overruns_only_past_the_receivers_depth() {
    let (buckets, clock) = buckets();
    let mut buckets = buckets.with_receive_window(WINDOW);
    let depth = METERED_RATE.receiver_depth(WINDOW);
    assert_eq!(depth, 100_000);
    // Long enough after its epoch, the bucket holds its whole depth.
    clock.advance(secs(60));
    for _ in 0..depth / 10_000 {
        assert_eq!(
            buckets.charge_frame(LAYOUT, &METERED, 10_000),
            CadenceCharge::Admit
        );
    }
    assert_eq!(
        buckets.charge_frame(LAYOUT, &METERED, 1),
        CadenceCharge::Overrun
    );
    // The receiver refills an eighth faster than the sender's rate.
    clock.advance(secs(1));
    assert_eq!(
        buckets.charge_frame(LAYOUT, &METERED, 11_250),
        CadenceCharge::Admit
    );
    assert_eq!(
        buckets.charge_frame(LAYOUT, &METERED, 1),
        CadenceCharge::Overrun
    );
}

/// A sender that pays its own bucket and keeps at most its sender allowance
/// plus the stream window unread, through a stall of `stall` that starts at
/// `stall_at`, after which the receiver reads everything at once. Returns
/// the bytes the receiver read.
fn stall_and_resume(stall_at: Duration, stall: Duration, frame: u64) -> u64 {
    let (receiver, clock) = buckets();
    // The sender's bucket starts at the receiver's epoch, the earliest a
    // conformant sender's can.
    let start = clock.now();
    let mut receiver = receiver.with_receive_window(WINDOW);
    let mut sender = ByteBucket::sender(METERED_RATE, start);
    let limit = u64::from(METERED_RATE.sender_allowance) + WINDOW;
    let step = Duration::from_millis(10);
    let mut unread: Vec<u64> = Vec::new();
    let mut read = 0;
    let mut at = Duration::ZERO;
    while at < stall_at + stall + secs(120) {
        let now = start + at;
        let buffered: u64 = unread.iter().sum();
        if buffered + frame <= limit && sender.try_pay(frame, now) {
            unread.push(frame);
        }
        let stalled = at >= stall_at && at < stall_at + stall;
        if !stalled {
            for bytes in unread.drain(..) {
                let charge = receiver.charge_frame(
                    LAYOUT,
                    &METERED,
                    usize::try_from(bytes).expect("test frames fit usize"),
                );
                assert_eq!(charge, CadenceCharge::Admit, "at {at:?}");
                read += bytes;
            }
        }
        at += step;
        clock.advance(step);
    }
    read
}

#[test]
fn a_sender_that_stalls_and_resumes_never_overruns_the_receiver() {
    // Stalls short and long, at the start and later, fill the window and the
    // sender's buffers and let its bucket refill before the resume. The
    // short ones end before the receiver's bucket first fills.
    for (stall_at, stall) in [
        (secs(0), secs(2)),
        (secs(1), secs(2)),
        (secs(0), secs(5)),
        (secs(3), secs(60)),
        (secs(30), secs(600)),
    ] {
        for frame in [1, 700, 10_000] {
            assert!(stall_and_resume(stall_at, stall, frame) > 0);
        }
    }
}

proptest! {
    /// A peer that sends a burst, ends its session and reopens it, twenty
    /// times, on one connection. The connection's bucket outlives each
    /// session, so the bytes admitted stay within the opening allowance it
    /// started with plus the rate times the time elapsed.
    #[test]
    fn reopening_sessions_on_one_connection_buys_no_bytes(
        gaps in proptest::collection::vec(0u64..2_000, 20),
    ) {
        let (buckets, clock) = buckets();
        let mut buckets = buckets.with_receive_window(WINDOW);
        let mut admitted = 0u64;
        let mut elapsed = Duration::ZERO;
        for gap in gaps {
            // A burst until the bucket refuses it.
            while buckets.charge_frame(LAYOUT, &METERED, 1_000) == CadenceCharge::Admit {
                admitted += 1_000;
            }
            // The session ends and reopens after `gap` milliseconds.
            clock.advance(Duration::from_millis(gap));
            elapsed += Duration::from_millis(gap);
        }
        let earned = elapsed.as_millis() * u128::from(METERED_RATE.receiver_bytes_per_second()) / 1_000;
        prop_assert!(
            u128::from(admitted) <= u128::from(METERED_RATE.opening_allowance) + earned
        );
    }
}

/// A fresh connection's byte bucket holds the opening allowance, plus what
/// the time since its epoch earned, up to its depth. So a reconnect buys no
/// more than the allowance a conformant sender starts with.
#[test]
fn a_receivers_byte_bucket_starts_at_the_opening_allowance() {
    let charge_all = |buckets: &mut CadenceBuckets<TestClock>| {
        let mut admitted = 0;
        while buckets.charge_frame(LAYOUT, &METERED, 1_000) == CadenceCharge::Admit {
            admitted += 1_000;
        }
        admitted
    };
    // At its epoch: the allowance.
    let (fresh, _clock) = buckets();
    let mut fresh = fresh.with_receive_window(WINDOW);
    assert_eq!(charge_all(&mut fresh), 50_000);

    // First used two seconds after its epoch: the allowance and 22,500 bytes
    // earned, charged in 1,000 byte frames.
    let (late, clock) = buckets();
    let epoch = clock.now();
    clock.advance(secs(2));
    let mut late = late.with_receive_window(WINDOW).with_metered_epoch(epoch);
    assert_eq!(charge_all(&mut late), 72_000);

    // Long after its epoch: no more than its depth.
    let (old, clock) = buckets();
    clock.advance(secs(600));
    let mut old = old.with_receive_window(WINDOW);
    assert_eq!(charge_all(&mut old), 100_000);
}

#[test]
fn a_charge_reports_a_low_bucket_below_a_quarter_of_capacity() {
    let cadence = Cadence {
        capacity: 8,
        refill_interval: secs(15),
        send_interval: secs(30),
    };
    let key = CadenceKey {
        layout: LAYOUT,
        message_type: STATUS.message_type,
    };
    let now = Instant::now();
    let mut ledger = CadenceLedger::default();
    // Tokens left after each charge: 7, 6, 5, 4, 3, 2, 1, 0.
    for (charge, low) in [false, false, false, false, false, false, true, true]
        .into_iter()
        .enumerate()
    {
        assert_eq!(
            ledger.charge(key, cadence, now, false),
            Charged::Admit { low },
            "charge {charge}"
        );
    }
    assert_eq!(ledger.charge(key, cadence, now, false), Charged::Exhausted);
}

#[test]
fn a_parked_frame_takes_the_next_token_at_its_exact_instant() {
    let (mut buckets, clock) = buckets();
    for _ in 0..EVERY_30_SECONDS.capacity {
        buckets.charge(LAYOUT, &STATUS);
    }
    assert_eq!(buckets.charge(LAYOUT, &STATUS), CadenceCharge::Exhausted);
    // The exhausted charge took nothing.
    assert_eq!(buckets.tokens(LAYOUT, STATUS.message_type), Some(0));
    let due = clock.now() + EVERY_30_SECONDS.refill_interval;
    assert_eq!(buckets.take_parked(LAYOUT, &STATUS), Err(due));
    clock.advance(EVERY_30_SECONDS.refill_interval - Duration::from_nanos(1));
    assert_eq!(buckets.take_parked(LAYOUT, &STATUS), Err(due));
    clock.advance(Duration::from_nanos(1));
    assert_eq!(buckets.take_parked(LAYOUT, &STATUS), Ok(()));
    assert_eq!(buckets.tokens(LAYOUT, STATUS.message_type), Some(0));
    // The next parked frame waits a whole interval more.
    assert_eq!(
        buckets.take_parked(LAYOUT, &STATUS),
        Err(due + EVERY_30_SECONDS.refill_interval)
    );
    // A row without a cadence never waits.
    assert_eq!(buckets.take_parked(LAYOUT, &GET), Ok(()));
}

/// The production layouts: discovery parks; header sync, block sync and
/// transaction relay carry unpaced traffic beside their cadence rows.
#[test]
fn only_streams_whose_every_row_is_paced_park() {
    use crate::zakura::{
        block_sync::BLOCK_SYNC_SERVICE_STREAMS,
        discovery::wire::RULES as DISCOVERY,
        header_sync::v9::{CommonMessage, RegtestMessage},
        regulation::test_family::RULES as TEST_FAMILY,
        tx_relay::wire::TX_RELAY_STREAMS,
    };

    assert!(parks_on_exhaustion(DISCOVERY));
    // Header sync's Status shares its stream with subscription updates and
    // pages.
    assert!(!parks_on_exhaustion(&CommonMessage::ROWS));
    assert!(!parks_on_exhaustion(&RegtestMessage::ROWS));
    // Block sync's Status shares its stream with GetBlocks and bodies, and
    // transaction relay's Hello shares its stream with GetTxs.
    for stream in BLOCK_SYNC_SERVICE_STREAMS.iter().chain(&TX_RELAY_STREAMS) {
        let rules = stream
            .messages
            .expect("every production stream has a table");
        assert!(!parks_on_exhaustion(rules), "stream {}", stream.kind);
    }
    assert!(!parks_on_exhaustion(TEST_FAMILY));
    assert!(parks_on_exhaustion(&[STATUS, PING, PONG]));
    // A response whose request is not on the same stream isn't paced there.
    assert!(!parks_on_exhaustion(&[STATUS, PONG]));
    assert!(!parks_on_exhaustion(&[STATUS, GET, PART, DONE]));
}
