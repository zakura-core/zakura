//! Tests for the batch worker's arrival tracking.

use std::time::Duration;

use tokio::time::Instant;

use super::Arrivals;

const BURST_GAP: Duration = Duration::from_millis(5);
const LATENCY: Duration = Duration::from_millis(100);

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

/// Records one single-item burst every `gap`, `count` times, starting at `start`, and returns
/// the time of the last one.
fn steady(arrivals: &mut Arrivals, start: Instant, gap: Duration, count: u32) -> Instant {
    let mut now = start;
    for i in 0..count {
        if i > 0 {
            now += gap;
        }
        arrivals.record(now);
    }
    now
}

#[test]
fn without_history_a_batch_only_waits_for_its_burst() {
    let mut arrivals = Arrivals::new(BURST_GAP, LATENCY);
    assert_eq!(arrivals.idle_bound(LATENCY), BURST_GAP);

    arrivals.record(Instant::now());
    assert_eq!(arrivals.idle_bound(LATENCY), BURST_GAP);
}

#[test]
fn sparse_callers_only_wait_for_their_burst() {
    let mut arrivals = Arrivals::new(BURST_GAP, LATENCY);
    steady(&mut arrivals, Instant::now(), Duration::from_secs(3), 10);

    assert_eq!(arrivals.idle_bound(LATENCY), BURST_GAP);
}

#[test]
fn items_of_one_burst_do_not_count_as_callers() {
    let mut arrivals = Arrivals::new(BURST_GAP, LATENCY);
    let mut now = Instant::now();
    for _ in 0..5 {
        // One caller queues several items in quick succession, then the next caller comes
        // seconds later.
        for _ in 0..8 {
            arrivals.record(now);
            now += ms(1);
        }
        now += Duration::from_secs(3);
    }

    assert_eq!(arrivals.idle_bound(LATENCY), BURST_GAP);
}

#[test]
fn steady_callers_wait_until_they_stall() {
    let mut arrivals = Arrivals::new(BURST_GAP, LATENCY);
    steady(&mut arrivals, Instant::now(), ms(10), 50);

    assert_eq!(arrivals.idle_bound(LATENCY), ms(40));
    // Near the latency bound, another caller is unlikely to arrive in time.
    assert_eq!(arrivals.idle_bound(ms(4)), BURST_GAP);
}

#[test]
fn saturated_callers_wait_at_least_one_burst_gap() {
    let mut arrivals = Arrivals::new(BURST_GAP, LATENCY);
    steady(&mut arrivals, Instant::now(), ms(6), 50);

    assert_eq!(arrivals.idle_bound(LATENCY), ms(24));

    let mut arrivals = Arrivals::new(Duration::ZERO, LATENCY);
    steady(
        &mut arrivals,
        Instant::now(),
        Duration::from_micros(100),
        50,
    );

    assert_eq!(arrivals.idle_bound(LATENCY), Duration::from_micros(400));
}

#[test]
fn a_long_pause_makes_traffic_sparse_at_once() {
    let mut arrivals = Arrivals::new(BURST_GAP, LATENCY);
    let now = steady(&mut arrivals, Instant::now(), ms(10), 50);

    arrivals.record(now + Duration::from_secs(60));
    assert_eq!(arrivals.idle_bound(LATENCY), BURST_GAP);
}

#[test]
fn traffic_picking_up_is_recognised_within_a_few_bursts() {
    let mut arrivals = Arrivals::new(BURST_GAP, LATENCY);
    let now = steady(&mut arrivals, Instant::now(), Duration::from_secs(3600), 10);

    let mut bursts = 0;
    let mut now = now;
    while arrivals.idle_bound(LATENCY) == BURST_GAP {
        now += ms(10);
        arrivals.record(now);
        bursts += 1;
    }

    assert!(bursts <= 6, "took {bursts} bursts to notice steady traffic");
}

#[test]
fn extreme_durations_saturate() {
    let mut arrivals = Arrivals::new(Duration::MAX, Duration::MAX);
    let now = Instant::now();
    arrivals.record(now);
    arrivals.record(now);

    assert_eq!(arrivals.idle_bound(Duration::MAX), Duration::MAX);
}
