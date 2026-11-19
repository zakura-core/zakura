//! hdr-9d.1: `header-sync/outstanding-work-bounded` on `header-sync/storage`.
//! Every committed authority and reset witness holds it and applies, under
//! the empty Plan and under generated plans.
//! `cargo xtask planted header-sync/outstanding-work-bounded` checks that its
//! bypasses break it.
#![cfg(zakura_sim)]

mod support;

use std::{collections::BTreeSet, fs, path::PathBuf, time::Duration};

use support::{
    inputs,
    steer::{steer, Rule, Steer},
};
use zakura_network::zakura::observe::ServeInUse;
use zakura_sim::{
    check::PropertyOutcome,
    domain::header_sync::OUTSTANDING_WORK_BOUNDED,
    harness::{
        header_node::outstanding::OutstandingEvidence,
        header_sync::{Storage, StorageScenario},
        run_prepared, HarnessError, Subsystem,
    },
    observe::{Entry, ReactorFact, StorageFact, StorageResult},
    MemorySink, Observation, Plan, PlanInput, RunReport, StepCtx,
};

/// Every committed storage witness the outstanding-work gate runs, by its
/// fixture stem.
const WITNESSES: [&str; 10] = [
    "header-authority-forks-sharing-a-header",
    "header-authority-relinked-mid-round",
    "header-reset-held-apply",
    "header-reset-held-reply",
    "header-reset-quiet-reset",
    "header-reset-quiet-reset-deep",
    "header-outstanding-churn",
    "header-outstanding-serving-churn",
    "header-outstanding-serving-replaced",
    "header-outstanding-capacity-waits",
];

/// The committed fixture `<stem>.<extension>`.
fn fixture(stem: &str, extension: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("{stem}.{extension}"))
}

/// The committed scenario `stem`.
fn scenario(stem: &str) -> StorageScenario {
    serde_json::from_str(
        &fs::read_to_string(fixture(stem, "json"))
            .unwrap_or_else(|error| panic!("{stem}: {error}")),
    )
    .unwrap_or_else(|error| panic!("{stem}: {error}"))
}

/// Runs `scenario` under `input`, which must end without a violation or
/// harness failure.
fn run(stem: &str, scenario: &StorageScenario, input: PlanInput) -> RunReport {
    let report = run_prepared::<Storage>(
        scenario,
        Storage::prepare(scenario),
        input,
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{stem}: {error}"));
    assert_eq!(report.end.violation, None, "{stem}: {:?}", report.end);
    assert_eq!(report.end.harness, None, "{stem}: {:?}", report.end);
    report
}

/// Asserts `report` held the property and judged at least one `Sizes`.
fn held(stem: &str, report: &RunReport) {
    let outcome = &report
        .properties
        .iter()
        .find(|reported| reported.property == OUTSTANDING_WORK_BOUNDED)
        .unwrap_or_else(|| panic!("{stem}: the property reports"))
        .outcome;
    assert!(
        matches!(outcome, PropertyOutcome::Held { applicable } if *applicable > 0),
        "{stem}: {outcome:?}"
    );
}

/// Each committed witness holds the property and applies under the empty
/// Plan, which the planted gate runs.
#[test]
fn the_committed_witnesses_hold_outstanding_work_bounded() {
    for stem in WITNESSES {
        let report = run(stem, &scenario(stem), Plan::from_fuzz(&[]));
        held(stem, &report);
    }
}

/// Each committed witness holds the property under generated plans too, so
/// no honest schedule breaks a row.
#[test]
fn the_witnesses_hold_under_generated_plans() {
    for stem in WITNESSES {
        let scenario = scenario(stem);
        for input in inputs(8, 256) {
            held(stem, &run(stem, &scenario, input));
        }
    }
}

/// The limit fixtures. The slot and locator fixtures have 17 mirrors, each
/// at its own height of the main chain so each offers a distinct target. In
/// the slot fixture storage holds every prepare for the run, and each
/// mirror advertises 235 headers per response, so 17 rounds' reservations
/// fit the 4,000-header chunk budget. The queue fills its 16 slots, and the
/// slot cap, not the budget, holds the 17th offer back. In the locator
/// fixture storage holds every locator read until 16 are submitted, so the
/// reads fill the 16 permits and the 17th waits. In the serving fixture 11
/// mirrors sit below the node, so the node serves each of them, and storage
/// holds every serving read until 8 are submitted, so the reads fill the
/// node's 8 execution slots and the rest wait. In the peer-execution
/// fixture one mirror sits below the node and relinks twice. Storage holds
/// its first session's page read and its second session's lease acquire,
/// so those two page jobs hold the peer's 2 execution slots, and the third
/// session's job waits for one ([`the_peer_execution_fixture_waits_at_its_full_slots`]).
/// In the departed-limit fixture the node admits 4 inbound peers, drops a
/// peer at its first request timeout and holds its entries for 120 s, and
/// every mirror's first page on a link arrives past that timeout. So two
/// waves of 4 mirrors, each linked as the one before is dropped, fill the
/// departed strikes and cooldowns to their limit, the admitted peer limit's
/// floor of 8 ([`the_departed_rows_evict_their_earliest_entry_then_fall_and_rise`]).
/// Each row reaches its limit and no further. The harness's step budget
/// ends the first three runs before release and reuse, which the committed
/// witnesses cover at their own scale.
#[test]
fn the_limit_fixtures_reach_and_hold_their_bounds() {
    for (stem, row, limit, sessions) in [
        ("header-outstanding-slot-limit", "work slots", 16, 17),
        ("header-outstanding-locator-limit", "locator reads", 16, 17),
        (
            "header-outstanding-serving-limit",
            "node execution slots",
            8,
            11,
        ),
        (EXECUTION_WAIT_FIXTURE, "peer execution slots", 2, 1),
        (DEPARTED_FIXTURE, "departed strikes", 8, 4),
        (DEPARTED_FIXTURE, "cooldowns", 8, 4),
    ] {
        let (report, evidence) = run_limit_fixture(stem);
        held(stem, &report);
        let peaks = evidence.peaks();
        assert_eq!(peaks.get(row), Some(&limit), "{stem}: {peaks:?}");
        assert_eq!(
            peaks.get("session handles"),
            Some(&sessions),
            "{stem}: {peaks:?}"
        );
    }
}

/// The peer-execution limit fixture's stem.
const EXECUTION_WAIT_FIXTURE: &str = "header-outstanding-peer-execution";

/// The departed-limit fixture's stem.
const DEPARTED_FIXTURE: &str = "header-outstanding-departed-limit";

/// Runs the limit fixture `stem` under the empty Plan, which must end
/// without a violation or harness failure, with its outstanding-work
/// evidence.
fn run_limit_fixture(stem: &str) -> (RunReport, OutstandingEvidence) {
    let scenario = scenario(stem);
    let prepared = Storage::prepare(&scenario);
    let evidence = prepared.outstanding.clone();
    let report = run_prepared::<Storage>(
        &scenario,
        prepared,
        Plan::from_fuzz(&[]),
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{stem}: {error}"));
    assert_eq!(report.end.violation, None, "{stem}: {:?}", report.end);
    assert_eq!(report.end.harness, None, "{stem}: {:?}", report.end);
    (report, evidence)
}

/// The virtual time of the first page read that returns in `entries`, a
/// run's log. A page job holds its execution slots until its read returns.
fn first_page_returned_at(entries: &[Entry]) -> Option<Duration> {
    let mut now = Duration::ZERO;
    for entry in entries {
        match &entry.obs {
            Observation::TimeAdvanced { to_ns, .. } => now = Duration::from_nanos(*to_ns),
            Observation::Storage {
                instance: 0,
                fact:
                    StorageFact::Completed {
                        result: StorageResult::Page { .. },
                        ..
                    },
                ..
            } => return Some(now),
            _ => {}
        }
    }
    None
}

/// Whether `acquisitions`, a run's serving acquisitions with their virtual
/// times, show one push more than `limit` take both its peer and node
/// output while the others held all `limit` of the peer's execution slots,
/// before `released`. A push takes output before execution, so the others
/// already hold theirs and the output that grows in that window is the
/// extra push's. Its next step after the node output grant is the peer's
/// execution reserve, and with no page job done before `released`, no
/// push has given its slots back, so it waits there.
fn waited_at_full_execution(
    acquisitions: &[(Option<Duration>, ServeInUse)],
    released: Duration,
    limit: usize,
) -> bool {
    let mut window = acquisitions.iter().filter_map(|(at, in_use)| {
        let ServeInUse {
            peer_execution: executing,
            peer_responses: responding,
            ..
        } = in_use;
        (at.is_some_and(|at| at < released) && *executing == limit && *responding == limit + 1)
            .then_some(in_use)
    });
    let Some(first) = window.next() else {
        return false;
    };
    window.next_back().is_some_and(|last| {
        last.peer_output > first.peer_output && last.node_output > first.node_output
    })
}

/// In the peer-execution fixture the third session's page job takes its
/// output while the other two hold the peer's 2 execution slots, before
/// the first page returns, so it waits at the full execution reserve and
/// not at an output grant.
#[test]
fn the_peer_execution_fixture_waits_at_its_full_slots() {
    let stem = EXECUTION_WAIT_FIXTURE;
    let (report, evidence) = run_limit_fixture(stem);
    let released = first_page_returned_at(report.log.entries())
        .unwrap_or_else(|| panic!("{stem}: a page read returned"));
    let acquisitions = evidence.acquisitions();
    assert!(
        waited_at_full_execution(&acquisitions, released, 2),
        "{stem}: an extra push took its output with every execution slot taken before \
         {released:?}: {acquisitions:?}"
    );
}

/// Each round the node retired for its request timeout, by its peer, with
/// the virtual time then, in log order.
fn timeouts(entries: &[Entry]) -> Vec<(Duration, String)> {
    let mut now = Duration::ZERO;
    let mut found = Vec::new();
    for entry in entries {
        match &entry.obs {
            Observation::TimeAdvanced { to_ns, .. } => now = Duration::from_nanos(*to_ns),
            Observation::Reactor {
                event:
                    ReactorFact::Retired {
                        request, reason, ..
                    },
                ..
            } if reason == "timed_out" => found.push((now, request.peer.clone())),
            _ => {}
        }
    }
    found
}

/// Whether `history`, a row's member sets in order, takes `newcomer` in
/// while full by evicting one of `earliest`: the first set that holds it
/// and the set before both have `limit` members, and the one member they
/// don't share is one of `earliest`.
fn evicted_for(
    history: &[BTreeSet<String>],
    newcomer: &str,
    earliest: &BTreeSet<&String>,
    limit: usize,
) -> bool {
    let Some(at) = history.iter().position(|set| set.contains(newcomer)) else {
        return false;
    };
    let Some(before) = at.checked_sub(1).map(|index| &history[index]) else {
        return false;
    };
    let after = &history[at];
    let evicted: Vec<&String> = before.difference(after).collect();
    before.len() == limit
        && after.len() == limit
        && evicted.len() == 1
        && evicted.iter().all(|gone| earliest.contains(gone))
}

/// Whether `sizes`, a row's sizes in order, reach `limit`, then fall below
/// it, a release, and later rise from below it, a reuse.
fn sizes_fall_and_rise(sizes: &[usize], limit: usize) -> bool {
    let Some(full) = sizes.iter().position(|size| *size == limit) else {
        return false;
    };
    let Some(released) = sizes[full..].iter().position(|size| *size < limit) else {
        return false;
    };
    sizes[full + released..]
        .windows(2)
        .any(|pair| pair[0] < limit && pair[1] > pair[0])
}

/// The departed-limit fixture drops 9 peers within one 120 s cooldown, one
/// more than the departed strikes and cooldowns hold, so the 9th dropped
/// peer enters each full row by evicting one of the first wave's entries.
/// Each row then falls below its limit as entries end and rises again when
/// three returning peers are dropped once more. The node's per-direction
/// peer limits sum to 4, below the admitted peer limit's floor of 8, so a
/// reactor that capped the rows by that sum stops short and fails the read.
#[test]
fn the_departed_rows_evict_their_earliest_entry_then_fall_and_rise() {
    let stem = DEPARTED_FIXTURE;
    let limit = 8;
    let scenario = scenario(stem);
    let (report, evidence) = run_limit_fixture(stem);
    let config = scenario.node_settings.config();
    let per_direction = config
        .peer_limits
        .max_inbound_peers
        .saturating_add(config.peer_limits.max_outbound_peers);
    assert!(per_direction < limit, "{stem}: {per_direction} < {limit}");
    let timeouts = timeouts(report.log.entries());
    let hold = config.unproductive_peer_cooldown;
    let first = timeouts.first().map(|(at, _)| *at).unwrap_or_default();
    let held: BTreeSet<&String> = timeouts
        .iter()
        .filter(|(at, _)| *at < first + hold)
        .map(|(_, peer)| peer)
        .collect();
    assert_eq!(held.len(), limit + 1, "{stem}: {timeouts:?}");
    assert!(timeouts.len() > limit + 1, "{stem}: {timeouts:?}");
    let earliest: BTreeSet<&String> = timeouts
        .iter()
        .filter(|(at, _)| *at == first)
        .map(|(_, peer)| peer)
        .collect();
    let (_, newcomer) = timeouts
        .iter()
        .rev()
        .find(|(at, _)| *at < first + hold)
        .expect("the held timeouts are nonempty, as their count shows");
    for row in ["departed strikes", "cooldowns"] {
        let members = evidence.members(row);
        assert!(
            evicted_for(&members, newcomer, &earliest, limit),
            "{stem}: {row} took {newcomer} in by evicting one of {earliest:?}: {members:?}"
        );
        let sizes = evidence.sizes(row);
        assert!(
            sizes_fall_and_rise(&sizes, limit),
            "{stem}: {row} released and reused an entry: {sizes:?}"
        );
    }
}

/// The execution-wait read fails with no extra push past the full
/// execution slots, with one held at its output grants, with one only once
/// the slots aren't full, or with one only at or after the release.
#[test]
fn the_execution_wait_read_needs_the_extra_push_past_its_output_before_the_release() {
    let at = |ms: u64| Some(Duration::from_millis(ms));
    let in_use = |peer_execution, peer_responses, peer_output, node_output| ServeInUse {
        peer_execution,
        peer_responses,
        peer_output,
        node_output,
        ..ServeInUse::default()
    };
    let released = Duration::from_millis(1_000);
    let read = |acquisitions: &[(Option<Duration>, ServeInUse)]| {
        waited_at_full_execution(acquisitions, released, 2)
    };
    let responded = (at(0), in_use(2, 3, 20, 20));
    let peer_output = (at(0), in_use(2, 3, 30, 20));
    let node_output = (at(0), in_use(2, 3, 30, 30));
    assert!(read(&[
        (at(0), in_use(2, 2, 20, 20)),
        responded,
        peer_output,
        node_output
    ]));
    assert!(!read(&[(at(0), in_use(2, 2, 20, 20))]));
    assert!(!read(&[responded]));
    assert!(!read(&[responded, peer_output]));
    assert!(!read(&[
        (at(0), in_use(1, 3, 20, 20)),
        (at(0), in_use(1, 3, 30, 30))
    ]));
    assert!(!read(&[responded, (at(1_000), in_use(2, 3, 30, 30))]));
    assert!(!read(&[
        (None, in_use(2, 3, 20, 20)),
        (None, in_use(2, 3, 30, 30))
    ]));
}

/// A full row that never takes the newcomer in, grows past its limit to
/// take it, takes it in before it's full, or evicts an entry that wasn't
/// among the earliest fails the eviction read.
#[test]
fn the_eviction_read_needs_the_newcomer_to_replace_an_earliest_entry() {
    let sets = |sets: &[&[&str]]| -> Vec<BTreeSet<String>> {
        sets.iter()
            .map(|set| set.iter().map(|member| (*member).to_owned()).collect())
            .collect()
    };
    let a = "a".to_owned();
    let earliest = BTreeSet::from([&a]);
    let read = |history: &[&[&str]]| evicted_for(&sets(history), "c", &earliest, 2);
    assert!(read(&[&["a"], &["a", "b"], &["b", "c"]]));
    assert!(!read(&[&["a"], &["a", "b"], &["b"]]));
    assert!(!read(&[&["a"], &["a", "b"], &["a", "b", "c"]]));
    assert!(!read(&[&["a"], &["c"]]));
    assert!(!read(&[&["a"], &["a", "b"], &["a", "c"]]));
}

/// A row whose sizes never reach the limit, never fall below it, or never
/// rise again after falling fails the fall-and-rise read.
#[test]
fn the_held_row_read_needs_a_release_and_a_later_rise() {
    assert!(sizes_fall_and_rise(&[0, 8, 1, 0, 1, 0], 8));
    assert!(!sizes_fall_and_rise(&[0, 7, 6, 7], 8));
    assert!(!sizes_fall_and_rise(&[0, 8, 8], 8));
    assert!(!sizes_fall_and_rise(&[0, 8, 1, 0], 8));
}

/// Checks one capacity-waits run: no violation or harness failure, the
/// property held, the acquire was refused `CapacityBusy` both before and
/// after the commit, and the publisher kept one wait at a time.
fn refused_twice(label: &str, report: &RunReport, evidence: &OutstandingEvidence) {
    assert_eq!(report.end.violation, None, "{label}: {:?}", report.end);
    assert_eq!(report.end.harness, None, "{label}: {:?}", report.end);
    held(label, report);
    let refusals: Vec<u64> = report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage {
                instance: 0,
                fact:
                    StorageFact::Completed {
                        result: StorageResult::CapacityBusy { .. },
                        generation,
                        ..
                    },
                ..
            } => Some(*generation),
            _ => None,
        })
        .collect();
    assert!(
        refusals.len() >= 2 && refusals.first() < refusals.last(),
        "{label}: refused before and after the commit: {refusals:?}"
    );
    let peaks = evidence.peaks();
    assert_eq!(
        peaks.get("publisher capacity waits"),
        Some(&1),
        "{label}: {peaks:?}"
    );
}

/// The capacity-waits fixture: one mirror sits below the node, so the node
/// serves it, and storage holds its first serving read, which must finish
/// within 1 s, so the clock can't reach the old lease's 30 s idle deadline
/// while it's held. The mirror then relinks, so its new session's page job
/// finds the old lease still live, and the node's storage refuses the
/// acquire `CapacityBusy` for the mirror's one lease. The publisher
/// registers a wait for that refusal. A commit, offered once storage has
/// queued the refusal, changes the node's selected chain. Either the
/// publisher took the refusal first, so its subscription stalls on the
/// wait and the commit clears the stall, or the commit came first, so the
/// refusal installs no stall. Either way the next pass's pruning must drop
/// a wait no stall references. The retried acquire is refused again under
/// the new generation, after which the read is released. Under its
/// empty Plan and generated ones, the publisher keeps one wait at a
/// time and the property holds.
#[test]
fn the_capacity_waits_fixture_refuses_twice_and_holds() {
    let stem = "header-outstanding-capacity-waits";
    let scenario = scenario(stem);
    let mut plans = vec![Plan::from_fuzz(&[])];
    plans.extend(inputs(8, 256));
    for (index, input) in plans.into_iter().enumerate() {
        let prepared = Storage::prepare(&scenario);
        let evidence = prepared.outstanding.clone();
        let report =
            run_prepared::<Storage>(&scenario, prepared, input, &mut MemorySink::default(), None)
                .unwrap_or_else(|error| panic!("{stem} plan {index}: {error}"));
        refused_twice(&format!("plan {index}"), &report, &evidence);
    }
}

/// The capacity-waits fixture with the commit put off until nothing else
/// but a time advance can run, so the publisher takes the first refusal
/// and stalls on its wait before the commit. The empty Plan takes the
/// commit as soon as it's offered, the other order. The history holds in
/// both as [`refused_twice`] checks, and `waits-kept` breaks in both.
#[test]
fn a_late_commit_still_refuses_twice() {
    let scenario = scenario("header-outstanding-capacity-waits");
    let rule = |cx: &StepCtx<'_>| -> Result<Steer, String> {
        if !cx.pick.to_string().ends_with(":commit") {
            return Ok(Steer::Fine);
        }
        Ok(cx
            .enabled
            .iter()
            .map(|enabled| enabled.identity.to_string())
            .find(|name| !name.ends_with(":commit") && !name.starts_with("advance-time"))
            .map_or(Steer::Fine, Steer::Pick))
    };
    let (report, evidence) = steered(&scenario, &rule);
    refused_twice("late commit", &report, &evidence);
}

/// `scenario` steered by `rule` from the default Plan, with its
/// outstanding-work peaks.
fn steered(scenario: &StorageScenario, rule: &Rule<'_>) -> (RunReport, OutstandingEvidence) {
    let mut evidence = None;
    let (report, _, _) = steer(Plan::from_fuzz(&[]), rule, &mut |plan, checker| {
        let prepared = Storage::prepare(scenario);
        evidence = Some(prepared.outstanding.clone());
        let mut sink = MemorySink::default();
        let report =
            match run_prepared::<Storage>(scenario, prepared, plan, &mut sink, Some(checker)) {
                Ok(report) => report,
                Err(HarnessError::Run { report, .. }) => *report,
                Err(error) => panic!("{error}"),
            };
        (report, sink.lines())
    });
    (
        report,
        evidence.expect("the steered run prepared its ports"),
    )
}

/// The capacity-waits fixture with the relink put off: every step that can
/// advance time does while the relink waits. The held read's deadline
/// stops the clock first, so the old lease is still live at the relink,
/// and the history is the same.
#[test]
fn a_delayed_relink_still_finds_the_lease_live() {
    let scenario = scenario("header-outstanding-capacity-waits");
    let rule = |cx: &StepCtx<'_>| -> Result<Steer, String> {
        let names: Vec<String> = cx
            .enabled
            .iter()
            .map(|enabled| enabled.identity.to_string())
            .collect();
        let relink = names.iter().any(|name| name.ends_with(":relink"));
        Ok(
            match names.iter().find(|name| name.starts_with("advance-time")) {
                Some(advance) if relink => Steer::Pick(advance.clone()),
                _ => Steer::Fine,
            },
        )
    };
    let (report, evidence) = steered(&scenario, &rule);
    refused_twice("delayed relink", &report, &evidence);
}
