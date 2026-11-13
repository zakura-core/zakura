//! hdr-9d.1: `header-sync/outstanding-work-bounded` on `header-sync/storage`.
//! Every committed authority and reset witness holds it and applies, under
//! its committed Plan bytes and under generated plans.
//! `cargo xtask planted header-sync/outstanding-work-bounded` checks that its
//! bypasses break it.
#![cfg(zakura_sim)]

mod support;

use std::{collections::BTreeSet, fs, path::PathBuf, time::Duration};

use support::{
    inputs,
    steer::{steer, Rule, Steer},
};
use zakura_network::zakura::observe::{ReactorEvent, ServeInUse};
use zakura_sim::harness::header_acceptance::history::run_declared;
use zakura_sim::{
    check::PropertyOutcome,
    domain::header_sync::OUTSTANDING_WORK_BOUNDED,
    harness::{
        header_node::outstanding::OutstandingEvidence,
        header_sync::{Storage, StorageScenario},
        run_prepared, HarnessError, Subsystem,
    },
    observe::{Entry, ReactorCall, ReactorFact, StorageFact, StorageRequest, StorageResult},
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
    run_into(stem, scenario, input, &mut MemorySink::default())
}

/// [`run`], recording into `sink`.
fn run_into(
    stem: &str,
    scenario: &StorageScenario,
    input: PlanInput,
    sink: &mut dyn zakura_sim::RecordSink,
) -> RunReport {
    let report = run_prepared::<Storage>(scenario, Storage::prepare(scenario), input, sink, None)
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

/// Each committed witness holds the property and applies under its own
/// Plan bytes, which the planted gate runs.
#[test]
fn the_committed_witnesses_hold_outstanding_work_bounded() {
    for stem in WITNESSES {
        let plan =
            fs::read(fixture(stem, "plan")).unwrap_or_else(|error| panic!("{stem}: {error}"));
        // Each run is a manifest history, which the acceptance tool
        // records from this declaration (C18).
        let report = run_declared(
            Storage::NAME,
            stem,
            &scenario(stem),
            Plan::from_fuzz(&plan),
            |scenario, input, sink| run_into(stem, scenario, input, sink),
        );
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
/// session's job waits for one. In the departed-limit fixture the node
/// admits 4 inbound peers, drops a peer at its first request timeout and
/// holds its entries for 120 s, and every mirror's first page on a link
/// arrives past that timeout. So two waves of 4 mirrors, each linked as the
/// one before is dropped, fill the departed strikes and cooldowns to their
/// limit, the admitted peer limit's floor of 8, and the 9th mirror is
/// dropped while they're held, so the earliest entry goes. Each row
/// reaches its limit and no further.
/// In the slot fixture the 16 rounds' reservations, staged capacity entries
/// and request deadlines, one per round, reach 16 with them. Each run then
/// releases and reuses its row, which
/// [`the_limit_fixtures_release_and_reuse_their_bounds`] checks.
#[test]
fn the_limit_fixtures_reach_and_hold_their_bounds() {
    let per_round = ["reservations", "staged capacity", "request deadlines"];
    for (stem, row, limit, sessions, also) in [
        (
            "header-outstanding-slot-limit",
            "work slots",
            16,
            17,
            &per_round[..],
        ),
        (
            "header-outstanding-locator-limit",
            "locator reads",
            16,
            17,
            &[][..],
        ),
        (
            "header-outstanding-serving-limit",
            "node execution slots",
            8,
            11,
            &[][..],
        ),
        (
            "header-outstanding-peer-execution",
            "peer execution slots",
            2,
            1,
            &[][..],
        ),
        (
            "header-outstanding-departed-limit",
            "departed strikes",
            8,
            4,
            &["cooldowns"][..],
        ),
    ] {
        let scenario = scenario(stem);
        let prepared = Storage::prepare(&scenario);
        let evidence = prepared.outstanding.clone();
        let plan =
            fs::read(fixture(stem, "plan")).unwrap_or_else(|error| panic!("{stem}: {error}"));
        let report = run_prepared::<Storage>(
            &scenario,
            prepared,
            Plan::from_fuzz(&plan),
            &mut MemorySink::default(),
            None,
        )
        .unwrap_or_else(|error| panic!("{stem}: {error}"));
        assert_eq!(report.end.violation, None, "{stem}: {:?}", report.end);
        assert_eq!(report.end.harness, None, "{stem}: {:?}", report.end);
        held(stem, &report);
        let peaks = evidence.peaks();
        assert_eq!(peaks.get(row), Some(&limit), "{stem}: {peaks:?}");
        for also in also {
            assert_eq!(peaks.get(also), Some(&limit), "{stem}: {also}: {peaks:?}");
        }
        assert_eq!(
            peaks.get("session handles"),
            Some(&sessions),
            "{stem}: {peaks:?}"
        );
    }
}

/// The bound-table rows judged by membership or cleanup, as the rule's
/// peaks name them: each key must name something that still holds what
/// the entry is for, so a key kept past that breaks the row.
const MEMBERSHIP_ROWS: [&str; 15] = [
    "request deadlines",
    "close deadlines",
    "locator reads",
    "storage calls",
    "port futures",
    "cached offers",
    "supplier order",
    "completed targets",
    "subscriber unaccepted cursors",
    "subscriber live subscriptions",
    "subscriber ended keys",
    "publisher tasks",
    "session handles",
    "scheduling state",
    "expired serving peer budgets",
];

/// Gate 5's membership and cleanup witnesses: across the committed
/// witnesses, each of which holds the property under its committed Plan,
/// every membership or cleanup row held at least one key while the rule
/// judged it, so the rule saw entries come and go rather than an empty
/// container. A row's keep bypasses under
/// `qa/planted/header-sync/outstanding-work-bounded/`, where it has any,
/// show it breaks.
#[test]
fn each_membership_row_holds_keys_in_a_committed_witness() {
    let mut peaks = std::collections::BTreeMap::<&str, usize>::new();
    for stem in WITNESSES {
        let scenario = scenario(stem);
        let prepared = Storage::prepare(&scenario);
        let evidence = prepared.outstanding.clone();
        let plan =
            fs::read(fixture(stem, "plan")).unwrap_or_else(|error| panic!("{stem}: {error}"));
        let report = run_prepared::<Storage>(
            &scenario,
            prepared,
            Plan::from_fuzz(&plan),
            &mut MemorySink::default(),
            None,
        )
        .unwrap_or_else(|error| panic!("{stem}: {error}"));
        held(stem, &report);
        for (row, peak) in evidence.peaks() {
            let most = peaks.entry(row).or_default();
            *most = (*most).max(peak);
        }
    }
    let empty: Vec<&str> = MEMBERSHIP_ROWS
        .into_iter()
        .filter(|row| peaks.get(row).copied().unwrap_or(0) == 0)
        .collect();
    assert_eq!(empty, Vec::<&str>::new(), "{peaks:?}");
}

/// One limit fixture's row as its log, or an evidence store's facts of
/// type `T`, show it: which item opens a unit of the row, and for which
/// party, and which item releases one.
struct RowUnits<T = Entry> {
    /// The fixture's stem.
    stem: &'static str,
    /// The row's limit.
    limit: usize,
    /// The parties that want a unit, one more than the limit can hold.
    parties: usize,
    /// The party an item opens a unit for, if it opens one.
    opens: fn(&T) -> Option<String>,
    /// Whether an item releases a unit.
    releases: fn(&T) -> bool,
}

/// What a limit fixture's log shows of its row.
#[derive(Debug, PartialEq, Eq)]
struct RowUse {
    /// The most units open at once.
    peak: usize,
    /// The distinct parties that held a unit.
    parties: usize,
    /// The log index of the first release, if any.
    first_release: Option<usize>,
    /// The log index where the last party to get its first unit got it,
    /// with how many units were open just before.
    last_first_open: Option<(usize, usize)>,
}

/// Reads `row`'s units from `entries`, in their order.
fn row_use<T>(row: &RowUnits<T>, entries: &[T]) -> RowUse {
    let mut open = 0usize;
    let mut peak = 0;
    let mut seen = BTreeSet::new();
    let mut first_release = None;
    let mut last_first_open = None;
    for (at, entry) in entries.iter().enumerate() {
        if let Some(party) = (row.opens)(entry) {
            if seen.insert(party) {
                last_first_open = Some((at, open));
            }
            open += 1;
            peak = peak.max(open);
        } else if (row.releases)(entry) {
            open = open.saturating_sub(1);
            first_release.get_or_insert(at);
        }
    }
    RowUse {
        peak,
        parties: seen.len(),
        first_release,
        last_first_open,
    }
}

/// Each limit fixture's row, read from its log: work slots by rounds
/// started and retired, locator reads by locator calls dispatched and
/// locators returned, node execution slots by serving reads submitted and
/// pages returned, and peer execution slots by each session's lease
/// acquires submitted and pages returned. A page job takes its execution
/// slots before its acquire and keeps them until its read returns.
fn limit_rows() -> [RowUnits; 4] {
    [
        RowUnits {
            stem: "header-outstanding-slot-limit",
            limit: 16,
            parties: 17,
            opens: |entry| match &entry.obs {
                Observation::Reactor {
                    event: ReactorFact::Started { request, .. },
                    ..
                } => Some(request.peer.clone()),
                _ => None,
            },
            releases: |entry| {
                matches!(
                    &entry.obs,
                    Observation::Reactor {
                        event: ReactorFact::Retired { .. },
                        ..
                    }
                )
            },
        },
        RowUnits {
            stem: "header-outstanding-locator-limit",
            limit: 16,
            parties: 17,
            opens: |entry| match &entry.obs {
                Observation::Reactor {
                    event:
                        ReactorFact::Dispatched {
                            port: ReactorCall::Locator { peer, .. },
                            ..
                        },
                    ..
                } => Some(peer.clone()),
                _ => None,
            },
            releases: |entry| {
                matches!(
                    &entry.obs,
                    Observation::Storage {
                        instance: 0,
                        fact: StorageFact::Completed {
                            result: StorageResult::Locator { .. },
                            ..
                        },
                        ..
                    }
                )
            },
        },
        RowUnits {
            stem: "header-outstanding-serving-limit",
            limit: 8,
            parties: 11,
            opens: |entry| match &entry.obs {
                Observation::Storage {
                    instance: 0,
                    fact:
                        StorageFact::Submitted {
                            request:
                                StorageRequest::ReadHeaderPath {
                                    lease: Some(lease), ..
                                },
                            ..
                        },
                    ..
                } => Some(lease.peer.clone()),
                _ => None,
            },
            releases: |entry| {
                matches!(
                    &entry.obs,
                    Observation::Storage {
                        instance: 0,
                        fact: StorageFact::Completed {
                            result: StorageResult::Page { .. },
                            ..
                        },
                        ..
                    }
                )
            },
        },
        RowUnits {
            stem: EXECUTION_WAIT_FIXTURE,
            limit: 2,
            parties: 3,
            opens: |entry| match &entry.obs {
                Observation::Storage {
                    instance: 0,
                    fact:
                        StorageFact::Submitted {
                            request: StorageRequest::AcquireHeaderPath { session_id, .. },
                            ..
                        },
                    ..
                } => Some(session_id.to_string()),
                _ => None,
            },
            releases: |entry| {
                matches!(
                    &entry.obs,
                    Observation::Storage {
                        instance: 0,
                        fact: StorageFact::Completed {
                            result: StorageResult::Page { .. },
                            ..
                        },
                        ..
                    }
                )
            },
        },
    ]
}

/// The departed-limit fixture's rows, which keep one entry per dropped
/// peer until the node's cooldown ends and evict the earliest past their
/// limit, with that limit and the peers dropped while the first is held.
const HELD_ROWS: (&str, [&str; 2], usize, usize) = (
    "header-outstanding-departed-limit",
    ["departed strikes", "cooldowns"],
    8,
    9,
);

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

/// The peer-execution limit fixture's stem.
const EXECUTION_WAIT_FIXTURE: &str = "header-outstanding-peer-execution";

/// The virtual time of `row`'s first release in `entries`, its fixture's
/// log.
fn first_release_at(row: &RowUnits, entries: &[Entry]) -> Option<Duration> {
    let mut now = Duration::ZERO;
    for entry in entries {
        if let Observation::TimeAdvanced { to_ns, .. } = &entry.obs {
            now = Duration::from_nanos(*to_ns);
        }
        if (row.releases)(entry) {
            return Some(now);
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

/// The slot-limit fixture's reservation-map row, read from the work
/// queue's reservation facts ([`ReservationEvidence`]): a reservation
/// opens a unit for its peer and its end releases it. Those are the facts
/// the outstanding rule joins the reactor's reservation map's keys to.
///
/// [`ReservationEvidence`]: zakura_sim::harness::header_node::reservation::ReservationEvidence
fn reservation_row() -> RowUnits<ReactorEvent> {
    RowUnits {
        stem: "header-outstanding-slot-limit",
        limit: 16,
        parties: 17,
        opens: |event| match event {
            ReactorEvent::ReservationOpened { peer, .. } => Some(format!("{peer:?}")),
            _ => None,
        },
        releases: |event| matches!(event, ReactorEvent::ReservationEnded { .. }),
    }
}

/// Checks `used`, `row`'s units in its fixture's run: the row reaches its
/// limit and no further while one more party waits, so the next is denied,
/// and that party gets its first unit only after a release, with fewer than
/// the limit open, so a released unit is reused.
fn released_and_reused<T>(row: &RowUnits<T>, used: &RowUse) {
    let stem = row.stem;
    assert_eq!(
        (used.peak, used.parties),
        (row.limit, row.parties),
        "{stem}: {used:?}"
    );
    let (Some(release), Some((reused, open))) = (used.first_release, used.last_first_open) else {
        panic!("{stem}: the row released and reused a unit: {used:?}");
    };
    assert!(
        release < reused && open < row.limit,
        "{stem}: the waiting party's first unit came after a release: {used:?}"
    );
}

/// Gate 5's bound-table rows for work slots, locator reads, and node and
/// peer execution slots, by their limit fixtures' logs, and for the
/// reservation maps, by the slot-limit fixture's reservation facts
/// ([`reservation_row`]): each row is [`released_and_reused`]. The departed
/// strikes and cooldowns evict instead of making a party wait: more peers
/// are dropped within one cooldown than the rows hold, so an entry was
/// evicted, and each row then falls below its limit as entries end and
/// rises again when returning peers are dropped once more
/// ([`sizes_fall_and_rise`]).
#[test]
fn the_limit_fixtures_release_and_reuse_their_bounds() {
    let run_fixture = |stem: &str| {
        let scenario = scenario(stem);
        let prepared = Storage::prepare(&scenario);
        let reservations = prepared.reservations.clone();
        let outstanding = prepared.outstanding.clone();
        let plan =
            fs::read(fixture(stem, "plan")).unwrap_or_else(|error| panic!("{stem}: {error}"));
        let report = run_prepared::<Storage>(
            &scenario,
            prepared,
            Plan::from_fuzz(&plan),
            &mut MemorySink::default(),
            None,
        )
        .unwrap_or_else(|error| panic!("{stem}: {error}"));
        assert_eq!(report.end.violation, None, "{stem}: {:?}", report.end);
        assert_eq!(report.end.harness, None, "{stem}: {:?}", report.end);
        (report, reservations.facts(), outstanding)
    };
    for row in limit_rows() {
        let stem = row.stem;
        let (report, facts, outstanding) = run_fixture(stem);
        released_and_reused(&row, &row_use(&row, report.log.entries()));
        let reservations = reservation_row();
        if reservations.stem == stem {
            released_and_reused(&reservations, &row_use(&reservations, &facts));
        }
        if stem == EXECUTION_WAIT_FIXTURE {
            let released = first_release_at(&row, report.log.entries())
                .unwrap_or_else(|| panic!("{stem}: the row released a unit"));
            let acquisitions = outstanding.acquisitions();
            assert!(
                waited_at_full_execution(&acquisitions, released, row.limit),
                "{stem}: an extra push took its output with every execution slot taken \
                 before {released:?}: {acquisitions:?}"
            );
        }
    }
    let (stem, rows, limit, parties) = HELD_ROWS;
    let scenario = scenario(stem);
    let (report, _, evidence) = run_fixture(stem);
    let config = scenario.node_settings.config();
    let per_direction = config
        .peer_limits
        .max_inbound_peers
        .saturating_add(config.peer_limits.max_outbound_peers);
    // The rows' limit is the admitted peer limit, here its floor. Keeping the
    // per-direction sum below it makes a reactor that capped the rows by
    // that sum stop short of the limit and fail the read below.
    assert!(per_direction < limit, "{stem}: {per_direction} < {limit}");
    let timeouts = timeouts(report.log.entries());
    let hold = config.unproductive_peer_cooldown;
    let first = timeouts.first().map(|(at, _)| *at).unwrap_or_default();
    let held: BTreeSet<&String> = timeouts
        .iter()
        .filter(|(at, _)| *at < first + hold)
        .map(|(_, peer)| peer)
        .collect();
    assert_eq!(held.len(), parties, "{stem}: {timeouts:?}");
    assert!(timeouts.len() > parties, "{stem}: {timeouts:?}");
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
    for row in rows {
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

/// A held row whose sizes never reach the limit, never fall below it, or
/// never rise again after falling fails the release-and-reuse read.
#[test]
fn the_held_row_read_needs_a_release_and_a_later_rise() {
    assert!(sizes_fall_and_rise(&[0, 8, 1, 0, 1, 0], 8));
    assert!(!sizes_fall_and_rise(&[0, 7, 6, 7], 8));
    assert!(!sizes_fall_and_rise(&[0, 8, 8], 8));
    assert!(!sizes_fall_and_rise(&[0, 8, 1, 0], 8));
}

/// The acquisition read fails with no extra push past the full execution
/// slots, with one held at its output grants, with one only once the
/// slots aren't full, or with one only at or after the release.
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

/// A row read with no release, or whose waiting party got its unit before
/// any release, fails the reuse check's conditions, so the check sees the
/// release it claims.
#[test]
fn the_reuse_check_needs_a_release_before_the_waiting_party() {
    let row: RowUnits = RowUnits {
        stem: "control",
        limit: 1,
        parties: 2,
        opens: |entry| match &entry.obs {
            Observation::TaskSpawned { task } => Some(task.clone()),
            _ => None,
        },
        releases: |entry| matches!(&entry.obs, Observation::TaskExited { .. }),
    };
    let spawned = |task: &str| Observation::TaskSpawned {
        task: task.to_owned(),
    };
    let exited = |task: &str| Observation::TaskExited {
        task: task.to_owned(),
        exit: zakura_sim::observe::Exit::Completed,
    };
    let read = |log: Vec<Observation>| {
        let mut observed = zakura_sim::ObservationLog::default();
        for obs in log {
            observed.push(obs);
        }
        row_use(&row, observed.entries())
    };
    let reused = read(vec![spawned("a"), exited("a"), spawned("b")]);
    assert_eq!(reused.first_release, Some(1));
    assert_eq!(reused.last_first_open, Some((2, 0)));
    let never = read(vec![spawned("a"), spawned("b")]);
    assert_eq!((never.peak, never.first_release), (2, None));
    let early = read(vec![spawned("a"), spawned("b"), exited("a")]);
    assert!(early
        .last_first_open
        .is_some_and(|(at, _)| Some(at) < early.first_release));
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
/// committed Plan and generated ones, the publisher keeps one wait at a
/// time and the property holds.
#[test]
fn the_capacity_waits_fixture_refuses_twice_and_holds() {
    let stem = "header-outstanding-capacity-waits";
    let scenario = scenario(stem);
    let committed =
        fs::read(fixture(stem, "plan")).unwrap_or_else(|error| panic!("{stem}: {error}"));
    let mut plans = vec![Plan::from_fuzz(&committed)];
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
/// and stalls on its wait before the commit. The committed Plan takes the
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
