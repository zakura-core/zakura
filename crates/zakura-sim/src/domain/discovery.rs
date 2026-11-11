//! Discovery's properties (properties.md, Discovery).

use std::{
    collections::BTreeMap,
    ops::RangeInclusive,
    sync::{atomic::Ordering, Arc, OnceLock},
    time::Duration,
};

use zakura_network::zakura::{DISCOVERY_INITIAL_EXCHANGE_TIMEOUT, ZAKURA_STREAM_DISCOVERY};
use zakura_scheduler::Status as SchedulerStatus;

use crate::{
    harness::discovery::{CapacityEvidence, VERIFICATION_READERS, VERIFICATION_SLOTS},
    model::{is_fault_cause, Label},
    observe::{Entry, TransportFact},
    peer::discovery::DiscoveryRow,
    property::{
        CheckCtx, HasView, Kind, Property, PropertyId, Source, Status, TriggerHistory, Verdict,
    },
    table::judged_nonconformant,
    Observation, ObservationLog, Violation,
};

/// Verification capacity covers each result until its record installs.
pub const VERIFICATION_CAPACITY: &str = "discovery/verification-capacity";

/// The label of each real record-verification worker.
const VERIFICATION_WORKER: &str = "discovery_verify";

/// A nonblocking read of the real handle and the harness tasks' published facts.
#[derive(Clone, Debug)]
pub struct VerificationView {
    /// The handle's free verification slots.
    pub free_slots: u64,
    /// The harness tasks' facts.
    pub evidence: Arc<CapacityEvidence>,
}

/// The actual verification workers the scheduler started so far. Their count
/// comes from the schedule, independently of the slots that should bound them.
fn started_workers(log: &ObservationLog) -> u64 {
    let started = log.entries().iter().filter(|entry| {
        matches!(&entry.obs, Observation::TaskSpawned { task } if task.contains(VERIFICATION_WORKER))
    });
    started.count() as u64
}

/// [`VERIFICATION_CAPACITY`]: while a slow holder keeps the state lock, no result
/// can install, so each started worker without an installed record is an
/// unsettled result. Their count stays within the slots. After every reader
/// returns, all slots are free and every record is stored.
pub fn verification_capacity<S: HasView<View = VerificationView>>() -> Property<S> {
    Property {
        id: PropertyId::new(VERIFICATION_CAPACITY),
        claim: "unsettled verification results stay within the node's verification slots",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::Schedule, Source::View],
        status: Status::Active,
        check: check_verification_capacity::<S>,
        // A violation ends the run as soon as one worker exceeds the slots.
        witness: Some(TriggerHistory::Log(|log| {
            started_workers(log) > VERIFICATION_SLOTS
        })),
    }
}

/// Checks [`VERIFICATION_CAPACITY`] at each boundary with an applicable view.
fn check_verification_capacity<S: HasView<View = VerificationView>>(
    cx: &mut CheckCtx<'_, S>,
) -> Verdict {
    let view = cx.view();
    let started = started_workers(cx.log);
    if let Some(records) = view
        .evidence
        .held_records
        .load(Ordering::SeqCst)
        .checked_sub(1)
    {
        let unsettled = started.saturating_sub(records);
        view.evidence
            .peak_unsettled
            .fetch_max(unsettled, Ordering::SeqCst);
        if unsettled > VERIFICATION_SLOTS {
            return Verdict::Violated(Violation {
                property: VERIFICATION_CAPACITY.to_owned(),
                step: cx.step,
                witness: (1..=cx.step).collect(),
                expected: format!("at most {VERIFICATION_SLOTS} unsettled verification result"),
                actual: format!("{started} workers started with {records} records installed"),
            });
        }
        return Verdict::Holds;
    }
    if view.evidence.done.load(Ordering::SeqCst) < VERIFICATION_READERS {
        return Verdict::NotApplicable;
    }
    if view.free_slots != VERIFICATION_SLOTS {
        return Verdict::Violated(Violation {
            property: VERIFICATION_CAPACITY.to_owned(),
            step: cx.step,
            witness: (1..=cx.step).collect(),
            expected: format!("{VERIFICATION_SLOTS} free slot after every reader returned"),
            actual: format!("{} free slots", view.free_slots),
        });
    }
    Verdict::Holds
}

/// `wait_complete` wakes the source once the initial exchange completes.
pub const WAIT_COMPLETE_WAKES: &str = "discovery/wait-complete-wakes";

/// [`WAIT_COMPLETE_WAKES`]: at each step that leaves no task running after
/// the sink has run the `Hello` notify and both answer notifies, the source
/// must not be waiting after a point in `wait_complete`.
///
/// It reads only the step loop's task observations, so any harness that
/// runs discovery's service can register it.
pub fn wait_complete_wakes<S: HasView>() -> Property<S> {
    Property {
        id: PropertyId::new(WAIT_COMPLETE_WAKES),
        claim: "the source wakes once Hello, Peers and Services are marked",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::Schedule],
        status: Status::Active,
        check: check_wait_complete_wakes::<S>,
        witness: Some(TriggerHistory::Log(final_mark_raced_the_wait)),
    }
}

/// [`WAIT_COMPLETE_WAKES`]'s trigger history: the final mark raced the
/// source's wait, in one of two windows.
///
/// The source's window: the sink's final notify ran between the source's
/// failed check and its `notified()`, the two operations the fix reorders.
/// With the bug, the check comes first, and the notify lands before the
/// `Notified` exists to catch it. With the fix, the `Notified` comes first,
/// and the notify lands while the check is still reading the state.
///
/// The sink's window: the source's check ran while the final mark was half
/// done, between its record and its notify. With the sink's order, record
/// then notify, that check passes. With the reverse order, it fails after
/// the last notify, and nothing wakes the source.
///
/// A run where the final mark lands before the source has done either, or
/// after it has done both, never reaches the bug.
fn final_mark_raced_the_wait(log: &ObservationLog) -> bool {
    let mut state = WaitState::new();
    for step in WaitState::steps(log.entries()) {
        let raced = state.created_notified != state.check_failed;
        let marked = state.all_marked();
        let half_done = marked != state.all_recorded();
        let last_lock = state.last_lock;
        state.apply(step);
        if half_done && state.last_lock != last_lock {
            return true;
        }
        if !marked && state.all_marked() && raced {
            return true;
        }
        if state.all_marked() && state.all_recorded() {
            return false;
        }
    }
    false
}

/// Checks [`WAIT_COMPLETE_WAKES`] from the run's whole log. It applies only
/// at steps that leave no task running, so it reads the log rarely.
fn check_wait_complete_wakes<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    if cx.status == SchedulerStatus::Running {
        return Verdict::NotApplicable;
    }
    let state = WaitState::of(cx.log.entries());
    let Some(final_notify) = state.final_notify() else {
        return Verdict::NotApplicable;
    };
    if !state.in_wait {
        return Verdict::Holds;
    }
    Verdict::Violated(Violation {
        property: WAIT_COMPLETE_WAKES.to_owned(),
        step: cx.step,
        witness: [state.last_check, Some(final_notify), state.last_notified]
            .into_iter()
            .flatten()
            .collect(),
        expected: "the source wakes once Hello, Peers and Services are marked".to_owned(),
        actual: "the source still waits in wait_complete".to_owned(),
    })
}

/// What the observations show about the source's wait and the sink's marks.
/// It follows one source and one sink, as the single-peer harness runs
/// them; a harness with several peers needs each session's tasks told
/// apart first.
#[derive(Clone, Debug)]
pub struct WaitState {
    /// Each task's next point, as (kind, line), from its last stop.
    next: BTreeMap<String, (String, Option<u32>)>,
    /// The step that ran each mark's notify.
    marked: Vec<Option<u64>>,
    /// The step that ran each mark's record.
    recorded: Vec<Option<u64>>,
    /// The source's last check and `notified` steps in the waiting code.
    last_check: Option<u64>,
    last_notified: Option<u64>,
    /// The step at which the source's last check took the lock, and whether
    /// it passed: every record had landed by then.
    last_lock: Option<(u64, bool)>,
    /// True once the source has created its `Notified` or failed a check in
    /// the waiting code, and has neither passed a check nor stopped outside
    /// its wait since, so it can only go on when woken.
    in_wait: bool,
    /// True if the source's last stop was a poll that returned pending, so
    /// it runs again only once woken.
    source_parked: bool,
    /// True once the source has created its `Notified` in the current pass
    /// through the waiting code.
    created_notified: bool,
    /// True once the source's check has failed in the current pass through
    /// the waiting code: it took the state's lock before the final record. A
    /// pass ends when the source parks, leaves the waiting code, or runs its
    /// `notified` or check again.
    check_failed: bool,
}

impl WaitState {
    /// The state after `entries`, a log's entries from its start.
    ///
    /// A step that runs a task appends one observation that names it, so
    /// that observation says which task ran. The point it ran from is where
    /// the task last stopped.
    pub fn of(entries: &[Entry]) -> WaitState {
        let mut state = WaitState::new();
        WaitState::steps(entries).for_each(|step| state.apply(step));
        state
    }

    /// The state before any step.
    fn new() -> WaitState {
        WaitState {
            next: BTreeMap::new(),
            marked: vec![None; sites().marks.len()],
            recorded: vec![None; sites().records.len()],
            last_check: None,
            last_notified: None,
            last_lock: None,
            in_wait: false,
            source_parked: false,
            created_notified: false,
            check_failed: false,
        }
    }

    /// `entries` split into steps, without step 0, which is setup and runs
    /// no task.
    fn steps(entries: &[Entry]) -> impl Iterator<Item = &[Entry]> {
        let start = entries.partition_point(|e| e.step == 0);
        entries[start..].chunk_by(|a, b| a.step == b.step)
    }

    /// Applies one step's entries.
    fn apply(&mut self, step: &[Entry]) {
        let sites = sites();
        let task = step.iter().find_map(|e| e.obs.ran_task());
        // A parked source that runs resumes its await, not its next point.
        let source_ran = task.is_some_and(is_source);
        let from = |kind: &str| {
            source_ran && !self.source_parked && self.source_waits_at(|at| at.starts_with(kind))
        };
        let (created, checked) = (from("notified"), from("lock"));
        // A check reads the records, not the notifies: one that takes the lock
        // before the final record finds the exchange incomplete.
        let failed = checked && !self.all_recorded();
        if let Some(task) = task {
            self.ran(sites, task, step[0].step);
        }
        for entry in step {
            match &entry.obs {
                Observation::TaskReached { task, at, site, .. } => {
                    let line = sites.line(site);
                    if is_source(task) {
                        self.source_parked = false;
                        // A stop outside the waiting code, other than the
                        // sleep's first poll in the same `select!`, means the
                        // source left its wait.
                        let waits = line.is_some_and(|line| {
                            sites.waiting.contains(&line) || line == sites.initial_sleep
                        });
                        if !waits {
                            self.in_wait = false;
                        }
                    }
                    self.next.insert(task.clone(), (at.clone(), line));
                }
                Observation::TaskWaiting { task } | Observation::TaskParked { task }
                    if is_source(task) =>
                {
                    self.source_parked = true;
                }
                _ => {}
            }
        }
        if source_ran {
            // A source that repeats an operation of its pass without parking
            // went through a `notified.await` that was already woken, so it
            // starts a new pass.
            if (created && self.created_notified) || (checked && self.check_failed) {
                self.created_notified = false;
                self.check_failed = false;
            }
            self.created_notified |= created;
            self.check_failed |= failed;
            if checked {
                self.last_lock = Some((step[0].step, !failed));
            }
            if self.source_parked || !self.source_waits_at(|_| true) {
                self.created_notified = false;
                self.check_failed = false;
            }
        }
    }

    /// Notes that `task` ran at `step` from its last stop.
    fn ran(&mut self, sites: &Sites, task: &str, step: u64) {
        let Some((at, Some(line))) = self.next.get(task) else {
            return;
        };
        // Both answers share one notify, so each run fills the next open mark
        // at its line.
        let open = sites
            .marks
            .iter()
            .zip(&self.marked)
            .position(|(mark, marked)| mark == line && marked.is_none());
        if let Some(i) = open {
            self.marked[i] = Some(step);
        }
        // The record lands when the task runs from the lock, not from the
        // release on the same line.
        let open = sites
            .records
            .iter()
            .zip(&self.recorded)
            .position(|(record, recorded)| record == line && recorded.is_none());
        if let Some(i) = open.filter(|_| at.starts_with("lock")) {
            self.recorded[i] = Some(step);
        }
        if is_source(task) && sites.waiting.contains(line) {
            if at.starts_with("release") {
                self.last_check = Some(step);
                // A failed check leaves the source waiting: on the
                // `Notified` it created before the check, or on the one it
                // creates next.
                self.in_wait = self.last_lock.is_some_and(|(_, passed)| !passed);
            } else if at.starts_with("notified") {
                self.last_notified = Some(step);
                self.in_wait = true;
            }
        }
    }

    /// The last mark's step, once every mark has run.
    fn final_notify(&self) -> Option<u64> {
        self.marked
            .iter()
            .copied()
            .collect::<Option<Vec<u64>>>()?
            .into_iter()
            .max()
    }

    /// True if every mark's notify has run.
    pub fn all_marked(&self) -> bool {
        self.final_notify().is_some()
    }

    /// True if every mark's record has landed.
    pub fn all_recorded(&self) -> bool {
        self.recorded.iter().all(Option::is_some)
    }

    /// True if the source's next point is its `notified` in the waiting
    /// code.
    pub fn source_at_notified(&self) -> bool {
        self.source_waits_at(|at| at.starts_with("notified"))
    }

    /// True if the source finished the first of the two operations
    /// [`final_mark_raced_the_wait`] names and the second is its next point:
    /// the `notified` after a failed check, or the check after a `notified`.
    pub fn source_between_check_and_notified(&self) -> bool {
        (self.check_failed && self.source_at_notified())
            || (self.created_notified && self.source_waits_at(|at| at.starts_with("lock")))
    }

    /// True if the source's next point is in the waiting code, at a point
    /// whose kind `at` accepts.
    fn source_waits_at(&self, at: impl Fn(&str) -> bool) -> bool {
        let sites = sites();
        self.next.iter().any(|(task, (kind, line))| {
            is_source(task) && at(kind) && line.is_some_and(|line| sites.waiting.contains(&line))
        })
    }
}

/// True if `task` is the source task, which `spawn_supervised_peer_task`
/// labels.
pub fn is_source(task: &str) -> bool {
    task.ends_with("/discovery_source.1")
}

/// Where `SharedPeerState` lives, the lines of its `wait_complete` body and
/// of the `Hello` and answer records and notifies, and the line of
/// `wait_initial`'s sleep.
#[derive(Debug, PartialEq, Eq)]
struct Sites {
    file: &'static str,
    /// The body of `wait_complete`, the code a waiter runs between checking
    /// for completion and waiting.
    waiting: RangeInclusive<u32>,
    /// The `Hello` notify, then the answer notify once per answer.
    marks: Vec<u32>,
    /// The `Hello` record's lock, then the answer record's lock once per
    /// answer.
    records: Vec<u32>,
    /// The sleep in `wait_initial`, the initial wait's timeout arm.
    initial_sleep: u32,
}

/// The discovery peer's source, as the binary was built from it. The
/// observations carry the line numbers compiled into the binary, so the
/// sites must come from the same text, never from the checkout at run time.
const PEER_RS: &str = include_str!("../../../zakura-network/src/zakura/discovery/peer.rs");

/// The sites in [`PEER_RS`], parsed once.
fn sites() -> &'static Sites {
    static SITES: OnceLock<Sites> = OnceLock::new();
    SITES.get_or_init(|| Sites::parse(PEER_RS))
}

impl Sites {
    /// The sites in `source`, the text of the discovery peer's file.
    fn parse(source: &str) -> Sites {
        let file = "crates/zakura-network/src/zakura/discovery/peer.rs";
        let lines: Vec<&str> = source.lines().collect();
        let find = |from: usize, needle: &str| {
            from + lines[from..]
                .iter()
                .position(|l| l.contains(needle))
                .unwrap_or_else(|| panic!("peer.rs has `{needle}`"))
        };
        // Lines are 1-based, and the file has far fewer than 2^32 of them.
        let line = |i: usize| (i + 1) as u32;
        let progress = find(0, "impl SharedPeerState");
        let wait_complete = find(progress, "async fn wait_complete(&self)");
        let end = wait_complete
            + lines[wait_complete..]
                .iter()
                .position(|l| *l == "    }")
                .expect("wait_complete's body closes");
        let recorders = ["fn record_hello", "fn record_answer", "fn record_answer"];
        let in_recorders = |needle: &str| -> Vec<u32> {
            recorders
                .iter()
                .map(|recorder| line(find(find(progress, recorder), needle)))
                .collect()
        };
        let marks = in_recorders("notify_waiters()");
        let records = in_recorders("self.lock()");
        let initial_sleep = line(find(find(0, "async fn wait_initial("), "time::sleep("));
        Sites {
            file,
            waiting: line(wait_complete)..=line(end),
            marks,
            records,
            initial_sleep,
        }
    }

    /// The line of `site` if it's in the discovery peer's file.
    fn line(&self, site: &Option<String>) -> Option<u32> {
        let (file, line) = site.as_deref()?.rsplit_once(':')?;
        if file.ends_with(self.file) {
            line.parse().ok()
        } else {
            None
        }
    }
}

/// A correlated answer that the node reads after its initial exchange timed
/// out gets no punishment.
pub const LATE_RESPONSE_ADMITTED: &str = "discovery/late-response-admitted";

/// [`LATE_RESPONSE_ADMITTED`]: once the node's reader has the header of a
/// late answer ([`late_answer`]), the node never closes that answer's
/// connection with a fault cause and never reports a `Misbehavior` against
/// it, unless it judged a frame on the connection that isn't `Conformant`
/// before the punishment, as `conformant-never-closes` allows. The claim
/// stops at no punishment: whether the answer reaches its session is
/// `discovery/late-response-delivered`'s.
///
/// It reads the transport tap's sessions and header reads, the source's
/// stops at its initial wait's sleep, the peers' frames and labels, the
/// node's judgments and its closes. A late answer is conformant,
/// so a bypass that punishes it also violates
/// `discovery/s4v2/conformant-never-closes`. Its planted case lists that
/// ID in `also_violates`, an expected co-violation.
pub fn late_response_admitted<S: HasView>() -> Property<S> {
    Property {
        id: PropertyId::new(LATE_RESPONSE_ADMITTED),
        claim: "a correlated answer the node reads after its initial exchange timed out gets no \
                fault close and no Misbehavior",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::Schedule, Source::Frames, Source::Closes],
        status: Status::Active,
        check: check_late_response_admitted::<S>,
        witness: Some(TriggerHistory::Log(|log| {
            let entries = log.entries();
            (0..entries.len()).any(|at| matches!(late_answer(entries, at), Ok(Some(_))))
        })),
    }
}

/// Checks [`LATE_RESPONSE_ADMITTED`] over the step's entries. It applies at
/// each step where the reader has a late answer's header. Each fault close
/// or `Misbehavior` at the step, on a connection where the reader had a late
/// answer's header before it, is a violation of its own, unless the node
/// had judged a frame on that connection that isn't `Conformant`.
fn check_late_response_admitted<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let entries = cx.log.entries();
    let start = entries.partition_point(|e| e.step < cx.step);
    let mut verdict = Verdict::NotApplicable;
    let mut punished = Vec::new();
    for at in start..entries.len() {
        let (connection, punishment) = match &entries[at].obs {
            Observation::ConnectionClosed { connection, cause } if is_fault_cause(cause) => {
                (*connection, format!("a close with cause {cause}"))
            }
            Observation::Misbehavior { connection } => (*connection, "a Misbehavior".to_owned()),
            _ => {
                match late_answer(entries, at) {
                    Ok(Some(_)) => verdict = Verdict::Holds,
                    Ok(None) => {}
                    Err(missing) => {
                        cx.missing_evidence(missing);
                        return Verdict::NotApplicable;
                    }
                }
                continue;
            }
        };
        let late = (0..at).find_map(|read| match late_answer(entries, read) {
            Ok(Some(late)) if late.connection == connection => Some(Ok(late)),
            Ok(_) => None,
            Err(missing) => Some(Err(missing)),
        });
        let late = match late {
            Some(Ok(late)) => late,
            Some(Err(missing)) => {
                cx.missing_evidence(missing);
                return Verdict::NotApplicable;
            }
            None => continue,
        };
        // A frame the node judged wrong justifies the punishment.
        match judged_nonconformant(&entries[..at], connection) {
            Ok(true) => {}
            Ok(false) => punished.push((late, punishment)),
            Err(missing) => {
                cx.missing_evidence(missing);
                return Verdict::NotApplicable;
            }
        }
    }
    let expected = |late: &LateAnswer| {
        format!(
            "no punishment on connection {} after the node read its {} {:?} after the initial \
             exchange timed out",
            late.connection, late.row, late.after_admission
        )
    };
    let mut punished = punished.into_iter();
    let Some((late, punishment)) = punished.next() else {
        return verdict;
    };
    for (late, punishment) in punished {
        cx.record_violation(expected(&late), punishment);
    }
    cx.violated(expected(&late), punishment)
}

/// A late answer: a correlated answer whose header the node's reader has
/// after its initial exchange timed out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LateAnswer {
    /// The connection.
    pub connection: u64,
    /// The answer's row, `peers` or `services`.
    pub row: String,
    /// How long after the session's admission the reader had the header.
    pub after_admission: Duration,
}

/// The late answer whose header read is `entries[at]`, if it is one.
///
/// The entry must be the reader's [`TransportFact::HeaderRead`] of a
/// `Peers` or `Services` on a discovery session. Its peer frame
/// must be `Conformant` and name the node's query, which makes it a
/// correlated answer. The session's initial wait must have timed out
/// before the read ([`initial_wait_timed_out`]). It must be the session's
/// first frame of its row: completing the exchange needs an answer of each
/// row, so the exchange hadn't completed. The header read comes first,
/// because a reader that refuses the answer never reads its payload.
///
/// An answer's header read on a discovery session with no peer frame is
/// missing evidence.
pub fn late_answer(entries: &[Entry], at: usize) -> Result<Option<LateAnswer>, String> {
    let Observation::Transport {
        connection: Some(connection),
        event:
            TransportFact::HeaderRead {
                quic_stream,
                seq,
                message_type,
                ..
            },
        ..
    } = entries[at].obs
    else {
        return Ok(None);
    };
    let Some(answer) = [DiscoveryRow::Peers, DiscoveryRow::Services]
        .into_iter()
        .find(|row| row.rule().message_type == message_type)
    else {
        return Ok(None);
    };
    let before = &entries[..at];
    let Some(admitted) = before.iter().rposition(|entry| {
        matches!(entry.obs, Observation::Session { connection: c, quic_stream: q, kind, .. }
            if (c, q, kind) == (connection, quic_stream, ZAKURA_STREAM_DISCOVERY))
    }) else {
        return Ok(None);
    };
    let frames = || {
        before.iter().filter_map(|entry| match &entry.obs {
            Observation::PeerFrame {
                connection: c,
                stream,
                quic_stream: Some(q),
                seq,
                row,
                label,
                exchange,
                ..
            } if (*c, *q, stream.kind) == (connection, quic_stream, ZAKURA_STREAM_DISCOVERY) => {
                Some((*seq, row.as_deref(), *label, *exchange))
            }
            _ => None,
        })
    };
    // The peer's frames go to the log before the node reads them. Only a
    // request past the margin waits for its read.
    let Some((_, row, label, exchange)) = frames().find(|frame| frame.0 == seq) else {
        return Err(format!(
            "connection {connection}'s discovery reader read answer {seq}'s header on QUIC \
             stream {quic_stream}, which no peer frame names"
        ));
    };
    if row != Some(answer.name())
        || label != Label::Conformant
        || exchange.is_none()
        || frames().any(|(earlier, other, ..)| earlier < seq && other == row)
    {
        return Ok(None);
    }
    if !initial_wait_timed_out(&entries[admitted..at], time_at(entries, admitted)) {
        return Ok(None);
    }
    Ok(Some(LateAnswer {
        connection,
        row: answer.name().to_owned(),
        after_admission: time_at(entries, at).saturating_sub(time_at(entries, admitted)),
    }))
}

/// True if a discovery source spawned in `window`, a session's entries
/// from its admission at time `start`, took its initial wait's timeout
/// there.
///
/// The source reaches `wait_initial`'s sleep when it first polls it, and
/// the simulated sleep stops there again only once its deadline,
/// [`DISCOVERY_INITIAL_EXCHANGE_TIMEOUT`] after that first poll, has
/// passed. The wait's biased select reaches the sleep only after the
/// cancel and `wait_complete` arms were pending in the same poll, so a stop
/// at or after the deadline ends the wait `TimedOut`. A clock past the
/// deadline isn't enough: the peer's answer and the node's timer can fall
/// due at the same instant, and an answer the reader handles before the
/// source polls again completes the wait.
///
/// The window starts at the session's admission, so a source spawned for
/// an earlier session doesn't count. Like [`WaitState`], it can't tell
/// apart two sessions admitted in the same window.
fn initial_wait_timed_out(window: &[Entry], start: Duration) -> bool {
    let sites = sites();
    let mut now = start;
    // Each source spawned in the window, and its first stop at the sleep.
    let mut sources = BTreeMap::<&str, Option<Duration>>::new();
    for entry in window {
        match &entry.obs {
            Observation::TimeAdvanced { to_ns, .. } => now = Duration::from_nanos(*to_ns),
            Observation::TaskSpawned { task } if is_source(task) => {
                sources.insert(task, None);
            }
            Observation::TaskReached { task, site, .. }
                if sites.line(site) == Some(sites.initial_sleep) =>
            {
                match sources.get_mut(task.as_str()) {
                    Some(Some(first)) if now >= *first + DISCOVERY_INITIAL_EXCHANGE_TIMEOUT => {
                        return true;
                    }
                    Some(first @ None) => *first = Some(now),
                    _ => {}
                }
            }
            _ => {}
        }
    }
    false
}

/// The run's virtual time at `entries[at]`: the last time it advanced to
/// before then.
fn time_at(entries: &[Entry], at: usize) -> Duration {
    entries[..=at]
        .iter()
        .rev()
        .find_map(|entry| match entry.obs {
            Observation::TimeAdvanced { to_ns, .. } => Some(Duration::from_nanos(to_ns)),
            _ => None,
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use zakura_scheduler::SimInstant;

    use super::*;
    use crate::{
        check::{Phase, StepCtx},
        observe::StreamId,
        Identity,
    };

    /// The sites come from the embedded source, and only from the text
    /// they're given: a copy shifted down by 100 lines moves every site by
    /// 100, while the cached sites stay put.
    #[test]
    fn sites_come_from_the_embedded_source() {
        let built = sites();
        assert_eq!(*built, Sites::parse(PEER_RS));
        assert!(built.waiting.start() < built.waiting.end());
        assert_eq!(built.marks.len(), 3);
        assert_eq!(built.records.len(), 3);
        assert_eq!(
            built.initial_sleep,
            line_of("zakura_rt::time::sleep(timeout)")
        );

        let shifted = Sites::parse(&format!("{}{PEER_RS}", "\n".repeat(100)));
        assert_eq!(
            shifted.waiting,
            built.waiting.start() + 100..=built.waiting.end() + 100
        );
        assert_eq!(
            shifted.marks,
            built
                .marks
                .iter()
                .map(|line| line + 100)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            shifted.records,
            built
                .records
                .iter()
                .map(|line| line + 100)
                .collect::<Vec<_>>()
        );
        assert_eq!(shifted.initial_sleep, built.initial_sleep + 100);
        assert_eq!(*sites(), Sites::parse(PEER_RS));
    }

    const SINK: &str = "discovery_admission.1/discovery_pipe.1";
    const SOURCE: &str = "discovery_admission.1/discovery_source.1";

    /// A log built one step at a time, each step running one task.
    struct History {
        log: ObservationLog,
        step: u64,
    }

    impl History {
        fn new() -> History {
            History {
                log: ObservationLog::default(),
                step: 0,
            }
        }

        /// One step that runs `task` to point `at` on `line` of the
        /// peer's file, or parks it for `None`.
        fn run(&mut self, task: &str, to: Option<(&str, u32)>) -> &mut History {
            self.step += 1;
            self.log.begin(self.step);
            let task = task.to_owned();
            self.log.push(match to {
                Some((at, line)) => Observation::TaskReached {
                    task,
                    at: at.to_owned(),
                    object: None,
                    site: Some(format!("{}:{line}", sites().file)),
                },
                None => Observation::TaskWaiting { task },
            });
            self
        }

        /// The sink records part `i` and runs its notify, stopping before
        /// the next mark's lock.
        fn mark(&mut self, i: usize) -> &mut History {
            let notify = sites().marks[i];
            self.run(SINK, Some(("lock#1", notify - 1)))
                .run(SINK, Some(("notify#1", notify)))
                .run(SINK, Some(("send#1", 1)))
        }

        fn raced(&self) -> bool {
            final_mark_raced_the_wait(&self.log)
        }
    }

    /// The line of the peer file's first line that contains `needle`.
    fn line_of(needle: &str) -> u32 {
        let index = PEER_RS
            .lines()
            .position(|l| l.contains(needle))
            .unwrap_or_else(|| panic!("peer.rs has `{needle}`"));
        // The file has far fewer than 2^32 lines.
        index as u32 + 1
    }

    /// The point before `wait_complete`'s check takes the lock.
    fn check() -> Option<(&'static str, u32)> {
        Some(("lock#1", line_of("self.lock().is_complete()")))
    }

    /// The point right after that check releases the lock.
    fn checked() -> Option<(&'static str, u32)> {
        Some(("release#1", line_of("self.lock().is_complete()")))
    }

    /// The point before `wait_complete`'s `notified()`.
    fn notified() -> Option<(&'static str, u32)> {
        Some(("notified#1", line_of("self.0.progressed.notified()")))
    }

    /// The bug's order: the check fails, and every mark lands while the source
    /// sits at its `notified`.
    #[test]
    fn marks_after_a_failed_check_race_the_wait() {
        let mut bug = History::new();
        bug.run(SOURCE, check())
            .run(SOURCE, checked())
            .run(SOURCE, notified());
        bug.mark(0).mark(1).mark(2);
        assert!(bug.raced());
    }

    /// The bug's order, with the source held right after its failed check.
    #[test]
    fn marks_right_after_the_failed_check_race_the_wait() {
        let mut bug = History::new();
        bug.run(SOURCE, check()).run(SOURCE, checked());
        bug.mark(0).mark(1).mark(2);
        assert!(bug.raced());
    }

    /// The fix's order: the source created its `Notified`, and the final
    /// mark lands before its check takes the lock.
    #[test]
    fn marks_after_the_notified_race_the_check() {
        let mut fix = History::new();
        fix.run(SOURCE, notified()).run(SOURCE, check());
        fix.mark(0).mark(1).mark(2);
        assert!(fix.raced());
    }

    /// With the bug, a mark that lands before the check takes the lock is one
    /// the check sees. The source never waits.
    #[test]
    fn marks_before_the_bugs_check_miss_the_race() {
        let mut bug = History::new();
        bug.run(SOURCE, check());
        bug.mark(0).mark(1).mark(2);
        assert!(!bug.raced());
    }

    /// A source that has done both, or neither, misses the race: parked
    /// after creating its `Notified` and failing its check, under either
    /// order, or back at its check after a wake.
    #[test]
    fn a_source_outside_the_window_misses_the_race() {
        let mut bug = History::new();
        bug.run(SOURCE, check())
            .run(SOURCE, checked())
            .run(SOURCE, notified())
            .run(SOURCE, None);
        bug.mark(0).mark(1).mark(2);
        assert!(!bug.raced());

        let mut fix = History::new();
        fix.run(SOURCE, notified())
            .run(SOURCE, check())
            .run(SOURCE, checked())
            .run(SOURCE, None);
        fix.mark(0).mark(1).mark(2);
        assert!(!fix.raced());

        // Woken by the first answer, the bug's source runs its check again, and
        // the final mark lands before that check takes the lock.
        let mut woken = History::new();
        woken
            .run(SOURCE, check())
            .run(SOURCE, checked())
            .run(SOURCE, notified())
            .run(SOURCE, None);
        woken.mark(0).mark(1);
        woken.run(SOURCE, check());
        woken.mark(2);
        assert!(!woken.raced());
    }

    /// The first answer lands between the fix's `notified()` and its failed
    /// check, so the source's `notified.await` returns without parking. The
    /// next pass starts over: the final mark lands before that pass's check,
    /// as in [`marks_after_the_notified_race_the_check`].
    #[test]
    fn a_wait_that_was_already_woken_starts_a_new_pass() {
        let mut fix = History::new();
        fix.run(SOURCE, notified())
            .run(SOURCE, check())
            .run(SOURCE, checked());
        fix.mark(1);
        fix.run(SOURCE, notified()).run(SOURCE, check());
        fix.mark(0).mark(2);
        assert!(fix.raced());
    }

    /// The point after `wait_complete` returns, outside the waiting code.
    fn returned() -> Option<(&'static str, u32)> {
        Some((
            "lock#2",
            line_of("step = self.state.lock().on_input(input)"),
        ))
    }

    /// The source's check lands between the final record and the final
    /// notify. It passes, and the source leaves before the notify, so only
    /// the sink's window counts it.
    #[test]
    fn a_check_inside_the_final_mark_races_it() {
        let mut fix = History::new();
        fix.run(SOURCE, notified()).run(SOURCE, check());
        fix.mark(0).mark(1);
        let notify = sites().marks[2];
        fix.run(SINK, Some(("lock#1", notify - 1)))
            .run(SINK, Some(("notify#1", notify)))
            .run(SOURCE, checked())
            .run(SOURCE, returned())
            .run(SINK, Some(("send#1", 1)));
        assert!(fix.raced());
        assert!(!WaitState::of(fix.log.entries()).in_wait);
    }

    /// The sink notifies before it records. The source's check runs after
    /// the final notify and before the final record, so it fails, and the
    /// source parks on a `Notified` that no notify wakes.
    #[test]
    fn a_check_before_the_final_record_strands_the_source() {
        let mut fix = History::new();
        fix.run(SOURCE, notified()).run(SOURCE, check());
        fix.mark(0).mark(1);
        let notify = sites().marks[2];
        fix.run(SINK, Some(("notify#1", notify)))
            .run(SINK, Some(("lock#1", notify - 1)))
            .run(SOURCE, checked())
            .run(SOURCE, notified())
            .run(SOURCE, check())
            .run(SOURCE, checked())
            .run(SINK, Some(("release#1", notify - 1)))
            .run(SOURCE, None);
        assert!(fix.raced());
        let state = WaitState::of(fix.log.entries());
        assert!(state.all_marked() && state.all_recorded());
        assert!(state.in_wait);
    }

    /// Both answers run one notify, and each run fills the next open mark.
    #[test]
    fn each_answer_fills_its_own_mark() {
        let mut history = History::new();
        history.mark(1);
        let state = WaitState::of(history.log.entries());
        assert_eq!(state.marked.iter().flatten().count(), 1);
        history.mark(2);
        let state = WaitState::of(history.log.entries());
        assert_eq!(state.marked.iter().flatten().count(), 2);
        assert!(!state.all_marked());
    }

    /// A connection's discovery session, one entry per step, for
    /// [`late_answer`] and [`LATE_RESPONSE_ADMITTED`]'s check.
    struct Wire {
        log: ObservationLog,
        step: u64,
    }

    const CONNECTION: u64 = 1;
    const QUIC_STREAM: u64 = 4;
    const WIRE_SOURCE: &str = "node.1/discovery_admission.1/discovery_source.1";

    /// A harness with no view, for checks that read only the log.
    struct NoView;

    impl HasView for NoView {
        type View = ();

        fn view(&self) {}
    }

    impl Wire {
        /// The node admits the session at time 0, spawns its source, and
        /// the source first polls its initial wait's sleep.
        fn admitted() -> Wire {
            let mut wire = Wire {
                log: ObservationLog::default(),
                step: 0,
            };
            wire.push(Observation::Session {
                connection: CONNECTION,
                peer: "peer-0".to_owned(),
                quic_stream: QUIC_STREAM,
                kind: ZAKURA_STREAM_DISCOVERY,
                version: 2,
                session_id: 1,
            });
            wire.push(Observation::TaskSpawned {
                task: WIRE_SOURCE.to_owned(),
            });
            wire.sleep();
            wire
        }

        /// One step with `obs`.
        fn push(&mut self, obs: Observation) -> &mut Wire {
            self.step += 1;
            self.log.begin(self.step);
            self.log.push(obs);
            self
        }

        /// Virtual time advances to `ms`.
        fn at(&mut self, ms: u64) -> &mut Wire {
            self.push(Observation::TimeAdvanced {
                to_ns: ms * 1_000_000,
                fired: 1,
            })
        }

        /// The source stops at its initial wait's sleep.
        fn sleep(&mut self) -> &mut Wire {
            self.push(Observation::TaskReached {
                task: WIRE_SOURCE.to_owned(),
                at: "sleep#1".to_owned(),
                object: None,
                site: Some(format!("{}:{}", sites().file, sites().initial_sleep)),
            })
        }

        /// The peer sends frame `seq` of `row`, labelled `label`.
        fn sent(&mut self, seq: u64, row: DiscoveryRow, label: Label) -> &mut Wire {
            let stream = StreamId {
                kind: ZAKURA_STREAM_DISCOVERY,
                version: 2,
            };
            self.push(Observation::PeerFrame {
                connection: CONNECTION,
                stream,
                quic_stream: Some(QUIC_STREAM),
                seq,
                row: Some(row.name().to_owned()),
                rule_stream: stream,
                label,
                exhausted: false,
                exchange: matches!(row, DiscoveryRow::Peers | DiscoveryRow::Services).then_some(1),
                payload: 0,
            })
        }

        /// The node's reader has frame `seq`'s header, of `row`'s type.
        fn read(&mut self, seq: u64, row: DiscoveryRow) -> &mut Wire {
            self.push(Observation::Transport {
                instance: 0,
                seq,
                node: "node".to_owned(),
                connection: Some(CONNECTION),
                remote: "10.1.0.1:4000".to_owned(),
                event: TransportFact::HeaderRead {
                    quic_stream: QUIC_STREAM,
                    seq,
                    message_type: row.rule().message_type,
                    flags: 0,
                    payload_len: 0,
                },
            })
        }

        /// The node judges frame `seq`.
        fn judged(&mut self, seq: u64) -> &mut Wire {
            self.push(Observation::FrameJudged {
                connection: CONNECTION,
                stream: StreamId {
                    kind: ZAKURA_STREAM_DISCOVERY,
                    version: 2,
                },
                quic_stream: Some(QUIC_STREAM),
                seq,
            })
        }

        /// The node closes the connection with a fault cause.
        fn fault_close(&mut self) -> &mut Wire {
            self.push(Observation::ConnectionClosed {
                connection: CONNECTION,
                cause: "service_protocol_reject".to_owned(),
            })
        }

        /// The late answer whose header read is the last entry, if it is one.
        fn last_read_is_late(&self) -> bool {
            let entries = self.log.entries();
            late_answer(entries, entries.len() - 1)
                .expect("every answer has its peer frame")
                .is_some()
        }

        /// The property's verdict at the last step. The check finds no
        /// evidence missing.
        fn verdict(&self) -> Verdict {
            let registered = [late_response_admitted::<NoView>()];
            let pick = Identity::advance_time(SimInstant::ZERO);
            let step = StepCtx {
                step: self.step,
                pick: &pick,
                enabled: &[],
                phase: Phase::Live,
                budget: u64::MAX,
                now: SimInstant::ZERO,
                status: SchedulerStatus::Quiescent,
                log: &self.log,
            };
            let mut cx = CheckCtx::new(&step, &NoView, (&registered[0], &registered), None);
            let verdict = (registered[0].check)(&mut cx);
            assert_eq!(cx.into_parts().missing, None);
            verdict
        }
    }

    /// An answer the reader has after the source's sleep stopped past its
    /// deadline is late.
    #[test]
    fn an_answer_after_the_timeout_is_late() {
        let mut wire = Wire::admitted();
        wire.sent(1, DiscoveryRow::Peers, Label::Conformant)
            .at(2_000)
            .sleep()
            .read(1, DiscoveryRow::Peers);
        assert!(wire.last_read_is_late());
    }

    /// The peer's answer and the node's timer fall due at the same instant.
    /// The reader has the answer before the source polls its sleep again, so
    /// the answer completes the wait and isn't late, though the clock is past
    /// the timeout.
    #[test]
    fn an_answer_before_the_sleep_stops_is_on_time() {
        let mut wire = Wire::admitted();
        wire.sent(1, DiscoveryRow::Peers, Label::Conformant)
            .at(2_000)
            .read(1, DiscoveryRow::Peers);
        assert!(!wire.last_read_is_late());
        // A stop before the deadline isn't the timeout either.
        let mut early = Wire::admitted();
        early
            .sent(1, DiscoveryRow::Peers, Label::Conformant)
            .at(1_999)
            .sleep()
            .read(1, DiscoveryRow::Peers);
        assert!(!early.last_read_is_late());
    }

    /// Only the session's first answer of a row can complete the exchange,
    /// so a second `Peers` read after the timeout isn't a late first answer,
    /// whether the first one came on time or late.
    #[test]
    fn a_second_answer_of_a_row_is_not_late() {
        let mut wire = Wire::admitted();
        wire.sent(1, DiscoveryRow::Peers, Label::Conformant)
            .sent(2, DiscoveryRow::Peers, Label::Conformant)
            .read(1, DiscoveryRow::Peers)
            .at(2_000)
            .sleep()
            .read(2, DiscoveryRow::Peers);
        assert!(!wire.last_read_is_late());

        let mut both_late = Wire::admitted();
        both_late
            .sent(1, DiscoveryRow::Peers, Label::Conformant)
            .sent(2, DiscoveryRow::Peers, Label::Conformant)
            .at(2_000)
            .sleep()
            .read(1, DiscoveryRow::Peers);
        assert!(both_late.last_read_is_late());
        both_late.read(2, DiscoveryRow::Peers);
        assert!(!both_late.last_read_is_late());
    }

    /// A fault close after a late answer violates the property, unless the
    /// node judged a frame on the connection that isn't `Conformant` before
    /// it, such as a `Hello` with bad content or a `GetPeers` past the margin.
    #[test]
    fn a_wrong_frame_after_the_late_answer_justifies_the_close() {
        let late = || {
            let mut wire = Wire::admitted();
            wire.sent(1, DiscoveryRow::Peers, Label::Conformant)
                .at(2_000)
                .sleep()
                .read(1, DiscoveryRow::Peers);
            // The property applies at the late answer's read.
            assert_eq!(wire.verdict(), Verdict::Holds);
            wire.judged(1);
            wire
        };
        let mut wire = late();
        wire.fault_close();
        assert!(matches!(wire.verdict(), Verdict::Violated(_)));

        for (row, label) in [
            (DiscoveryRow::Hello, Label::BadContent),
            (DiscoveryRow::GetPeers, Label::OverMargin),
        ] {
            let mut wire = late();
            wire.sent(2, row, label).judged(2).fault_close();
            assert_eq!(wire.verdict(), Verdict::NotApplicable, "{label:?}");
        }
    }
}
