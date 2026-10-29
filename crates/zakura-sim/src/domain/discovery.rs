//! Discovery's properties (properties.md, Discovery).

use std::{collections::BTreeMap, ops::RangeInclusive, sync::OnceLock};

use zakura_scheduler::Status as SchedulerStatus;

use crate::{
    observe::Entry,
    property::{CheckCtx, HasView, Kind, Property, PropertyId, Source, Status, Verdict},
    Observation, ObservationLog, Violation,
};

/// `wait_complete` wakes the source once the initial exchange completes.
pub const WAIT_COMPLETE_WAKES: &str = "discovery/wait-complete-wakes";

/// [`WAIT_COMPLETE_WAKES`]: at each step that leaves no task running after
/// the sink has run all three `mark_*` notifies, the source must not be
/// waiting after a point in `complete` or `wait_complete`.
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
        witness: Some(final_mark_raced_the_wait),
    }
}

/// [`WAIT_COMPLETE_WAKES`]'s trigger history: the sink's final mark ran
/// between the source's failed check and its `notified()`, the two
/// operations the fix reorders. With the bug, the check comes first, and
/// the mark lands before the `Notified` exists to catch it. With the fix,
/// the `Notified` comes first, and the mark lands while the check is still
/// loading flags. A run where the final mark lands before the source has
/// done either, or after it has done both, never reaches the bug.
fn final_mark_raced_the_wait(log: &ObservationLog) -> bool {
    let mut state = WaitState::new();
    for step in WaitState::steps(log.entries()) {
        let raced = state.created_notified != state.check_failed;
        let marked = state.all_marked();
        state.apply(step);
        if !marked && state.all_marked() {
            return raced;
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
        witness: [state.last_load, Some(final_notify), state.last_notified]
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
    /// The source's last load and `notified` steps in the waiting code.
    last_load: Option<u64>,
    last_notified: Option<u64>,
    /// True once the source has created its `Notified` in the waiting code
    /// and hasn't checked for completion since, so it can only go on when
    /// woken.
    in_wait: bool,
    /// True if the source's last stop was a poll that returned pending, so
    /// it runs again only once woken.
    source_parked: bool,
    /// True once the source has created its `Notified` in the current pass
    /// through the waiting code.
    created_notified: bool,
    /// True once the source's check has failed in the current pass through
    /// the waiting code: it ran a load and then parked or stopped at its
    /// `notified`. A pass ends when the source parks, leaves the waiting
    /// code, or runs its `notified` or check again.
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
            last_load: None,
            last_notified: None,
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
        let (created, loaded) = (from("notified"), from("load"));
        if let Some(task) = task {
            self.ran(sites, task, step[0].step);
        }
        for entry in step {
            match &entry.obs {
                Observation::TaskReached { task, at, site, .. } => {
                    if is_source(task) {
                        self.source_parked = false;
                    }
                    self.next
                        .insert(task.clone(), (at.clone(), sites.line(site)));
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
            if (created && self.created_notified) || (loaded && self.check_failed) {
                self.created_notified = false;
                self.check_failed = false;
            }
            self.created_notified |= created;
            self.check_failed |= loaded && (self.source_parked || self.source_at_notified());
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
        if let Some(i) = sites.marks.iter().position(|m| m == line) {
            self.marked[i] = Some(step);
        }
        if is_source(task) && sites.waiting.contains(line) {
            if at.starts_with("load") {
                self.last_load = Some(step);
                self.in_wait = false;
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

    /// True if the source's next point is its `notified` in the waiting
    /// code.
    pub fn source_at_notified(&self) -> bool {
        self.source_waits_at(|at| at.starts_with("notified"))
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

/// Where `DiscoveryExchangeProgress` lives, and the lines of its
/// `complete` and `wait_complete` bodies and of the three `mark_*` notifies.
#[derive(Debug, PartialEq, Eq)]
struct Sites {
    file: &'static str,
    /// `complete` through the end of `wait_complete`, the code a waiter runs
    /// between checking for completion and waiting.
    waiting: RangeInclusive<u32>,
    marks: Vec<u32>,
}

/// The discovery service's source, as the binary was built from it. The
/// observations carry the line numbers compiled into the binary, so the
/// sites must come from the same text, never from the checkout at run time.
const SERVICE_RS: &str = include_str!("../../../zakura-network/src/zakura/discovery/service.rs");

/// The sites in [`SERVICE_RS`], parsed once.
fn sites() -> &'static Sites {
    static SITES: OnceLock<Sites> = OnceLock::new();
    SITES.get_or_init(|| Sites::parse(SERVICE_RS))
}

impl Sites {
    /// The sites in `source`, the text of the discovery service's file.
    fn parse(source: &str) -> Sites {
        let file = "crates/zakura-network/src/zakura/discovery/service.rs";
        let lines: Vec<&str> = source.lines().collect();
        let find = |from: usize, needle: &str| {
            from + lines[from..]
                .iter()
                .position(|l| l.contains(needle))
                .unwrap_or_else(|| panic!("service.rs has `{needle}`"))
        };
        // Lines are 1-based, and the file has far fewer than 2^32 of them.
        let line = |i: usize| (i + 1) as u32;
        let progress = find(0, "impl DiscoveryExchangeProgress");
        let complete = find(progress, "fn complete(&self)");
        let wait_complete = find(complete, "async fn wait_complete(&self)");
        let end = wait_complete
            + lines[wait_complete..]
                .iter()
                .position(|l| *l == "    }")
                .expect("wait_complete's body closes");
        let marks = ["fn mark_hello", "fn mark_peers", "fn mark_services"]
            .iter()
            .map(|mark| line(find(find(progress, mark), "notify_waiters()")))
            .collect();
        Sites {
            file,
            waiting: line(complete)..=line(end),
            marks,
        }
    }

    /// The line of `site` if it's in the discovery service's file.
    fn line(&self, site: &Option<String>) -> Option<u32> {
        let (file, line) = site.as_deref()?.rsplit_once(':')?;
        if file.ends_with(self.file) {
            line.parse().ok()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The sites come from the embedded source, and only from the text
    /// they're given: a copy shifted down by 100 lines moves every site by
    /// 100, while the cached sites stay put.
    #[test]
    fn sites_come_from_the_embedded_source() {
        let built = sites();
        assert_eq!(*built, Sites::parse(SERVICE_RS));
        assert!(built.waiting.start() < built.waiting.end());
        assert_eq!(built.marks.len(), 3);

        let shifted = Sites::parse(&format!("{}{SERVICE_RS}", "\n".repeat(100)));
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
        assert_eq!(*sites(), Sites::parse(SERVICE_RS));
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
        /// service's file, or parks it for `None`.
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

        /// The sink stores flag `i` and runs its notify, stopping before
        /// the next mark's store.
        fn mark(&mut self, i: usize) -> &mut History {
            let notify = sites().marks[i];
            self.run(SINK, Some(("store#1", notify - 1)))
                .run(SINK, Some(("notify#1", notify)))
                .run(SINK, Some(("send#1", 1)))
        }

        fn raced(&self) -> bool {
            final_mark_raced_the_wait(&self.log)
        }
    }

    /// The line of the service's first line that contains `needle`.
    fn line_of(needle: &str) -> u32 {
        let index = SERVICE_RS
            .lines()
            .position(|l| l.contains(needle))
            .unwrap_or_else(|| panic!("service.rs has `{needle}`"));
        // The file has far fewer than 2^32 lines.
        index as u32 + 1
    }

    /// The point before flag `i`'s load in `complete`.
    fn load(i: usize) -> Option<(&'static str, u32)> {
        let flag = ["hello", "peers", "services"][i];
        Some(("load#1", line_of(&format!("self.{flag}.load("))))
    }

    /// The point before `wait_complete`'s `notified()`.
    fn notified() -> Option<(&'static str, u32)> {
        Some(("notified#1", line_of("self.notify.notified()")))
    }

    /// The bug's order: the check fails on Hello, and every mark lands
    /// while the source sits at its `notified`.
    #[test]
    fn marks_after_a_failed_check_race_the_wait() {
        let mut bug = History::new();
        bug.run(SOURCE, load(0)).run(SOURCE, notified());
        bug.mark(0).mark(1).mark(2);
        assert!(bug.raced());
    }

    /// The fix's order: the source created its `Notified`, and the final
    /// mark lands while its check still has the Services load to run.
    #[test]
    fn marks_after_the_notified_race_the_check() {
        let mut fix = History::new();
        fix.run(SOURCE, notified()).run(SOURCE, load(0));
        fix.mark(0).mark(1);
        fix.run(SOURCE, load(1)).run(SOURCE, load(2));
        fix.mark(2);
        assert!(fix.raced());
    }

    /// The same order without the `Notified`: with the bug, the final mark
    /// lands while the check still has the Services load to run, which
    /// sees the mark. The source never waits.
    #[test]
    fn a_mark_during_the_bugs_check_misses_the_race() {
        let mut bug = History::new();
        bug.run(SOURCE, load(0));
        bug.mark(0).mark(1);
        bug.run(SOURCE, load(1)).run(SOURCE, load(2));
        bug.mark(2);
        assert!(!bug.raced());
    }

    /// A source that has done both, or neither, misses the race: parked
    /// after creating its `Notified` and failing its check, under either
    /// order, or back at its `notified` after a wake.
    #[test]
    fn a_source_outside_the_window_misses_the_race() {
        let mut bug = History::new();
        bug.run(SOURCE, load(0))
            .run(SOURCE, notified())
            .run(SOURCE, None);
        bug.mark(0).mark(1).mark(2);
        assert!(!bug.raced());

        let mut fix = History::new();
        fix.run(SOURCE, notified())
            .run(SOURCE, load(0))
            .run(SOURCE, None);
        fix.mark(0).mark(1).mark(2);
        assert!(!fix.raced());

        // Woken by Peers, the bug's source runs its check again, and Services
        // lands mid-check.
        let mut woken = History::new();
        woken
            .run(SOURCE, load(0))
            .run(SOURCE, notified())
            .run(SOURCE, None);
        woken.mark(0).mark(1);
        woken.run(SOURCE, load(0)).run(SOURCE, load(1));
        woken.mark(2);
        assert!(!woken.raced());
    }

    /// Peers lands between the fix's `notified()` and its failed check, so
    /// the source's `notified.await` returns without parking. The next
    /// pass starts over: the final mark lands during that pass's check, as
    /// in [`marks_after_the_notified_race_the_check`].
    #[test]
    fn a_wait_that_was_already_woken_starts_a_new_pass() {
        let mut fix = History::new();
        fix.run(SOURCE, notified()).run(SOURCE, load(0));
        fix.mark(1);
        fix.run(SOURCE, notified()).run(SOURCE, load(0));
        fix.mark(0);
        fix.run(SOURCE, load(1)).run(SOURCE, load(2));
        fix.mark(2);
        assert!(fix.raced());
    }
}
