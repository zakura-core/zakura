//! Discovery's properties (properties.md, Discovery).

use std::{collections::BTreeMap, ops::RangeInclusive, sync::OnceLock};

use zakura_scheduler::Status as SchedulerStatus;

use crate::{
    observe::Entry,
    property::{CheckCtx, HasView, Kind, Property, PropertyId, Source, Status, Verdict},
    Observation, Violation,
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
        witness: None,
    }
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
}

impl WaitState {
    /// The state after `entries`, a log's entries from its start.
    ///
    /// A step that runs a task appends one observation that names it, so
    /// that observation says which task ran. The point it ran from is where
    /// the task last stopped.
    pub fn of(entries: &[Entry]) -> WaitState {
        let sites = sites();
        let mut state = WaitState {
            next: BTreeMap::new(),
            marked: vec![None; sites.marks.len()],
            last_load: None,
            last_notified: None,
            in_wait: false,
        };
        // Step 0 is setup, which runs no task.
        let start = entries.partition_point(|e| e.step == 0);
        for step in entries[start..].chunk_by(|a, b| a.step == b.step) {
            if let Some(task) = step.iter().find_map(|e| ran(&e.obs)) {
                state.ran(sites, task, step[0].step);
            }
            for entry in step {
                if let Observation::TaskReached { task, at, site } = &entry.obs {
                    state
                        .next
                        .insert(task.clone(), (at.clone(), sites.line(site)));
                }
            }
        }
        state
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
        let sites = sites();
        self.next.iter().any(|(task, (at, line))| {
            is_source(task)
                && at.starts_with("notified")
                && line.is_some_and(|line| sites.waiting.contains(&line))
        })
    }
}

/// The task a step ran, from the observation its run appended.
fn ran(obs: &Observation) -> Option<&str> {
    match obs {
        Observation::TaskReached { task, .. }
        | Observation::TaskWaiting { task }
        | Observation::TaskParked { task }
        | Observation::TaskExited { task, .. } => Some(task),
        _ => None,
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
}
