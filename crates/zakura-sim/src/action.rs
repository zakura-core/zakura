//! The actions a step can pick, and when each is enabled.
//!
//! Running a task and advancing time are built in. Datagram, storage and peer
//! actions come from [`ActionSource`]s. [`enabled_set`] combines them, and is
//! pure so tests can check it against a model (M1.1, M1.2).

use zakura_scheduler::{SimInstant, TaskId};

use crate::{check::Phase, Identity, ObservationLog};

/// A source's index in its run, from [`Sim::add_source`](crate::Sim::add_source).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceId(pub(crate) usize);

/// What a step does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Action {
    /// Run a ready task to its next point.
    RunTask(TaskId),
    /// Move virtual time to this instant and fire the timers due by then.
    AdvanceTime(SimInstant),
    /// One of a source's actions, by the source's own index.
    Source {
        /// The source.
        source: SourceId,
        /// The action's index within the source.
        local: u64,
    },
}

/// An enabled action and its identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Enabled {
    /// The action's name.
    pub identity: Identity,
    /// What running it does.
    pub action: Action,
}

/// An instant that matters to a source, so time must stop there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Boundary {
    /// New actions open at this instant, such as a delivery window.
    Opens(SimInstant),
    /// The source must act by this instant. Once it's reached, time can't
    /// advance until the source acts.
    Deadline(SimInstant),
}

/// What a source offers the drain: the actions that finish work already
/// submitted to it.
///
/// Each index is in the source's own index space, the one `enabled` uses,
/// and [`ActionSource::execute`] accepts it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoveryOffer {
    /// Actions that finish submitted work. They aren't limited to what
    /// `enabled` lists: a policy lists completions its holds filter out.
    pub actions: Vec<(Identity, u64)>,
    /// Actions eligible only at a step with no ready task, as
    /// [`ActionSource::quiet`]'s are.
    pub quiet: Vec<(Identity, u64)>,
    /// Future instants where a recovery action becomes available.
    pub boundaries: Vec<SimInstant>,
}

/// Actions outside the scheduler, such as datagrams or storage completions.
///
/// `enabled`, `boundaries`, `quiet` and `recovery` must be pure functions of
/// the source's state, so a run replays. Only `execute`, `observe` and
/// `enter_drain` change it. `execute` accepts every index any of them
/// offers.
pub trait ActionSource {
    /// Appends each action enabled at `now`, with the index `execute` takes.
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>);

    /// Appends the instants where time must stop for this source.
    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>);

    /// Appends each action enabled at `now` only because no task is ready,
    /// with the index `execute` takes. The run asks only at such a step, so
    /// every wake a previous step queued has been handled. Like `enabled`,
    /// it must be pure. The default appends nothing.
    fn quiet(&self, _now: SimInstant, _out: &mut Vec<(Identity, u64)>) {}

    /// Runs the action `enabled` listed with index `local`, appending what
    /// happened to `log`.
    fn execute(&mut self, local: u64, log: &mut ObservationLog);

    /// Appends what happened inside the source since the last call that no
    /// action of its own did, such as work a task submitted to it. The run
    /// calls it after setup and after every step, so each entry lands on the
    /// step that caused it. An `Err` says the source's evidence is
    /// incomplete, which ends the run as a harness failure before that
    /// step's checker runs. The default appends nothing.
    fn observe(&mut self, _log: &mut ObservationLog) -> Result<(), String> {
        Ok(())
    }

    /// True once one of this source's own actions started the run's
    /// recovery phase. The run asks after each step that runs one of the
    /// source's actions. From the next pick on, picks and draws follow the
    /// recovery policy instead of the input (see the `plan` module), and
    /// it stays on for the rest of the run, so a source must keep returning
    /// true. The default never starts recovery.
    fn recovering(&self) -> bool {
        false
    }

    /// Appends what the drain may run at `now` to finish work already
    /// submitted to this source, ignoring the source's holds, as
    /// [`RecoveryOffer`] describes. It offers no adversary act. Like
    /// `enabled`, it must be pure.
    fn recovery(&self, now: SimInstant, out: &mut RecoveryOffer);

    /// Runs once, when the run enters its drain, before the first
    /// `recovery` call. It retires the source's policy and task holds, and
    /// may append what it did to `log`.
    fn enter_drain(&mut self, log: &mut ObservationLog);

    /// Names submitted work this source still holds that no recovery action
    /// or boundary will finish, or `None` if there's none. The run asks only
    /// once nothing else can move, so the source may assume no task runs
    /// first.
    fn holds_work(&self) -> Option<String>;
}

/// What one source offers at one step.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Offer {
    pub(crate) actions: Vec<(Identity, u64)>,
    pub(crate) boundaries: Vec<Boundary>,
}

impl Offer {
    /// The drain's form of `recovery`: its actions, with its quiet ones
    /// when `quiet`, which the run sets when no task is ready, and each
    /// boundary as an opening.
    pub(crate) fn recovery(recovery: RecoveryOffer, quiet: bool) -> Self {
        let mut actions = recovery.actions;
        if quiet {
            actions.extend(recovery.quiet);
        }
        Offer {
            actions,
            boundaries: recovery
                .boundaries
                .into_iter()
                .map(Boundary::Opens)
                .collect(),
        }
    }
}

/// Everything [`enabled_set`] reads.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EnabledInputs<'a> {
    pub(crate) phase: Phase,
    pub(crate) now: SimInstant,
    /// The scheduler's ready tasks, with their identities.
    pub(crate) ready: &'a [(Identity, TaskId)],
    /// The scheduler's earliest timer.
    pub(crate) next_deadline: Option<SimInstant>,
    /// Each source's offer, indexed by [`SourceId`]: its live one, or in the
    /// drain its recovery one ([`Offer::recovery`]).
    pub(crate) offers: &'a [Offer],
    /// What may move the drain's clock. Read only in the drain, at a step
    /// with no task ready and no recovery action.
    pub(crate) clock: DrainClock,
}

/// What may move the clock in the drain, besides a recovery boundary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DrainClock {
    /// Whether the checker still has an obligation open
    /// ([`Checker::unfinished`](crate::Checker::unfinished)), pending marks
    /// aside.
    pub(crate) unfinished: bool,
    /// The earliest open check's deadline, which it expires just after.
    pub(crate) earliest_deadline: Option<SimInstant>,
    /// The last instant a scheduler timer alone may move the clock to: the
    /// drain's entry plus the run's harness timer cap.
    pub(crate) timer_cap: Option<SimInstant>,
}

/// The actions enabled now, sorted by identity.
///
/// In the live phase that's every ready task and every source action. When no
/// task is ready, it also offers one `AdvanceTime` to the earliest boundary:
/// the scheduler's next timer, which may be due now after a clamp, or a source
/// boundary after now. A source deadline at or before now blocks time instead.
///
/// In the drain it's every ready task and every recovery action. With no
/// task ready and no recovery action, it offers one `AdvanceTime` if an
/// obligation is still open, a recovery boundary is ahead, or a scheduler
/// timer is due by the timer cap (see [`drain_time`]).
///
/// Returns the identity that two enabled actions share, if any.
pub(crate) fn enabled_set(i: &EnabledInputs<'_>) -> Result<Vec<Enabled>, Identity> {
    let mut enabled: Vec<Enabled> = i
        .ready
        .iter()
        .map(|(identity, task)| Enabled {
            identity: identity.clone(),
            action: Action::RunTask(*task),
        })
        .collect();
    for (index, offer) in i.offers.iter().enumerate() {
        enabled.extend(offer.actions.iter().map(|(identity, local)| Enabled {
            identity: identity.clone(),
            action: Action::Source {
                source: SourceId(index),
                local: *local,
            },
        }));
    }
    let to = match i.phase {
        Phase::Live if i.ready.is_empty() => next_time(i),
        Phase::Drain if enabled.is_empty() => drain_time(i),
        Phase::Live | Phase::Drain => None,
    };
    if let Some(to) = to {
        enabled.push(Enabled {
            identity: Identity::advance_time(to),
            action: Action::AdvanceTime(to),
        });
    }
    enabled.sort_by(|a, b| a.identity.cmp(&b.identity));
    match enabled.windows(2).find(|w| w[0].identity == w[1].identity) {
        Some(w) => Err(w[0].identity.clone()),
        None => Ok(enabled),
    }
}

/// The instant `AdvanceTime` goes to, or `None` when time can't move.
fn next_time(i: &EnabledInputs<'_>) -> Option<SimInstant> {
    let boundaries = i.offers.iter().flat_map(|o| o.boundaries.iter().copied());
    // A raw timer can have a deadline behind the clock. It's due now, and
    // `advance_to` can't go back to it.
    let mut earliest = i.next_deadline.map(|t| t.max(i.now));
    for boundary in boundaries {
        match boundary {
            Boundary::Deadline(t) if t <= i.now => return None,
            Boundary::Opens(t) | Boundary::Deadline(t) if t > i.now => {
                earliest = Some(earliest.map_or(t, |e| e.min(t)));
            }
            // An opening at or before now is already open.
            Boundary::Opens(_) | Boundary::Deadline(_) => {}
        }
    }
    earliest
}

/// The instant the drain's `AdvanceTime` goes to, or `None` when time
/// can't move. Whether it may move comes first: only for an obligation
/// still open, a recovery boundary ahead, or a scheduler timer due by the
/// timer cap, so a periodic timer with nothing open doesn't keep the drain alive
/// forever. A timer due now fires whatever the cap, since it doesn't move
/// the clock. Where it lands comes second: the earliest of the next timer,
/// the next recovery boundary and just past the earliest deadline, since a
/// check expires once the clock is past its deadline. So a later boundary
/// never skips an earlier timer.
fn drain_time(i: &EnabledInputs<'_>) -> Option<SimInstant> {
    let timer = i.next_deadline.map(|t| t.max(i.now));
    let boundary = i
        .offers
        .iter()
        .flat_map(|o| o.boundaries.iter())
        .filter_map(|boundary| match *boundary {
            Boundary::Opens(t) | Boundary::Deadline(t) if t > i.now => Some(t),
            Boundary::Opens(_) | Boundary::Deadline(_) => None,
        })
        .min();
    let timer_counts =
        timer.is_some_and(|t| t <= i.now || i.clock.timer_cap.is_some_and(|cap| t <= cap));
    if !(i.clock.unfinished || boundary.is_some() || timer_counts) {
        return None;
    }
    let expiry = i
        .clock
        .earliest_deadline
        .and_then(|deadline| deadline.checked_add(std::time::Duration::from_nanos(1)))
        .filter(|t| *t > i.now);
    [timer, boundary, expiry].into_iter().flatten().min()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use zakura_scheduler::{At, Op};

    use super::*;

    fn ms(n: u64) -> SimInstant {
        SimInstant::from_start(Duration::from_millis(n))
    }

    fn task(name: &str) -> Identity {
        Identity::task(
            name,
            At {
                op: Op::Poll,
                n: 1,
                object: None,
                site: None,
            },
        )
    }

    /// The identities `enabled_set` returns for these inputs.
    fn shown(
        phase: Phase,
        ready: &[(Identity, TaskId)],
        next_deadline: Option<SimInstant>,
        offers: &[Offer],
    ) -> Vec<String> {
        shown_with(phase, ready, next_deadline, offers, DrainClock::default())
    }

    /// [`shown`], with the drain's `clock`.
    fn shown_with(
        phase: Phase,
        ready: &[(Identity, TaskId)],
        next_deadline: Option<SimInstant>,
        offers: &[Offer],
        clock: DrainClock,
    ) -> Vec<String> {
        enabled_set(&EnabledInputs {
            phase,
            now: ms(5),
            ready,
            next_deadline,
            offers,
            clock,
        })
        .unwrap()
        .iter()
        .map(|e| e.identity.to_string())
        .collect()
    }

    /// A recovery offer of `actions` and `quiet`, opening at `boundaries`,
    /// in the drain's form for a step where no task is ready if `idle`.
    fn recovery(actions: &[&str], quiet: &[&str], boundaries: &[SimInstant], idle: bool) -> Offer {
        let acts = |peers: &[&str], verb: &str| -> Vec<(Identity, u64)> {
            peers
                .iter()
                .enumerate()
                // A test offers a handful of actions.
                .map(|(i, peer)| (Identity::peer(peer, verb), i as u64))
                .collect()
        };
        Offer::recovery(
            RecoveryOffer {
                actions: acts(actions, "act"),
                quiet: acts(quiet, "quiet"),
                boundaries: boundaries.to_vec(),
            },
            idle,
        )
    }

    fn offer(actions: &[&str], boundaries: &[Boundary]) -> Offer {
        Offer {
            actions: actions
                .iter()
                .enumerate()
                // A test offers a handful of actions.
                .map(|(i, peer)| (Identity::peer(peer, "act"), i as u64))
                .collect(),
            boundaries: boundaries.to_vec(),
        }
    }

    #[test]
    fn time_waits_for_ready_tasks() {
        let ready = [(task("a.1"), first_task())];
        let shown = shown(Phase::Live, &ready, Some(ms(9)), &[]);
        assert_eq!(shown, ["task:a.1:poll#1"]);
    }

    #[test]
    fn time_goes_to_the_earliest_boundary() {
        let offers = [offer(&[], &[Boundary::Opens(ms(7))])];
        assert_eq!(
            shown(Phase::Live, &[], Some(ms(10)), &offers),
            ["advance-time:7000000"]
        );
        // A clamped timer due now may advance to now.
        assert_eq!(
            shown(Phase::Live, &[], Some(ms(5)), &offers),
            ["advance-time:5000000"]
        );
        // Openings at or before now don't stop time.
        let open = [offer(&["p1"], &[Boundary::Opens(ms(5))])];
        assert_eq!(
            shown(Phase::Live, &[], Some(ms(10)), &open),
            ["peer:p1:act", "advance-time:10000000"]
        );
    }

    #[test]
    fn a_due_source_deadline_blocks_time() {
        let offers = [offer(&["p1"], &[Boundary::Deadline(ms(5))])];
        assert_eq!(
            shown(Phase::Live, &[], Some(ms(6)), &offers),
            ["peer:p1:act"]
        );
    }

    #[test]
    fn drain_offers_ready_tasks_and_recovery_actions() {
        let ready = [(task("a.1"), first_task())];
        let offers = [recovery(&["p1"], &[], &[ms(6)], false)];
        assert_eq!(
            shown(Phase::Drain, &ready, Some(ms(6)), &offers),
            ["task:a.1:poll#1", "peer:p1:act"]
        );
        // A recovery action keeps the clock still, as a ready task does.
        let offers = [recovery(&["p1"], &[], &[ms(6)], true)];
        assert_eq!(
            shown(Phase::Drain, &[], Some(ms(6)), &offers),
            ["peer:p1:act"]
        );
        // Nothing to finish, nothing open and no timer by the cap: dry.
        assert!(shown(Phase::Drain, &[], Some(ms(6)), &[]).is_empty());
    }

    #[test]
    fn drain_adds_quiet_actions_only_with_no_task_ready() {
        let ready = [(task("a.1"), first_task())];
        let busy = [recovery(&[], &["p1"], &[], false)];
        assert_eq!(
            shown(Phase::Drain, &ready, None, &busy),
            ["task:a.1:poll#1"]
        );
        let quiet = [recovery(&[], &["p1"], &[], true)];
        assert_eq!(shown(Phase::Drain, &[], None, &quiet), ["peer:p1:quiet"]);
    }

    #[test]
    fn drain_time_lands_on_the_earliest_timer_first() {
        let open = DrainClock {
            unfinished: true,
            earliest_deadline: Some(ms(20)),
            timer_cap: Some(ms(5)),
        };
        // A boundary at 10 ms never skips the timer at 7 ms.
        let offers = [recovery(&[], &[], &[ms(10)], true)];
        assert_eq!(
            shown_with(Phase::Drain, &[], Some(ms(7)), &offers, open),
            ["advance-time:7000000"]
        );
        // With no earlier timer, the boundary.
        assert_eq!(
            shown_with(Phase::Drain, &[], Some(ms(30)), &offers, open),
            ["advance-time:10000000"]
        );
    }

    #[test]
    fn drain_time_lands_just_past_a_deadline() {
        let open = DrainClock {
            unfinished: true,
            earliest_deadline: Some(ms(8)),
            timer_cap: None,
        };
        // Expiry is `now > deadline`, so 1 ns past it.
        assert_eq!(
            shown_with(Phase::Drain, &[], Some(ms(30)), &[], open),
            ["advance-time:8000001"]
        );
        // An open check with no deadline still lets a timer past the cap
        // move the clock: a deadline must be reached.
        let ops = DrainClock {
            earliest_deadline: None,
            ..open
        };
        assert_eq!(
            shown_with(Phase::Drain, &[], Some(ms(30)), &[], ops),
            ["advance-time:30000000"]
        );
        // With nothing to land on, the drain is dry.
        assert!(shown_with(Phase::Drain, &[], None, &[], ops).is_empty());
    }

    #[test]
    fn drain_timers_move_the_clock_only_within_the_cap() {
        let capped = DrainClock {
            unfinished: false,
            earliest_deadline: None,
            timer_cap: Some(ms(10)),
        };
        assert_eq!(
            shown_with(Phase::Drain, &[], Some(ms(10)), &[], capped),
            ["advance-time:10000000"]
        );
        // A periodic timer past the cap, with nothing open and no boundary,
        // doesn't keep the drain alive.
        assert!(shown_with(Phase::Drain, &[], Some(ms(11)), &[], capped).is_empty());
        // A timer already due fires whatever the cap, since the clock stays.
        let past = DrainClock {
            timer_cap: Some(ms(1)),
            ..capped
        };
        assert_eq!(
            shown_with(Phase::Drain, &[], Some(ms(5)), &[], past),
            ["advance-time:5000000"]
        );
        // A recovery boundary moves it whatever the cap.
        let offers = [recovery(&[], &[], &[ms(40)], true)];
        assert_eq!(
            shown_with(Phase::Drain, &[], Some(ms(50)), &offers, past),
            ["advance-time:40000000"]
        );
    }

    #[test]
    fn a_shared_identity_is_reported() {
        let offers = [offer(&["p1"], &[]), offer(&["p1"], &[])];
        let duplicate = enabled_set(&EnabledInputs {
            phase: Phase::Live,
            now: ms(0),
            ready: &[],
            next_deadline: None,
            offers: &offers,
            clock: DrainClock::default(),
        });
        assert_eq!(
            duplicate.map(|_| ()).unwrap_err().to_string(),
            "peer:p1:act"
        );
    }

    /// A real `TaskId`, which only a scheduler hands out.
    fn first_task() -> TaskId {
        let mut sched = zakura_scheduler::Scheduler::new(Default::default(), SimInstant::ZERO);
        let id = sched.spawn_root("t", async {}).task();
        sched.shutdown();
        id
    }
}
