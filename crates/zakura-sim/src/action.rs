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

/// Actions outside the scheduler, such as datagrams or storage completions.
///
/// `enabled` and `boundaries` must be pure functions of the source's state, so
/// a run replays. Only `execute` changes it.
pub trait ActionSource {
    /// Appends each action enabled at `now`, with the index `execute` takes.
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>);

    /// Appends the instants where time must stop for this source.
    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>);

    /// Runs the action `enabled` listed with index `local`, appending what
    /// happened to `log`.
    fn execute(&mut self, local: u64, log: &mut ObservationLog);
}

/// What one source offers at one step.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Offer {
    pub(crate) actions: Vec<(Identity, u64)>,
    pub(crate) boundaries: Vec<Boundary>,
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
    /// Each source's offer, indexed by [`SourceId`]. Empty in the drain.
    pub(crate) offers: &'a [Offer],
}

/// The actions enabled now, sorted by identity.
///
/// In the live phase that's every ready task and every source action. When no
/// task is ready, it also offers one `AdvanceTime` to the earliest boundary:
/// the scheduler's next timer, which may be due now after a clamp, or a source
/// boundary after now. A source deadline at or before now blocks time instead.
/// In the drain only ready tasks run.
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
    if i.phase == Phase::Live {
        for (index, offer) in i.offers.iter().enumerate() {
            enabled.extend(offer.actions.iter().map(|(identity, local)| Enabled {
                identity: identity.clone(),
                action: Action::Source {
                    source: SourceId(index),
                    local: *local,
                },
            }));
        }
        if i.ready.is_empty() {
            if let Some(to) = next_time(i) {
                enabled.push(Enabled {
                    identity: Identity::advance_time(to),
                    action: Action::AdvanceTime(to),
                });
            }
        }
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
    let mut earliest = i.next_deadline;
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
        enabled_set(&EnabledInputs {
            phase,
            now: ms(5),
            ready,
            next_deadline,
            offers,
        })
        .unwrap()
        .iter()
        .map(|e| e.identity.to_string())
        .collect()
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
    fn drain_offers_only_ready_tasks() {
        let ready = [(task("a.1"), first_task())];
        let offers = [offer(&["p1"], &[Boundary::Opens(ms(6))])];
        assert_eq!(
            shown(Phase::Drain, &ready, None, &offers),
            ["task:a.1:poll#1"]
        );
        assert!(shown(Phase::Drain, &[], Some(ms(6)), &offers).is_empty());
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
