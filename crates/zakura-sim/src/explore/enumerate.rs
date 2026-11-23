//! Delay-bounded schedule enumeration (Emmi, Qadeer and Rakamarić, POPL
//! 2011) for the generator's searches: every live pick takes the first
//! enabled action, except at up to [`DEPTH`] steps, where it takes another.
//!
//! A run under PCT ([`super::pct`]) samples one ordering. An ordering bug
//! that needs one action to run before another, at two points, is a
//! schedule with two delays, so enumerating every schedule with at most
//! two delays finds it on a scenario whose runs are short. A race
//! fixture's run makes tens of picks; a wire run makes tens of thousands,
//! whose depth-2 enumeration no search finishes.
//!
//! [`Enumeration`] visits the schedules breadth first: the schedule with
//! no delay, then every one-delay schedule, then every two-delay one. A
//! run's option counts at each step give its children, since a schedule
//! with one more delay later replays the run up to that step.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use crate::{Identity, ObservationLog, Steer};

/// The most delays a schedule takes.
pub const DEPTH: usize = 2;

/// One delay: at the 1-based live step, the pick takes this option index
/// instead of the first.
pub type Delay = (u64, usize);

/// The steer of one schedule: the first enabled action at every step but
/// its delays. It records each step's option count in `seen`.
#[derive(Debug)]
pub struct Delays {
    delays: VecDeque<Delay>,
    step: u64,
    seen: Arc<Mutex<Vec<u32>>>,
}

impl Delays {
    /// The steer of `delays`, sorted by step, recording into `seen`.
    pub fn new(delays: &[Delay], seen: Arc<Mutex<Vec<u32>>>) -> Self {
        Delays {
            delays: delays.iter().copied().collect(),
            step: 0,
            seen,
        }
    }
}

impl Steer for Delays {
    fn pick(&mut self, options: &[Identity], _: &ObservationLog) -> Option<usize> {
        self.step += 1;
        let count = u32::try_from(options.len()).unwrap_or(u32::MAX);
        self.seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(count);
        if options.is_empty() {
            return None;
        }
        match self.delays.front() {
            Some(&(step, option)) if step == self.step => {
                self.delays.pop_front();
                Some(option.min(options.len() - 1))
            }
            _ => Some(0),
        }
    }
}

/// The schedules left to run on one input, breadth first.
#[derive(Debug)]
pub struct Enumeration {
    depth: usize,
    queue: VecDeque<Vec<Delay>>,
}

impl Enumeration {
    /// The enumeration of schedules with at most `depth` delays, which
    /// starts with the schedule that takes none.
    pub fn new(depth: usize) -> Self {
        Enumeration {
            depth,
            queue: VecDeque::from([Vec::new()]),
        }
    }

    /// The next schedule to run, or `None` once every one has run.
    pub fn next_schedule(&mut self) -> Option<Vec<Delay>> {
        self.queue.pop_front()
    }

    /// Queues the children of `schedule`, whose run saw `counts[i]` options
    /// at step `i + 1`: each schedule with one more delay, at a later step
    /// than its last, on another option.
    pub fn expand(&mut self, schedule: &[Delay], counts: &[u32]) {
        if schedule.len() >= self.depth {
            return;
        }
        let after = schedule.last().map_or(0, |(step, _)| *step);
        for (step, count) in (1u64..)
            .zip(counts)
            .skip(usize::try_from(after).unwrap_or(usize::MAX))
        {
            for option in 1..*count as usize {
                let mut child = schedule.to_vec();
                child.push((step, option));
                self.queue.push_back(child);
            }
        }
    }

    /// True once every schedule has run.
    pub fn is_done(&self) -> bool {
        self.queue.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::{Enumeration, DEPTH};

    /// On runs of three steps with two options each, the enumeration visits
    /// one schedule with no delay, three with one, and three with two, each
    /// once, breadth first.
    #[test]
    fn the_enumeration_visits_each_schedule_once() {
        let mut enumeration = Enumeration::new(DEPTH);
        let mut seen = Vec::new();
        while let Some(schedule) = enumeration.next_schedule() {
            enumeration.expand(&schedule, &[2, 2, 2]);
            seen.push(schedule);
        }
        let lengths: Vec<usize> = seen.iter().map(Vec::len).collect();
        assert_eq!(lengths, [0, 1, 1, 1, 2, 2, 2]);
        assert_eq!(
            seen[4..],
            [
                vec![(1, 1), (2, 1)],
                vec![(1, 1), (3, 1)],
                vec![(2, 1), (3, 1)]
            ]
        );
        assert!(enumeration.is_done());
    }
}
