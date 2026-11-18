//! A PCT-style steer for the generator's searches (Burckhardt et al.,
//! ASPLOS 2010): each task, delivery or source action takes a random
//! priority the first time it's enabled, every live pick runs the enabled
//! action with the highest priority, and at [`CHANGES`] random steps the
//! one about to run drops below every action seen so far.
//!
//! A uniform Plan stops choosing once its bytes run out, and round-robin
//! picks the rest of a long run. PCT spends a few random choices on the
//! whole run instead: an ordering that needs one action to wait behind
//! another for a long stretch, as a late completion after a reset does,
//! takes one priority order and one change point. The input's bytes seed
//! the priorities and change points, so an input fixes its schedule, and a
//! steered run records the Plan bytes that replay it without the steer
//! ([`crate::Sim::steer`]).

use std::collections::BTreeMap;

use crate::{Category, Identity, ObservationLog, Steer};

/// The priority changes a run makes: PCT's depth 3, for an ordering bug
/// that needs three ordering constraints.
pub const CHANGES: usize = 2;

/// The scales a change point's step falls within: 4, 16, 64 and so on to
/// 65,536 live steps, each equally likely. PCT draws its change points
/// within the run's length, which a search doesn't know, and a race
/// fixture's run takes tens of steps where a wire run takes tens of
/// thousands.
const SCALES: u64 = 8;

/// The steer: priorities by action, the steps that change one, and the
/// priority the next change gives.
#[derive(Debug)]
pub struct Pct {
    state: u64,
    priorities: BTreeMap<(Category, String), u64>,
    changes: Vec<u64>,
    step: u64,
    demoted: u64,
}

impl Pct {
    /// The steer whose priorities and change points `seed` draws.
    pub fn new(seed: u64) -> Self {
        let mut pct = Pct {
            state: seed,
            priorities: BTreeMap::new(),
            changes: Vec::new(),
            step: 0,
            demoted: u64::from(u32::MAX),
        };
        let mut changes: Vec<u64> = (0..CHANGES)
            .map(|_| {
                let scale = 4 << (2 * (pct.next() % SCALES));
                1 + pct.next() % scale
            })
            .collect();
        changes.sort_unstable();
        pct.changes = changes;
        pct
    }

    /// [`Pct::new`] seeded by every byte of `input`.
    pub fn for_input(input: &[u8]) -> Box<dyn Steer> {
        let seed = input
            .iter()
            .fold(0x5043_5400_u64, |seed, byte| mix(seed ^ u64::from(*byte)));
        Box::new(Pct::new(seed))
    }

    /// The next value of the steer's SplitMix64 stream.
    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        mix(self.state)
    }

    /// `id`'s priority, drawn above every demoted one the first time it's
    /// seen. A task keeps one priority wherever it resumes.
    fn priority(&mut self, id: &Identity) -> u64 {
        let key = (id.category, id.key.clone());
        if let Some(priority) = self.priorities.get(&key) {
            return *priority;
        }
        let priority = (1 << 32) | (self.next() >> 32);
        self.priorities.insert(key, priority);
        priority
    }
}

impl Steer for Pct {
    /// The enabled action with the highest priority, after dropping it
    /// below every other if this step is a change point.
    fn pick(&mut self, options: &[Identity], _: &ObservationLog) -> Option<usize> {
        self.step += 1;
        let priorities: Vec<u64> = options.iter().map(|id| self.priority(id)).collect();
        let best = (0..options.len()).max_by_key(|index| priorities[*index])?;
        if self.changes.first() != Some(&self.step) {
            return Some(best);
        }
        self.changes.remove(0);
        self.demoted -= 1;
        let id = &options[best];
        self.priorities
            .insert((id.category, id.key.clone()), self.demoted);
        (0..options.len())
            .filter(|index| *index != best)
            .max_by_key(|index| priorities[*index])
            .or(Some(best))
    }
}

/// SplitMix64's finalizer.
fn mix(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}
