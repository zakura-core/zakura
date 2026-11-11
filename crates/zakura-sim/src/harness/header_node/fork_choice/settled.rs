//! `header-sync/expected-chain-selected`'s settled-convergence rule, in the
//! fork fixture only (Rule 3).
//!
//! The fork fixture is declared from the scenario: the node at genesis, two
//! or three mirrors whose initial chains are forks from one ancestor and
//! hold few enough headers that the node's body window never runs out,
//! storage success-only within a 10 ms bound, far below the 30 s request
//! timeout, no holds and no controls (`header_sync::fork_fixture`). The
//! harness checks those conditions when it builds the run, and hands the
//! check the mirrors' chains only if they hold. With no growth control, a
//! mirror can later take only a fork some mirror held at the start, so the
//! heaviest initial chain stays the heaviest there is.
//!
//! A header that admission defers is never promoted here, so a correct node
//! can't select it. So the rule applies only if the run's clock at its
//! first step's drain, before any header can be admitted, admits every mirror
//! header valid. Clocks only move forward, so each stays admissible
//! whenever the node fetches it. Admission is the model's own rule, from
//! the fixture headers' times.
//!
//! A settled point is a step after which no task can run, once every mirror
//! has a session admitted, with no work slot, no storage call the reactor
//! counts as pending or any port future it polls, no operation pending or
//! held at the port, and no cached-offer pass deferred to a refill that
//! hasn't come yet. The slots come from the queue's `Slots`, the calls and
//! futures from the reactor's `Sizes` at each unit of work's end, and the
//! deferral from `CachedPassDeferred`. Links are in-memory, so by then every
//! mirror's `Status` has reached the reactor. Whether no task can run is the
//! scheduler's, which the property reads after the step.
//!
//! At each settled point where the harness hasn't closed or relinked any
//! mirror (Rule 13: a mirror the node dropped still counts), the engine's
//! selected tip, from its newest record, must be one of the max-work tips
//! over all mirrors' chains, by the fixture's work. A node that settles
//! short of it is idle with a heavier offer it never fetched. It's judged
//! at the point, with no deadline, so no fairness or bounded-delivery
//! assumption is needed.

use std::collections::HashSet;

use zakura_chain::{block, work::difficulty::U256};
use zakura_network::zakura::{observe::ReactorEvent, ZakuraPeerId};
use zakura_scheduler::SimInstant;
use zakura_storage::header::fixture::HeaderFixture;

use crate::{harness::header_node::Chain, storage::hash};

/// The fork fixture's mirrors, whose chains settled convergence judges
/// against.
#[derive(Clone, Debug)]
pub struct ForkFixture {
    /// Each mirror's fixed chain, in scenario order.
    pub mirrors: Vec<Chain>,
}

/// What the tap reads at a drain for a settled point: the port's operations
/// and the harness's environment actions.
#[derive(Clone, Copy, Debug)]
pub(in crate::harness::header_node) struct Outside {
    /// Whether any operation is pending, or committed with its reply held,
    /// at the node's port.
    pub(in crate::harness::header_node) port_busy: bool,
    /// Whether the harness closed or relinked any mirror's link.
    pub(in crate::harness::header_node) environment_acted: bool,
    /// Virtual time now.
    pub(in crate::harness::header_node) now: SimInstant,
    /// The run's scenario clock now.
    pub(in crate::harness::header_node) clock: chrono::DateTime<chrono::Utc>,
}

/// The rule's state.
pub(super) struct Settled {
    /// How many mirrors the fixture has.
    mirrors: usize,
    /// The mirrors' headers' times, which admission's time rule reads.
    times: Vec<chrono::DateTime<chrono::Utc>>,
    /// Whether the clock at the first drain admits every mirror header
    /// valid, once a drain has judged it.
    admissible: Option<bool>,
    /// The max-work tips over the mirrors' chains.
    heaviest: HashSet<block::Hash>,
    /// The peers the reactor has admitted a session from.
    admitted: HashSet<ZakuraPeerId>,
    /// Whether the queue's newest `Slots` listed none.
    no_slots: bool,
    /// Whether the reactor's newest `Sizes` counted no pending storage call,
    /// locator read or port future. False until the first.
    no_calls: bool,
    /// When the newest deferred cached-offer pass's refill comes.
    deferred_until: Option<SimInstant>,
}

impl Settled {
    /// The rule for `fixture`'s mirrors, among `fixture`'s headers.
    pub(super) fn new(headers: &HeaderFixture, fixture: &ForkFixture) -> Settled {
        let tips: Vec<(block::Hash, U256)> = fixture
            .mirrors
            .iter()
            .map(|chain| (chain.tip(headers).hash, chain_work(headers, chain)))
            .collect();
        let best = tips.iter().map(|(_, work)| *work).max();
        Settled {
            mirrors: fixture.mirrors.len(),
            times: fixture
                .mirrors
                .iter()
                .flat_map(|chain| chain.segments(headers))
                .flat_map(|(_, segment)| segment.into_iter().map(|header| header.time))
                .collect(),
            admissible: None,
            heaviest: tips
                .into_iter()
                .filter(|(_, work)| Some(*work) == best)
                .map(|(tip, _)| tip)
                .collect(),
            admitted: HashSet::new(),
            no_slots: true,
            no_calls: false,
            deferred_until: None,
        }
    }

    /// Notes `event`, which the reactor's own task reported at `now`.
    pub(super) fn observe(&mut self, event: &ReactorEvent, now: SimInstant) {
        match event {
            ReactorEvent::Slots { slots } => self.no_slots = slots.is_empty(),
            ReactorEvent::Sizes { sizes } => {
                self.no_calls = sizes.storage_calls.is_empty()
                    && sizes.locator_reads.is_empty()
                    && sizes.port_futures == 0;
            }
            ReactorEvent::CachedPassDeferred { refill_in } => {
                self.deferred_until = now.checked_add(*refill_in);
            }
            ReactorEvent::SessionAdmitted { peer, .. } => {
                self.admitted.insert(peer.clone());
            }
            _ => {}
        }
    }

    /// Whether a step that leaves the run with no task able to run is a
    /// settled point, given `outside`, and if so whether `selected`, the
    /// engine's newest selected tip, breaks the rule: `Some(None)` if it
    /// holds, `Some(Some(detail))` if it breaks, `None` if it isn't one.
    /// The first call fixes whether the run's clock admits every mirror
    /// header; the check makes it at the first drain past setup.
    pub(super) fn judge(
        &mut self,
        selected: block::Hash,
        outside: Outside,
    ) -> Option<Option<String>> {
        let times = &self.times;
        let admissible = *self.admissible.get_or_insert_with(|| {
            times
                .iter()
                .all(|time| !super::deferred_at(*time, outside.clock))
        });
        let settled = admissible
            && self.admitted.len() >= self.mirrors
            && self.no_slots
            && self.no_calls
            && !outside.port_busy
            && !outside.environment_acted
            && self.deferred_until.is_none_or(|until| until <= outside.now);
        if !settled {
            return None;
        }
        if self.heaviest.contains(&selected) {
            return Some(None);
        }
        let mut heaviest: Vec<String> = self.heaviest.iter().map(|tip| hash(*tip)).collect();
        heaviest.sort_unstable();
        Some(Some(format!(
            "ExpectedChainSelected: the node settled with the engine on {}, not one of the \
             heaviest mirror tips {heaviest:?}",
            hash(selected)
        )))
    }
}

/// `chain`'s work above genesis, from `headers`' branch targets.
fn chain_work(headers: &HeaderFixture, chain: &Chain) -> U256 {
    let per = |branch: u8| {
        U256::from(
            headers
                .target(branch)
                .to_work()
                .expect("every fixture target is a valid difficulty"),
        )
    };
    let fork = chain
        .fork
        .map_or(U256::zero(), |fork| per(fork.branch) * U256::from(fork.len));
    per(0) * U256::from(chain.main) + fork
}

#[cfg(test)]
mod tests;
