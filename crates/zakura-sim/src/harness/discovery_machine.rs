//! Discovery's record book and session owner as pure machines.
//!
//! Each history runs the real owner against its independent model. A fixed
//! disclosure key, a seeded shuffle and a fixture clock make signed samples replay.

use zakura_network::zakura::{BookFact, InstallFact};

use super::{machine::Machine, HarnessEntry};
use crate::{record::machine::MachineStep, witness::Expect};

/// One operation per four of the 512 causal bytes.
const BUDGET: u64 = 128;

/// The model's facts show that the input reached each resource-bearing rule,
/// including when a mutation stops the real owner from applying it.
///
/// A passing run reaches every rule. A violation ends its history at the first
/// failure, so a failing run needs only its last step to apply a rule. A
/// replacement bug, for example, stops the history before any eviction.
#[derive(Default)]
struct Reached {
    last_step_applied_rule: bool,
    installed: bool,
    replaced: bool,
    evicted: bool,
    sampled: bool,
    stale_result: bool,
    current_result: bool,
    retained_hint: bool,
}

impl Reached {
    fn of(steps: &[MachineStep]) -> Self {
        let mut reached = Self::default();
        for step in steps {
            reached.note(step);
        }
        reached
    }

    fn note(&mut self, step: &MachineStep) {
        match step {
            MachineStep::DiscoveryBook(book) => {
                for change in &book.expected.changes {
                    match change {
                        BookFact::Installed { .. } => self.installed = true,
                        BookFact::Replaced { .. } => self.replaced = true,
                        BookFact::Evicted { .. } => self.evicted = true,
                    }
                }
                self.sampled |= !book.sample.is_empty();
                self.last_step_applied_rule =
                    !book.expected.changes.is_empty() || !book.sample.is_empty();
            }
            MachineStep::DiscoveryState(state) => {
                let installation = state.expected.installation.as_ref();
                let stale_result = matches!(installation, Some(InstallFact::Superseded));
                let current_result = matches!(installation, Some(InstallFact::Installed(_)));
                let retained_hint = !state.expected.hints.is_empty();
                self.stale_result |= stale_result;
                self.current_result |= current_result;
                self.retained_hint |= retained_hint;
                self.last_step_applied_rule = stale_result || current_result || retained_hint;
            }
            _ => {}
        }
    }
}

const BOOK: Machine = Machine {
    name: "discovery/record-book",
    identity: "discovery-book",
    claim: "After each operation, the book's entries, relay attribution and exact install, replacement and eviction facts agree with the independent model, and each signed sample stays inside its requester's disclosure window.",
    generator: 2,
    budget: BUDGET,
    history: |input| {
        zakura_network::zakura::trace_discovery_book_bytes(input)
            .into_iter()
            .map(|step| (step.step, MachineStep::DiscoveryBook(step)))
            .collect()
    },
    reached: |steps, expect| {
        let reached = Reached::of(steps);
        match expect {
            Expect::Holds => {
                reached.installed && reached.replaced && reached.evicted && reached.sampled
            }
            _ => reached.last_step_applied_rule,
        }
    },
};

const STATE: Machine = Machine {
    name: "discovery/session-installation",
    identity: "discovery-state",
    claim: "After each operation, admitted sessions, retained service hints and each verification result's installation agree with the independent model.",
    generator: 2,
    budget: BUDGET,
    history: |input| {
        zakura_network::zakura::trace_discovery_state_bytes(input)
            .into_iter()
            .map(|step| (step.step, MachineStep::DiscoveryState(step)))
            .collect()
    },
    reached: |steps, expect| {
        let reached = Reached::of(steps);
        match expect {
            Expect::Holds => {
                reached.stale_result && reached.current_result && reached.retained_hint
            }
            _ => reached.last_step_applied_rule,
        }
    },
};

/// The record book's history under the shared registry, recorder, replay verifier and explorer.
pub const BOOK_ENTRY: HarnessEntry = crate::machine_entry!(BOOK);

/// The session owner's history under the same pipeline. This target establishes
/// completed installation rules. `discovery/verification-capacity` runs the real
/// handle's asynchronous verification.
pub const STATE_ENTRY: HarnessEntry = crate::machine_entry!(STATE);
