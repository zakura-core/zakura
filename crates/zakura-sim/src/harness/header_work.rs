//! Transport-free header machines. Each is a [`Machine`] constant with its history
//! and its witness rule, run by [`super::machine`].

use super::{machine::Machine, HarnessEntry};
use crate::record::machine::MachineStep;

/// Header reactor policy: the production owner's invariants and the facts that
/// follow from the inputs.
pub const REACTOR: Machine = Machine {
    name: "header/reactor-policy",
    identity: "header-reactor",
    claim: "The header reactor keeps its invariants after every input: each started \
            locator call settles once, by its exact identity; owed closes wait no longer \
            than their interval; cached passes stay within their budget; and a stalled \
            repair reports once per generation.",
    generator: 1,
    budget: 128,
    history: reactor_history,
    reached: |steps, _| steps.iter().any(reactor_reached),
};

/// [`REACTOR`]'s harness entry.
pub const REACTOR_ENTRY: HarnessEntry = crate::machine_entry!(REACTOR);

/// Header publisher policy: the production owner's invariants and the facts that
/// follow from the inputs.
pub const PUBLISHER: Machine = Machine {
    name: "header/publisher-policy",
    identity: "header-publisher",
    claim: "The header publisher keeps its invariants after every input: it spends only \
            granted credit, extends the last header sent, ends every closing subscription, \
            writes terminal outcomes one at a time in order, and holds one slot per live \
            subscription and unwritten outcome.",
    // Version 2 reads a `Finish` with an odd third byte as a storage reply.
    generator: 2,
    budget: 128,
    history: publisher_history,
    reached: |steps, _| steps.iter().any(publisher_reached),
};

/// [`PUBLISHER`]'s harness entry.
pub const PUBLISHER_ENTRY: HarnessEntry = crate::machine_entry!(PUBLISHER);

fn reactor_history(input: &[u8]) -> Vec<(usize, MachineStep)> {
    zakura_network::zakura::trace_header_reactor_bytes(input)
        .steps
        .into_iter()
        .map(|step| (step.step, MachineStep::HeaderReactor(step.into())))
        .collect()
}

fn publisher_history(input: &[u8]) -> Vec<(usize, MachineStep)> {
    zakura_network::zakura::trace_header_publisher_bytes(input)
        .steps
        .into_iter()
        .map(|step| (step.step, MachineStep::HeaderPublisher(step.into())))
        .collect()
}

/// Whether a storage call is pending after `step`.
fn reactor_reached(step: &MachineStep) -> bool {
    matches!(step, MachineStep::HeaderReactor(work) if !work.actual.calls.is_empty())
}

/// Whether the publisher holds a subscription slot after `step`.
fn publisher_reached(step: &MachineStep) -> bool {
    matches!(step, MachineStep::HeaderPublisher(work) if work.actual.slots > 0)
}
