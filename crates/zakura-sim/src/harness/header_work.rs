//! Transport-free header machines. Each is a [`Machine`] constant with its history
//! and its witness rule, run by [`super::machine`].

use super::{machine::Machine, HarnessEntry};
use crate::record::machine::MachineStep;

/// Header reactor policy, against its independent model.
pub const REACTOR: Machine = Machine {
    name: "header/reactor-policy",
    identity: "header-reactor",
    claim: "The header reactor's scheduling and locator state agree with the \
            independent model after every input, with exact storage call identities.",
    generator: 1,
    budget: 128,
    history: reactor_history,
    reached: |steps, _| steps.iter().any(reactor_reached),
};

/// [`REACTOR`]'s harness entry.
pub const REACTOR_ENTRY: HarnessEntry = crate::machine_entry!(REACTOR);

/// Header publisher policy, against its independent model.
pub const PUBLISHER: Machine = Machine {
    name: "header/publisher-policy",
    identity: "header-publisher",
    claim: "The header publisher's credit, jobs, terminal outcomes, and slots agree with \
            the independent model after every input.",
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

/// Whether `step`'s model saw a resource-bearing transition. The model's view
/// proves it even when a mutation keeps the actual owner from reaching it.
fn reactor_reached(step: &MachineStep) -> bool {
    matches!(step, MachineStep::HeaderReactor(work) if !work.expected.calls.is_empty())
}

/// Whether `step`'s model holds a subscription slot.
fn publisher_reached(step: &MachineStep) -> bool {
    matches!(step, MachineStep::HeaderPublisher(work) if work.expected.slots > 0)
}
