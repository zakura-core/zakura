//! Transport-free execution of block policy and runtime funding models.

use super::{machine::Machine, HarnessEntry};
use crate::{
    record::machine::{MachineStep, ReactorAction},
    witness::Expect,
};

const OWNERSHIP: Machine = Machine {
    name: "block/work-ownership",
    identity: "block-work",
    claim: "The block work owner agrees with the independent ownership model after every input.",
    generator: 1,
    budget: 128,
    history: |input| {
        zakura_network::zakura::trace_work_ownership_bytes(input)
            .steps
            .into_iter()
            .map(|step| (step.step, MachineStep::Work(step.into())))
            .collect()
    },
    reached: |steps, _| {
        // The model's nonzero charge proves the input reached resource-bearing
        // work, including when a mutation prevents the actual owner from charging.
        steps.iter().any(|step| {
            matches!(step, MachineStep::Work(work) if work
                .expected
                .items
                .iter()
                .any(|item| item.in_flight && item.owner.is_some() && item.charge > 0))
        })
    },
};

const REACTOR: Machine = Machine {
    name: "block/reactor-policy",
    identity: "block-reactor",
    claim: "After every input, the reactor's pending query, next query ID, retry gate, owed force, closed dispatch, and progress mirrors agree with the independent model, and every accepted or refused query carries the model's exact identity.",
    generator: 1,
    budget: 128,
    history: |input| {
        zakura_network::zakura::trace_block_reactor_bytes(input)
            .steps
            .into_iter()
            .map(|step| (step.step, MachineStep::Reactor(step.into())))
            .collect()
    },
    // The reactor's trigger is an offered query, not a byte charge.
    reached: |steps, _| {
        steps.iter().any(|step| {
            matches!(step, MachineStep::Reactor(reactor) if reactor.actions.iter().any(|action| {
                matches!(
                    action,
                    ReactorAction::QueryAccepted { .. } | ReactorAction::QueryRefused { .. }
                )
            }))
        })
    },
};

const FUNDING: Machine = Machine {
    name: "block/work-funding",
    identity: "block-work-funding",
    claim: "At each completed driver operation and before queue destruction, actual queue funding and retained request/receipt reservations agree with the independent model.",
    generator: 1,
    budget: 129,
    history: |input| {
        zakura_network::zakura::trace_work_funding_bytes(input)
            .steps
            .into_iter()
            .map(|step| (step.step, MachineStep::WorkFunding(step.into())))
            .collect()
    },
    reached: |steps, expect| {
        let funding = || {
            steps.iter().filter_map(|step| match step {
                MachineStep::WorkFunding(work) => Some(work),
                _ => None,
            })
        };
        funding().any(|work| work.expected.reserved > 0)
            && (expect != Expect::Holds
                || funding().any(|work| {
                    work.drain && work.actual.reserved == 0 && work.expected.reserved == 0
                }))
    },
};

/// Pure policy uses the shared registry, recorder, replay verifier and explorer.
pub const ENTRY: HarnessEntry = crate::machine_entry!(OWNERSHIP);

/// Reactor query and view policy uses reactor decoder-v2 and the same evidence
/// loop. It decides pure transitions: the real reactor's action channel, reply
/// routes, and shutdown have their witnesses in zakura-network.
pub const REACTOR_ENTRY: HarnessEntry = crate::machine_entry!(REACTOR);

/// Real queue owners use funding decoder-v1 and the same execution/evidence loop.
/// This target establishes completed-operation retention, not asynchronous races
/// or actual decoded-body/backend-worker lifetime.
pub const FUNDING_ENTRY: HarnessEntry = crate::machine_entry!(FUNDING);
