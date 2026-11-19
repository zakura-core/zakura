//! The `discovery` seeds: the record-book and session-installation op
//! tapes `zakura-network` encodes, the `discovery/wire` saved Plans, the
//! Plans that hold a discovery source back until a race shows, and the
//! `discovery/composed` and `discovery/spam` prefixes.
//!
//! [`wait_complete_input`] and [`late_response_plan`] build their Plans by
//! rerunning the simulator, once per step their Plan holds the source back.
//! Each run stops at the first step it needs a new choice for, and each
//! process builds each Plan once.

use std::sync::OnceLock;

use zakura_network::zakura::{DISCOVERY_INITIAL_EXCHANGE_TIMEOUT, ZAKURA_STREAM_DISCOVERY};

use super::Seed;
use crate::{
    check::{after_step, Violation},
    domain::discovery::{is_source, late_answer, WaitState},
    explore::generate::draws,
    harness::{
        discovery::{run_config, spawn},
        discovery_service::{
            composed_prefix, spam_prefix, DiscoveryComposed, DiscoverySpam, DiscoveryWire,
            SavedPlan,
        },
        run_prepared, HarnessError, Subsystem,
    },
    peer::{
        discovery::{DiscoveryAct, Request},
        PeerAction,
    },
    plan::width,
    record::{DrawRecord, Record},
    witness::Expect,
    MemorySink, Observation, Plan, Sim,
};

/// Every `discovery` seed.
pub(super) fn seeds() -> Vec<Seed> {
    let replay_exact = "crates/zakura-sim/tests/replay_exact.rs";
    let mut seeds = vec![
        Seed::new(
            "discovery-record-book",
            "discovery/record-book",
            replay_exact,
            zakura_network::zakura::discovery_book_witness_bytes(),
        )
        .witness("discovery/record-book", Expect::Holds),
        Seed::new(
            "discovery-session-installation",
            "discovery/session-installation",
            replay_exact,
            zakura_network::zakura::DISCOVERY_STATE_WITNESS.to_vec(),
        )
        .witness("discovery/session-installation", Expect::Holds),
        // One Plan byte: the default Plan, past the empty one.
        Seed::new(
            "discovery-verification-capacity",
            "discovery/verification-capacity",
            "crates/zakura-sim/tests/discovery.rs",
            vec![0],
        )
        .witness("discovery/verification-capacity", Expect::Holds),
        Seed::built(
            "discovery-wait-complete",
            "discovery/wait-complete",
            "crates/zakura-sim/tests/witness.rs",
            || {
                static INPUT: OnceLock<Vec<u8>> = OnceLock::new();
                INPUT.get_or_init(wait_complete_input)
            },
        )
        .witness("discovery/wait-complete-wakes", Expect::Holds),
    ];
    for plan in SavedPlan::ALL {
        let name = format!("discovery-wire-{}", plan.name());
        let source = "crates/zakura-sim/tests/discovery_wire_lifecycle.rs";
        seeds.push(match plan {
            SavedPlan::LateResponse => Seed::built(name, DiscoveryWire::NAME, source, || {
                static INPUT: OnceLock<Vec<u8>> = OnceLock::new();
                INPUT.get_or_init(|| {
                    [&SavedPlan::LateResponse.tag()[..], &late_response_plan()].concat()
                })
            })
            .witness("discovery/late-response-admitted", Expect::Holds),
            _ => Seed::new(name, DiscoveryWire::NAME, source, plan.tag().to_vec()),
        });
    }
    seeds.extend(composed_seeds());
    seeds.extend(spam_seeds());
    seeds
}

/// `request` as a peer's step.
fn ask(request: Request) -> PeerAction {
    PeerAction::Ask { request }
}

/// An undeclared message type, as a peer's step.
const UNDECLARED: PeerAction = PeerAction::Discover {
    act: DiscoveryAct::UndeclaredType { message_type: 0 },
};

/// The `discovery/composed` seeds, each a prefix under the empty Plan:
///
/// - `two-requests`: a peer that holds its `Services` and asks for one
///   record and for the header-sync service.
/// - `undeclared`: a peer that asks for peers, then sends an undeclared
///   message type, which the node refuses.
fn composed_seeds() -> Vec<Seed> {
    let source = "crates/zakura-sim/tests/targets.rs";
    let two_requests = [ask(Request::GetOnePeer), ask(Request::GetHeaderSyncService)];
    let undeclared = [ask(Request::GetPeers), UNDECLARED];
    [
        (
            "two-requests",
            composed_prefix(0, 1, 0b0000_1100, &two_requests),
        ),
        ("undeclared", composed_prefix(0, 0x81, 0, &undeclared)),
    ]
    .into_iter()
    .map(|(name, prefix)| {
        let prefix = prefix.unwrap_or_else(|error| panic!("discovery-composed-{name}: {error}"));
        Seed::new(
            format!("discovery-composed-{name}"),
            DiscoveryComposed::NAME,
            source,
            prefix.to_vec(),
        )
    })
    .collect()
}

/// The search that found `discovery-spam-content-before-margin`: generator
/// seed 4106, inputs of up to 512 bytes, run 302.
const CONTENT_BEFORE_MARGIN: (u64, usize, usize) = (4106, 512, 302);

/// The `discovery/spam` seeds:
///
/// - `bursts`: three spammers send `Hello` bursts and repeated requests past
///   the margin, beside an honest peer that asks for services and peers.
/// - `reconnect`: one spammer sends undeclared types back to back,
///   disconnects and dials again, beside an honest peer that asks for
///   peers.
/// - `content-before-margin`: the generator's input
///   [`CONTENT_BEFORE_MARGIN`], whose spammer sends a `GetServices` with bad
///   wanted services past the margin, which the node refuses at decode.
fn spam_seeds() -> Vec<Seed> {
    let hello_burst = PeerAction::Discover {
        act: DiscoveryAct::HelloBurst,
    };
    let bursts = [
        hello_burst,
        hello_burst,
        ask(Request::GetPeers),
        ask(Request::GetPeers),
        ask(Request::GetServices),
        ask(Request::GetServices),
        ask(Request::GetPeers),
    ];
    let reconnect = [
        UNDECLARED,
        UNDECLARED,
        PeerAction::Disconnect,
        UNDECLARED,
        UNDECLARED,
    ];
    let prefix = |name: &str, spammers, asks: &[Request], steps: &[PeerAction]| {
        spam_prefix(0, spammers, asks, steps)
            .unwrap_or_else(|error| panic!("discovery-spam-{name}: {error}"))
            .to_vec()
    };
    let (seed, max_bytes, run) = CONTENT_BEFORE_MARGIN;
    let (_, content_before_margin) = draws(seed, max_bytes, DiscoverySpam::families())
        .nth(run)
        .expect("the generator's draws never end");
    let targets = "crates/zakura-sim/tests/targets.rs";
    let source = "crates/zakura-sim/tests/discovery_spam.rs";
    [
        (
            "bursts",
            targets,
            prefix(
                "bursts",
                3,
                &[Request::GetServices, Request::GetPeers],
                &bursts,
            ),
        ),
        (
            "reconnect",
            source,
            prefix("reconnect", 1, &[Request::GetPeers], &reconnect),
        ),
        ("content-before-margin", source, content_before_margin),
    ]
    .into_iter()
    .map(|(name, source, input)| {
        Seed::new(
            format!("discovery-spam-{name}"),
            DiscoverySpam::NAME,
            source,
            input,
        )
    })
    .collect()
}

/// A step's option count, the index it picked, and the index of the option
/// that holds a task back there, where the steer holds it back.
type Choice = (usize, usize, Option<usize>);

/// Each step's [`Choice`].
type Choices = Vec<Choice>;

/// The Plan that follows the round-robin default, except that it picks the
/// held-back option wherever `run` reports one. Each round runs the Plan
/// with `run`, which returns the run's records and each step's choices,
/// finds the first step where the Plan didn't pick the held-back option,
/// and rewrites the Plan up to that step. `run` may end its run at that
/// step ([`stop`]), or once no later step can hold anything back.
///
/// # Panics
///
/// If 1,000 rounds don't settle the Plan.
fn hold_back(mut run: impl FnMut(&[u8]) -> (Vec<Record>, Choices)) -> Vec<u8> {
    let mut bytes = Vec::new();
    for _ in 0..1_000 {
        let (records, choices) = run(&bytes);
        let Some(deviation) = choices.iter().position(deviates) else {
            return bytes;
        };
        let mut next = Vec::new();
        for record in &records {
            if let Record::Init(init) = record {
                init.draws.iter().for_each(|d| push_draw(&mut next, d));
            }
        }
        let steps = records.iter().filter_map(|record| match record {
            Record::Step(step) => Some(step),
            _ => None,
        });
        for (at, (step, &(options, picked, hold))) in steps.zip(&choices).enumerate() {
            if options > 1 {
                // A step's index and option count fit in u64 and u128.
                push(&mut next, hold.unwrap_or(picked) as u64, options as u128);
            }
            if at == deviation {
                break;
            }
            step.draws.iter().for_each(|d| push_draw(&mut next, d));
        }
        bytes = next;
    }
    panic!("no Plan settled in 1,000 rounds");
}

/// Whether a step's choice differs from the option that holds a task back.
fn deviates(&(_, picked, hold): &Choice) -> bool {
    hold.is_some_and(|hold| hold != picked)
}

/// The violation a steering checker returns to end its run at `step`: the
/// run's later steps can't change the next round's Plan.
fn stop(step: u64) -> Violation {
    Violation {
        property: "seed/hold-back".to_owned(),
        step,
        witness: vec![step],
        expected: "a step a later round's Plan depends on".to_owned(),
        actual: "nothing left to steer".to_owned(),
    }
}

/// Appends `value` as the little-endian bytes the Plan decodes a choice
/// among `range` values from.
fn push(bytes: &mut Vec<u8>, value: u64, range: u128) {
    bytes.extend_from_slice(&value.to_le_bytes()[..width(range)]);
}

/// Appends a draw's answer, unless its bound makes it forced.
fn push_draw(bytes: &mut Vec<u8>, draw: &DrawRecord) {
    match draw.bound.0 {
        1 => {}
        0 => push(bytes, draw.value.0, 1 << 64),
        bound => push(bytes, draw.value.0, u128::from(bound)),
    }
}

/// The task an identity runs, if it runs one: `<task>` of
/// `task:<task>:<point>`.
fn task_of(identity: &str) -> Option<&str> {
    let rest = identity.strip_prefix("task:")?;
    Some(&rest[..rest.rfind(':')?])
}

/// S4.7's input for `discovery/wait-complete`: it holds the source back at
/// its `notified` until the sink has run all three marks. On the fixed code
/// the source still wakes. With the lost-wakeup patch, the same input
/// violates `discovery/wait-complete-wakes`.
fn wait_complete_input() -> Vec<u8> {
    hold_back(|plan| {
        let mut choices = Choices::new();
        let mut steer = after_step(|cx| {
            let enabled: Vec<String> = cx.enabled.iter().map(|e| e.identity.to_string()).collect();
            let pick = cx.pick.to_string();
            // The loop picks from the enabled set, so the pick is always in it.
            let index = enabled.iter().position(|e| *e == pick).unwrap_or_default();
            // The state before this step, as the pick saw it.
            let before = cx.log.entries().partition_point(|e| e.step < cx.step);
            let state = WaitState::of(&cx.log.entries()[..before]);
            let choice = (enabled.len(), index, hold_source(&state, &enabled));
            choices.push(choice);
            (deviates(&choice) || state.all_marked()).then(|| stop(cx.step))
        });
        let mut sim = Sim::new(run_config(), Plan::from_fuzz(plan));
        spawn(&mut sim);
        let mut sink = MemorySink::default();
        sim.run(&mut steer, &mut sink);
        drop(steer);
        (sink.records, choices)
    })
}

/// An enabled action other than the source, when the source stands between
/// its check and its `notified` and a mark is still to come.
fn hold_source(state: &WaitState, enabled: &[String]) -> Option<usize> {
    let source_enabled = enabled.iter().filter_map(|e| task_of(e)).any(is_source);
    if !source_enabled || !state.source_between_check_and_notified() || state.all_marked() {
        return None;
    }
    enabled
        .iter()
        .position(|e| !task_of(e).is_some_and(is_source))
}

/// [`SavedPlan::LateResponse`]'s Plan: from the step after the node's
/// discovery source took its initial wait's timeout, it picks another
/// action over the source until the node's reader has the late `Peers`
/// header or the node closes the connection.
fn late_response_plan() -> Vec<u8> {
    let scenario = SavedPlan::LateResponse.scenario();
    hold_back(|plan| {
        let mut choices = Choices::new();
        let mut admitted = None;
        let mut timed_out = false;
        let mut settled = false;
        let source = |identity: &str| task_of(identity).is_some_and(is_source);
        let mut steer = after_step(|cx| {
            let enabled: Vec<String> = cx.enabled.iter().map(|e| e.identity.to_string()).collect();
            let pick = cx.pick.to_string();
            // The loop picks from the enabled set, so the pick is always in it.
            let index = enabled.iter().position(|e| *e == pick).unwrap_or_default();
            let hold = (timed_out && !settled && source(&pick))
                .then(|| enabled.iter().position(|e| !source(e)))
                .flatten();
            let choice = (enabled.len(), index, hold);
            choices.push(choice);
            let discovery_session = |obs: &Observation| matches!(obs, Observation::Session { kind, .. } if *kind == ZAKURA_STREAM_DISCOVERY);
            if admitted.is_none() && cx.observations().any(discovery_session) {
                admitted = Some(cx.now);
            }
            timed_out |= source(&pick)
                && pick
                    .rsplit(':')
                    .next()
                    .is_some_and(|point| point.starts_with("sleep"))
                && admitted.is_some_and(|at| {
                    cx.now.saturating_duration_since(at) >= DISCOVERY_INITIAL_EXCHANGE_TIMEOUT
                });
            let entries = cx.log.entries();
            let start = entries.partition_point(|entry| entry.step < cx.step);
            settled |= (start..entries.len()).any(|at| {
                matches!(entries[at].obs, Observation::ConnectionClosed { .. })
                    || matches!(late_answer(entries, at), Ok(Some(_)))
            });
            (deviates(&choice) || settled).then(|| stop(cx.step))
        });
        let mut sink = MemorySink::default();
        match run_prepared::<DiscoveryWire>(
            &scenario,
            DiscoveryWire::prepare(&scenario),
            Plan::from_fuzz(plan),
            &mut sink,
            Some(&mut steer),
        ) {
            Ok(_) | Err(HarnessError::Run { .. }) => {}
            Err(error) => panic!("the late-response scenario runs: {error}"),
        }
        drop(steer);
        (sink.records, choices)
    })
}
