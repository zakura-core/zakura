//! A block-sync node on controlled block storage (BS1): the node's real
//! reactor, routines and sequencer send every block operation to one
//! `ControlledBlockService`, and every completion comes from the `Plan`.
#![cfg(zakura_sim)]

mod support;

use serde::{Deserialize, Serialize};
use support::{config, inputs};
use zakura_scheduler::SimInstant;
use zakura_sim::{
    harness::{
        block_node::{
            control::{ControlAction, ControlStep},
            policy::{BlockAllow, BlockKind, Hold, Select, Trigger},
            Policy,
        },
        block_sync::{
            storage_properties, storage_sources, BlockScenario, MirrorSpec, Ports, Storage,
            OPERATION_IDS,
        },
        header_sync::Dialer,
        run_harness, run_prepared, Subsystem,
    },
    observe::{BodyScope, StorageBranch, StorageFact, StorageRequest, StorageResult, WorkScope},
    property::{HasView, Property, Sources},
    ActionSource, Boundary, Identity, MemorySink, Observation, ObservationLog, PlanInput,
    PropertyOutcome, RunConfig, RunReport, Sim,
};

/// Runs `scenario` under `input`, and returns the report with the ports,
/// which outlive the run.
fn run(scenario: &BlockScenario, input: PlanInput) -> (RunReport, Ports) {
    let ports = Storage::prepare(scenario);
    let report = run_prepared::<Storage>(
        scenario,
        ports.clone(),
        input,
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    (report, ports)
}

/// `property`'s outcome in `report`.
fn outcome<'a>(report: &'a RunReport, property: &str) -> &'a PropertyOutcome {
    &report
        .properties
        .iter()
        .find(|r| r.property == property)
        .unwrap_or_else(|| panic!("the run reports {property}: {:?}", report.properties))
        .outcome
}

/// Every block completion's outcome, in order.
fn completions(report: &RunReport) -> Vec<String> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage {
                fact: StorageFact::BlockCompleted { outcome, .. },
                ..
            } => Some(outcome.clone()),
            _ => None,
        })
        .collect()
}

/// BS.2: the node starts with 64 headers and no bodies, between two
/// mirrors that hold the whole chain, one dialing it and one it dials.
/// Under every plan, its real routines download each body, its sequencer
/// submits them, and its block service commits them through the engine, so
/// the published view's verified tip is the fixture tip and full state's
/// best chain is the fixture chain. Every completion is a success the
/// converging policy offers, and the operation-ID property holds.
#[test]
fn honest_sync_reaches_the_fixture_tip() {
    let scenario = BlockScenario {
        blocks: 64,
        mirrors: vec![
            MirrorSpec {
                dialer: Dialer::Mirror,
            },
            MirrorSpec {
                dialer: Dialer::Node,
            },
        ],
        ..BlockScenario::default()
    };
    for input in inputs(8, 256) {
        let (report, ports) = run(&scenario, input);
        assert!(
            matches!(
                outcome(&report, OPERATION_IDS),
                PropertyOutcome::Held { applicable } if *applicable > 0
            ),
            "{:?}",
            report.properties
        );
        let tip = ports.chain.last().expect("the chain has blocks").frontier();
        assert_eq!(ports.node.view().frontiers.verified_best, tip);
        let best: Vec<_> = ports
            .node
            .with_full_state(|chain| chain.best_path())
            .into_iter()
            .map(|header| (header.height, header.hash))
            .collect();
        let fixture: Vec<_> = ports
            .chain
            .iter()
            .map(|block| (block.height, block.hash()))
            .collect();
        assert_eq!(best, fixture, "full state commits every height in order");
        let outcomes = completions(&report);
        assert!(
            outcomes.iter().filter(|o| *o == "committed").count() >= 64,
            "{outcomes:?}"
        );
        for outcome in &outcomes {
            assert!(
                ["answer", "admit", "apply", "committed", "duplicate"].contains(&outcome.as_str()),
                "{outcome}"
            );
        }
    }
}

/// The default scenario, which every trace test records, converges too.
#[test]
fn the_default_scenario_converges() {
    let scenario = BlockScenario::default();
    for input in inputs(4, 64) {
        let (_, ports) = run(&scenario, input);
        let tip = ports.chain.last().expect("the chain has blocks").frontier();
        assert_eq!(ports.node.view().frontiers.verified_best, tip);
    }
}

/// A node that starts with the first four of eight bodies downloads and
/// submits only the other four, and reaches the tip.
#[test]
fn a_node_with_bodies_submits_only_the_rest() {
    let scenario = BlockScenario {
        node_bodies: 4,
        ..BlockScenario::default()
    };
    for input in inputs(4, 64) {
        let (report, ports) = run(&scenario, input);
        let tip = ports.chain.last().expect("the chain has blocks").frontier();
        assert_eq!(ports.node.view().frontiers.verified_best, tip);
        let mut heights: Vec<_> = report
            .log
            .entries()
            .iter()
            .filter_map(|entry| match &entry.obs {
                Observation::Storage {
                    fact:
                        StorageFact::BlockSubmitted {
                            request: StorageRequest::SubmitBlock { height, .. },
                            ..
                        },
                    ..
                } => *height,
                _ => None,
            })
            .collect();
        heights.sort_unstable();
        heights.dedup();
        assert_eq!(heights, [5, 6, 7, 8]);
    }
}

/// A node can't hold more bodies than the chain has.
#[test]
#[should_panic(expected = "a node holds at most the scenario's 8 blocks, not 9")]
fn a_node_holds_at_most_the_chain() {
    Storage::prepare(&BlockScenario {
        node_bodies: 9,
        ..BlockScenario::default()
    });
}

/// A hold can't wait for block sync to take a reply its handle only queues.
#[test]
#[should_panic(expected = "a block scenario can't wait for ReplyTaken")]
fn a_hold_cant_wait_for_a_taken_handle_reply() {
    Storage::prepare(&BlockScenario {
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(BlockKind::Admit, 2),
                until: Trigger::ReplyTaken(Select::nth(BlockKind::Apply, 1)),
                then: vec![BlockAllow::Admit],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        ..BlockScenario::default()
    });
}

/// A control step can't wait for a handled call.
#[test]
#[should_panic(expected = "a block scenario can't wait for Handled")]
fn a_control_step_cant_wait_for_a_handled_call() {
    Storage::prepare(&BlockScenario {
        control: vec![ControlStep {
            action: ControlAction::Close {
                peer: "mirror-0".to_owned(),
            },
            when: Some(Trigger::Count(
                1,
                Box::new(Trigger::Handled(Select::nth(BlockKind::Query, 1))),
            )),
        }],
        ..BlockScenario::default()
    });
}

/// One planted block storage fact.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Fact {
    /// Operation `id` arrived.
    Submitted(u64),
    /// Operation `id` completed.
    Completed(u64),
}

impl Fact {
    /// The fact as block storage logs it.
    fn storage(self) -> StorageFact {
        match self {
            Fact::Submitted(id) => StorageFact::BlockSubmitted {
                id,
                request: StorageRequest::QueryNeededBlocks {
                    query_id: id,
                    from: 1,
                    limit: 1,
                    scope: BodyScope {
                        scope: WorkScope {
                            header_generation: 1,
                            branch: StorageBranch {
                                anchor: "00".to_owned(),
                                target: "11".to_owned(),
                            },
                        },
                        verified_generation: 1,
                        body_work_epoch: 1,
                    },
                    best_header_tip: 1,
                },
                stage: Some("pending".to_owned()),
            },
            Fact::Completed(id) => StorageFact::BlockCompleted {
                id,
                outcome: "fail".to_owned(),
                result: StorageResult::NeededBlocksFailed,
                delivered: true,
                state_version: 0,
            },
        }
    }
}

/// Facts planted for the node's block storage, instance 0, one per step.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Plant {
    facts: Vec<Fact>,
}

/// Logs a [`Plant`]'s facts in order, one per pick of `peer:storage:fact`.
struct Facts {
    facts: Vec<Fact>,
    /// The next fact to log.
    next: usize,
}

impl ActionSource for Facts {
    fn enabled(&self, _: SimInstant, out: &mut Vec<(Identity, u64)>) {
        if self.next < self.facts.len() {
            out.push((Identity::peer("storage", "fact"), 0));
        }
    }

    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    fn execute(&mut self, _: u64, log: &mut ObservationLog) {
        let fact = self.facts[self.next];
        self.next += 1;
        let seq = u64::try_from(self.next).expect("a plant is far shorter than u64");
        log.push(Observation::Storage {
            service: "block".to_owned(),
            instance: 0,
            seq,
            txn: seq,
            fact: fact.storage(),
        });
    }
}

/// The block storage harness's properties on a [`Plant`] instead of a node,
/// so a test can plant what no honest run does.
struct Planted;

impl HasView for Planted {
    type View = ();

    /// Nothing, since no property reads it.
    fn view(&self) {}
}

impl Subsystem for Planted {
    const NAME: &'static str = "test/planted-block-storage";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Plant;
    type Prepared = ();

    /// 2 workers and room for every plant.
    fn config(_: &Plant) -> RunConfig {
        config(2, 100)
    }

    /// Nothing to set up.
    fn prepare(_: &Plant) {}

    /// Adds the plant's facts.
    fn build(sim: &mut Sim, plant: &Plant, (): ()) -> Self {
        sim.add_source(Box::new(Facts {
            facts: plant.facts.clone(),
            next: 0,
        }));
        Planted
    }

    /// The block storage harness's properties.
    fn properties() -> Vec<Property<Self>> {
        storage_properties()
    }

    /// The block storage harness's sources.
    fn sources() -> Sources {
        storage_sources()
    }

    /// Unused: plants are only built by hand.
    fn draw(_: &[u8]) -> Plant {
        Plant::default()
    }
}

/// Runs `plant` under the block storage harness's properties.
fn planted(plant: &Plant) -> RunReport {
    run_harness::<Planted>(
        plant,
        inputs(1, 0).remove(0),
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{plant:?}: {error}"))
}

/// No honest run trips `block-sync/storage-operation-ids`, so planted
/// facts do: a skipped submission, a completion before its submission, and
/// a second completion each violate it at the fact that breaks it. In-order
/// facts hold at every step that logs one.
#[test]
fn planted_block_operation_ids_violate_the_property() {
    use Fact::{Completed, Submitted};
    let report = planted(&Plant {
        facts: vec![Submitted(1), Completed(1), Submitted(2), Completed(2)],
    });
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    assert_eq!(
        *outcome(&report, OPERATION_IDS),
        PropertyOutcome::Held { applicable: 4 }
    );
    for (facts, expected, actual) in [
        (
            vec![Submitted(1), Submitted(3)],
            "operation 2 submitted next",
            "operation 3 submitted",
        ),
        (
            vec![Submitted(1), Completed(2)],
            "a completion of a submitted operation, at most 1",
            "operation 2 completed before its submission",
        ),
        (
            vec![Submitted(1), Completed(0)],
            "a completion of a submitted operation, at most 1",
            "operation 0 completed, an ID no operation gets",
        ),
        (
            vec![Submitted(1), Completed(1), Completed(1)],
            "operation 1 completes once",
            "operation 1 completed again",
        ),
    ] {
        let report = planted(&Plant {
            facts: facts.clone(),
        });
        let violation = report
            .end
            .violation
            .as_ref()
            .unwrap_or_else(|| panic!("{facts:?} violates the property"));
        assert_eq!(
            (
                violation.property.as_str(),
                violation.expected.as_str(),
                violation.actual.as_str()
            ),
            (OPERATION_IDS, expected, actual)
        );
    }
}

/// Header storage facts don't count toward the block property: the same
/// out-of-order facts logged as the `header` service leave it inapplicable.
#[test]
fn header_facts_leave_the_block_property_inapplicable() {
    struct HeaderFacts(bool);
    impl ActionSource for HeaderFacts {
        fn enabled(&self, _: SimInstant, out: &mut Vec<(Identity, u64)>) {
            if !self.0 {
                out.push((Identity::peer("storage", "fact"), 0));
            }
        }
        fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}
        fn execute(&mut self, _: u64, log: &mut ObservationLog) {
            self.0 = true;
            log.push(Observation::Storage {
                service: "header".to_owned(),
                instance: 0,
                seq: 1,
                txn: 1,
                fact: Fact::Submitted(3).storage(),
            });
        }
    }
    struct Headers;
    impl HasView for Headers {
        type View = ();
        fn view(&self) {}
    }
    impl Subsystem for Headers {
        const NAME: &'static str = "test/header-facts";
        const GENERATOR: u32 = 1;
        const SCENARIO_BYTES: usize = 0;
        type Scenario = Plant;
        type Prepared = ();
        fn config(_: &Plant) -> RunConfig {
            config(2, 100)
        }
        fn prepare(_: &Plant) {}
        fn build(sim: &mut Sim, _: &Plant, (): ()) -> Self {
            sim.add_source(Box::new(HeaderFacts(false)));
            Headers
        }
        fn properties() -> Vec<Property<Self>> {
            storage_properties()
        }
        fn sources() -> Sources {
            storage_sources()
        }
        fn draw(_: &[u8]) -> Plant {
            Plant::default()
        }
    }
    let report = run_harness::<Headers>(
        &Plant::default(),
        inputs(1, 0).remove(0),
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(report.end.violation, None, "{:?}", report.end);
    assert_eq!(
        *outcome(&report, OPERATION_IDS),
        PropertyOutcome::NotApplicable
    );
}
