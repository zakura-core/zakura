//! A header-sync node on controlled storage (M4-B1): the node's real reactor
//! and publisher submit every storage call to one `ControlledPort`, and every
//! completion comes from the `Plan`.
#![cfg(zakura_sim)]

mod support;

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use support::{config, inputs};
use zakura_quic::NodeSecretKey;
use zakura_rt::sync::mpsc;
use zakura_scheduler::SimInstant;
use zakura_sim::{
    harness::{
        header_node::{guard_dispatch, Chain, ControlAction, ControlStep, DispatchFlag, Fork},
        header_sync::{
            storage_properties, storage_sources, Dialer, MirrorSpec, Ports, Storage,
            StorageScenario, StorageView, DIRECT_DISPATCH, OPERATION_IDS,
        },
        run_harness, run_prepared, Subsystem,
    },
    observe::{StorageFact, StorageRequest, StorageResult},
    property::{HasView, Property, Sources},
    ActionSource, Boundary, Identity, MemorySink, Observation, ObservationLog, PlanInput,
    PropertyOutcome, RunConfig, RunReport, Sim,
};
use zakura_storage::header::{controlled::ControlledPort, fixture::HeaderFixture};

/// Runs `scenario` under `input`, and returns the report with the ports,
/// which outlive the run.
fn run(scenario: &StorageScenario, input: PlanInput) -> (RunReport, Ports) {
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

/// Every request the node's storage saw submitted, in order.
fn requests(report: &RunReport) -> Vec<StorageRequest> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage {
                instance: 0,
                fact: StorageFact::Submitted { request, .. },
                ..
            } => Some(request.clone()),
            _ => None,
        })
        .collect()
}

/// A mirror holding `chain`.
fn mirror(chain: Chain, dialer: Dialer) -> MirrorSpec {
    MirrorSpec { chain, dialer }
}

/// The hex peer identity of the actor whose key is `seed` repeated, as
/// storage attribution writes it.
fn peer(seed: u8) -> String {
    let id = NodeSecretKey::from_bytes(&[seed; 32]).public();
    id.as_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

/// The node at genesis syncs to `mirror-0`'s A5 under every plan, through a
/// locator, a prepare and an apply on its own port, each completed by the
/// converging policy. The harness's properties apply and hold.
#[test]
fn node_syncs_from_a_mirror_on_controlled_storage() {
    let scenario = StorageScenario::default();
    for input in inputs(32, 256) {
        let (report, ports) = run(&scenario, input);
        for property in [OPERATION_IDS, DIRECT_DISPATCH] {
            assert!(
                matches!(
                    outcome(&report, property),
                    PropertyOutcome::Held { applicable } if *applicable > 0
                ),
                "{property}: {:?}",
                report.properties
            );
        }
        for entry in report.log.entries() {
            if let Observation::Storage {
                fact: StorageFact::Completed { outcome, .. },
                ..
            } = &entry.obs
            {
                assert!(
                    ["engine", "rejected"].contains(&outcome.as_str()),
                    "{outcome}"
                );
            }
        }
        assert_eq!(
            ports.node.engine().snapshot().frontiers.header_best,
            Chain::main(5).tip(&ports.fixture),
            "the node reaches mirror-0's tip"
        );
        let requests = requests(&report);
        for kind in [
            |r: &StorageRequest| matches!(r, StorageRequest::ContinuationLocator),
            |r: &StorageRequest| matches!(r, StorageRequest::PrepareHeaderTarget { .. }),
            |r: &StorageRequest| matches!(r, StorageRequest::ApplyHeaderTarget { .. }),
        ] {
            assert!(requests.iter().any(kind), "{requests:?}");
        }
    }
}

/// The worked example: `mirror-0`, on a heavier fork, dials the node, and the
/// node dials `mirror-1` at genesis. The node's requester and publisher both
/// use its one port, so its log holds both in one ID sequence, which
/// `header-sync/storage-operation-ids` checks, and `mirror-1` follows it to
/// `mirror-0`'s tip.
#[test]
fn one_port_serves_requester_and_publisher() {
    let scenario = StorageScenario {
        node: Chain::main(3),
        mirrors: vec![
            mirror(
                Chain {
                    main: 1,
                    fork: Some(Fork { branch: 1, len: 4 }),
                },
                Dialer::Mirror,
            ),
            mirror(Chain::default(), Dialer::Node),
        ],
        ..StorageScenario::default()
    };
    // Actors' key seeds follow their order, and links count sessions from 1
    // in mirror order. `mirror-0` also pulls the node's own branch, so both
    // peers acquire from the node's publisher.
    let (mirror_0, mirror_1) = ((peer(2), 1), (peer(3), 2));
    for input in inputs(16, 256) {
        let (report, ports) = run(&scenario, input);
        let requests = requests(&report);
        let (mut requester, mut mirror_1_acquires, mut reads) = (0, 0, 0);
        for request in &requests {
            let lease = match request {
                StorageRequest::ContinuationLocator
                | StorageRequest::PrepareHeaderTarget { .. }
                | StorageRequest::ApplyHeaderTarget { .. } => {
                    requester += 1;
                    continue;
                }
                StorageRequest::AcquireHeaderPath {
                    peer, session_id, ..
                } => {
                    mirror_1_acquires += usize::from(*peer == mirror_1.0);
                    Some((peer, *session_id))
                }
                StorageRequest::ReadHeaderPath { lease, .. } => {
                    reads += 1;
                    lease.as_ref().map(|lease| (&lease.peer, lease.session_id))
                }
                StorageRequest::ReleaseHeaderPath { lease, .. } => {
                    lease.as_ref().map(|lease| (&lease.peer, lease.session_id))
                }
                // Membership reads carry no peer or session.
                _ => continue,
            };
            // Each path operation carries the session of the peer it serves.
            assert!(
                [&mirror_0, &mirror_1]
                    .into_iter()
                    .any(|(peer, session)| lease == Some((peer, *session))),
                "{request:?}"
            );
        }
        assert!(
            requester > 0 && mirror_1_acquires > 0 && reads > 0,
            "the node's log has requester and publisher operations: {requests:?}"
        );
        let tip = ports.node.engine().snapshot().frontiers.header_best;
        assert_eq!(tip, scenario.mirrors[0].chain.tip(&ports.fixture));
        assert_eq!(
            ports.mirrors[1].snapshot().frontiers.header_best,
            tip,
            "mirror-1 follows the node"
        );
    }
}

/// One fact [`Planted`] logs for the node's storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Fact {
    /// Operation `id` submitted.
    Submitted(u64),
    /// Operation `id` completed.
    Completed(u64),
}

impl Fact {
    /// The storage fact, with placeholder fields the checks don't read.
    fn storage(self) -> StorageFact {
        match self {
            Fact::Submitted(id) => StorageFact::Submitted {
                id,
                request: StorageRequest::ContinuationLocator,
                digest: String::new(),
            },
            Fact::Completed(id) => StorageFact::Completed {
                id,
                outcome: "engine".to_owned(),
                result: StorageResult::NoLocator,
                delivered: true,
                generation: 0,
                state: String::new(),
            },
        }
    }
}

/// What [`Planted`] plants in a run of the storage harness's properties.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Plant {
    /// A task sends to a reactor's action channel, which a guard watches.
    dispatch: bool,
    /// Facts logged for the node's storage, instance 0, one per step.
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
            service: "header".to_owned(),
            instance: 0,
            seq,
            txn: seq,
            fact: fact.storage(),
        });
    }
}

/// The storage harness's properties on a [`Plant`] instead of a node, so a
/// test can plant what no M4 run does.
struct Planted {
    port: Arc<ControlledPort>,
    dispatch: DispatchFlag,
}

impl HasView for Planted {
    type View = StorageView;

    /// An untouched port and the guard's flag.
    fn view(&self) -> StorageView {
        StorageView {
            port: self.port.clone(),
            direct_dispatch_broken: self.dispatch.get(),
        }
    }
}

impl Subsystem for Planted {
    const NAME: &'static str = "test/planted-storage";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Plant;
    type Prepared = Arc<ControlledPort>;

    /// 2 workers and room for every plant.
    fn config(_: &Plant) -> RunConfig {
        config(2, 100)
    }

    /// A port at genesis, for the view.
    fn prepare(_: &Plant) -> Arc<ControlledPort> {
        Arc::new(ControlledPort::new(&HeaderFixture::regtest()))
    }

    /// Spawns a task that sends to a guarded channel if the plant says to,
    /// and adds the plant's facts.
    fn build(sim: &mut Sim, plant: &Plant, port: Arc<ControlledPort>) -> Self {
        let dispatch = DispatchFlag::default();
        if plant.dispatch {
            let (actions, received) = mpsc::channel(1);
            sim.scheduler().spawn_root("reactor", async move {
                actions
                    .send(())
                    .await
                    .expect("the guard holds the receiver");
            });
            guard_dispatch(sim, received, dispatch.clone());
        }
        sim.add_source(Box::new(Facts {
            facts: plant.facts.clone(),
            next: 0,
        }));
        Planted { port, dispatch }
    }

    /// The storage harness's properties.
    fn properties() -> Vec<Property<Self>> {
        storage_properties()
    }

    /// The storage harness's sources.
    fn sources() -> Sources {
        storage_sources()
    }

    /// Unused: plants are only built by hand.
    fn draw(_: &[u8]) -> Plant {
        Plant::default()
    }
}

/// Runs `plant` under the storage harness's properties.
fn planted(plant: &Plant) -> RunReport {
    run_harness::<Planted>(
        plant,
        inputs(1, 0).remove(0),
        &mut MemorySink::default(),
        None,
    )
    .unwrap_or_else(|error| panic!("{plant:?}: {error}"))
}

/// No M4 run trips `header-sync/storage-operation-ids`, so planted facts do:
/// a skipped submission, a completion before its submission, and a second
/// completion each violate it at the fact that breaks it. In-order facts
/// hold at every step that logs one.
#[test]
fn planted_operation_ids_violate_the_property() {
    use Fact::{Completed, Submitted};
    let facts = vec![Submitted(1), Completed(1), Submitted(2), Completed(2)];
    let report = planted(&Plant {
        dispatch: false,
        facts,
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
            vec![Submitted(1), Completed(1), Completed(1)],
            "operation 1 completes once",
            "operation 1 completed again",
        ),
    ] {
        let report = planted(&Plant {
            dispatch: false,
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
        let last = report
            .picks
            .iter()
            .rfind(|pick| pick.identity == "peer:storage:fact")
            .expect("the plant logs its facts");
        assert_eq!(violation.step, last.step, "at the last fact");
        assert!(matches!(
            outcome(&report, OPERATION_IDS),
            PropertyOutcome::Violated { .. }
        ));
    }
}

/// A reactor that sends an operation to its action channel, which only
/// `External` dispatch does, fails the run as `harness/direct-dispatch` on
/// the step the guard sees it.
#[test]
fn direct_dispatch_guard_reports_a_violation() {
    let report = planted(&Plant {
        dispatch: true,
        facts: Vec::new(),
    });
    let violation = report.end.violation.expect("the guard trips");
    assert_eq!(violation.property, DIRECT_DISPATCH);
    let pick = &report.picks[usize::try_from(violation.step).unwrap() - 1];
    assert!(
        pick.identity.starts_with("task:dispatch-guard"),
        "{:?}",
        report.picks
    );
}

/// `Control` closes `mirror-0`'s link and relinks it on a new connection and
/// session, at points the `Plan` picks. The node still reaches `mirror-0`'s
/// tip, and some plan closes early enough that it syncs over the new session.
#[test]
fn control_closes_and_relinks_a_mirror() {
    let scenario = StorageScenario {
        controls: [ControlAction::Close, ControlAction::Relink]
            .into_iter()
            .map(|action| ControlStep {
                peer: "mirror-0".to_owned(),
                action,
            })
            .collect(),
        ..StorageScenario::default()
    };
    let mut over_relink = 0;
    for input in inputs(16, 256) {
        let (report, ports) = run(&scenario, input);
        let picks: Vec<&str> = report.picks.iter().map(|p| p.identity.as_str()).collect();
        assert!(picks.contains(&"peer:mirror-0:close") && picks.contains(&"peer:mirror-0:relink"));
        assert_eq!(
            ports.node.engine().snapshot().frontiers.header_best,
            Chain::main(5).tip(&ports.fixture)
        );
        // The relink is the second link, so its session is 2.
        over_relink += usize::from(requests(&report).iter().any(|request| {
            matches!(request, StorageRequest::ApplyHeaderTarget { owner, .. } if owner.session_id == 2)
        }));
    }
    assert!(over_relink > 0, "no plan synced over the new session");
}

/// A `Control` close takes effect, not just its pick. With no relink to
/// retire the session, only the close can end it, so both sides' session
/// pipes must exit, and only after the close.
#[test]
fn control_close_ends_both_sides_of_the_session() {
    let scenario = StorageScenario {
        controls: vec![ControlStep {
            peer: "mirror-0".to_owned(),
            action: ControlAction::Close,
        }],
        ..StorageScenario::default()
    };
    for input in inputs(8, 256) {
        let (report, _) = run(&scenario, input);
        let close = report
            .picks
            .iter()
            .find(|pick| pick.identity == "peer:mirror-0:close")
            .expect("the close is picked")
            .step;
        let pipes: Vec<(u64, &str)> = report
            .log
            .entries()
            .iter()
            .filter_map(|entry| match &entry.obs {
                Observation::TaskExited { task, .. } if task.starts_with("header_sync_pipe") => {
                    Some((entry.step, task.as_str()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            pipes.len(),
            2,
            "the node's and mirror-0's pipes both end: {pipes:?}"
        );
        assert!(
            pipes.iter().all(|(step, _)| *step > close),
            "after the close at step {close}: {pipes:?}"
        );
    }
}
