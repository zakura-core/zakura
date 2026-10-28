//! Holds, control steps and the probe on a block-sync node (BS2): each hold
//! and each control action changes only what it names, and the probe's
//! frames reach the node's routine.
#![cfg(zakura_sim)]

mod support;

use support::{inputs, picks};
use zakura_network::zakura::BlockSyncMessage;
use zakura_sim::{
    harness::{
        block_node::{
            control::{ControlAction, ControlStep, Fork},
            policy::{BlockAllow, BlockKind, Hold, MirrorEvent, Select, Trigger},
            probe::ProbeFrame,
            Policy, PROBE,
        },
        block_sync::{BlockScenario, MirrorSpec, Ports, Storage},
        header_sync::Dialer,
        run_prepared, Subsystem,
    },
    observe::{StorageFact, StorageRequest},
    MemorySink, Observation, PlanInput, RunReport,
};

/// Runs `scenario` under `input`, and returns the report with the ports,
/// which outlive the run. The run must end without a violation.
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

/// Every block storage fact of `report`, in order.
fn facts(report: &RunReport) -> Vec<&StorageFact> {
    report
        .log
        .entries()
        .iter()
        .filter_map(|entry| match &entry.obs {
            Observation::Storage { service, fact, .. } if service == "block" => Some(fact),
            _ => None,
        })
        .collect()
}

/// The node converged on the live chain: its published verified tip is the
/// chain's tip.
fn assert_converged(ports: &Ports) {
    assert_eq!(
        ports.node.view().frontiers.verified_best,
        ports.live.tip(),
        "the node commits the live chain"
    );
}

/// A scenario of `blocks` from one mirror that dials the node, under
/// `holds` and `control`.
fn scenario(blocks: u32, holds: Vec<Hold>, control: Vec<ControlStep>) -> BlockScenario {
    BlockScenario {
        blocks,
        policy: Policy {
            holds,
            ..Policy::converging()
        },
        control,
        ..BlockScenario::default()
    }
}

/// A hold on the first admission until the third submission arrives keeps
/// that admission back until then, and the node still converges.
#[test]
fn a_held_admission_waits_for_its_trigger() {
    let holds = vec![Hold {
        select: Select::nth(BlockKind::Admit, 1),
        until: Trigger::Submitted(Select::nth(BlockKind::Admit, 3)),
        then: vec![BlockAllow::Admit],
        deadline_ms: None,
    }];
    for input in inputs(8, 256) {
        let (report, ports) = run(&scenario(16, holds.clone(), Vec::new()), input);
        assert_converged(&ports);
        let facts = facts(&report);
        let submits: Vec<_> = facts
            .iter()
            .enumerate()
            .filter_map(|(at, fact)| match fact {
                StorageFact::BlockSubmitted {
                    id,
                    request: StorageRequest::SubmitBlock { .. },
                    ..
                } => Some((at, *id)),
                _ => None,
            })
            .collect();
        let first = submits[0].1;
        let admitted = facts
            .iter()
            .position(|fact| matches!(fact, StorageFact::Admitted { id } if *id == first))
            .expect("the first submission is admitted");
        assert!(
            admitted > submits[2].0,
            "the first admission waits for the third submission: {submits:?}, admitted at \
             {admitted}"
        );
    }
}

/// A hold on the first mirror read until the second arrives keeps the
/// first mirror from serving it until then. A mirror serves one read at a
/// time, so the second comes from the other mirror, which serves at once.
#[test]
fn a_held_mirror_read_waits_for_its_trigger() {
    let holds = vec![Hold {
        select: Select::nth(BlockKind::MirrorRead, 1),
        until: Trigger::Submitted(Select::nth(BlockKind::MirrorRead, 2)),
        then: vec![BlockAllow::Serve],
        deadline_ms: None,
    }];
    for input in inputs(8, 256) {
        let scenario = BlockScenario {
            mirrors: vec![
                MirrorSpec {
                    dialer: Dialer::Mirror,
                };
                2
            ],
            ..scenario(16, holds.clone(), Vec::new())
        };
        let (_, ports) = run(&scenario, input);
        assert_converged(&ports);
        let history = ports.reads.history();
        let at = |event| history.iter().position(|e| *e == event);
        let second = at(MirrorEvent::Submitted(2)).expect("a second read arrives");
        let served = at(MirrorEvent::Served(1)).expect("the first read is served");
        assert!(served > second, "{history:?}");
    }
}

/// Closing `mirror-0`'s link after four verdicts, then relinking it once
/// that connection has closed, leaves the node to finish over the new link.
/// The first link closes, and only it: `mirror-1`'s stays open.
#[test]
fn close_and_relink_change_only_the_named_link() {
    let control = vec![
        ControlStep {
            action: ControlAction::Close {
                peer: "mirror-0".to_owned(),
            },
            when: Some(Trigger::Count(
                4,
                Box::new(Trigger::Completed(Select {
                    kind: Some(BlockKind::Apply),
                    ..Select::default()
                })),
            )),
        },
        ControlStep {
            action: ControlAction::Relink {
                peer: "mirror-0".to_owned(),
            },
            when: Some(Trigger::Closed("mirror-0".to_owned())),
        },
    ];
    let scenario = BlockScenario {
        mirrors: vec![
            MirrorSpec {
                dialer: Dialer::Mirror,
            },
            MirrorSpec {
                dialer: Dialer::Node,
            },
        ],
        ..scenario(16, Vec::new(), control)
    };
    for input in inputs(8, 256) {
        let (_, ports) = run(&scenario, input);
        assert_converged(&ports);
        let links = ports.effects.links.borrow();
        let state: Vec<_> = links
            .iter()
            .map(|link| (link.peer.as_str(), link.is_closed()))
            .collect();
        assert_eq!(
            state,
            [("mirror-0", true), ("mirror-1", false), ("mirror-0", false)],
            "{links:?}"
        );
    }
}

/// Growing the chain after eight verdicts moves every actor's tip, and the
/// node downloads and commits the new blocks.
#[test]
fn growth_extends_every_actor_and_the_node_follows() {
    let control = vec![ControlStep {
        action: ControlAction::Grow {
            len: 8,
            node_first: false,
        },
        when: Some(Trigger::Count(
            8,
            Box::new(Trigger::Completed(Select {
                kind: Some(BlockKind::Apply),
                ..Select::default()
            })),
        )),
    }];
    for input in inputs(4, 256) {
        let (_, ports) = run(&scenario(8, Vec::new(), control.clone()), input);
        assert_eq!(ports.live.tip().height.0, 16);
        assert_converged(&ports);
        assert_eq!(
            ports.effects.chains.borrow().len(),
            2,
            "the node and the mirror grow once each"
        );
    }
}

/// A growth that reaches the node first gives it the new headers before
/// the mirror holds their bodies, in two steps of the node's own: one
/// prepares the headers and one applies them. The node still downloads and
/// commits the new blocks once the mirror has them.
#[test]
fn a_node_first_growth_runs_the_headers_ahead_of_the_bodies() {
    let control = vec![ControlStep {
        action: ControlAction::Grow {
            len: 4,
            node_first: true,
        },
        when: Some(Trigger::Count(
            8,
            Box::new(Trigger::Completed(Select {
                kind: Some(BlockKind::Apply),
                ..Select::default()
            })),
        )),
    }];
    for input in inputs(4, 256) {
        let (report, ports) = run(&scenario(8, Vec::new(), control.clone()), input);
        assert_eq!(ports.live.tip().height.0, 12);
        assert_converged(&ports);
        let chains = ports.effects.chains.borrow();
        let order: Vec<_> = chains.iter().map(|(_, actor)| actor.as_str()).collect();
        assert_eq!(order, ["node", "mirror-0"], "the node changes first");
        let picks = picks(&report);
        let at = |pick| picks.iter().position(|p| *p == pick);
        let prepare = at("peer:node:grow").expect("the node prepares the headers");
        let apply = at("peer:node:grow-apply").expect("the node applies them");
        assert!(prepare < apply, "{picks:?}");
    }
}

/// A fork from the chain's tip is a growth, not a reset, so the step fails
/// the run instead of recording a reset that never happened.
#[test]
fn a_reset_from_the_tip_fails_the_run() {
    let control = vec![ControlStep {
        action: ControlAction::Reset {
            fork: Fork {
                branch: 1,
                at: 8,
                len: 2,
            },
            node_first: false,
        },
        when: None,
    }];
    let scenario = scenario(8, Vec::new(), control);
    let Err(failure) = run_prepared::<Storage>(
        &scenario,
        Storage::prepare(&scenario),
        PlanInput::Bytes(Vec::new()),
        &mut MemorySink::default(),
        None,
    ) else {
        panic!("a reset from the tip fails the run");
    };
    let failure = failure.to_string();
    assert!(
        failure.contains("ControlFailed: a fork at 8 doesn't branch below the chain's tip"),
        "{failure}"
    );
}

/// Switching to a fork at height 4 after the node commits the first 12
/// blocks reorganizes its engine, and once it commits the fork's bodies its
/// verified chain resets: the engine installs a reset, and the body work
/// epoch moves past the one the run started with.
#[test]
fn a_reset_switches_the_node_to_the_fork() {
    let control = vec![ControlStep {
        action: ControlAction::Reset {
            fork: Fork {
                branch: 1,
                at: 4,
                len: 12,
            },
            node_first: false,
        },
        when: Some(Trigger::Count(
            12,
            Box::new(Trigger::Completed(Select {
                kind: Some(BlockKind::Apply),
                ..Select::default()
            })),
        )),
    }];
    let scenario = scenario(12, Vec::new(), control);
    let epoch = Storage::prepare(&scenario).node.view().body_work_epoch;
    for input in inputs(4, 256) {
        let (report, ports) = run(&scenario, input);
        assert_eq!(ports.live.tip().height.0, 16);
        assert_converged(&ports);
        assert_ne!(ports.node.view().body_work_epoch, epoch);
        assert!(
            facts(&report).iter().any(|fact| matches!(
                fact,
                StorageFact::TransitionInstalled { cause: Some(cause), .. } if cause == "reset"
            )),
            "the node's verified chain resets"
        );
    }
}

/// A consensus-invalid body recorded between the node's two reset steps
/// moves its header generation, which makes the preparation stale, so the
/// node prepares the reset again and still switches to the fork. The
/// invalid body is the old branch's, above the fork point, and its record
/// waits for the last mirror's reset, so it races the node's steps. Long
/// inputs keep the plan's choices fuzzed that late in the run. Few
/// schedules win that race: of the first 96 seeded inputs, only the 82nd
/// does, so the test runs the first 82.
#[test]
fn an_invalid_body_between_the_reset_steps_prepares_it_again() {
    let holds = vec![
        Hold {
            select: Select::nth(BlockKind::Apply, 8),
            until: Trigger::VirtualTime(0),
            then: vec![BlockAllow::ConsensusInvalid],
            deadline_ms: None,
        },
        Hold {
            select: Select::nth(BlockKind::Record, 1),
            until: Trigger::Reset("mirror-1".to_owned()),
            then: vec![BlockAllow::Apply],
            deadline_ms: None,
        },
    ];
    let control = vec![ControlStep {
        action: ControlAction::Reset {
            fork: Fork {
                branch: 1,
                at: 4,
                len: 12,
            },
            node_first: false,
        },
        when: Some(Trigger::Completed(Select::nth(BlockKind::Apply, 8))),
    }];
    let scenario = BlockScenario {
        mirrors: vec![
            MirrorSpec {
                dialer: Dialer::Mirror,
            },
            MirrorSpec {
                dialer: Dialer::Node,
            },
        ],
        ..scenario(12, holds, control)
    };
    let mut prepared_again = 0;
    for input in inputs(82, 16_384) {
        let (report, ports) = run(&scenario, input);
        assert_converged(&ports);
        let picks = picks(&report);
        let prepares = picks.iter().filter(|p| **p == "peer:node:reset").count();
        if prepares > 1 {
            prepared_again += 1;
        }
    }
    assert!(
        prepared_again > 0,
        "no schedule records the invalid body between the steps"
    );
}

/// A node holding every body serves the probe's requests through its real
/// routine, serving session and block service: the run serves its range
/// reads, and the probe gets each body and each ending. The node advertises
/// one body per response, so the probe asks for one at a time.
#[test]
fn the_probe_gets_the_range_it_asks_for() {
    let scenario = BlockScenario {
        blocks: 4,
        node_bodies: 4,
        mirrors: Vec::new(),
        probe: true,
        control: vec![ControlStep {
            action: ControlAction::ProbeSend {
                frames: vec![
                    ProbeFrame::GetBlocks {
                        start_height: 1,
                        count: 1,
                    },
                    ProbeFrame::GetBlocks {
                        start_height: 2,
                        count: 1,
                    },
                ],
            },
            when: None,
        }],
        ..BlockScenario::default()
    };
    for input in inputs(8, 256) {
        let (report, ports) = run(&scenario, input);
        let received = ports.probe.received();
        let bodies: Vec<_> = received
            .iter()
            .filter_map(|message| match message {
                Some(BlockSyncMessage::Block(block)) => Some(block.hash()),
                _ => None,
            })
            .collect();
        let chain: Vec<_> = ports.chain[..2].iter().map(|block| block.hash()).collect();
        assert_eq!(bodies, chain, "{received:?}");
        let endings = received
            .iter()
            .filter(|message| {
                matches!(
                    message,
                    Some(BlockSyncMessage::BlocksDone { returned: 1, .. })
                )
            })
            .count();
        assert_eq!(endings, 2, "{received:?}");
        let facts = facts(&report);
        assert!(facts.iter().any(|fact| matches!(
            fact,
            StorageFact::BlockSubmitted {
                request: StorageRequest::ReadRange {
                    start_height: 2,
                    ..
                },
                ..
            }
        )));
        assert!(facts.iter().any(|fact| matches!(
            fact,
            StorageFact::BlockCompleted { outcome, .. } if outcome == "serve"
        )));
        assert!(
            facts
                .iter()
                .any(|fact| matches!(fact, StorageFact::ReadTaken { .. })),
            "serving takes the read's reply"
        );
        assert!(!ports.effects.links.borrow()[0].is_closed());
    }
}

/// An ending for a range nobody asked for breaks the stream-6 table: the
/// node's stream reader refuses it, the probe's connection closes with
/// `ordered_read_rejected`, and a step gated on that closure runs. The
/// mirror's link stays open, and the node still converges.
#[test]
fn an_unsolicited_probe_ending_closes_only_the_probe() {
    let scenario = BlockScenario {
        blocks: 8,
        mirrors: vec![MirrorSpec {
            dialer: Dialer::Mirror,
        }],
        probe: true,
        control: vec![
            ControlStep {
                action: ControlAction::ProbeSend {
                    frames: vec![ProbeFrame::BlocksDone {
                        start_height: 1,
                        returned: 1,
                    }],
                },
                when: None,
            },
            ControlStep {
                action: ControlAction::Grow {
                    len: 2,
                    node_first: false,
                },
                when: Some(Trigger::Closed(PROBE.to_owned())),
            },
        ],
        ..BlockScenario::default()
    };
    for input in inputs(8, 256) {
        let (_, ports) = run(&scenario, input);
        assert_eq!(ports.live.tip().height.0, 10, "the gated growth ran");
        assert_converged(&ports);
        let links = ports.effects.links.borrow();
        let probe = links
            .iter()
            .find(|link| link.peer == PROBE)
            .expect("the probe links");
        assert!(probe.is_closed());
        assert_eq!(probe.cause.get(), Some("ordered_read_rejected"));
        let mirror = links
            .iter()
            .find(|link| link.peer == "mirror-0")
            .expect("the mirror links");
        assert!(!mirror.is_closed(), "{links:?}");
    }
}
