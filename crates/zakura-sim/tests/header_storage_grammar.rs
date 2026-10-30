//! G1: `header-sync/storage`'s scenario prefix grammar. Every prefix of the
//! harness's length decodes to a bounded scenario, or to a defined
//! rejection when its version byte names another generator. A scenario's
//! canonical prefix decodes back to it, and each action and trigger has a
//! decoder case at the byte offsets the grammar documents.
#![cfg(zakura_sim)]

mod support;

use proptest::prelude::*;
use support::byte_inputs;
use zakura_sim::{
    explore::{classify, ExploreOutcome},
    harness::{
        self,
        header_node::{
            policy::{Allow, Hold, OpKind, Select, Trigger},
            Base, Chain, ControlAction, ControlStep, Fork, Policy,
        },
        header_sync::{
            grammar::{self, GENERATOR, LAYOUT, MAX_ACTIONS, MAX_HEADERS, MAX_MIRRORS},
            Dialer, MirrorSpec, Storage, StorageScenario,
        },
        scenario::{Rejection, MAX_INPUT_BYTES},
        Subsystem,
    },
    NullSink, Outcome,
};

/// The header's length, and each slot's, as the grammar documents them.
const HEADER: usize = 24;
const SLOT: usize = 10;

/// Opcodes and trigger kinds, as the grammar documents them.
const CLOSE: u8 = 0x01;
const RELINK: u8 = 0x02;
const GROW: u8 = 0x03;
const HOLD: u8 = 0x10;

/// The default scenario's canonical prefix with two mirrors, `mirror-1` at
/// A3 dialed by the node, and `slots` as its actions.
fn prefix(slots: &[[u8; SLOT]]) -> Vec<u8> {
    let mut prefix = grammar::encode(&StorageScenario::default()).unwrap();
    prefix[7] = 1;
    prefix[12..16].copy_from_slice(&[3, 0, 0, 1]);
    prefix[1] = u8::try_from(slots.len()).unwrap();
    for (index, slot) in slots.iter().enumerate() {
        prefix[HEADER + SLOT * index..][..SLOT].copy_from_slice(slot);
    }
    prefix
}

/// The two mirrors [`prefix`] writes.
fn two_mirrors() -> Vec<MirrorSpec> {
    vec![
        MirrorSpec {
            chain: Chain::main(5),
            dialer: Dialer::Mirror,
        },
        MirrorSpec {
            chain: Chain::main(3),
            dialer: Dialer::Node,
        },
    ]
}

/// The scenario [`prefix`] decodes to with these holds and control steps.
fn expected(holds: Vec<Hold>, controls: Vec<ControlStep>) -> StorageScenario {
    StorageScenario {
        mirrors: two_mirrors(),
        policy: Policy {
            holds,
            ..Policy::converging()
        },
        controls,
        ..StorageScenario::default()
    }
}

/// A slot with this opcode, operands and trigger.
fn slot(opcode: u8, operands: [u8; 4], trigger: [u8; 5]) -> [u8; SLOT] {
    let mut slot = [0; SLOT];
    slot[0] = opcode;
    slot[1..5].copy_from_slice(&operands);
    slot[5..].copy_from_slice(&trigger);
    slot
}

/// `peer`'s control step `action`, offered once `when` holds.
fn step(peer: &str, action: ControlAction, when: Option<Trigger>) -> ControlStep {
    ControlStep {
        peer: peer.to_owned(),
        action,
        when,
    }
}

/// Decodes `prefix`, which the grammar accepts.
fn decode(prefix: &[u8]) -> StorageScenario {
    grammar::decode(prefix).unwrap_or_else(|rejection| panic!("{rejection}"))
}

/// Checks that `scenario` stays inside the grammar's limits, and that its
/// canonical prefix decodes back to it and is its own canonical prefix.
fn assert_bounded_and_canonical(scenario: &StorageScenario) {
    assert!((1..=MAX_MIRRORS).contains(&scenario.mirrors.len()));
    assert!(scenario.policy.holds.len() + scenario.controls.len() <= MAX_ACTIONS);
    for chain in std::iter::once(&scenario.node).chain(scenario.mirrors.iter().map(|m| &m.chain)) {
        assert!(chain.main <= MAX_HEADERS);
        if let Some(fork) = chain.fork {
            assert!((1..=2).contains(&fork.branch) && (1..=MAX_HEADERS).contains(&fork.len));
        }
    }
    assert!(scenario.policy.bound_ms.is_none_or(|ms| ms <= 25_500));
    assert!(scenario
        .policy
        .holds
        .iter()
        .all(|h| h.deadline_ms.is_none()));
    let canonical = grammar::encode(scenario).unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(canonical.len(), Storage::SCENARIO_BYTES);
    assert_eq!(&decode(&canonical), scenario);
    assert_eq!(grammar::encode(&decode(&canonical)).unwrap(), canonical);
}

/// The prefix's layout is the documented one: a 24-byte header and 32
/// ten-byte slots, under generator 2, which the registry reports.
#[test]
fn the_layout_is_the_documented_one() {
    assert_eq!(
        (LAYOUT.header, LAYOUT.slot, LAYOUT.slots),
        (HEADER, SLOT, 32)
    );
    assert_eq!(Storage::SCENARIO_BYTES, 344);
    assert_eq!(GENERATOR, 2);
    let entry = harness::find(Storage::NAME).unwrap();
    assert_eq!((entry.generator, entry.scenario_bytes), (2, 344));
}

// The prefix leaves room for Plan bytes under the input cap.
const _: () = assert!(Storage::SCENARIO_BYTES < MAX_INPUT_BYTES);

/// The default scenario's canonical prefix is the documented bytes, and
/// decodes back to the default scenario.
#[test]
fn the_default_scenario_round_trips() {
    let canonical = grammar::encode(&StorageScenario::default()).unwrap();
    let mut expected = vec![0; 344];
    // Version 2, no actions, success-only within 5 s, the node at genesis,
    // and one mirror at A5 that dials.
    expected[..9].copy_from_slice(&[2, 0, 0, 50, 0, 0, 0, 0, 5]);
    assert_eq!(canonical, expected);
    assert_eq!(Storage::draw(&canonical), StorageScenario::default());
    assert_bounded_and_canonical(&StorageScenario::default());
}

/// Scenarios the M4 tests build by hand round-trip through their canonical
/// prefixes, and a full input is that prefix followed by the Plan bytes.
#[test]
fn hand_built_scenarios_round_trip() {
    let mirror = || "mirror-0".to_owned();
    let scenarios = [
        StorageScenario {
            controls: vec![
                step("mirror-0", ControlAction::Close, None),
                step("mirror-0", ControlAction::Relink, None),
            ],
            ..StorageScenario::default()
        },
        StorageScenario {
            policy: Policy {
                holds: vec![Hold {
                    select: Select::nth(OpKind::Apply, 1),
                    until: Trigger::VirtualTime(10_000),
                    then: vec![Allow::Engine],
                    deadline_ms: None,
                }],
                ..Policy::converging()
            },
            ..StorageScenario::default()
        },
        StorageScenario {
            controls: vec![
                step("mirror-0", ControlAction::Close, None),
                step(
                    "mirror-0",
                    ControlAction::Relink,
                    Some(Trigger::Count(1, Box::new(Trigger::Closed(mirror())))),
                ),
                step(
                    "mirror-0",
                    ControlAction::Close,
                    Some(Trigger::Count(2, Box::new(Trigger::Linked(mirror())))),
                ),
            ],
            ..StorageScenario::default()
        },
        StorageScenario {
            controls: vec![
                step("mirror-0", ControlAction::Grow { len: 1 }, None),
                step(
                    "mirror-0",
                    ControlAction::Close,
                    Some(Trigger::Grown(mirror())),
                ),
                step(
                    "mirror-0",
                    ControlAction::Relink,
                    Some(Trigger::Closed(mirror())),
                ),
            ],
            ..StorageScenario::default()
        },
        StorageScenario {
            node: Chain::main(3),
            mirrors: vec![
                MirrorSpec {
                    chain: Chain {
                        main: 1,
                        fork: Some(Fork { branch: 1, len: 4 }),
                    },
                    dialer: Dialer::Mirror,
                },
                MirrorSpec {
                    chain: Chain::default(),
                    dialer: Dialer::Node,
                },
            ],
            policy: Policy {
                base: Base::Any,
                bound_ms: None,
                holds: vec![Hold {
                    select: Select {
                        kind: Some(OpKind::Read),
                        nth: Some(64),
                        session: Some(36),
                        held: false,
                    },
                    until: Trigger::Count(16, Box::new(Trigger::CallerDropped(Select::held()))),
                    then: vec![Allow::Unavailable, Allow::Rejected],
                    deadline_ms: None,
                }],
            },
            controls: vec![step(
                "mirror-1",
                ControlAction::Close,
                Some(Trigger::Handled(Select::nth(OpKind::Prepare, 2))),
            )],
        },
    ];
    for scenario in &scenarios {
        assert_bounded_and_canonical(scenario);
        let input = grammar::input(scenario, &[7, 1, 9]).unwrap();
        assert_eq!(&input[344..], [7, 1, 9]);
        assert_eq!(&Storage::draw(&input[..344]), scenario);
    }
}

/// The encoder refuses what the grammar can't express, and names it.
#[test]
fn the_encoder_refuses_what_the_grammar_cannot_express() {
    let with_holds = |holds| StorageScenario {
        policy: Policy {
            holds,
            ..Policy::converging()
        },
        ..StorageScenario::default()
    };
    let hold = |until, then, deadline_ms| Hold {
        select: Select::default(),
        until,
        then,
        deadline_ms,
    };
    let unencodable = [
        (
            with_holds(vec![hold(
                Trigger::ReplyQueued(Select::default()),
                vec![],
                None,
            )]),
            "ReplyQueued",
        ),
        (
            with_holds(vec![hold(Trigger::VirtualTime(0), vec![], Some(1_000))]),
            "deadline",
        ),
        (
            with_holds(vec![hold(
                Trigger::VirtualTime(0),
                vec![Allow::Rejected, Allow::Engine],
                None,
            )]),
            "mask order",
        ),
        (
            with_holds(vec![hold(
                Trigger::VirtualTime(0),
                vec![Allow::Engine, Allow::Engine],
                None,
            )]),
            "mask order",
        ),
        (
            with_holds(vec![hold(Trigger::VirtualTime(150), vec![], None)]),
            "a virtual time",
        ),
        (
            with_holds(vec![hold(
                Trigger::All(vec![Trigger::VirtualTime(0)]),
                vec![],
                None,
            )]),
            "All",
        ),
        (
            with_holds(vec![hold(
                Trigger::Count(2, Box::new(Trigger::VirtualTime(0))),
                vec![],
                None,
            )]),
            "VirtualTime",
        ),
        (
            with_holds(vec![hold(
                Trigger::Closed("nobody".to_owned()),
                vec![],
                None,
            )]),
            "`nobody`",
        ),
        (
            with_holds(vec![
                hold(Trigger::VirtualTime(0), vec![], None);
                MAX_ACTIONS + 1
            ]),
            "more than 32 actions",
        ),
        (
            StorageScenario {
                mirrors: vec![StorageScenario::default().mirrors[0].clone(); 5],
                ..StorageScenario::default()
            },
            "5 mirrors",
        ),
        (
            StorageScenario {
                node: Chain::main(17),
                ..StorageScenario::default()
            },
            "a branch",
        ),
        (
            StorageScenario {
                node: Chain {
                    main: 1,
                    fork: Some(Fork { branch: 0, len: 1 }),
                },
                ..StorageScenario::default()
            },
            "branch 0",
        ),
        (
            StorageScenario {
                policy: Policy {
                    bound_ms: Some(150),
                    ..Policy::converging()
                },
                ..StorageScenario::default()
            },
            "the base bound",
        ),
        (
            StorageScenario {
                controls: vec![step("mirror-0", ControlAction::Grow { len: 17 }, None)],
                ..StorageScenario::default()
            },
            "a growth",
        ),
    ];
    for (scenario, reason) in unencodable {
        let error = grammar::encode(&scenario).expect_err(reason).to_string();
        assert!(error.contains(reason), "{error} doesn't name {reason}");
    }
}

/// Each opcode decodes to its action, at the documented operand offsets.
/// Operands reduce into their ranges, so every byte picks a defined value.
#[test]
fn each_opcode_has_a_decoder_case() {
    let none = [0; 5];
    let cases = [
        (
            slot(CLOSE, [5, 0, 0, 0], none),
            expected(vec![], vec![step("mirror-1", ControlAction::Close, None)]),
        ),
        (
            slot(RELINK, [2, 0, 0, 0], none),
            expected(vec![], vec![step("mirror-0", ControlAction::Relink, None)]),
        ),
        (
            slot(GROW, [1, 20, 0, 0], none),
            expected(
                vec![],
                vec![step("mirror-1", ControlAction::Grow { len: 5 }, None)],
            ),
        ),
        (
            // Apply (kind 7), nth 3, session 2, offering engine,
            // unavailable and rejected, opening at once. A hold's own
            // selection ignores the held bit, since `held` matches nothing
            // there.
            slot(HOLD, [0x87, 3, 2, 0b1_0101], none),
            expected(
                vec![Hold {
                    select: Select {
                        kind: Some(OpKind::Apply),
                        nth: Some(3),
                        session: Some(2),
                        held: false,
                    },
                    until: Trigger::VirtualTime(0),
                    then: vec![Allow::Engine, Allow::Unavailable, Allow::Rejected],
                    deadline_ms: None,
                }],
                vec![],
            ),
        ),
        (
            // Kind 9 reduces to none, nth 65 and session 37 to none, and
            // mask bits past the fifth are ignored.
            slot(HOLD, [9, 65, 37, 0b1110_0010], none),
            expected(
                vec![Hold {
                    select: Select::default(),
                    until: Trigger::VirtualTime(0),
                    then: vec![Allow::Timeout],
                    deadline_ms: None,
                }],
                vec![],
            ),
        ),
    ];
    for (slot, scenario) in cases {
        assert_eq!(decode(&prefix(&[slot])), scenario, "{slot:?}");
        assert_bounded_and_canonical(&scenario);
    }
}

/// Each trigger kind decodes to its trigger on a control step and on a
/// hold, at the documented operand offsets, and a nonzero count wraps an
/// event trigger.
#[test]
fn each_trigger_kind_has_a_decoder_case() {
    let locators = Select::nth(OpKind::Locator, 2);
    let mirror_1 = || "mirror-1".to_owned();
    let cases = [
        ([1, 0, 1, 2, 0], Trigger::Submitted(locators.clone())),
        ([2, 0, 1, 2, 0], Trigger::CallerDropped(locators.clone())),
        ([3, 0, 1, 2, 0], Trigger::Completed(locators.clone())),
        ([4, 0, 1, 2, 0], Trigger::ReplyTaken(locators.clone())),
        ([5, 0, 1, 2, 0], Trigger::Handled(locators.clone())),
        ([6, 0, 3, 0, 0], Trigger::Closed(mirror_1())),
        ([7, 0, 1, 0, 0], Trigger::Linked(mirror_1())),
        ([8, 0, 1, 0, 0], Trigger::Grown(mirror_1())),
        // 300 units of 100 ms, little-endian; the count is ignored.
        ([9, 4, 0x2c, 0x01, 0], Trigger::VirtualTime(30_000)),
        (
            [6, 3, 1, 0, 0],
            Trigger::Count(3, Box::new(Trigger::Closed(mirror_1()))),
        ),
        (
            [3, 16, 1, 2, 0],
            Trigger::Count(16, Box::new(Trigger::Completed(locators.clone()))),
        ),
    ];
    for (trigger, decoded) in cases {
        let control = slot(CLOSE, [0; 4], trigger);
        assert_eq!(
            decode(&prefix(&[control])),
            expected(
                vec![],
                vec![step(
                    "mirror-0",
                    ControlAction::Close,
                    Some(decoded.clone())
                )]
            ),
            "{trigger:?}"
        );
        let hold = slot(HOLD, [0; 4], trigger);
        let scenario = expected(
            vec![Hold {
                select: Select::default(),
                until: decoded,
                then: vec![],
                deadline_ms: None,
            }],
            vec![],
        );
        assert_eq!(decode(&prefix(&[hold])), scenario, "{trigger:?}");
        assert_bounded_and_canonical(&scenario);
    }
}

/// Only a hold's trigger reads a selection's held bit. A hold's own
/// selection and a control step's trigger ignore it, since `held` matches
/// nothing there, and the encoder refuses a `held` selection in either.
#[test]
fn the_held_bit_counts_only_in_a_hold_trigger() {
    // Submitted, with the held bit on the second Locator.
    let trigger = [1, 0, 0x81, 2, 0];
    let locators = Select::nth(OpKind::Locator, 2);
    let held = Select {
        held: true,
        ..locators.clone()
    };
    let scenario = decode(&prefix(&[
        slot(HOLD, [0x81, 2, 0, 1], trigger),
        slot(CLOSE, [0; 4], trigger),
    ]));
    assert_eq!(
        scenario,
        expected(
            vec![Hold {
                select: locators.clone(),
                until: Trigger::Submitted(held.clone()),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            vec![step(
                "mirror-0",
                ControlAction::Close,
                Some(Trigger::Submitted(locators.clone()))
            )],
        )
    );
    assert_bounded_and_canonical(&scenario);

    let held_select = StorageScenario {
        policy: Policy {
            holds: vec![Hold {
                select: held.clone(),
                until: Trigger::VirtualTime(0),
                then: vec![],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        ..StorageScenario::default()
    };
    let held_control = StorageScenario {
        controls: vec![step(
            "mirror-0",
            ControlAction::Close,
            Some(Trigger::Count(2, Box::new(Trigger::Completed(held)))),
        )],
        ..StorageScenario::default()
    };
    for scenario in [held_select, held_control] {
        let error = grammar::encode(&scenario).expect_err("held").to_string();
        assert!(error.contains("matches nothing"), "{error}");
    }
}

/// Unlisted opcodes and trigger kinds are inert: their slots decode to no
/// action, and the listed slots around them keep their order.
#[test]
fn unlisted_opcodes_and_trigger_kinds_are_inert() {
    let close = slot(CLOSE, [0; 4], [0; 5]);
    let relink = slot(RELINK, [0; 4], [0; 5]);
    let scenario = expected(
        vec![],
        vec![
            step("mirror-0", ControlAction::Close, None),
            step("mirror-0", ControlAction::Relink, None),
        ],
    );
    for opcode in (0..=u8::MAX).filter(|op| ![CLOSE, RELINK, GROW, HOLD].contains(op)) {
        let inert = slot(opcode, [1; 4], [1; 5]);
        assert_eq!(
            decode(&prefix(&[close, inert, relink])),
            scenario,
            "{opcode}"
        );
    }
    for kind in 10..=u8::MAX {
        for opcode in [CLOSE, HOLD] {
            let inert = slot(opcode, [0; 4], [kind, 0, 0, 0, 0]);
            assert_eq!(
                decode(&prefix(&[close, inert, relink])),
                scenario,
                "{opcode} with trigger kind {kind}"
            );
        }
    }
}

/// The header's fields reduce into their ranges: the action count modulo
/// 33, the mirror count to 1 to 4, and each chain's branches to 16
/// headers. A fork with no headers is no fork, whatever its branch byte.
#[test]
fn header_fields_reduce_into_their_ranges() {
    let mut bytes = prefix(&[slot(CLOSE, [0; 4], [0; 5]); 32]);
    bytes[1] = 33;
    assert_eq!(decode(&bytes).controls.len(), 0);
    bytes[1] = 34;
    assert_eq!(decode(&bytes).controls.len(), 1);
    bytes[1] = 32;
    assert_eq!(decode(&bytes).controls.len(), 32);
    bytes[2] = 0xff;
    bytes[3] = 255;
    bytes[4..7].copy_from_slice(&[17, 7, 33]);
    bytes[7] = 7;
    bytes[16..24].copy_from_slice(&[16, 1, 0, 2, 0, 0, 16, 0]);
    let scenario = decode(&bytes);
    assert_eq!(scenario.policy.base, Base::Any);
    assert_eq!(scenario.policy.bound_ms, Some(25_500));
    assert_eq!(
        scenario.node,
        Chain {
            main: 0,
            fork: Some(Fork { branch: 2, len: 16 })
        }
    );
    assert_eq!(
        scenario
            .mirrors
            .iter()
            .map(|mirror| (mirror.chain, mirror.dialer))
            .collect::<Vec<_>>(),
        [
            (Chain::main(5), Dialer::Mirror),
            (Chain::main(3), Dialer::Node),
            (Chain::main(16), Dialer::Mirror),
            (
                Chain {
                    main: 0,
                    fork: Some(Fork { branch: 1, len: 16 })
                },
                Dialer::Mirror
            ),
        ]
    );
    assert_bounded_and_canonical(&scenario);
}

/// A prefix whose version byte isn't [`GENERATOR`] is rejected, and draws
/// the default scenario, so an input written for another generator, or by
/// none, is never read under this one. One of the wrong length is
/// rejected too.
#[test]
fn generator_changes_isolate_corpora() {
    let canonical = prefix(&[slot(CLOSE, [0; 4], [0; 5])]);
    assert_eq!(canonical[0], 2);
    for version in (0..=u8::MAX).filter(|version| u32::from(*version) != GENERATOR) {
        let mut other = canonical.clone();
        other[0] = version;
        assert_eq!(
            grammar::decode(&other),
            Err(Rejection::Version { found: version })
        );
        assert_eq!(Storage::draw(&other), StorageScenario::default());
    }
    for len in [0, 1, 343, 345] {
        assert_eq!(
            grammar::decode(&vec![2; len]),
            Err(Rejection::Length { found: len })
        );
    }
}

/// `draw` takes exactly the harness's prefix, as `run_bytes` cuts it, so
/// a prefix of another length is a caller's bug, not the default scenario.
#[test]
#[should_panic(expected = "exactly SCENARIO_BYTES")]
fn draw_refuses_a_prefix_of_another_length() {
    Storage::draw(&[2; 343]);
}

/// Empty and truncated inputs draw defined scenarios, as `run_bytes`
/// zero-pads them. The empty input draws the default scenario. Any
/// truncation keeps the version byte, and decodes to the header the kept
/// bytes give, with each cut slot inert.
#[test]
fn empty_and_truncated_inputs_draw_defined_scenarios() {
    let full = prefix(&[
        slot(GROW, [1, 3, 0, 0], [0; 5]),
        slot(HOLD, [6, 1, 0, 1], [9, 0, 10, 0, 0]),
        slot(CLOSE, [1, 0, 0, 0], [8, 0, 1, 0, 0]),
    ]);
    for len in 0..=full.len() {
        let mut padded = full[..len].to_vec();
        padded.resize(full.len(), 0);
        let drawn = Storage::draw(&padded);
        if len == 0 {
            assert_eq!(drawn, StorageScenario::default());
            continue;
        }
        let actions = drawn.policy.holds.len() + drawn.controls.len();
        let kept = len.saturating_sub(HEADER).div_ceil(SLOT).min(3);
        assert!(actions <= kept, "{len} bytes kept {actions} actions");
        assert_bounded_and_canonical(&drawn);
    }
    assert_eq!(
        Storage::draw(&full),
        expected(
            vec![Hold {
                select: Select::nth(OpKind::Prepare, 1),
                until: Trigger::VirtualTime(1_000),
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            vec![
                step("mirror-1", ControlAction::Grow { len: 4 }, None),
                step(
                    "mirror-1",
                    ControlAction::Close,
                    Some(Trigger::Grown("mirror-1".to_owned()))
                ),
            ]
        )
    );
}

/// `bytes` as a prefix the grammar accepts: the version byte set, and each
/// slot's opcode and trigger kind mapped onto a listed one, an unlisted
/// one, or none, so most slots hold actions.
fn accepted(mut bytes: Vec<u8>) -> Vec<u8> {
    bytes.resize(Storage::SCENARIO_BYTES, 0);
    bytes[0] = 2;
    for slot in bytes[HEADER..].as_chunks_mut::<SLOT>().0 {
        slot[0] = [CLOSE, RELINK, GROW, HOLD, HOLD, 0xff][usize::from(slot[0]) % 6];
        slot[5] %= 11;
    }
    bytes
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Arbitrary prefixes decode to bounded scenarios, whose canonical
    /// prefixes decode back to them, or are rejected for their version
    /// byte. A third of the cases keep the raw bytes, which are mostly
    /// rejected. A third set the version byte, and their slots then mostly
    /// hit inert opcodes. The rest also map each slot onto a listed opcode.
    #[test]
    fn arbitrary_prefixes_decode_bounded_and_canonical(
        mut bytes in proptest::collection::vec(any::<u8>(), 344),
        mode in 0..3u8,
    ) {
        match mode {
            0 => {}
            1 => bytes[0] = 2,
            _ => bytes = accepted(bytes),
        }
        match grammar::decode(&bytes) {
            Ok(scenario) => assert_bounded_and_canonical(&scenario),
            Err(rejection) => {
                prop_assert_eq!(rejection, Rejection::Version { found: bytes[0] });
                prop_assert_eq!(Storage::draw(&bytes), StorageScenario::default());
            }
        }
    }
}

/// Generated inputs run to a defined end: each prepares, runs within its
/// step limits, ends at its budget or idle with nothing leaked, and the
/// explorer classifies it clean. Some run takes a control step.
#[test]
fn generated_inputs_run_to_a_defined_end() {
    let entry = harness::find(Storage::NAME).unwrap();
    let config = Storage::config(&StorageScenario::default());
    let mut picked = 0;
    for bytes in byte_inputs(32, 1_024) {
        let mut input = accepted(bytes.clone());
        input.extend_from_slice(bytes.get(344..).unwrap_or_default());
        let scenario = Storage::draw(&input[..344]);
        let run = (entry.run_bytes)(&input, &mut NullSink, None);
        let report = run
            .report()
            .unwrap_or_else(|| panic!("{scenario:?}: {:?}", run.result));
        assert!(
            matches!(report.end.outcome, Outcome::Budget | Outcome::Idle { .. }),
            "{scenario:?}: {:?}",
            report.end
        );
        assert!(report.end.steps <= config.budget + config.drain_bound);
        assert_eq!(run.leaked(), 0, "{scenario:?}");
        assert_eq!(
            classify(entry, &run).outcome,
            ExploreOutcome::Clean,
            "{scenario:?}"
        );
        picked += report
            .picks
            .iter()
            .filter(|pick| pick.identity.starts_with("peer:mirror-"))
            .count();
    }
    assert!(picked > 0, "no run took a control step");
}
