//! The `handshake` seeds: one `handshake/wire` seed per control, prelude,
//! QUIC and TLS guard, the wire witnesses `tests/handshake.rs` runs, and the
//! `handshake/counts` op tapes.
//!
//! Each `handshake/wire` seed's input is a few decoder bytes, zero-padded to
//! the decoder's 32. Its scenario is built from the scenario's own fields,
//! not by the decoder, and a test checks the input decodes to it.

use super::Seed;
use crate::{
    harness::{
        handshake_wire::{
            CapabilityFlip, Scenario, Wire, ACK_FAULTS, HELLO_FAULTS, PRELUDE_FAULTS,
        },
        LinkEntry, LinkSpec, Subsystem,
    },
    peer::{
        handshake::options::{ControlFault, HandshakeSpec, PreludeFault},
        PeerAction, PeerSpec, NODE_ADDR,
    },
    table::handshake::PRELUDE,
    witness::Expect,
};

/// The source of the guard seeds, whose scenarios this module builds.
const GUARDS: &str = "crates/zakura-sim/src/seed/handshake.rs";

/// The source of the wire witnesses and the owners op tape.
const TESTS: &str = "crates/zakura-sim/tests/handshake.rs";

/// The source of the pending and cadence op tapes.
const COUNTS: &str = "crates/zakura-sim/src/harness/handshake_counts.rs";

/// Every `handshake` seed.
pub(super) fn seeds() -> Vec<Seed> {
    let wire = wire_seeds().into_iter().map(|(seed, _)| seed);
    let counts = |name: &str, source: &str, input: Vec<u8>| {
        Seed::new(name, "handshake/counts", source, input)
    };
    wire.chain([
        // One host's connection moves through QUIC, control and registration.
        counts(
            "handshake-counts-owners",
            TESTS,
            vec![0, 0, 0, 2, 0, 0, 3, 0, 0, 5, 0, 0],
        )
        .witness("handshake/counts-match-permits", Expect::Holds),
        // Inbound TLS owners contend with outbound control claims for the
        // same budget.
        counts("handshake-counts-pending", COUNTS, vec![128, 1, 128])
            .witness("handshake/pending-bounded", Expect::Holds),
        // Thirty-two immediate failures exhaust one address's sixteen-token
        // burst.
        counts("handshake-counts-cadence", COUNTS, [8, 0, 0].repeat(32))
            .witness("handshake/attempts-within-cadence", Expect::Holds)
            .witness("handshake/cadence-never-refuses-conformant", Expect::Holds),
    ])
    .collect()
}

/// Every `handshake/wire` seed, with the scenario its input decodes to.
fn wire_seeds() -> Vec<(Seed, Scenario)> {
    let mut seeds = guard_seeds();
    seeds.extend(witness_seeds());
    seeds
}

/// A `handshake/wire` seed named `name` whose input is `prefix`,
/// zero-padded to the decoder's 32 bytes, and whose run holds each of
/// `holds`.
fn wire(
    name: &str,
    source: &str,
    prefix: &[u8],
    scenario: Scenario,
    holds: &[&str],
) -> (Seed, Scenario) {
    let mut input = vec![0; Wire::SCENARIO_BYTES];
    input[..prefix.len()].copy_from_slice(prefix);
    let seed = holds.iter().fold(
        Seed::new(name, Wire::NAME, source, input),
        |seed, property| seed.witness(property, Expect::Holds),
    );
    (seed, scenario)
}

/// A one-peer scenario whose peer runs `handshake`.
fn one_peer(peer: PeerSpec, handshake: HandshakeSpec) -> Scenario {
    Scenario {
        peers: vec![PeerSpec {
            handshake: Some(handshake),
            ..peer
        }],
        ..Scenario::default()
    }
}

/// `count` dialing peers on zero-delay links, each 500 ms from the node,
/// each running `handshake`.
fn quic_pressure(count: usize, handshake: Option<HandshakeSpec>) -> Scenario {
    let mut scenario = Scenario::default();
    scenario.net.default_link = LinkSpec {
        min_delay_ms: 0,
        max_delay_ms: 0,
        reorder: false,
        loss_budget: 0,
    };
    scenario.peers = vec![
        PeerSpec {
            handshake,
            ..PeerSpec::dialing()
        };
        count
    ];
    for (index, peer) in scenario.peers.iter().enumerate() {
        scenario.net.links.push(LinkEntry {
            src: NODE_ADDR,
            dst: peer.address(index, 0),
            model: LinkSpec {
                min_delay_ms: 500,
                max_delay_ms: 500,
                ..scenario.net.default_link
            },
        });
    }
    scenario
}

/// One seed per control, prelude, QUIC and TLS guard, so the
/// `handshake_wire` fuzz target starts from inputs that apply each of the
/// harness's own properties.
fn guard_seeds() -> Vec<(Seed, Scenario)> {
    let mut seeds = Vec::new();
    for (index, &fault) in HELLO_FAULTS.iter().enumerate().skip(1) {
        let label = fault.label();
        seeds.push(wire(
            &format!("handshake-hello-{label}"),
            GUARDS,
            &[0, u8::try_from(index).expect("the hello faults fit a byte")],
            one_peer(
                PeerSpec::dialing(),
                HandshakeSpec {
                    control: fault,
                    ..HandshakeSpec::default()
                },
            ),
            &[
                &format!("handshake/hello/{label}-refused"),
                "handshake/hello/refusal-releases-slot",
            ],
        ));
    }
    for (index, &fault) in ACK_FAULTS.iter().enumerate().skip(1) {
        let label = fault.label();
        seeds.push(wire(
            &format!("handshake-ack-{label}"),
            GUARDS,
            &[1, u8::try_from(index).expect("the ack faults fit a byte")],
            one_peer(
                PeerSpec::listening(),
                HandshakeSpec {
                    control: fault,
                    ..HandshakeSpec::default()
                },
            ),
            &[
                &format!("handshake/ack/{label}-refused"),
                "handshake/ack/refusal-releases-slot",
            ],
        ));
    }
    for (index, &fault) in PRELUDE_FAULTS.iter().enumerate().skip(1) {
        let label = fault.label();
        let effect = PRELUDE
            .iter()
            .find(|(name, _)| *name == label)
            .expect("each prelude fault has a table row")
            .1;
        let mut peer = if fault == PreludeFault::IneligibleOpener {
            PeerSpec::listening()
        } else {
            PeerSpec::dialing()
        };
        if fault == PreludeFault::Version {
            peer.capabilities |= (1 << 60) | (1 << 61);
        }
        if fault == PreludeFault::MissingRequestId {
            peer.capabilities |= 1 << 62;
        }
        seeds.push(wire(
            &format!("handshake-prelude-{label}"),
            GUARDS,
            &[
                2,
                u8::try_from(index).expect("the prelude faults fit a byte"),
            ],
            one_peer(
                peer,
                HandshakeSpec {
                    prelude: fault,
                    ..HandshakeSpec::default()
                },
            ),
            &[&format!("handshake/prelude/{label}-{effect}")],
        ));
    }
    let mut pending = quic_pressure(
        2,
        Some(HandshakeSpec {
            bind_ip: Some("192.0.2.1".parse().expect("a fixture IP")),
            ..HandshakeSpec::default()
        }),
    );
    pending.quic_pending_per_ip = Some(1);
    seeds.push(wire(
        "handshake-quic-pending",
        GUARDS,
        &[254, 0, 0],
        pending,
        &["handshake/quic/pending-per-address-bounded"],
    ));
    let mut retry = quic_pressure(2, None);
    retry.quic_retry_threshold = Some(0);
    seeds.push(wire(
        "handshake-quic-retry",
        GUARDS,
        &[254, 1, 0],
        retry,
        &["handshake/quic/retry-before-unvalidated-accept"],
    ));
    seeds.push(wire(
        "handshake-quic-alpn",
        GUARDS,
        &[253, 0],
        one_peer(
            PeerSpec::dialing(),
            HandshakeSpec {
                alpn: Some(b"handshake-test/unsupported".to_vec()),
                ..HandshakeSpec::default()
            },
        ),
        &["handshake/quic/alpn-required"],
    ));
    seeds.push(wire(
        "handshake-quic-identity",
        GUARDS,
        &[253, 1],
        one_peer(
            PeerSpec::listening(),
            HandshakeSpec {
                advertised_identity: Some(1),
                ..HandshakeSpec::default()
            },
        ),
        &["handshake/quic/identity-required"],
    ));
    seeds
}

/// The wire witnesses `tests/handshake.rs` runs.
fn witness_seeds() -> Vec<(Seed, Scenario)> {
    // Enable header sync after the handshake snapshot and before
    // registration.
    let mut flip = one_peer(
        PeerSpec::dialing(),
        HandshakeSpec {
            dial_delay_ms: 1,
            ..HandshakeSpec::default()
        },
    );
    flip.capability_flip = Some(CapabilityFlip::BeforeRegistration);
    let mut partial = PeerSpec::dialing();
    partial.capabilities |= 1 << 59;
    partial.handshake = Some(HandshakeSpec::default());
    partial.script = vec![
        PeerAction::Offer {
            kind: 902,
            version: 1,
            wire_id: 1,
        },
        PeerAction::Close,
    ];
    let mut churn = Scenario::default();
    churn.net.default_link.min_delay_ms = 0;
    churn.net.default_link.max_delay_ms = 0;
    churn.peers[0].handshake = Some(HandshakeSpec::default());
    churn.peers[0].script = vec![
        PeerAction::Open {
            kind: u16::MAX,
            version: 1
        };
        64
    ];
    churn.peers[0].script.push(PeerAction::Close);
    // Two nine-second steps share the ten-second control deadline.
    let mut slow = Scenario::default();
    slow.peers[0].handshake = Some(HandshakeSpec {
        length_delay_ms: 9_000,
        payload_delay_ms: 9_000,
        ..HandshakeSpec::default()
    });
    vec![
        wire(
            "handshake-capability-before",
            TESTS,
            &[6, 1],
            flip,
            &["session/renegotiation-covers-inflight-handshakes"],
        ),
        wire(
            "handshake-partial-session",
            TESTS,
            &[9, 3],
            Scenario {
                peers: vec![partial; 3],
                ..Scenario::default()
            },
            &["session/capacity-conserved", "session/setup-slot-bounded"],
        ),
        wire(
            "handshake-stream-churn",
            TESTS,
            &[8, 64],
            churn,
            &["session/stream-opens-bounded"],
        ),
        wire(
            "handshake-installed-services",
            TESTS,
            &[1],
            one_peer(PeerSpec::listening(), HandshakeSpec::default()),
            &["session/installed-services-authorized"],
        ),
        wire(
            "handshake-slow-control",
            TESTS,
            &[3, 0, 0xe1, 0xe1],
            slow,
            &["handshake/finishes-in-time"],
        ),
        wire(
            "handshake-cadence-burst",
            TESTS,
            &[5, 31, 0, 1, 1],
            cadence(false),
            &["handshake/attempts-within-cadence"],
        ),
        wire(
            "handshake-cadence-conformant",
            TESTS,
            &[5, 1, 0, 1, 3, 15, 15],
            cadence(true),
            &["handshake/cadence-never-refuses-conformant"],
        ),
    ]
}

/// Dialers from one address whose control handshakes fail on the network
/// byte: sixteen conformant ones, 60 ms apart, each retrying once after a
/// second, or one that retries 31 times at once.
fn cadence(conformant: bool) -> Scenario {
    let mut scenario = Scenario::default();
    scenario.net.default_link.min_delay_ms = 0;
    scenario.net.default_link.max_delay_ms = 0;
    scenario.net.default_link.reorder = false;
    scenario.peers = (0..if conformant { 16 } else { 1 })
        .map(|index| PeerSpec {
            handshake: Some(HandshakeSpec {
                control: ControlFault::Network,
                bind_ip: Some("192.0.2.1".parse().expect("a fixture IP")),
                retries: if conformant { 1 } else { 31 },
                retry_delay_ms: if conformant { 1_000 } else { 0 },
                dial_delay_ms: index * 60,
                ..HandshakeSpec::default()
            }),
            ..PeerSpec::dialing()
        })
        .collect();
    scenario
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{harness, seed::scenario_sha256};

    /// Each `handshake/wire` seed's input decodes to the scenario built
    /// from its fields.
    #[test]
    fn wire_seeds_decode_to_their_scenarios() {
        let entry = harness::find(Wire::NAME).expect("the harness is registered");
        for (seed, scenario) in wire_seeds() {
            let intended = serde_json::to_value(scenario).expect("scenarios serialize");
            assert_eq!(
                scenario_sha256(&entry.scenario(&seed.input)),
                scenario_sha256(&intended),
                "{}",
                seed.name
            );
        }
    }
}
