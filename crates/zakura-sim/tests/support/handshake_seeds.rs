//! The curated `handshake/wire` seeds: one per control, prelude, QUIC and
//! TLS guard, so the `handshake_wire` fuzz target starts from inputs that
//! apply each of the harness's own properties.
//!
//! Each seed's scenario is built from the scenario's own fields, not by the
//! decoder. `tests/seeds.rs` checks each committed seed decodes to it.

use zakura_sim::{
    harness::{
        handshake_wire::{CapabilityFlip, Scenario, ACK_FAULTS, HELLO_FAULTS, PRELUDE_FAULTS},
        LinkEntry, LinkSpec,
    },
    peer::{
        handshake::options::{HandshakeSpec, PreludeFault},
        PeerSpec, NODE_ADDR,
    },
    table::handshake::PRELUDE,
};

/// One curated seed.
pub struct HandshakeSeed {
    /// The fixture's file stem.
    pub stem: String,
    /// The decoder input that draws `scenario`.
    pub input: Vec<u8>,
    /// The scenario, built from its fields.
    pub scenario: Scenario,
    /// The properties the seed's run must apply and hold.
    pub holds: Vec<String>,
}

/// A seed whose input is `prefix`, zero-padded to the decoder's 32 bytes.
fn seed(stem: String, prefix: &[u8], scenario: Scenario, holds: Vec<String>) -> HandshakeSeed {
    let mut input = vec![0; 32];
    input[..prefix.len()].copy_from_slice(prefix);
    HandshakeSeed {
        stem,
        input,
        scenario,
        holds,
    }
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

/// Every curated seed.
pub fn handshake_seeds() -> Vec<HandshakeSeed> {
    let mut seeds = Vec::new();
    for (index, &fault) in HELLO_FAULTS.iter().enumerate().skip(1) {
        let label = fault.label();
        seeds.push(seed(
            format!("handshake-hello-{label}"),
            &[0, u8::try_from(index).expect("the hello faults fit a byte")],
            one_peer(
                PeerSpec::dialing(),
                HandshakeSpec {
                    control: fault,
                    ..HandshakeSpec::default()
                },
            ),
            vec![
                format!("handshake/hello/{label}-refused"),
                "handshake/hello/refusal-releases-slot".to_owned(),
            ],
        ));
    }
    for (index, &fault) in ACK_FAULTS.iter().enumerate().skip(1) {
        let label = fault.label();
        seeds.push(seed(
            format!("handshake-ack-{label}"),
            &[1, u8::try_from(index).expect("the ack faults fit a byte")],
            one_peer(
                PeerSpec::listening(),
                HandshakeSpec {
                    control: fault,
                    ..HandshakeSpec::default()
                },
            ),
            vec![
                format!("handshake/ack/{label}-refused"),
                "handshake/ack/refusal-releases-slot".to_owned(),
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
        seeds.push(seed(
            format!("handshake-prelude-{label}"),
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
            vec![format!("handshake/prelude/{label}-{effect}")],
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
    seeds.push(seed(
        "handshake-quic-pending".to_owned(),
        &[254, 0, 0],
        pending,
        vec!["handshake/quic/pending-per-address-bounded".to_owned()],
    ));
    let mut retry = quic_pressure(2, None);
    retry.quic_retry_threshold = Some(0);
    seeds.push(seed(
        "handshake-quic-retry".to_owned(),
        &[254, 1, 0],
        retry,
        vec!["handshake/quic/retry-before-unvalidated-accept".to_owned()],
    ));
    seeds.push(seed(
        "handshake-quic-alpn".to_owned(),
        &[253, 0],
        one_peer(
            PeerSpec::dialing(),
            HandshakeSpec {
                alpn: Some(b"handshake-test/unsupported".to_vec()),
                ..HandshakeSpec::default()
            },
        ),
        vec!["handshake/quic/alpn-required".to_owned()],
    ));
    seeds.push(seed(
        "handshake-quic-identity".to_owned(),
        &[253, 1],
        one_peer(
            PeerSpec::listening(),
            HandshakeSpec {
                advertised_identity: Some(1),
                ..HandshakeSpec::default()
            },
        ),
        vec!["handshake/quic/identity-required".to_owned()],
    ));
    let mut flip = one_peer(
        PeerSpec::dialing(),
        HandshakeSpec {
            dial_delay_ms: 1,
            ..HandshakeSpec::default()
        },
    );
    flip.capability_flip = Some(CapabilityFlip::BeforeRegistration);
    seeds.push(seed(
        "handshake-capability-before".to_owned(),
        &[6, 1],
        flip,
        vec!["session/renegotiation-covers-inflight-handshakes".to_owned()],
    ));
    seeds
}
