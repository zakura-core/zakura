//! Control-handshake labels keyed to production's validation errors.

use zakura_network::zakura::ZakuraValidationError;

/// Maps every production validation error to its stable oracle label.
pub fn validation_label(error: ZakuraValidationError) -> &'static str {
    match error {
        ZakuraValidationError::UnsupportedVersion(_) => "bad-version",
        ZakuraValidationError::IncompatibleZakuraProtocol => "bad-protocol",
        ZakuraValidationError::WrongNetwork => "wrong-network",
        ZakuraValidationError::WrongChain => "wrong-chain",
        ZakuraValidationError::MissingRequiredCapability => "missing-capability",
        ZakuraValidationError::ResourceLimit => "bad-limits",
        ZakuraValidationError::IdentityMismatch => "identity-mismatch",
        ZakuraValidationError::TranscriptMismatch => "nonzero-native-transcript",
        ZakuraValidationError::UpgradeNonceMismatch => "nonzero-native-nonce",
        ZakuraValidationError::MalformedControl => "malformed-control",
        ZakuraValidationError::ControlNonceMismatch => "bad-nonce-echo",
        ZakuraValidationError::UnofferedCapability => "unoffered-capability",
    }
}

/// Maps every production stream-setup refusal to its table label.
pub fn prelude_label(rejection: zakura_network::zakura::PreludeRejection) -> &'static str {
    use zakura_network::zakura::PreludeRejection;
    match rejection {
        PreludeRejection::BadPrelude => "bad-magic",
        PreludeRejection::UnknownKind => "unknown-kind",
        PreludeRejection::UnnegotiatedCapability => "unnegotiated-capability",
        PreludeRejection::UnselectedVersion => "unselected-version",
        PreludeRejection::UnexpectedRequestId => "unexpected-request-id",
        PreludeRejection::RequestWithoutId => "request-without-id",
        PreludeRejection::UndersizedFrameCap => "undersized-cap",
        PreludeRejection::IneligibleOpener => "ineligible-opener",
    }
}

/// The prelude row's stream resets and fault closes.
pub const PRELUDE: &[(&str, &str)] = &[
    ("bad-magic", "resets"),
    ("unknown-kind", "resets"),
    ("unnegotiated-capability", "resets"),
    ("unselected-version", "resets"),
    ("undersized-cap", "closes"),
    ("unexpected-request-id", "closes"),
    ("request-without-id", "closes"),
    ("ineligible-opener", "closes"),
];

use crate::{
    observe::TransportFact,
    property::{
        CheckCtx, HasView, Kind, LivenessBound, ObligationKey, Property, PropertyId, Source,
        Status, Subject, TriggerHistory, Verdict,
    },
    Observation,
};

/// Labels for the hello row. Each label changes one guard's input.
pub const HELLO: &[&str] = &[
    "bad-magic",
    "bad-version",
    "bad-protocol",
    "bad-path",
    "bad-role",
    "wrong-network",
    "wrong-chain",
    "identity-mismatch",
    "missing-capability",
    "bad-frame-limit",
    "bad-message-limit",
    "bad-stream-limit",
    "bad-queue-limit",
    "bad-idle-limit",
    "nonzero-initiator-nonce",
    "nonzero-responder-nonce",
    "nonzero-native-transcript",
    "oversize",
];
/// Labels for the ack row, including the five negotiated limit fields.
pub const ACK: &[&str] = &[
    "bad-magic",
    "bad-version",
    "bad-protocol",
    "bad-nonce-echo",
    "unoffered-capability",
    "bad-frame-limit",
    "bad-message-limit",
    "bad-stream-limit",
    "bad-queue-limit",
    "bad-idle-limit",
    "limits-above-request",
    "oversize",
];

/// Generates the control rows through the table property's usual registry and obligations.
pub fn properties<S: HasView>() -> Vec<Property<S>> {
    let mut properties = Vec::new();
    for (id, claim) in [
        (
            "handshake/quic/pending-per-address-bounded",
            "QUIC refuses an attempt whose address already holds its configured pending limit.",
        ),
        (
            "handshake/quic/retry-before-unvalidated-accept",
            "QUIC never accepts an unvalidated source at or above its Retry threshold.",
        ),
    ] {
        properties.push(Property {
            id: PropertyId::new(id),
            claim,
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::Closes],
            status: Status::Active,
            check: check_quic_admission::<S>,
            witness: Some(TriggerHistory::PropertyLog(quic_admission_reached)),
        });
    }
    properties.push(Property {
        id: PropertyId::new("handshake/quic/finishes-in-time"),
        claim: "An inbound TLS attempt finishes within the ten-second timeout and three-second shutdown allowance.",
        kind: Kind::Liveness { bound: LivenessBound::VirtualTime(std::time::Duration::from_secs(13)) },
        assumes: &[], needs: &[Source::Closes], status: Status::Active,
        check: check_quic_deadline::<S>,
        witness: Some(TriggerHistory::Log(|log| log.entries().iter().any(|entry| matches!(
            entry.obs, Observation::Transport { event: TransportFact::QuicTlsStarted { deadline_ms: Some(10_000), .. }, .. }
        )))),
    });
    for (id, claim) in [
        ("session/capacity-conserved", "Live reservations, including retained owners, account for every direction and setup permit."),
        ("session/setup-slot-bounded", "Inbound partial sessions leave the outbound setup slot available and release setup permits within the prelude deadline.")
    ] {
        properties.push(Property {
            id: PropertyId::new(id), claim, kind: Kind::Safety,
            assumes: &[], needs: &[Source::Closes], status: Status::Active,
            check: check_session_capacity::<S>,
            witness: Some(TriggerHistory::Log(if id == "session/capacity-conserved" {
                |log| log.entries().iter().any(|entry| matches!(entry.obs,
                    Observation::Transport { event: TransportFact::SessionCapacity { held, .. }, .. } if held[0] > 0 || held[1] > 0
                ))
            } else {
                |log| log.entries().iter().any(|entry| matches!(entry.obs,
                    Observation::Transport { event: TransportFact::SessionCapacity { held, .. }, .. } if held[2] > 0
                ))
            })),
        });
    }
    for (id, claim) in [
        (
            "handshake/attempts-within-cadence",
            "Accepted TLS attempts consume only initial, refilled, or refunded address tokens.",
        ),
        (
            "handshake/cadence-never-refuses-conformant",
            "The cadence never refuses an attempt whose address has a token.",
        ),
    ] {
        properties.push(Property {
            id: PropertyId::new(id),
            claim,
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::Closes],
            status: Status::Active,
            check: check_cadence::<S>,
            witness: Some(TriggerHistory::Log(
                if id == "handshake/attempts-within-cadence" {
                    cadence_burst_reached
                } else {
                    cadence_attempt_reached
                },
            )),
        });
    }
    properties.push(Property {
        id: PropertyId::new("session/stream-opens-bounded"),
        claim: "Setup and installed stream owners stay within the negotiated limit; inbound setup starts stay within the configured burst and refill rate, and teardown returns every stream permit.",
        kind: Kind::Safety,
        assumes: &[], needs: &[Source::Closes], status: Status::Active,
        check: check_stream_opens::<S>,
        witness: Some(TriggerHistory::Log(|log| log.entries().iter().any(|entry| matches!(entry.obs,
            Observation::Transport { event: TransportFact::StreamSetupStarted { .. }, .. }
        )))),
    });
    properties.push(Property {
        id: PropertyId::new("session/renegotiation-covers-inflight-handshakes"),
        claim: "Capability enablement removes every registration whose handshake captured the old capability mask.",
        kind: Kind::Liveness { bound: LivenessBound::BeforeQuiescence },
        assumes: &[], needs: &[Source::Closes], status: Status::Active,
        check: check_epoch::<S>, witness: Some(TriggerHistory::Log(capability_flip_reached)),
    });
    for (id, claim) in [
        (
            "handshake/pending-bounded",
            "QUIC and control handshakes stay within total, inbound, and address bounds.",
        ),
        (
            "handshake/connections-per-address-bounded",
            "Registered connections stay within the configured address cap.",
        ),
        (
            "handshake/counts-match-permits",
            "Admission machine charges equal the live handshake and connection semaphore permits.",
        ),
    ] {
        properties.push(Property {
            id: PropertyId::new(id),
            claim,
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::Closes],
            status: Status::Active,
            check: check_counts::<S>,
            witness: Some(TriggerHistory::PropertyLog(admission_reached)),
        });
    }
    properties.push(Property {
        id: PropertyId::new("handshake/dial-reserve-kept"),
        claim: "Inbound load preserves each dial reserve and cannot refuse a dial within that reserve.",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::Closes],
        status: Status::Active,
        check: check_reserve::<S>,
        witness: Some(TriggerHistory::PropertyLog(admission_reached)),
    });
    properties.push(Property {
        id: PropertyId::new("handshake/finishes-in-time"),
        claim: "An inbound control handshake releases its claim within ten seconds plus the shutdown drain.",
        kind: Kind::Liveness {
            bound: LivenessBound::VirtualTime(std::time::Duration::from_secs(13)),
        },
        assumes: &[],
        needs: &[Source::Closes],
        status: Status::Active,
        check: check_deadline::<S>,
        witness: Some(TriggerHistory::Log(paced_control_reached)),
    });
    for (row, labels) in [("hello", HELLO), ("ack", ACK)] {
        for label in labels {
            properties.push(Property {
                id: PropertyId::generated(format!("handshake/{row}/{label}-refused"))
                    .expect("handshake labels follow the ID grammar"),
                claim: "The node refuses the malformed control exchange before quiescence.",
                kind: Kind::Liveness {
                    bound: LivenessBound::BeforeQuiescence,
                },
                assumes: &[],
                needs: &[Source::Closes],
                status: Status::Active,
                check: check_refused::<S>,
                witness: Some(TriggerHistory::PropertyLog(handshake_label_reached)),
            });
        }
        properties.push(Property {
            id: PropertyId::generated(format!("handshake/{row}/conformant-never-refused"))
                .expect("handshake rows follow the ID grammar"),
            claim:
                "The node accepts a conformant control exchange unless local capacity refuses it.",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::Closes],
            status: Status::Active,
            check: check_conformant::<S>,
            witness: Some(TriggerHistory::PropertyLog(handshake_label_reached)),
        });
    }
    for row in ["hello", "ack"] {
        properties.push(Property {
            id: PropertyId::generated(format!("handshake/{row}/refusal-releases-slot")).expect("handshake rows follow the ID grammar"),
            claim: "A refused control handshake releases its IP charge, handler permits, and QUIC slot within the shutdown drain.",
            kind: Kind::Liveness {
                bound: LivenessBound::VirtualTime(std::time::Duration::from_secs(3)),
            },
            assumes: &[],
            needs: &[Source::Closes],
            status: Status::Active,
            check: check_released::<S>,
            witness: Some(TriggerHistory::PropertyLog(handshake_refusal_reached)),
        });
    }
    properties.push(Property {
        id: PropertyId::new("handshake/prelude/conformant-never-refused"),
        claim: "The prelude guards accept a conformant negotiated stream without a fault or protocol reset.",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::Closes],
        status: Status::Active,
        check: check_conformant_prelude::<S>,
        witness: Some(TriggerHistory::PropertyLog(handshake_label_reached)),
    });
    for &(label, consequence) in PRELUDE {
        properties.push(Property {
            id: PropertyId::generated(format!("handshake/prelude/{label}-{consequence}"))
                .expect("prelude labels follow the ID grammar"),
            claim:
                "The node applies the prelude's declared consequence without installing a session.",
            kind: Kind::Liveness {
                bound: LivenessBound::BeforeQuiescence,
            },
            assumes: &[],
            needs: &[Source::Closes],
            status: Status::Active,
            check: check_prelude::<S>,
            witness: Some(TriggerHistory::PropertyLog(handshake_label_reached)),
        });
    }
    properties
}

/// TLS policy checks use the fixture's offered protocol and advertised identity.
pub fn tls_authorization<S>() -> Vec<Property<S>>
where
    S: HasView<View = std::sync::Arc<crate::harness::handshake_wire::Scenario>>,
{
    vec![
        Property {
            id: PropertyId::new("handshake/quic/alpn-required"),
            claim: "TLS rejects an unsupported ALPN before control starts.",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::View, Source::Closes],
            status: Status::Active,
            check: check_tls_authorization::<S>,
            witness: Some(TriggerHistory::View(
                |view: &std::sync::Arc<crate::harness::handshake_wire::Scenario>, log| {
                    tls_attempt_reached(view, log, false)
                },
            )),
        },
        Property {
            id: PropertyId::new("handshake/quic/identity-required"),
            claim: "A key different from the dialed identity never reaches the control handshake.",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::View, Source::Closes],
            status: Status::Active,
            check: check_tls_authorization::<S>,
            witness: Some(TriggerHistory::View(
                |view: &std::sync::Arc<crate::harness::handshake_wire::Scenario>, log| {
                    tls_attempt_reached(view, log, true)
                },
            )),
        },
    ]
}

fn tls_fault(peer: &crate::peer::PeerSpec, index: usize, identity: bool) -> bool {
    let Some(spec) = &peer.handshake else {
        return false;
    };
    if identity {
        peer.dialer == crate::peer::Dialer::Node
            && spec
                .advertised_identity
                .is_some_and(|advertised| advertised != spec.identity.unwrap_or(index))
    } else {
        spec.alpn.as_deref() == Some(b"handshake-test/unsupported")
    }
}

fn tls_attempt_reached(
    scenario: &crate::harness::handshake_wire::Scenario,
    log: &crate::ObservationLog,
    identity: bool,
) -> bool {
    scenario.peers.iter().enumerate().any(|(index, peer)| {
        tls_fault(peer, index, identity)
            && log.entries().iter().any(|entry| {
                let Observation::Transport { remote, event, .. } = &entry.obs else {
                    return false;
                };
                let source = match event {
                    TransportFact::QuicTlsStarted { remote, .. }
                    | TransportFact::QuicAdmission { remote, .. } => remote,
                    TransportFact::DialStarted => remote,
                    _ => return false,
                };
                *source == peer.address(index, 0).to_string()
            })
    })
}

fn check_tls_authorization<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView<View = std::sync::Arc<crate::harness::handshake_wire::Scenario>>,
{
    let identity = cx.id().as_str() == "handshake/quic/identity-required";
    let scenario = cx.view();
    let mut applies = false;
    for (index, peer) in scenario.peers.iter().enumerate() {
        if !tls_fault(peer, index, identity) {
            continue;
        }
        let address = peer.address(index, 0).to_string();
        for obs in cx.observations() {
            let Observation::Transport { remote, event, .. } = obs else {
                continue;
            };
            let source = match event {
                TransportFact::QuicHandshakeFinished { remote, .. } => remote,
                _ => remote,
            };
            if *source != address {
                continue;
            }
            match event {
                TransportFact::Handshake { .. }
                | TransportFact::QuicHandshakeFinished { error: None, .. }
                | TransportFact::Registered { .. } => {
                    return cx
                        .violated("TLS rejects the unsupported protocol or wrong key", address);
                }
                TransportFact::DialFailed { .. }
                | TransportFact::QuicHandshakeFinished { error: Some(_), .. } => applies = true,
                _ => {}
            }
        }
    }
    if applies {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

fn quic_admission_applies(property: &str, event: &TransportFact) -> bool {
    let TransportFact::QuicAdmission {
        validated,
        pending_total,
        pending_from_ip,
        max_pending_per_ip,
        retry_threshold,
        ..
    } = event
    else {
        return false;
    };
    match property {
        "handshake/quic/pending-per-address-bounded" => max_pending_per_ip.is_some_and(|limit| {
            *pending_from_ip >= usize::try_from(limit).expect("QUIC limit fits usize")
        }),
        "handshake/quic/retry-before-unvalidated-accept" => {
            !validated
                && retry_threshold.is_some_and(|threshold| {
                    *pending_total >= usize::try_from(threshold).expect("QUIC threshold fits usize")
                })
        }
        _ => false,
    }
}

fn quic_admission_reached(property: &str, log: &crate::ObservationLog) -> bool {
    log.entries().iter().any(|entry| {
        matches!(&entry.obs,
            Observation::Transport { event, .. } if quic_admission_applies(property, event)
        )
    })
}

fn check_quic_admission<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let mut applies = false;
    for obs in cx.observations() {
        let Observation::Transport { event, .. } = obs else {
            continue;
        };
        if !quic_admission_applies(cx.id().as_str(), event) {
            continue;
        }
        applies = true;
        if let TransportFact::QuicAdmission {
            decision, remote, ..
        } = event
        {
            if decision == "accept" {
                return cx.violated(
                    "the endpoint refuses or retries the attempt",
                    format!("accepted {remote}"),
                );
            }
        }
    }
    if applies {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

fn quic_handshake_finished(key: &ObligationKey, obs: &Observation) -> bool {
    matches!(obs, Observation::Transport {
        event: TransportFact::QuicHandshakeFinished { remote, .. }, ..
    } if key.subject == Subject::Owner(remote.clone()))
}

fn check_quic_deadline<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let mut pending = Vec::new();
    let mut applies = false;
    for (at, obs) in cx.observations().enumerate() {
        let Observation::Transport {
            event:
                TransportFact::QuicTlsStarted {
                    remote,
                    deadline_ms: Some(10_000),
                },
            ..
        } = obs
        else {
            continue;
        };
        applies = true;
        let obligation = cx.obligation(
            Some(at),
            Subject::Owner(remote.clone()),
            quic_handshake_finished,
        );
        if !cx
            .log
            .entries()
            .iter()
            .any(|entry| quic_handshake_finished(&obligation.key, &entry.obs))
        {
            pending.push(obligation);
        }
    }
    for obligation in pending {
        cx.oblige(obligation);
    }
    if applies {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

fn cadence_attempt_reached(log: &crate::ObservationLog) -> bool {
    log.entries().iter().any(|entry| {
        matches!(&entry.obs,
            Observation::Transport { event: TransportFact::HandshakeCadence { action, .. }, .. }
                if action == "admitted" || action == "exhausted"
        )
    })
}

fn cadence_burst_reached(log: &crate::ObservationLog) -> bool {
    let mut now = 0;
    let mut attempts = std::collections::BTreeMap::<_, std::collections::VecDeque<u64>>::new();
    for entry in log.entries() {
        if let Observation::TimeAdvanced { to_ns, .. } = entry.obs {
            now = to_ns;
        }
        let Observation::Transport {
            instance,
            remote,
            event,
            ..
        } = &entry.obs
        else {
            continue;
        };
        if !matches!(event, TransportFact::QuicHandshakeStarted)
            && !matches!(event, TransportFact::HandshakeCadence { action, .. } if action == "exhausted")
        {
            continue;
        }
        let remote: std::net::SocketAddr = remote
            .parse()
            .expect("transport addresses use socket syntax");
        let times = attempts
            .entry((*instance, scripted_host(remote.ip())))
            .or_default();
        while times.front().is_some_and(|at| now - at >= 1_000_000_000) {
            times.pop_front();
        }
        times.push_back(now);
        if times.len() > 16 {
            return true;
        }
    }
    false
}

// The scripted address families use native IPv4, mapped IPv4, and global IPv6 /64s.
fn scripted_host(ip: std::net::IpAddr) -> std::net::IpAddr {
    use std::net::{IpAddr, Ipv6Addr};
    match ip {
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return IpAddr::V4(v4);
            }
            let segments = v6.segments();
            if (0x2000..0x4000).contains(&segments[0]) {
                IpAddr::V6(Ipv6Addr::new(
                    segments[0],
                    segments[1],
                    segments[2],
                    segments[3],
                    0,
                    0,
                    0,
                    0,
                ))
            } else {
                ip
            }
        }
        _ => ip,
    }
}

/// Counts actual QUIC attempts for the rate bound. Checks admission decisions
/// against a separate balance that includes charges cancelled before TLS.
fn check_cadence<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    use std::collections::BTreeMap;
    use zakura_network::zakura::DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF;

    let rate = cx.id().as_str() == "handshake/attempts-within-cadence";
    if !cx.observations().any(|obs| {
        matches!(
            obs,
            Observation::Transport {
                event: TransportFact::HandshakeCadence { .. },
                ..
            }
        ) || (rate
            && matches!(
                obs,
                Observation::Transport {
                    event: TransportFact::QuicHandshakeStarted,
                    ..
                }
            ))
    }) {
        return Verdict::NotApplicable;
    }
    let mut now = 0u64;
    let mut limits = BTreeMap::new();
    let mut balances = BTreeMap::new();
    for entry in cx.log.entries() {
        if let Observation::TimeAdvanced { to_ns, .. } = entry.obs {
            now = to_ns;
        }
        let Observation::Transport {
            instance,
            remote,
            event,
            ..
        } = &entry.obs
        else {
            continue;
        };
        if let TransportFact::HandshakeCounts {
            address_cap,
            connection_cap,
            cap,
            ..
        } = event
        {
            limits.insert(*instance, (*address_cap, connection_cap + cap));
        }
        if !matches!(
            event,
            TransportFact::HandshakeCadence { .. } | TransportFact::QuicHandshakeStarted
        ) {
            continue;
        }
        let Some(&(address_cap, expected_hosts)) = limits.get(instance) else {
            return cx.violated(
                "admission limits observed before a cadence charge",
                "missing limits",
            );
        };
        let cap = u32::try_from(address_cap).unwrap();
        let interval = u64::try_from(DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF.as_nanos()).unwrap()
            / u64::from(cap);
        let (action, fresh) = match event {
            TransportFact::QuicHandshakeStarted => ("started", false),
            TransportFact::HandshakeCadence {
                action,
                fresh,
                capacity,
                refill_ns,
                hosts,
                host_limit,
            } => {
                if rate
                    && (*capacity != cap
                        || *refill_ns != interval
                        || *host_limit != expected_hosts
                        || hosts > host_limit)
                {
                    return cx.violated("cadence derives its rate and memory bounds from admission and redial limits", format!("{event:?}"));
                }
                if action == "capacity" {
                    if !rate && hosts < host_limit {
                        return cx.violated(
                            "a new address enters a non-full cadence ledger",
                            format!("{event:?}"),
                        );
                    }
                    continue;
                }
                (action.as_str(), *fresh)
            }
            _ => unreachable!("only cadence and QUIC attempt events pass the filter"),
        };
        let remote: std::net::SocketAddr = remote
            .parse()
            .expect("transport addresses use socket syntax");
        let (tokens, last) = balances
            .entry((*instance, scripted_host(remote.ip())))
            .or_insert((cap, now));
        let earned = now.saturating_sub(*last) / interval;
        *tokens = tokens
            .saturating_add(u32::try_from(earned).unwrap_or(u32::MAX))
            .min(cap);
        *last += earned * interval;
        if fresh {
            if rate && *tokens != cap {
                return cx.violated(
                    "a depleted address retains its cadence debt",
                    remote.to_string(),
                );
            }
            *last = now;
        }
        match action {
            "started" if rate => {
                if *tokens == 0 {
                    return cx.violated("TLS requires an address token", remote.to_string());
                }
                *tokens = tokens.saturating_sub(1);
            }
            "admitted" if !rate => *tokens = tokens.saturating_sub(1),
            "registered" => *tokens = tokens.saturating_add(1).min(cap),
            "cancelled" if !rate => *tokens = tokens.saturating_add(1).min(cap),
            "exhausted" if !rate && *tokens != 0 => {
                return cx.violated(
                    "an address with a token is never exhausted",
                    remote.to_string(),
                );
            }
            _ => {}
        }
    }
    Verdict::Holds
}

fn epoch_closed(key: &ObligationKey, obs: &Observation) -> bool {
    // Local deregistration completes before the peer receives the neutral close.
    match obs {
        Observation::PeerConnectionClosed { shared_id, .. } => {
            key.subject == Subject::Session(shared_id.clone())
        }
        Observation::Transport {
            event: TransportFact::Handshake {
                shared_id, stage, ..
            },
            ..
        } => stage == "registration-released" && key.subject == Subject::Session(shared_id.clone()),
        _ => false,
    }
}

fn check_epoch<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    use zakura_network::zakura::ZAKURA_CAP_HEADER_SYNC;
    let mut pending = Vec::new();
    let mut applies = false;
    for (at, obs) in cx.observations().enumerate() {
        let Observation::Transport {
            instance,
            seq,
            event: TransportFact::CapabilitiesChanged { supported },
            ..
        } = obs
        else {
            continue;
        };
        if supported & ZAKURA_CAP_HEADER_SYNC == 0 {
            continue;
        }
        for entry in cx.log.entries() {
            let Observation::Transport {
                instance: source,
                seq: captured,
                event:
                    TransportFact::HandshakeCapabilities {
                        shared_id,
                        supported: snapshot,
                    },
                ..
            } = &entry.obs
            else {
                continue;
            };
            if source != instance || captured >= seq || snapshot & ZAKURA_CAP_HEADER_SYNC != 0 {
                continue;
            }
            applies = true;
            let obligation =
                cx.obligation(Some(at), Subject::Session(shared_id.clone()), epoch_closed);
            if !cx
                .log
                .entries()
                .iter()
                .any(|entry| epoch_closed(&obligation.key, &entry.obs))
            {
                pending.push(obligation);
            }
        }
    }
    for obligation in pending {
        cx.oblige(obligation);
    }
    if applies {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

fn capability_flip_reached(log: &crate::ObservationLog) -> bool {
    use zakura_network::zakura::ZAKURA_CAP_HEADER_SYNC;
    let mut snapshot = false;
    for entry in log.entries() {
        match &entry.obs {
            Observation::Transport {
                event: TransportFact::HandshakeCapabilities { supported, .. },
                ..
            } if supported & ZAKURA_CAP_HEADER_SYNC == 0 => snapshot = true,
            Observation::Transport {
                event: TransportFact::CapabilitiesChanged { supported },
                ..
            } if snapshot && supported & ZAKURA_CAP_HEADER_SYNC != 0 => return true,
            _ => {}
        }
    }
    false
}

fn admission_reached(property: &str, log: &crate::ObservationLog) -> bool {
    log.entries().iter().any(|entry| {
        let Observation::Transport {
            event:
                TransportFact::HandshakeCounts {
                    address_cap,
                    hosts,
                    pending,
                    quic_pending,
                    inbound,
                    cap,
                    connections,
                    inbound_connections,
                    connection_cap,
                    ..
                },
            ..
        } = &entry.obs
        else {
            return false;
        };
        match property {
            "handshake/connections-per-address-bounded" => {
                *address_cap > 0
                    && hosts
                        .iter()
                        .any(|(_, registered, _)| registered >= address_cap)
            }
            "handshake/counts-match-permits" => *pending > 0 || *connections > 0,
            "handshake/pending-bounded" => {
                *cap > 0
                    && (pending + quic_pending >= *cap
                        || inbound + quic_pending >= cap - reserve(*cap))
            }
            "handshake/dial-reserve-kept" => {
                (*cap > 0 && *inbound >= cap - reserve(*cap))
                    || (*connection_cap > 0
                        && *inbound_connections >= connection_cap - reserve(*connection_cap))
            }
            _ => false,
        }
    })
}

fn check_counts<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let mut applies = false;
    for obs in cx.observations() {
        let Observation::Transport {
            event:
                TransportFact::HandshakeCounts {
                    connections,
                    inbound_connections,
                    connection_permits,
                    inbound_connection_permits,
                    pending,
                    inbound,
                    pending_permits,
                    inbound_permits,
                    cap,
                    address_cap,
                    hosts,
                    quic_hosts,
                    quic_pending,
                    quic_handoffs,
                    ..
                },
            ..
        } = obs
        else {
            continue;
        };
        applies = true;
        match cx.id().as_str() {
            "handshake/counts-match-permits"
                if pending != pending_permits
                    || inbound != inbound_permits
                    || connections != connection_permits
                    || inbound_connections != inbound_connection_permits =>
            {
                return cx.violated(
                    "machine charges equal handshake and connection permits",
                    format!(
                        "pending {pending}/{inbound}, permits {pending_permits}/{inbound_permits}; connections {connections}/{inbound_connections}, permits {connection_permits}/{inbound_connection_permits}"
                    ),
                );
            }
            "handshake/connections-per-address-bounded" => {
                for (host, registered, _) in hosts {
                    if registered > address_cap {
                        return cx.violated(
                            format!("at most {address_cap} registrations per address"),
                            format!("{host}: {registered}"),
                        );
                    }
                }
            }
            "handshake/pending-bounded" => {
                let inbound_cap = cap - if *cap >= 2 { (cap / 4).max(1) } else { 0 };
                // The synchronous handoff counts one owner in both stages until QUIC releases it.
                if *quic_handoffs == 0
                    && (pending + quic_pending > *cap || inbound + quic_pending > inbound_cap)
                {
                    return cx.violated(
                        format!("pending <= {cap}; inbound <= {inbound_cap}"),
                        format!("control {pending}/{inbound}, QUIC {quic_pending}"),
                    );
                }
                // QUIC and the handler charge the same connection during synchronous transfer.
                // Judge address sums only after that transfer commits.
                if *quic_handoffs == 0 {
                    let mut addresses = std::collections::BTreeMap::new();
                    for (host, _, handshaking) in hosts {
                        addresses.insert(host, *handshaking);
                    }
                    for (host, pending) in quic_hosts {
                        *addresses.entry(host).or_default() += pending;
                    }
                    let host_cap = (cap / 8).max(1);
                    for (host, pending) in addresses {
                        if pending > host_cap {
                            return cx.violated(
                                format!("at most {host_cap} pending per address"),
                                format!("{host}: {pending}"),
                            );
                        }
                    }
                }
            }
            _ => {}
        }
    }
    if applies {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

fn reserve(cap: usize) -> usize {
    if cap >= 2 {
        (cap / 4).max(1)
    } else {
        0
    }
}

fn check_reserve<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let mut applies = false;
    for obs in cx.observations() {
        let Observation::Transport {
            instance,
            seq,
            remote,
            event,
            ..
        } = obs
        else {
            continue;
        };
        match event {
            TransportFact::HandshakeCounts {
                inbound_connections,
                connection_cap,
                inbound,
                cap,
                quic_inbound,
                quic_max_inbound,
                quic_below_reserve,
                ..
            } => {
                applies = true;
                if *inbound_connections > connection_cap - reserve(*connection_cap)
                    || *inbound > cap - reserve(*cap)
                    || *quic_inbound
                        > connection_cap + cap - reserve(*connection_cap) - reserve(*cap)
                    || quic_inbound > quic_max_inbound
                    || quic_inbound > quic_below_reserve
                {
                    return cx.violated("inbound charges leave the connection, QUIC, and handshake reserves",
                        format!("connections {inbound_connections}/{connection_cap}; pending {inbound}/{cap}; QUIC {quic_inbound}/{quic_max_inbound}, inbound ceiling {quic_below_reserve}"));
                }
            }
            TransportFact::DialFailed { error } => {
                let mut started = 0;
                let mut own_dial = false;
                let mut capacities = None;
                for entry in cx.log.entries() {
                    let Observation::Transport {
                        instance: source,
                        seq: before,
                        remote: target,
                        event,
                        ..
                    } = &entry.obs
                    else {
                        continue;
                    };
                    if source != instance || before > seq {
                        continue;
                    }
                    match event {
                        TransportFact::DialStarted => {
                            started += 1;
                            own_dial |= target == remote;
                        }
                        TransportFact::HandshakeCounts {
                            connection_cap,
                            cap,
                            ..
                        } => {
                            capacities = Some((
                                reserve(*connection_cap),
                                reserve(*cap),
                                reserve(*connection_cap) + reserve(*cap),
                            ));
                        }
                        _ => {}
                    }
                }
                let Some((connections, pending, quic)) = capacities else {
                    continue;
                };
                let protected = match error.as_str() {
                    "Zakura resource limit exceeded: admission" => connections,
                    "Zakura resource limit exceeded: pending handshake" => pending,
                    "Zakura resource limit exceeded: transport connections" => quic,
                    _ => continue,
                };
                // Every scripted node dial makes one attempt. Counting even finished attempts
                // bounds its own load without mistaking a delayed teardown for inbound load.
                if own_dial && started <= protected {
                    return cx.violated(
                        "a dial within the reserved share is admitted",
                        format!("{remote}: {error}; {started} attempts within reserve {protected}"),
                    );
                }
            }
            _ => {}
        }
    }
    if applies {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

fn refused(key: &ObligationKey, obs: &Observation) -> bool {
    if let Subject::Owner(shared_id) = &key.subject {
        return matches!(obs, Observation::PeerConnectionClosed { shared_id: closed, code: 0 }
            if closed == shared_id);
    }
    let Observation::Transport {
        event:
            TransportFact::Handshake {
                shared_id,
                stage,
                validation,
                error: Some(error),
                ..
            },
        ..
    } = obs
    else {
        return false;
    };
    if key.subject != Subject::Session(shared_id.clone()) {
        return false;
    }
    let mut parts = key.property.as_str().split('/');
    let _ = parts.next();
    let Some(row) = parts.next() else {
        return false;
    };
    let Some(label) = parts
        .next()
        .and_then(|claim| claim.strip_suffix("-refused"))
    else {
        return false;
    };
    if stage != row {
        return false;
    }
    let expected = match (row, label) {
        (_, "oversize") => {
            return validation.is_none() && error == "Zakura payload exceeded its cap"
        }
        (_, "bad-magic" | "bad-path" | "bad-role") | ("ack", "bad-version") => "malformed-control",
        (
            _,
            "bad-frame-limit"
            | "bad-message-limit"
            | "bad-stream-limit"
            | "bad-queue-limit"
            | "bad-idle-limit"
            | "limits-above-request",
        ) => "bad-limits",
        (_, "nonzero-initiator-nonce" | "nonzero-responder-nonce") => "nonzero-native-nonce",
        _ => label,
    };
    validation.as_deref() == Some(expected)
}

fn released(key: &ObligationKey, obs: &Observation) -> bool {
    matches!(obs, Observation::Transport {
        event: TransportFact::Handshake { shared_id, stage, .. }, ..
    } if stage == "released" && key.subject == Subject::Session(shared_id.clone()))
}

fn check_deadline<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let mut pending = Vec::new();
    let mut applies = false;
    for (at, obs) in cx.observations().enumerate() {
        let Observation::Transport {
            event:
                TransportFact::Handshake {
                    shared_id,
                    stage,
                    elapsed_ms,
                    ..
                },
            ..
        } = obs
        else {
            continue;
        };
        if stage == "released" {
            applies = true;
            if elapsed_ms.is_some_and(|elapsed| elapsed > 13_000) {
                return cx.violated(
                    "control claim released within 13000 ms",
                    format!("{shared_id}: {elapsed_ms:?} ms"),
                );
            }
        }
        if stage == "started" {
            applies = true;
            pending.push(cx.obligation(Some(at), Subject::Session(shared_id.clone()), released));
        }
    }
    for obligation in pending {
        cx.oblige(obligation);
    }
    if applies {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

// A deadline violation stops the trace before the handshake releases its claim.
fn paced_control_reached(log: &crate::ObservationLog) -> bool {
    log.entries().iter().any(|entry| {
        let Observation::PeerHandshake {
            shared_id,
            row,
            label,
            ..
        } = &entry.obs
        else {
            return false;
        };
        if row != "hello" || label != "paced" {
            return false;
        }
        let mut now = 0;
        let mut started = None;
        for entry in log.entries() {
            match &entry.obs {
                Observation::TimeAdvanced { to_ns, .. } => {
                    now = *to_ns;
                    if started.is_some_and(|start| now.saturating_sub(start) >= 9_000_000_000) {
                        return true;
                    }
                }
                Observation::Transport {
                    event:
                        TransportFact::Handshake {
                            shared_id: observed,
                            stage,
                            ..
                        },
                    ..
                } if observed == shared_id => match stage.as_str() {
                    "started" => started = Some(now),
                    "released" => started = None,
                    _ => {}
                },
                _ => {}
            }
        }
        false
    })
}

/// A generated rejection witness reaches the labelled exchange and its production judgement.
fn handshake_label_reached(property: &str, log: &crate::ObservationLog) -> bool {
    let mut parts = property.split('/');
    let (Some("handshake"), Some(row), Some(claim)) = (parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    let Some(label) = claim
        .strip_suffix("-never-refused")
        .or_else(|| claim.strip_suffix("-refused"))
        .or_else(|| claim.strip_suffix("-resets"))
        .or_else(|| claim.strip_suffix("-closes"))
    else {
        return false;
    };
    log.entries().iter().any(|entry| {
        let Observation::PeerHandshake {
            shared_id,
            quic_stream,
            row: sent_row,
            label: sent_label,
            ..
        } = &entry.obs
        else {
            return false;
        };
        sent_row == row
            && sent_label == label
            && log.entries().iter().any(|entry| match &entry.obs {
                Observation::Transport {
                    event:
                        TransportFact::Handshake {
                            shared_id: judged,
                            stage,
                            ..
                        },
                    ..
                } => judged == shared_id && stage == row,
                Observation::Transport {
                    event:
                        TransportFact::PreludeJudged {
                            shared_id: judged,
                            quic_stream: judged_stream,
                            ..
                        },
                    ..
                } => row == "prelude" && judged == shared_id && judged_stream == quic_stream,
                _ => false,
            })
    })
}

fn check_refused<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let id = cx.id().to_string();
    let mut pieces = id.split('/');
    let _ = pieces.next();
    let row = pieces.next().unwrap();
    let label = pieces.next().unwrap().strip_suffix("-refused").unwrap();
    let mut pending = Vec::new();
    let mut applies = false;
    for obs in cx.observations() {
        let Observation::Transport {
            event:
                TransportFact::Handshake {
                    shared_id,
                    stage,
                    error,
                    ..
                },
            ..
        } = obs
        else {
            continue;
        };
        if (stage == row && error.is_none()) || stage == "registered" {
            if cx.log.entries().iter().any(|entry| matches!(&entry.obs,
                Observation::PeerHandshake { shared_id: sent_id, row: sent_row, label: sent_label, .. }
                    if sent_id == shared_id && sent_row == row && sent_label == label)) {
                return cx.violated("the node never accepts a malformed control exchange", shared_id);
            }
        }
    }
    for (at, obs) in cx.observations().enumerate() {
        let Observation::PeerHandshake {
            shared_id,
            row: sent_row,
            label: sent_label,
            ..
        } = obs
        else {
            continue;
        };
        if sent_row != row || sent_label != label {
            continue;
        }
        applies = true;
        if cx.log.entries().iter().any(|entry| matches!(&entry.obs,
            Observation::Transport { event: TransportFact::Handshake { shared_id: accepted, stage, error, .. }, .. }
                if accepted == shared_id && ((stage == row && error.is_none()) || stage == "registered"))) {
            return cx.violated("the node never accepts a malformed control exchange", shared_id);
        }
        if let Some(code) = cx.log.entries().iter().find_map(|entry| match &entry.obs {
            Observation::PeerConnectionClosed {
                shared_id: closed,
                code,
            } if closed == shared_id && *code != 0 => Some(code),
            _ => None,
        }) {
            return cx.violated(
                "a malformed control exchange receives a neutral close",
                format!("application close code {code}"),
            );
        }
        let close = cx.obligation(Some(at), Subject::Owner(shared_id.clone()), refused);
        if !cx
            .log
            .entries()
            .iter()
            .any(|entry| refused(&close.key, &entry.obs))
        {
            pending.push(close);
        }
        let obligation = cx.obligation(Some(at), Subject::Session(shared_id.clone()), refused);
        if !cx
            .log
            .entries()
            .iter()
            .any(|entry| refused(&obligation.key, &entry.obs))
        {
            pending.push(obligation);
        }
    }
    for obligation in pending {
        cx.oblige(obligation);
    }
    if applies {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

fn resource_released(key: &ObligationKey, obs: &Observation) -> bool {
    matches!(obs, Observation::Transport {
        event: TransportFact::Handshake { shared_id, stage, .. }, ..
    } if key.subject == Subject::Owner(format!("{shared_id}/{stage}")))
}

fn handshake_refusal_reached(property: &str, log: &crate::ObservationLog) -> bool {
    let Some(row) = property.split('/').nth(1) else {
        return false;
    };
    log.entries().iter().any(|entry| {
        matches!(&entry.obs, Observation::Transport {
            event: TransportFact::Handshake { stage, error: Some(_), .. }, ..
        } if stage == row)
    })
}

fn check_released<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let id = cx.id().to_string();
    let row = id.split('/').nth(1).unwrap();
    let mut pending = Vec::new();
    let mut applies = false;
    for (at, obs) in cx.observations().enumerate() {
        let Observation::Transport {
            event:
                TransportFact::Handshake {
                    shared_id,
                    stage,
                    error: Some(_),
                    ..
                },
            ..
        } = obs
        else {
            continue;
        };
        if stage != row {
            continue;
        }
        applies = true;
        for stage in [
            "released",
            "pending-released",
            "connection-released",
            "quic-released",
        ] {
            // The handler charges an IP only for inbound control work.
            if row == "ack" && stage == "released" {
                continue;
            }
            let obligation = cx.obligation(
                Some(at),
                Subject::Owner(format!("{shared_id}/{stage}")),
                resource_released,
            );
            if !cx
                .log
                .entries()
                .iter()
                .any(|entry| resource_released(&obligation.key, &entry.obs))
            {
                pending.push(obligation);
            }
        }
    }
    for obligation in pending {
        cx.oblige(obligation);
    }
    if applies {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

fn check_conformant<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let id = cx.id().to_string();
    let row = id.split('/').nth(1).unwrap();
    let mut applies = false;
    for obs in cx.observations() {
        let Observation::Transport {
            event:
                TransportFact::Handshake {
                    shared_id,
                    stage,
                    error,
                    ..
                },
            ..
        } = obs
        else {
            continue;
        };
        if stage != row {
            continue;
        }
        let conformant = cx.log.entries().iter().any(|entry| {
            matches!(&entry.obs,
            Observation::PeerHandshake { shared_id: sent_id, row: sent_row, label, .. }
                if sent_id == shared_id && sent_row == row && label == "conformant")
        });
        if !conformant {
            continue;
        }
        applies = true;
        if let Some(error) = error {
            if !error.starts_with("Zakura resource limit exceeded:") {
                return cx.violated("a conformant control exchange succeeds", error);
            }
        }
    }
    if applies {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

fn check_conformant_prelude<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let mut applies = false;
    for obs in cx.observations() {
        let Observation::Transport {
            event:
                TransportFact::PreludeJudged {
                    shared_id,
                    quic_stream,
                    rejection,
                },
            ..
        } = obs
        else {
            continue;
        };
        if !cx.log.entries().iter().any(|entry| matches!(&entry.obs,
            Observation::PeerHandshake { shared_id: sent_id, quic_stream: sent_stream, row, label, .. }
                if sent_id == shared_id && sent_stream == quic_stream && row == "prelude" && label == "conformant")) {
            continue;
        }
        applies = true;
        if let Some(rejection) = rejection {
            return cx.violated("a conformant prelude passes the protocol guards", rejection);
        }
    }
    if applies {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

fn prelude_judged(key: &ObligationKey, obs: &Observation) -> bool {
    let Observation::Transport {
        event:
            TransportFact::PreludeJudged {
                shared_id,
                quic_stream,
                rejection: Some(rejection),
            },
        ..
    } = obs
    else {
        return false;
    };
    let subject = Subject::Session(format!("{shared_id}/{quic_stream}"));
    let claim = key
        .property
        .as_str()
        .strip_prefix("handshake/prelude/")
        .unwrap();
    key.subject == subject
        && PRELUDE
            .iter()
            .any(|(label, effect)| *label == rejection && claim == format!("{label}-{effect}"))
}

fn check_prelude<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let id = cx.id().to_string();
    let claim = id.strip_prefix("handshake/prelude/").unwrap();
    let label = PRELUDE
        .iter()
        .find(|(label, effect)| claim == format!("{label}-{effect}"))
        .unwrap()
        .0;
    let mut pending = Vec::new();
    let mut applies = false;
    for (at, obs) in cx.observations().enumerate() {
        let Observation::PeerHandshake {
            shared_id,
            quic_stream,
            row,
            label: sent_label,
            ..
        } = obs
        else {
            continue;
        };
        if row != "prelude" || sent_label != label {
            continue;
        }
        applies = true;
        if cx.log.entries().iter().any(|entry| {
            matches!(&entry.obs,
                Observation::Transport { event: TransportFact::PreludeJudged {
                    shared_id: judged_id, quic_stream: judged_stream, rejection: None,
                }, .. } if judged_id == shared_id && judged_stream == quic_stream
            )
        }) {
            return cx.violated("a prelude requiring rejection is never accepted", label);
        }
        let obligation = cx.obligation(
            Some(at),
            Subject::Session(format!("{shared_id}/{quic_stream}")),
            prelude_judged,
        );
        if !cx
            .log
            .entries()
            .iter()
            .any(|entry| prelude_judged(&obligation.key, &entry.obs))
        {
            pending.push(obligation);
        }
    }
    for (at, obs) in cx.observations().enumerate() {
        if let Observation::Transport {
            event:
                TransportFact::PreludeJudged {
                    shared_id,
                    quic_stream,
                    rejection: None,
                },
            ..
        } = obs
        {
            if cx.log.entries().iter().any(|entry| matches!(&entry.obs,
                Observation::PeerHandshake { shared_id: sent_id, quic_stream: sent_stream, row, label: sent_label, .. }
                    if sent_id == shared_id && sent_stream == quic_stream && row == "prelude" && sent_label == label
            )) {
                return cx.violated("a prelude requiring rejection is never accepted", label);
            }
        }
        if let Observation::Transport {
            connection: Some(connection),
            event:
                TransportFact::PreludeJudged {
                    shared_id,
                    quic_stream,
                    rejection: Some(rejection),
                },
            ..
        } = obs
        {
            if rejection != label {
                continue;
            }
            applies = true;
            let subject = if claim.ends_with("-resets") {
                Subject::Owner(format!("reset/{shared_id}/{quic_stream}"))
            } else {
                Subject::Owner(format!("close/{connection}"))
            };
            let obligation = cx.obligation(Some(at), subject, prelude_effect);
            if !cx
                .log
                .entries()
                .iter()
                .any(|entry| prelude_effect(&obligation.key, &entry.obs))
            {
                pending.push(obligation);
            }
        }
        if let Observation::Session {
            connection,
            quic_stream,
            ..
        } = obs
        {
            if cx.log.entries().iter().any(|entry| matches!(&entry.obs,
                Observation::Transport { connection: Some(judged_connection), event: TransportFact::PreludeJudged { quic_stream: judged_stream, rejection: Some(rejection), .. }, .. }
                if judged_connection == connection && judged_stream == quic_stream && rejection == label)) {
                return cx.violated("a refused prelude installs no session", "the refused stream installed a session");
            }
        }
        if let Observation::ConnectionClosed { connection, cause } = obs {
            if claim.ends_with("-resets") && crate::model::is_fault_cause(cause)
                && cx.log.entries().iter().any(|entry| matches!(&entry.obs,
                    Observation::Transport { connection: Some(judged_connection), event: TransportFact::PreludeJudged { rejection: Some(rejection), .. }, .. }
                    if judged_connection == connection && rejection == label)) {
                return cx.violated("a reset-only prelude never charges a fault", cause);
            }
        }
    }
    for obligation in pending {
        cx.oblige(obligation);
    }
    if applies {
        Verdict::Holds
    } else {
        Verdict::NotApplicable
    }
}

fn prelude_effect(key: &ObligationKey, obs: &Observation) -> bool {
    let claim = key
        .property
        .as_str()
        .strip_prefix("handshake/prelude/")
        .unwrap();
    if claim.ends_with("-resets") {
        return matches!(obs, Observation::PeerStreamReset { shared_id, quic_stream, code }
            if key.subject == Subject::Owner(format!("reset/{shared_id}/{quic_stream}"))
                && *code == if claim == "bad-magic-resets" { 2 } else { 5 });
    }
    let expected = match claim {
        "undersized-cap-closes" => "undersized_frame_cap",
        "unexpected-request-id-closes" => "unexpected_request_id",
        "request-without-id-closes" => "request_without_id",
        "ineligible-opener-closes" => "unexpected_stream",
        _ => return false,
    };
    matches!(obs, Observation::ConnectionClosed { connection, cause }
        if key.subject == Subject::Owner(format!("close/{connection}")) && cause == expected)
}

/// Checks worker installation against this fixture's offered capabilities and layouts.
pub fn installed_services_authorized<S>() -> Property<S>
where
    S: HasView<View = std::sync::Arc<crate::harness::handshake_wire::Scenario>>,
{
    Property {
        id: PropertyId::new("session/installed-services-authorized"),
        claim: "Every installed stream belongs to the connection's negotiated and enabled layout.",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::View, Source::Closes],
        status: Status::Active,
        check: check_installed_services::<S>,
        witness: Some(TriggerHistory::Log(|log| {
            log.entries().iter().any(|entry| {
                matches!(
                    entry.obs,
                    Observation::Transport {
                        event: TransportFact::Admitted { .. },
                        ..
                    }
                )
            })
        })),
    }
}

/// The fixture declares these layouts independently of the registry's selection code.
fn authorized_stream(capabilities: u64, kind: u16, version: u16) -> bool {
    use zakura_network::zakura::{
        ZAKURA_CAP_DISCOVERY, ZAKURA_CAP_HEADER_SYNC, ZAKURA_DISCOVERY_STREAM_VERSION,
        ZAKURA_HEADER_SYNC_STREAM_VERSION, ZAKURA_STREAM_DISCOVERY, ZAKURA_STREAM_HEADER_SYNC,
    };
    match kind {
        ZAKURA_STREAM_HEADER_SYNC => {
            capabilities & ZAKURA_CAP_HEADER_SYNC != 0
                && version == ZAKURA_HEADER_SYNC_STREAM_VERSION
        }
        ZAKURA_STREAM_DISCOVERY => {
            capabilities & ZAKURA_CAP_DISCOVERY != 0 && version == ZAKURA_DISCOVERY_STREAM_VERSION
        }
        900 => match version {
            2 => capabilities & (1 << 61) != 0,
            1 => capabilities & (1 << 60) != 0 && capabilities & (1 << 61) == 0,
            _ => false,
        },
        901 => version == 1 && capabilities & (1 << 62) != 0,
        902 | 903 => version == 1 && capabilities & (1 << 59) != 0,
        _ => false,
    }
}

fn check_installed_services<S>(cx: &mut CheckCtx<'_, S>) -> Verdict
where
    S: HasView<View = std::sync::Arc<crate::harness::handshake_wire::Scenario>>,
{
    use std::collections::BTreeMap;
    if !cx.observations().any(|obs| {
        matches!(
            obs,
            Observation::Transport {
                event: TransportFact::Admitted { .. },
                ..
            }
        )
    }) {
        return Verdict::NotApplicable;
    }
    let scenario = cx.view();
    let mut handshakes = BTreeMap::new();
    let mut connections = BTreeMap::new();
    let mut enabled = BTreeMap::new();
    let mut finding = None;
    for entry in cx.log.entries() {
        let Observation::Transport {
            instance,
            connection,
            remote,
            event,
            ..
        } = &entry.obs
        else {
            continue;
        };
        match event {
            TransportFact::CapabilitiesChanged { supported } => {
                enabled.insert(*instance, *supported);
            }
            TransportFact::Negotiated {
                local, negotiated, ..
            } => {
                let peer = remote
                    .parse()
                    .ok()
                    .and_then(crate::peer::peer_of)
                    .and_then(|index| scenario.peers.get(index));
                let expected = peer.map(|peer| peer.capabilities & local);
                handshakes.insert((*instance, remote.clone()), (*negotiated, expected));
            }
            TransportFact::Registered => {
                if let Some(connection) = connection {
                    connections.insert(
                        (*instance, *connection),
                        handshakes.get(&(*instance, remote.clone())).copied(),
                    );
                }
            }
            TransportFact::TornDown => {
                if let Some(connection) = connection {
                    connections.remove(&(*instance, *connection));
                }
            }
            TransportFact::Admitted {
                kind,
                version,
                session_id,
                ..
            } if entry.step == cx.step => {
                let permission = connection.and_then(|connection| {
                    connections.get(&(*instance, connection)).copied().flatten()
                });
                let allowed = permission.is_some_and(|(negotiated, expected)| {
                    expected == Some(negotiated)
                        && authorized_stream(
                            negotiated & enabled.get(instance).copied().unwrap_or(u64::MAX),
                            *kind,
                            *version,
                        )
                });
                if !allowed {
                    finding = Some(format!("connection {connection:?}, session {session_id}, stream {kind}v{version}, permissions {permission:?}"));
                    break;
                }
            }
            _ => {}
        }
    }
    match finding {
        Some(actual) => cx.violated(
            "an installed stream has fixture-negotiated permission on a live connection",
            actual,
        ),
        None => Verdict::Holds,
    }
}

/// Counts created stream setups, independently of the limiter's decisions.
fn check_stream_opens<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    use std::collections::{BTreeMap, BTreeSet};
    if !cx.observations().any(|obs| {
        matches!(
            obs,
            Observation::Transport {
                event: TransportFact::StreamSetupStarted { .. }
                    | TransportFact::StreamLoad { .. }
                    | TransportFact::TornDown,
                ..
            }
        )
    }) {
        return Verdict::NotApplicable;
    }
    let mut now = 0u64;
    let mut balances = BTreeMap::new();
    let mut loads = BTreeMap::new();
    let mut torn_down = BTreeSet::new();
    let mut finding = None;
    for entry in cx.log.entries() {
        if let Observation::TimeAdvanced { to_ns, .. } = entry.obs {
            now = to_ns;
        }
        let Observation::Transport {
            instance,
            connection: Some(connection),
            event,
            ..
        } = &entry.obs
        else {
            continue;
        };
        if let TransportFact::StreamLoad {
            available,
            capacity,
            ..
        } = event
        {
            loads.insert((*instance, *connection), (*available, *capacity));
        }
        if matches!(event, TransportFact::TornDown) {
            torn_down.insert((*instance, *connection));
        }
        match event {
            TransportFact::StreamLoad {
                setup,
                installed,
                available,
                capacity,
                ..
            } if setup + installed > *capacity || available > capacity => {
                finding = Some(format!("connection {connection}: {setup} setup, {installed} installed, {available} free, capacity {capacity}"));
                break;
            }
            TransportFact::StreamSetupStarted { quic_stream, rate } => {
                // Credits use billionths of a token, so refill never rounds upward.
                let capacity = u128::from(*rate) * 1_000_000_000;
                let (credit, last) = balances
                    .entry((*instance, *connection))
                    .or_insert((capacity, now));
                *credit = credit
                    .saturating_add(u128::from(now - *last) * u128::from(*rate))
                    .min(capacity);
                *last = now;
                if *credit < 1_000_000_000 {
                    finding = Some(format!("connection {connection} started stream {quic_stream} without stream-open credit"));
                    break;
                }
                *credit -= 1_000_000_000;
            }
            _ => {}
        }
    }
    if finding.is_none() {
        for connection in torn_down {
            if let Some((available, capacity)) = loads.get(&connection) {
                if available != capacity {
                    finding = Some(format!(
                        "torn-down connection {connection:?} retains {} stream permits",
                        capacity.saturating_sub(*available)
                    ));
                    break;
                }
            }
        }
    }
    match finding {
        Some(actual) => cx.violated(
            "stream owners and setup starts respect the connection's bounds",
            actual,
        ),
        None => Verdict::Holds,
    }
}

fn check_session_capacity<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let bounds = cx.id().as_str() == "session/setup-slot-bounded";
    if !cx.observations().any(|obs| {
        matches!(
            obs,
            Observation::Transport {
                event: TransportFact::SessionCapacity { .. },
                ..
            }
        ) || (bounds && matches!(obs, Observation::TimeAdvanced { .. }))
    }) {
        return Verdict::NotApplicable;
    }
    // The wire fixture uses the production three-second prelude deadline.
    // No await separates reservation from insertion into the pending session.
    let mut now = 0;
    let mut seen = std::collections::BTreeMap::new();
    let mut applicable = false;
    let mut finding = None;
    for entry in cx.log.entries() {
        if let Observation::TimeAdvanced { to_ns, .. } = entry.obs {
            now = to_ns;
            if bounds {
                for (id, start) in &seen {
                    if now.saturating_sub(*start) > 3_000_000_000 {
                        finding = Some(format!(
                            "inbound reservation {id} retained setup for {} ns",
                            now - start
                        ));
                        break;
                    }
                }
                if finding.is_some() {
                    break;
                }
            }
        }
        let Observation::Transport {
            event:
                TransportFact::SessionCapacity {
                    free,
                    held,
                    capacity,
                    inbound_setups,
                },
            ..
        } = &entry.obs
        else {
            continue;
        };
        applicable = true;
        if bounds {
            seen.retain(|id, _| inbound_setups.contains(id));
            for id in inbound_setups {
                let start = seen.entry(*id).or_insert(now);
                if now.saturating_sub(*start) > 3_000_000_000 {
                    finding = Some(format!(
                        "inbound reservation {id} retained setup for {} ns",
                        now - *start
                    ));
                    break;
                }
            }
            if capacity[1] > 0 && held[3] >= capacity[2] && held[3] > 0 {
                finding = Some(format!(
                    "{} inbound setups consumed all {} setup slots",
                    held[3], capacity[2]
                ));
            }
        } else {
            for index in 0..4 {
                if free[index].checked_add(held[index]) != Some(capacity[index]) {
                    finding = Some(format!(
                        "pool {index}: {} free + {} held != {} capacity",
                        free[index], held[index], capacity[index]
                    ));
                    break;
                }
            }
        }
        if finding.is_some() {
            break;
        }
    }
    if bounds && finding.is_none() {
        for (id, start) in seen {
            if now.saturating_sub(start) > 3_000_000_000 {
                finding = Some(format!(
                    "inbound reservation {id} retained setup for {} ns",
                    now - start
                ));
                break;
            }
        }
    }
    match finding {
        Some(actual) => cx.violated(
            "reservations conserve capacity and preserve outbound setup",
            actual,
        ),
        None if applicable => Verdict::Holds,
        None => Verdict::NotApplicable,
    }
}
