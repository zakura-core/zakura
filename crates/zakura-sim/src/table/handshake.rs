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
    properties.push(Property {
        id: PropertyId::new("session/renegotiation-covers-inflight-handshakes"),
        claim: "Capability enablement closes every connection whose handshake captured the old capability mask.",
        kind: Kind::Liveness { bound: LivenessBound::BeforeQuiescence },
        assumes: &[], needs: &[Source::Closes], status: Status::Known { issue: 2334 },
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
            witness: None,
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
        witness: None,
    });
    properties.push(Property {
        id: PropertyId::new("handshake/finishes-in-time"),
        claim: "An inbound control handshake releases its claim within ten seconds plus the shutdown drain.",
        kind: Kind::Liveness {
            bound: LivenessBound::VirtualTime(std::time::Duration::from_secs(13)),
        },
        assumes: &[],
        needs: &[Source::Closes],
        status: Status::Known { issue: 2333 },
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
                witness: None,
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
            witness: None,
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
            witness: None,
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
        witness: None,
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
            witness: None,
        });
    }
    properties
}

fn epoch_closed(key: &ObligationKey, obs: &Observation) -> bool {
    matches!(obs, Observation::PeerConnectionClosed { shared_id, .. }
        if key.subject == Subject::Session(shared_id.clone()))
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
                if pending + quic_pending > *cap || inbound + quic_pending > inbound_cap {
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

fn paced_control_reached(log: &crate::ObservationLog) -> bool {
    log.entries().iter().any(|entry| {
        let Observation::Transport {
            event:
                TransportFact::Handshake {
                    shared_id,
                    stage,
                    elapsed_ms: Some(elapsed),
                    ..
                },
            ..
        } = &entry.obs
        else {
            return false;
        };
        stage == "released" && *elapsed >= 9_000 && log.entries().iter().any(|entry| {
            matches!(&entry.obs, Observation::PeerHandshake { shared_id: sent_id, row, label, .. }
                if sent_id == shared_id && row == "hello" && label == "paced")
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
            Observation::Transport { event: TransportFact::Handshake { shared_id: registered, stage, .. }, .. }
                if registered == shared_id && stage == "registered")) {
            return cx.violated("a malformed control exchange never registers", shared_id);
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
