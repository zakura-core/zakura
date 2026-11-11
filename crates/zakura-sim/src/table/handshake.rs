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

use crate::{
    observe::TransportFact,
    property::{
        CheckCtx, HasView, Kind, LivenessBound, ObligationKey, Property, PropertyId, Source,
        Status, Subject, Verdict,
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
    properties.push(Property {
        id: PropertyId::new("handshake/hello/refusal-releases-slot"),
        claim: "A refused inbound handshake releases its admission charge before quiescence.",
        kind: Kind::Liveness {
            bound: LivenessBound::BeforeQuiescence,
        },
        assumes: &[],
        needs: &[Source::Closes],
        status: Status::Active,
        check: check_released::<S>,
        witness: None,
    });
    properties
}

fn refused(key: &ObligationKey, obs: &Observation) -> bool {
    matches!(obs, Observation::Transport {
        event: TransportFact::Handshake { shared_id, stage, error: Some(_), .. }, ..
    } if key.subject == Subject::Session(shared_id.clone())
        && key.property.as_str().split('/').nth(1) == Some(stage.as_str()))
}

fn released(key: &ObligationKey, obs: &Observation) -> bool {
    matches!(obs, Observation::Transport {
        event: TransportFact::Handshake { shared_id, stage, .. }, ..
    } if stage == "released" && key.subject == Subject::Session(shared_id.clone()))
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

fn check_released<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
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
        if stage != "hello" {
            continue;
        }
        applies = true;
        let obligation = cx.obligation(Some(at), Subject::Session(shared_id.clone()), released);
        if !cx
            .log
            .entries()
            .iter()
            .any(|entry| released(&obligation.key, &entry.obs))
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
