//! A check of each storage call's identity and retained-header ownership.
//!
//! [`HeaderWorkLedger`] folds one reactor's events and refuses a history that
//! attributes a call or its headers to the wrong work. It checks events against
//! direct facts, the call's own `Dispatched` and the peer's latest `Started`,
//! and infers nothing from ordering. The reactor's tests and the simulator's tap
//! share it.

use std::collections::BTreeMap;

use zakura_rt::collections::HashMap;

use super::{CallEnd, HeaderWorkEnd, PortCall, ReactorEvent, StorageCallKind};
use crate::zakura::ZakuraPeerId;

/// The ownership one call holds.
#[derive(Debug)]
struct Held {
    incarnation: u64,
    headers: usize,
    ended: bool,
}

/// What the ledger knows about one dispatched call.
#[derive(Debug)]
struct CallRecord {
    op: PortCall,
    end: Option<CallEnd>,
    held: Option<Held>,
}

/// Folds one reactor's events. See the module doc.
#[derive(Debug, Default)]
pub struct HeaderWorkLedger {
    calls: BTreeMap<u64, CallRecord>,
    last_incarnation: u64,
    /// Each peer's latest started, unretired round.
    rounds: HashMap<ZakuraPeerId, u64>,
}

impl HeaderWorkLedger {
    /// Checks `event`, and returns the broken rule's name and detail.
    pub fn observe(&mut self, event: &ReactorEvent) -> Result<(), String> {
        match event {
            ReactorEvent::Dispatched { call, op } => {
                let record = CallRecord {
                    op: op.clone(),
                    end: None,
                    held: None,
                };
                if self.calls.insert(*call, record).is_some() {
                    return Err(format!("CallReused: call {call} dispatched twice"));
                }
            }
            ReactorEvent::CallEnded { call, end } => {
                let record = self.record_mut(*call)?;
                if record.end.is_some() {
                    return Err(format!("CallEndedTwice: call {call} ended again"));
                }
                record.end = Some(*end);
            }
            ReactorEvent::Handled { call, .. } => {
                let record = self.record_mut(*call)?;
                match record.end {
                    None => {
                        return Err(format!(
                            "MissingCallEnd: call {call} was handled before it ended"
                        ))
                    }
                    Some(CallEnd::Dropped) => {
                        return Err(format!(
                            "MissingCallEnd: call {call} was handled after the reactor dropped it"
                        ))
                    }
                    Some(_) => {}
                }
                if record.held.as_ref().is_some_and(|held| !held.ended) {
                    return Err(format!(
                        "OpenOwnership: call {call} was handled with its headers still held"
                    ));
                }
            }
            ReactorEvent::Started { request, .. } => {
                if request.incarnation != self.last_incarnation + 1 {
                    return Err(format!(
                        "RoundGap: round {} started after round {}",
                        request.incarnation, self.last_incarnation
                    ));
                }
                self.last_incarnation = request.incarnation;
                self.rounds
                    .insert(request.peer.clone(), request.incarnation);
            }
            ReactorEvent::Retired { request, .. } => {
                if self.rounds.get(&request.peer) == Some(&request.incarnation) {
                    self.rounds.remove(&request.peer);
                }
            }
            ReactorEvent::HeadersHeld {
                call,
                kind,
                peer,
                incarnation,
                headers,
            } => {
                let current = self.rounds.get(peer).copied();
                let record = self.record_mut(*call)?;
                let (expected_kind, call_peer) = match &record.op {
                    PortCall::Prepare { peer, .. } => (StorageCallKind::Prepare, peer),
                    PortCall::Apply { peer, .. } => (StorageCallKind::Apply, peer),
                    other => {
                        return Err(format!(
                            "WrongCall: call {call} holds headers but asks for {other:?}"
                        ))
                    }
                };
                if expected_kind != *kind || call_peer != peer {
                    return Err(format!(
                        "WrongCall: call {call} names {kind:?} for another peer or kind"
                    ));
                }
                if current != Some(*incarnation) {
                    return Err(format!(
                        "WrongRound: call {call} holds round {incarnation}, but its peer's \
                         current round is {current:?}"
                    ));
                }
                if record.held.is_some() {
                    return Err(format!("HeldTwice: call {call} took headers again"));
                }
                record.held = Some(Held {
                    incarnation: *incarnation,
                    headers: *headers,
                    ended: false,
                });
            }
            ReactorEvent::HeadersEnded {
                call,
                incarnation,
                headers,
                end,
            } => {
                let record = self.record_mut(*call)?;
                let Some(held) = record.held.as_mut() else {
                    return Err(format!("NotHeld: call {call} ended headers it never held"));
                };
                if held.ended {
                    return Err(format!("EndedTwice: call {call} ended its headers again"));
                }
                held.ended = true;
                if held.incarnation != *incarnation || held.headers != *headers {
                    return Err(format!(
                        "WrongRound: call {call} held round {} with {} headers, and ended round \
                         {incarnation} with {headers}",
                        held.incarnation, held.headers
                    ));
                }
                if let HeaderWorkEnd::HandedToApply { apply } = end {
                    self.check_handoff(*call, *apply)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Requires `apply` to be an apply call for the same work as the prepare `call`.
    fn check_handoff(&self, call: u64, apply: u64) -> Result<(), String> {
        let prepare = self.calls.get(&call).map(|record| &record.op);
        let applied = self.calls.get(&apply).map(|record| &record.op);
        match (prepare, applied) {
            (
                Some(PortCall::Prepare {
                    peer,
                    source,
                    owner,
                }),
                Some(PortCall::Apply {
                    peer: apply_peer,
                    source: apply_source,
                    owner: apply_owner,
                }),
            ) if peer == apply_peer && source == apply_source && owner == apply_owner => Ok(()),
            _ => Err(format!(
                "BadHandoff: call {call} handed its headers to call {apply}, which is not an \
                 apply for the same work"
            )),
        }
    }

    /// Requires every call to have ended and every held header to have left its
    /// call, once the reactor's teardown is done.
    pub fn finish(&self) -> Result<(), String> {
        for (call, record) in &self.calls {
            if record.end.is_none() {
                return Err(format!("UnendedCall: call {call} never reported its end"));
            }
            if record.held.as_ref().is_some_and(|held| !held.ended) {
                return Err(format!(
                    "OpenOwnership: call {call} never ended its headers"
                ));
            }
        }
        Ok(())
    }

    fn record_mut(&mut self, call: u64) -> Result<&mut CallRecord, String> {
        self.calls
            .get_mut(&call)
            .ok_or_else(|| format!("UnknownCall: event names undispatched call {call}"))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use zakura_chain::block;
    use zakura_header_chain::{
        BranchId, HeaderGeneration, HeaderSyncWorkOwner, HeaderWorkAuthority, HeaderWorkOwner,
        SourceId,
    };

    use super::{super::RequestRef, *};

    fn peer(byte: u8) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![byte; 32]).expect("the test peer ID has the required length")
    }

    fn owner() -> HeaderSyncWorkOwner {
        HeaderWorkOwner {
            authority: HeaderWorkAuthority {
                header_generation: HeaderGeneration::new(1),
                branch: BranchId {
                    anchor_hash: block::Hash([1; 32]),
                    target_tip_hash: block::Hash([2; 32]),
                },
            },
            session_id: 1,
            request_id: NonZeroU64::new(1).expect("one is nonzero"),
        }
        .into()
    }

    fn prepare(byte: u8) -> PortCall {
        PortCall::Prepare {
            peer: peer(byte),
            source: SourceId::from_digest([byte; 32]),
            owner: owner(),
        }
    }

    fn apply(byte: u8) -> PortCall {
        PortCall::Apply {
            peer: peer(byte),
            source: SourceId::from_digest([byte; 32]),
            owner: owner(),
        }
    }

    fn started(byte: u8, incarnation: u64) -> ReactorEvent {
        ReactorEvent::Started {
            request: RequestRef {
                peer: peer(byte),
                session_id: 1,
                request_id: 1,
                incarnation,
                owner: owner(),
                target_tip_hash: block::Hash([2; 32]),
            },
            purpose: crate::zakura::header_sync::HeaderTargetPurpose::Normal,
        }
    }

    fn held(call: u64, kind: StorageCallKind, byte: u8, incarnation: u64) -> ReactorEvent {
        ReactorEvent::HeadersHeld {
            call,
            kind,
            peer: peer(byte),
            incarnation,
            headers: 5,
        }
    }

    fn ended(call: u64, incarnation: u64, end: HeaderWorkEnd) -> ReactorEvent {
        ReactorEvent::HeadersEnded {
            call,
            incarnation,
            headers: 5,
            end,
        }
    }

    fn dispatched(call: u64, op: PortCall) -> ReactorEvent {
        ReactorEvent::Dispatched { call, op }
    }

    /// Folds `events`, and returns the first broken rule's name.
    fn fold(events: Vec<ReactorEvent>) -> Result<HeaderWorkLedger, String> {
        let mut ledger = HeaderWorkLedger::default();
        for event in &events {
            ledger.observe(event)?;
        }
        Ok(ledger)
    }

    fn refused_as(events: Vec<ReactorEvent>, rule: &str) {
        let error = fold(events).expect_err("the ledger refuses this history");
        assert!(error.starts_with(rule), "{error}");
    }

    #[test]
    fn a_prepare_handed_to_its_apply_folds_clean() {
        let ledger = fold(vec![
            started(1, 1),
            dispatched(1, prepare(1)),
            held(1, StorageCallKind::Prepare, 1, 1),
            ReactorEvent::CallEnded {
                call: 1,
                end: CallEnd::Replied,
            },
            dispatched(2, apply(1)),
            ended(1, 1, HeaderWorkEnd::HandedToApply { apply: 2 }),
            held(2, StorageCallKind::Apply, 1, 1),
            ReactorEvent::Handled {
                call: 1,
                panicked: false,
            },
            ReactorEvent::CallEnded {
                call: 2,
                end: CallEnd::Replied,
            },
            ended(2, 1, HeaderWorkEnd::Settled),
            ReactorEvent::Handled {
                call: 2,
                panicked: false,
            },
        ])
        .expect("the history is exact");
        ledger.finish().expect("every call ended");
    }

    /// #1845: a capture naming a retired round fails.
    #[test]
    fn a_capture_must_name_its_peers_current_round() {
        refused_as(
            vec![
                started(1, 1),
                started(2, 2),
                dispatched(1, prepare(1)),
                held(1, StorageCallKind::Prepare, 1, 2),
            ],
            "WrongRound",
        );
    }

    /// #1846: a handoff to the prepare's own call fails.
    #[test]
    fn a_handoff_must_name_an_apply_for_the_same_work() {
        let base = vec![
            started(1, 1),
            dispatched(1, prepare(1)),
            held(1, StorageCallKind::Prepare, 1, 1),
        ];
        let mut own_call = base.clone();
        own_call.push(ended(1, 1, HeaderWorkEnd::HandedToApply { apply: 1 }));
        refused_as(own_call, "BadHandoff");

        let mut other_peer = base;
        other_peer.push(dispatched(2, apply(2)));
        other_peer.push(ended(1, 1, HeaderWorkEnd::HandedToApply { apply: 2 }));
        refused_as(other_peer, "BadHandoff");
    }

    /// #1842: `Handled` without `CallEnded` fails without waiting for teardown.
    #[test]
    fn a_handled_call_must_have_reported_its_end() {
        refused_as(
            vec![
                dispatched(1, prepare(1)),
                ReactorEvent::Handled {
                    call: 1,
                    panicked: false,
                },
            ],
            "MissingCallEnd",
        );
        refused_as(
            vec![
                dispatched(1, prepare(1)),
                ReactorEvent::CallEnded {
                    call: 1,
                    end: CallEnd::Dropped,
                },
                ReactorEvent::Handled {
                    call: 1,
                    panicked: false,
                },
            ],
            "MissingCallEnd",
        );
    }

    #[test]
    fn ownership_ends_once_with_its_exact_count() {
        let held_call = vec![
            started(1, 1),
            dispatched(1, prepare(1)),
            held(1, StorageCallKind::Prepare, 1, 1),
        ];
        let mut twice = held_call.clone();
        twice.push(ended(1, 1, HeaderWorkEnd::Stale));
        twice.push(ended(1, 1, HeaderWorkEnd::Stale));
        refused_as(twice, "EndedTwice");

        let mut wrong = held_call.clone();
        wrong.push(ReactorEvent::HeadersEnded {
            call: 1,
            incarnation: 1,
            headers: 4,
            end: HeaderWorkEnd::Stale,
        });
        refused_as(wrong, "WrongRound");

        let open = fold(held_call).expect("a held call folds");
        assert!(open
            .finish()
            .expect_err("a held call stays open")
            .starts_with("UnendedCall"));
    }

    #[test]
    fn rounds_count_without_gaps() {
        refused_as(vec![started(1, 2)], "RoundGap");
    }
}
