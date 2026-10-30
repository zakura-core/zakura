//! A tapped reactor's work identities (M6a-4).
//!
//! The reactor's work queue reports each header capacity reservation as it
//! places it, `Placed`, and the work incarnation it then belongs to,
//! `Bound`: at once for an active request's continuation, or when the
//! request it was reserved for starts. A request's `Started` and
//! `Retired` name their incarnation, and each prepare or apply call's
//! `LeasesCaptured` joins the call to the incarnation and the lease IDs it
//! holds. [`WorkLedger`] folds those records in log order and checks them
//! against each settled snapshot, whose holdings it reads from the queue's
//! own maps. Any break is a structural failure of the tap's evidence.

use std::collections::{BTreeMap, BTreeSet};

use crate::observe::{
    ReactorFact, ReactorPlacement, ReactorRequest, ReactorSnapshot, ReactorWorkState,
};

/// One placed reservation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reservation {
    /// The peer it was placed for.
    pub peer: String,
    /// The headers it reserved.
    pub count: u64,
    /// The work the queue held for the peer then.
    pub placement: Option<ReactorPlacement>,
    /// The incarnation it's bound to, once it is.
    pub incarnation: Option<u64>,
}

/// One work incarnation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Incarnation {
    /// The peer whose work it is.
    pub peer: String,
    /// The reservations bound to it, in binding order.
    pub reservations: Vec<u64>,
    /// Its request, once it started.
    pub started: Option<ReactorRequest>,
    /// Whether its request was retired.
    pub retired: bool,
    /// The prepare and apply calls that captured its leases, in call order.
    pub calls: Vec<u64>,
}

/// A tapped reactor's placements, bindings and captures, folded in log
/// order.
#[derive(Debug, Default)]
pub struct WorkLedger {
    reservations: BTreeMap<u64, Reservation>,
    incarnations: BTreeMap<u64, Incarnation>,
    /// Calls the reactor dispatched.
    calls: BTreeSet<u64>,
    /// Calls whose leases were captured.
    captured: BTreeSet<u64>,
    /// Each peer's most recently started incarnation.
    latest: BTreeMap<String, u64>,
}

impl WorkLedger {
    /// Placed reservation `id`, if it was placed.
    pub fn reservation(&self, id: u64) -> Option<&Reservation> {
        self.reservations.get(&id)
    }

    /// Incarnation `id`, if anything bound to it.
    pub fn incarnation(&self, id: u64) -> Option<&Incarnation> {
        self.incarnations.get(&id)
    }

    /// Every incarnation, by ID.
    pub fn incarnations(&self) -> &BTreeMap<u64, Incarnation> {
        &self.incarnations
    }

    /// Folds `fact`, failing with a detail if it breaks the identities
    /// before it.
    pub(crate) fn fold(&mut self, fact: &ReactorFact) -> Result<(), String> {
        match fact {
            ReactorFact::Placed {
                reservation,
                peer,
                count,
                placement,
            } => {
                let placed = Reservation {
                    peer: peer.clone(),
                    count: *count,
                    placement: placement.clone(),
                    incarnation: None,
                };
                if self.reservations.insert(*reservation, placed).is_some() {
                    return Err(format!(
                        "DuplicateResource: reservation {reservation} was placed twice"
                    ));
                }
            }
            ReactorFact::Bound {
                reservation,
                incarnation,
            } => {
                let Some(placed) = self.reservations.get_mut(reservation) else {
                    return Err(format!(
                        "UnplacedBinding: reservation {reservation} was bound to incarnation \
                         {incarnation} before it was placed"
                    ));
                };
                if let Some(bound) = placed.incarnation {
                    return Err(format!(
                        "ReboundReservation: reservation {reservation}, bound to incarnation \
                         {bound}, was bound again to {incarnation}"
                    ));
                }
                placed.incarnation = Some(*incarnation);
                let work = self
                    .incarnations
                    .entry(*incarnation)
                    .or_insert_with(|| Incarnation {
                        peer: placed.peer.clone(),
                        reservations: Vec::new(),
                        started: None,
                        retired: false,
                        calls: Vec::new(),
                    });
                // A continuation binds to work that already started, which
                // must be its peer's current round.
                if work.started.is_some()
                    && (work.retired || self.latest.get(&placed.peer) != Some(incarnation))
                {
                    return Err(format!(
                        "StaleBinding: reservation {reservation} was bound to {}'s incarnation \
                         {incarnation}, which isn't its current round",
                        placed.peer
                    ));
                }
                if work.peer != placed.peer {
                    return Err(format!(
                        "ForeignBinding: reservation {reservation}, placed for {}, was bound to \
                         incarnation {incarnation} of {}",
                        placed.peer, work.peer
                    ));
                }
                work.reservations.push(*reservation);
            }
            ReactorFact::Started { request, .. } => {
                let work = self.known(request)?;
                if work.started.is_some() {
                    return Err(format!(
                        "DuplicateIncarnation: incarnation {} started twice",
                        request.incarnation
                    ));
                }
                work.started = Some(request.clone());
                self.latest
                    .insert(request.peer.clone(), request.incarnation);
            }
            ReactorFact::Retired { request, .. } => {
                let work = self.known(request)?;
                if work.started.is_none() || work.retired {
                    return Err(format!(
                        "UnknownIncarnation: incarnation {} retired before it started, or twice",
                        request.incarnation
                    ));
                }
                work.retired = true;
            }
            ReactorFact::Dispatched { call, .. } => {
                self.calls.insert(*call);
            }
            ReactorFact::LeasesCaptured {
                call, incarnation, ..
            } => {
                if !self.calls.contains(call) {
                    return Err(format!(
                        "UncalledCapture: call {call} captured leases before its dispatch"
                    ));
                }
                if !self.captured.insert(*call) {
                    return Err(format!(
                        "DuplicateCapture: call {call} captured leases twice"
                    ));
                }
                if let Some(incarnation) = incarnation {
                    match self.incarnations.get_mut(incarnation) {
                        Some(work) if work.started.is_some() => work.calls.push(*call),
                        _ => {
                            return Err(format!(
                                "UnknownIncarnation: call {call} captured the leases of \
                                 incarnation {incarnation}, which never started"
                            ))
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Checks `snapshot`, a settled one, against the identities folded so
    /// far: each active request is a started incarnation of its peer, and
    /// each reservation the queue holds was placed for its peer and, when
    /// the peer's work is active, bound to that work's incarnation.
    pub(crate) fn settled(&self, snapshot: &ReactorSnapshot) -> Result<(), String> {
        let mut active = BTreeMap::new();
        for work in &snapshot.work {
            let ReactorWorkState::Active { incarnation, .. } = work.state else {
                continue;
            };
            match self.incarnations.get(&incarnation) {
                Some(known) if known.started.is_some() && known.peer == work.peer => {}
                _ => {
                    return Err(format!(
                        "UnknownIncarnation: {}'s active work is incarnation {incarnation}, \
                         which never started for it",
                        work.peer
                    ))
                }
            }
            active.insert(work.peer.as_str(), incarnation);
        }
        for holding in &snapshot.holdings {
            let Some(reservation) = holding.reservation else {
                continue;
            };
            let Some(placed) = self.reservations.get(&reservation) else {
                return Err(format!(
                    "UnplacedResource: the queue holds reservation {reservation} for {}, \
                     which was never placed",
                    holding.peer
                ));
            };
            if placed.peer != holding.peer {
                return Err(format!(
                    "ForeignHolding: the queue holds reservation {reservation}, placed for {}, \
                     for {}",
                    placed.peer, holding.peer
                ));
            }
            if let Some(incarnation) = active.get(holding.peer.as_str()) {
                if placed.incarnation != Some(*incarnation) {
                    return Err(format!(
                        "MisboundReservation: {}'s reservation {reservation} is bound to {:?}, \
                         but its active work is incarnation {incarnation}",
                        holding.peer, placed.incarnation
                    ));
                }
            }
        }
        Ok(())
    }

    /// `request`'s incarnation, which must be bound to its peer.
    fn known(&mut self, request: &ReactorRequest) -> Result<&mut Incarnation, String> {
        match self.incarnations.get_mut(&request.incarnation) {
            Some(work) if work.peer == request.peer => Ok(work),
            Some(work) => Err(format!(
                "ForeignBinding: {}'s request {} names incarnation {}, which is {}'s",
                request.peer, request.request_id, request.incarnation, work.peer
            )),
            None => Err(format!(
                "UnboundIncarnation: {}'s request {} names incarnation {}, which no \
                 reservation is bound to",
                request.peer, request.request_id, request.incarnation
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::{
        ReactorHolding, ReactorPurpose, ReactorWork, StorageBranch, WorkOwner, WorkScope,
    };

    /// Peer `byte`'s request `incarnation`.
    fn request(byte: &str, incarnation: u64) -> ReactorRequest {
        let scope = WorkScope {
            header_generation: 1,
            branch: StorageBranch {
                anchor: "aa".into(),
                target: "bb".into(),
            },
        };
        ReactorRequest {
            peer: byte.into(),
            session_id: 1,
            request_id: 1,
            incarnation,
            owner: WorkOwner {
                kind: "header".into(),
                scope,
                verified_generation: None,
                body_work_epoch: None,
                session_id: 1,
                request_id: 1,
            },
            target: "bb".into(),
        }
    }

    fn placed(reservation: u64, peer: &str) -> ReactorFact {
        ReactorFact::Placed {
            reservation,
            peer: peer.into(),
            count: 1,
            placement: None,
        }
    }

    fn bound(reservation: u64, incarnation: u64) -> ReactorFact {
        ReactorFact::Bound {
            reservation,
            incarnation,
        }
    }

    fn started(peer: &str, incarnation: u64) -> ReactorFact {
        ReactorFact::Started {
            request: request(peer, incarnation),
            purpose: ReactorPurpose::Normal,
        }
    }

    /// A settled snapshot where `peer` runs incarnation `incarnation` and
    /// the queue holds `reservation` for it.
    fn snapshot(peer: &str, incarnation: u64, reservation: u64) -> ReactorSnapshot {
        let request = request(peer, incarnation);
        ReactorSnapshot {
            work: vec![ReactorWork {
                peer: peer.into(),
                session_id: 1,
                target: "bb".into(),
                state: ReactorWorkState::Active {
                    incarnation,
                    request_id: 1,
                    owner: Box::new(request.owner),
                    purpose: ReactorPurpose::Normal,
                    target_phase: "receiving".into(),
                    held: 0,
                    cursor: None,
                },
            }],
            holdings: vec![ReactorHolding {
                peer: peer.into(),
                reservation: Some(reservation),
                leases: Vec::new(),
            }],
            ..ReactorSnapshot::default()
        }
    }

    /// Folds `facts` in order, returning the first failure's name.
    fn fold(ledger: &mut WorkLedger, facts: &[ReactorFact]) -> Option<String> {
        facts.iter().find_map(|fact| {
            ledger
                .fold(fact)
                .err()
                .map(|detail| detail.split(':').next().unwrap_or_default().to_owned())
        })
    }

    /// A placement, binding, start and continuation that agree fold
    /// cleanly and match their snapshot, and each kind of break fails with
    /// its own name.
    ///
    /// This fails if a check is dropped.
    #[test]
    fn the_ledger_refuses_each_broken_identity() {
        let good = [
            placed(1, "01"),
            bound(1, 1),
            started("01", 1),
            placed(2, "01"),
            bound(2, 1),
        ];
        let mut ledger = WorkLedger::default();
        assert_eq!(fold(&mut ledger, &good), None);
        assert_eq!(ledger.settled(&snapshot("01", 1, 2)), Ok(()));
        assert_eq!(ledger.incarnation(1).unwrap().reservations, [1, 2]);

        let retired = ReactorFact::Retired {
            request: request("01", 1),
            phase: "receiving".into(),
            purpose: ReactorPurpose::Normal,
            reason: "local_error".into(),
        };
        let broken: [(&[ReactorFact], &str); 8] = [
            (&[placed(1, "01"), placed(1, "01")], "DuplicateResource"),
            (&[bound(1, 1)], "UnplacedBinding"),
            (
                &[placed(1, "01"), bound(1, 1), bound(1, 2)],
                "ReboundReservation",
            ),
            (
                &[placed(1, "01"), bound(1, 1), placed(2, "02"), bound(2, 1)],
                "ForeignBinding",
            ),
            (&[started("01", 1)], "UnboundIncarnation"),
            (
                &[
                    placed(1, "01"),
                    bound(1, 1),
                    started("01", 1),
                    started("01", 1),
                ],
                "DuplicateIncarnation",
            ),
            (
                &[
                    placed(1, "01"),
                    bound(1, 1),
                    started("01", 1),
                    retired.clone(),
                    placed(2, "01"),
                    bound(2, 1),
                ],
                "StaleBinding",
            ),
            (
                &[
                    placed(1, "01"),
                    bound(1, 1),
                    started("01", 1),
                    placed(2, "01"),
                    bound(2, 2),
                    started("01", 2),
                    placed(3, "01"),
                    bound(3, 1),
                ],
                "StaleBinding",
            ),
        ];
        for (facts, name) in broken {
            assert_eq!(
                fold(&mut WorkLedger::default(), facts).as_deref(),
                Some(name),
                "{facts:?}"
            );
        }
        let capture = ReactorFact::LeasesCaptured {
            call: 1,
            incarnation: Some(1),
            leases: vec![3],
        };
        assert_eq!(
            fold(&mut WorkLedger::default(), std::slice::from_ref(&capture)).as_deref(),
            Some("UncalledCapture")
        );

        let named = |result: Result<(), String>| {
            result
                .err()
                .map(|detail| detail.split(':').next().unwrap_or_default().to_owned())
        };
        // The continuation's reservation 2 isn't the incarnation's in this
        // snapshot's world: 3 was never placed, and 1 is bound but 2 runs.
        assert_eq!(
            named(ledger.settled(&snapshot("01", 1, 3))).as_deref(),
            Some("UnplacedResource")
        );
        assert_eq!(
            named(ledger.settled(&snapshot("01", 2, 2))).as_deref(),
            Some("UnknownIncarnation")
        );
        let mut misbound = WorkLedger::default();
        fold(
            &mut misbound,
            &[
                placed(1, "01"),
                bound(1, 1),
                started("01", 1),
                placed(2, "01"),
                bound(2, 7),
            ],
        );
        assert_eq!(
            named(misbound.settled(&snapshot("01", 1, 2))).as_deref(),
            Some("MisboundReservation")
        );
    }
}
