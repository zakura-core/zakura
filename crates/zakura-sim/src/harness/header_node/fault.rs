//! Whether the node penalizes the peer behind each header fault its storage
//! reports, judged from the log and the storage source's view-side
//! [`FaultAttributions`].
//!
//! [`owed`] applies production's rule for an automatic header-peer fault on
//! its own: the failure blames the header peer, that peer is the source the
//! request named, and the category is `malformed_protocol` or
//! `invalid_header`. [`FaultEvidence::penalized`] then wants a matching
//! `Misbehavior` for each one. This is the first rule of
//! `header-sync/header-fault-penalized`. A disconnect before the failure
//! settles changes nothing it owes.

use std::{cell::Cell, collections::BTreeMap, rc::Rc};

use super::adoption::{settled, Acceptance};
use crate::{
    observe::{Entry, ReactorFact, StorageFact, StorageRequest, StorageResult},
    storage::{Blame, FaultAttributions, HEADER_SERVICE},
    Observation, RunEnd,
};

/// The node's storage instance in the header harnesses.
const NODE: u64 = 0;

/// A penalty the node owes for one failed operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwedPenalty {
    /// The failed operation's ID.
    pub op: u64,
    /// `prepare` or `apply`.
    pub stage: &'static str,
    /// The supplier the request named, as the hex of its digest.
    pub source: String,
    /// The session the work belonged to.
    pub session_id: u64,
    /// The `Misbehavior` reason its category calls for.
    pub reason: &'static str,
    /// The storage fact at which the reactor took the failed reply. The
    /// penalty comes after it.
    pub taken: u64,
}

/// The `Misbehavior` reason a failure owes, from production's rule applied
/// independently: `blame` names the header peer, that peer is `source`, the
/// request's supplier, and `category` is `malformed_protocol` or
/// `invalid_header`. `None` if it owes none.
pub fn owes_penalty(source: &str, category: &str, blame: &Blame) -> Option<&'static str> {
    let reason = match category {
        "malformed_protocol" => "malformed-message",
        "invalid_header" => "invalid-header",
        _ => return None,
    };
    matches!(blame, Blame::HeaderPeer(blamed) if blamed == source).then_some(reason)
}

/// Each penalty the node's storage facts in `entries` owe, in the order the
/// reactor took the failed replies. A failure whose reply the reactor never
/// took owes nothing, since the reactor never saw it. An error if a failure
/// has no attribution in `attributions`, or completes no logged request.
pub fn owed(
    entries: &[Entry],
    attributions: &FaultAttributions,
) -> Result<Vec<OwedPenalty>, String> {
    let mut requests: BTreeMap<u64, (&'static str, &str, u64)> = BTreeMap::new();
    let mut failed: BTreeMap<u64, (&'static str, &str, u64, &str)> = BTreeMap::new();
    let mut owed = Vec::new();
    for entry in entries {
        let Observation::Storage {
            service,
            instance: NODE,
            seq,
            fact,
            ..
        } = &entry.obs
        else {
            continue;
        };
        if service != HEADER_SERVICE {
            continue;
        }
        match fact {
            StorageFact::Submitted {
                id,
                request:
                    StorageRequest::PrepareHeaderTarget { owner, source, .. }
                    | StorageRequest::ApplyHeaderTarget { owner, source, .. },
                ..
            } => {
                let stage = match fact {
                    StorageFact::Submitted {
                        request: StorageRequest::PrepareHeaderTarget { .. },
                        ..
                    } => "prepare",
                    _ => "apply",
                };
                requests.insert(*id, (stage, source, owner.session_id));
            }
            StorageFact::Completed {
                id,
                result:
                    StorageResult::PrepareFailed { category } | StorageResult::ApplyFailed { category },
                ..
            } => {
                let (stage, source, session_id) = *requests.get(id).ok_or_else(|| {
                    format!("UnknownOperation: operation {id} failed with no logged request")
                })?;
                failed.insert(*id, (stage, source, session_id, category));
            }
            StorageFact::ReplyTaken { id } => {
                let Some((stage, source, session_id, category)) = failed.remove(id) else {
                    continue;
                };
                let blame = attributions.get(NODE, *id).ok_or_else(|| {
                    format!("MissingAttribution: failed operation {id} has no attribution")
                })?;
                if let Some(reason) = owes_penalty(source, category, &blame) {
                    owed.push(OwedPenalty {
                        op: *id,
                        stage,
                        source: source.to_owned(),
                        session_id,
                        reason,
                        taken: *seq,
                    });
                }
            }
            _ => {}
        }
    }
    Ok(owed)
}

/// The node's fault evidence: whom its storage failures blamed, and, once
/// the run ends, whether it settled. Cloning shares both.
#[derive(Clone, Debug, Default)]
pub struct FaultEvidence {
    attributions: FaultAttributions,
    settled: Rc<Cell<Option<bool>>>,
}

impl FaultEvidence {
    /// Evidence that reads `attributions`.
    pub fn new(attributions: FaultAttributions) -> Self {
        FaultEvidence {
            attributions,
            settled: Rc::default(),
        }
    }

    /// Whom each failed prepare and apply blamed.
    pub fn attributions(&self) -> &FaultAttributions {
        &self.attributions
    }

    /// Records how the run ended, and whether it [`settled`].
    pub fn end(&self, end: &RunEnd) {
        self.settled.set(Some(settled(end)));
    }

    /// The penalty rule over `entries`, the run's log: each penalty
    /// [`owed`] is matched by a `Misbehavior` the reactor reported after it
    /// took the failed reply, naming the source's peer, the work's session as
    /// its origin and the reason the category calls for. One `Misbehavior`
    /// matches one penalty. A penalty still unmatched when a settled run ends
    /// is a break, since the reactor reports it while it handles the reply
    /// it took. Unmatched at any other end, or before the run has ended, it's
    /// inconclusive.
    pub fn penalized(&self, entries: &[Entry]) -> Acceptance {
        let owed = match owed(entries, &self.attributions) {
            Ok(owed) => owed,
            Err(why) => return Acceptance::Violated(why),
        };
        let mut penalties: Vec<(u64, &str, &str, Option<u64>)> = entries
            .iter()
            .filter_map(|entry| match &entry.obs {
                Observation::Reactor {
                    storage_seq,
                    event:
                        ReactorFact::Misbehavior {
                            peer,
                            reason,
                            origin,
                            ..
                        },
                    ..
                } => Some((*storage_seq, peer.as_str(), reason.as_str(), *origin)),
                _ => None,
            })
            .collect();
        let mut unpenalized = Vec::new();
        for penalty in owed {
            let found = penalties.iter().position(|(after, peer, reason, origin)| {
                *after >= penalty.taken
                    && *peer == penalty.source
                    && *reason == penalty.reason
                    && *origin == Some(penalty.session_id)
            });
            match found {
                Some(at) => {
                    penalties.remove(at);
                }
                None => unpenalized.push(penalty),
            }
        }
        let Some(first) = unpenalized.first() else {
            return Acceptance::Holds;
        };
        let what = format!(
            "{} operation {} failed with a header-peer fault of {} in session {}, and no {} \
             Misbehavior from that session followed",
            first.stage, first.op, first.source, first.session_id, first.reason
        );
        match self.settled.get() {
            Some(true) => Acceptance::Violated(format!("UnpenalizedFault: {what}")),
            Some(false) => Acceptance::Inconclusive(format!(
                "the run stopped while a task could be partway through handling it: {what}"
            )),
            None => Acceptance::Inconclusive(format!("the run hasn't ended: {what}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::{StorageBranch, StoragePoint, WorkOwner, WorkScope};

    /// A header-peer fault of `source`.
    fn blame(source: &str) -> Blame {
        Blame::HeaderPeer(source.to_owned())
    }

    /// The node's storage fact `fact`, its `seq`th.
    fn storage(seq: u64, fact: StorageFact) -> Entry {
        Entry {
            step: seq,
            obs: Observation::Storage {
                service: HEADER_SERVICE.to_owned(),
                instance: NODE,
                seq,
                txn: seq,
                fact,
            },
        }
    }

    /// A point with an empty hash.
    fn point(height: u32) -> StoragePoint {
        StoragePoint {
            height,
            hash: String::new(),
        }
    }

    /// Operation `id`: a prepare from `source` in `session_id`, failing
    /// with `category` and taken by the reactor, as facts `id * 10` on.
    fn failed_prepare(id: u64, source: &str, session_id: u64, category: &str) -> Vec<Entry> {
        let owner = WorkOwner {
            kind: "header".to_owned(),
            scope: WorkScope {
                header_generation: 1,
                branch: StorageBranch {
                    anchor: String::new(),
                    target: String::new(),
                },
            },
            verified_generation: None,
            body_work_epoch: None,
            session_id,
            request_id: id,
        };
        vec![
            storage(
                id * 10,
                StorageFact::Submitted {
                    id,
                    request: StorageRequest::PrepareHeaderTarget {
                        owner,
                        source: source.to_owned(),
                        common_ancestor: point(0),
                        target: point(1),
                        headers: 1,
                    },
                    digest: String::new(),
                    call: None,
                },
            ),
            storage(
                id * 10 + 1,
                StorageFact::Completed {
                    id,
                    outcome: "engine".to_owned(),
                    result: StorageResult::PrepareFailed {
                        category: category.to_owned(),
                    },
                    delivered: true,
                    generation: 0,
                    state: String::new(),
                },
            ),
            storage(id * 10 + 2, StorageFact::ReplyTaken { id }),
        ]
    }

    /// The reactor's `Misbehavior` against `peer` from session `origin`,
    /// after storage fact `storage_seq`.
    fn misbehavior(storage_seq: u64, peer: &str, origin: u64) -> Entry {
        Entry {
            step: storage_seq,
            obs: Observation::Reactor {
                instance: 0,
                seq: storage_seq,
                storage_seq,
                node: "node".to_owned(),
                event: ReactorFact::Misbehavior {
                    peer: peer.to_owned(),
                    reason: "invalid-header".to_owned(),
                    origin: Some(origin),
                    current: None,
                },
            },
        }
    }

    /// Fault evidence for a settled run, where each operation in `blamed`
    /// blamed its source.
    fn evidence(blamed: &[(u64, Blame)]) -> FaultEvidence {
        let attributions = FaultAttributions::default();
        for (id, blame) in blamed {
            attributions.insert(NODE, *id, blame.clone());
        }
        let evidence = FaultEvidence::new(attributions);
        evidence.settled.set(Some(true));
        evidence
    }

    #[test]
    fn only_a_header_peer_fault_of_the_requests_source_owes_a_penalty() {
        assert_eq!(
            owes_penalty("aa", "invalid_header", &blame("aa")),
            Some("invalid-header")
        );
        assert_eq!(
            owes_penalty("aa", "malformed_protocol", &blame("aa")),
            Some("malformed-message")
        );
        // Another source, another kind of peer, no peer, or a category that
        // isn't the supplier's fault.
        assert_eq!(owes_penalty("aa", "invalid_header", &blame("bb")), None);
        assert_eq!(
            owes_penalty("aa", "invalid_header", &Blame::BodyPeer("aa".to_owned())),
            None
        );
        assert_eq!(owes_penalty("aa", "invalid_header", &Blame::None), None);
        assert_eq!(
            owes_penalty("aa", "local_resource_or_storage", &blame("aa")),
            None
        );
    }

    /// The mismatched-source control: a failure that blames a peer other
    /// than the request's source owes nothing, so its missing penalty is no
    /// break. The same log blaming the request's source owes one.
    #[test]
    fn a_fault_blaming_another_source_owes_nothing() {
        let log = failed_prepare(1, "aa", 3, "invalid_header");
        assert_eq!(
            evidence(&[(1, blame("bb"))]).penalized(&log),
            Acceptance::Holds
        );
        let owed = owed(&log, evidence(&[(1, blame("aa"))]).attributions()).unwrap();
        assert_eq!(
            owed,
            vec![OwedPenalty {
                op: 1,
                stage: "prepare",
                source: "aa".to_owned(),
                session_id: 3,
                reason: "invalid-header",
                taken: 12,
            }]
        );
    }

    #[test]
    fn each_penalty_needs_its_own_misbehavior_after_the_reply() {
        let attributed = evidence(&[(1, blame("aa")), (2, blame("aa"))]);
        let mut log = failed_prepare(1, "aa", 3, "invalid_header");
        // Before the reactor took the reply, or from another session.
        log.push(misbehavior(11, "aa", 3));
        log.push(misbehavior(12, "aa", 4));
        assert!(matches!(
            attributed.penalized(&log[..]),
            Acceptance::Violated(why) if why.starts_with("UnpenalizedFault")
        ));
        log.push(misbehavior(12, "aa", 3));
        assert_eq!(attributed.penalized(&log), Acceptance::Holds);
        // A second fault in the same session needs a second penalty.
        log.extend(failed_prepare(2, "aa", 3, "invalid_header"));
        assert!(matches!(
            attributed.penalized(&log),
            Acceptance::Violated(why) if why.contains("operation 2")
        ));
        log.push(misbehavior(22, "aa", 3));
        assert_eq!(attributed.penalized(&log), Acceptance::Holds);
    }

    #[test]
    fn an_unpenalized_fault_is_inconclusive_until_a_settled_end() {
        let attributed = evidence(&[(1, blame("aa"))]);
        let log = failed_prepare(1, "aa", 3, "invalid_header");
        attributed.settled.set(None);
        assert!(matches!(
            attributed.penalized(&log),
            Acceptance::Inconclusive(_)
        ));
        attributed.settled.set(Some(false));
        assert!(matches!(
            attributed.penalized(&log),
            Acceptance::Inconclusive(_)
        ));
    }

    #[test]
    fn a_failure_with_no_attribution_is_a_break() {
        let log = failed_prepare(1, "aa", 3, "invalid_header");
        assert!(matches!(
            evidence(&[]).penalized(&log),
            Acceptance::Violated(why) if why.starts_with("MissingAttribution")
        ));
    }
}
