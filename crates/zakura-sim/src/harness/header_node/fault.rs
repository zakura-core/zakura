//! Whether the node penalizes the peer behind each header fault its storage
//! reports, judged from the log and the storage source's view-side
//! [`FaultAttributions`].
//!
//! [`owed`] applies production's rule for an automatic header-peer fault on
//! its own: the failure blames the header peer, that peer is the source the
//! request named, and the category is `malformed_protocol` or
//! `invalid_header`. [`FaultEvidence::penalized`] then wants a matching
//! `Misbehavior` for each one, reported while the reactor handled the call
//! that took the failed reply. This is the first rule of
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
    /// The reactor port call that submitted the operation, if one did.
    pub call: Option<u64>,
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
/// took owes nothing, since the reactor never saw it ([`untaken`] lists
/// them). An error if a failure has no attribution in `attributions`, or
/// completes no logged request.
pub fn owed(
    entries: &[Entry],
    attributions: &FaultAttributions,
) -> Result<Vec<OwedPenalty>, String> {
    scan(entries, attributions).map(|(owed, _)| owed)
}

/// The operations in `entries` that failed with a fault that would owe a
/// penalty, but whose reply the reactor never took: it dropped or timed out
/// the call, or the log ends first. Whether the node should have penalized
/// them can't be judged from the reactor's side.
pub fn untaken(entries: &[Entry], attributions: &FaultAttributions) -> Result<Vec<u64>, String> {
    scan(entries, attributions).map(|(_, untaken)| untaken)
}

/// [`owed`] and [`untaken`] in one pass over `entries`.
fn scan(
    entries: &[Entry],
    attributions: &FaultAttributions,
) -> Result<(Vec<OwedPenalty>, Vec<u64>), String> {
    let mut requests: BTreeMap<u64, Request<'_>> = BTreeMap::new();
    let mut failed: BTreeMap<u64, (Request<'_>, &str)> = BTreeMap::new();
    let mut owed = Vec::new();
    for entry in entries {
        let Observation::Storage {
            service,
            instance: NODE,
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
                call,
                ..
            } => {
                let stage = match fact {
                    StorageFact::Submitted {
                        request: StorageRequest::PrepareHeaderTarget { .. },
                        ..
                    } => "prepare",
                    _ => "apply",
                };
                requests.insert(
                    *id,
                    Request {
                        stage,
                        source,
                        session_id: owner.session_id,
                        call: *call,
                    },
                );
            }
            StorageFact::Completed {
                id,
                result:
                    StorageResult::PrepareFailed { category } | StorageResult::ApplyFailed { category },
                ..
            } => {
                let request = *requests.get(id).ok_or_else(|| {
                    format!("UnknownOperation: operation {id} failed with no logged request")
                })?;
                failed.insert(*id, (request, category));
            }
            StorageFact::ReplyTaken { id } => {
                let Some((request, category)) = failed.remove(id) else {
                    continue;
                };
                if let Some(reason) = owes(attributions, *id, request, category)? {
                    owed.push(OwedPenalty {
                        op: *id,
                        stage: request.stage,
                        source: request.source.to_owned(),
                        session_id: request.session_id,
                        reason,
                        call: request.call,
                    });
                }
            }
            _ => {}
        }
    }
    let mut untaken = Vec::new();
    for (id, (request, category)) in failed {
        if owes(attributions, id, request, category)?.is_some() {
            untaken.push(id);
        }
    }
    Ok((owed, untaken))
}

/// The reason failed operation `id`, from `request`, with `category`, owes,
/// by its attribution, or an error if it has none.
fn owes(
    attributions: &FaultAttributions,
    id: u64,
    request: Request<'_>,
    category: &str,
) -> Result<Option<&'static str>, String> {
    let blame = attributions
        .get(NODE, id)
        .ok_or_else(|| format!("MissingAttribution: failed operation {id} has no attribution"))?;
    Ok(owes_penalty(request.source, category, &blame))
}

/// A logged prepare or apply request, as [`owed`] reads it.
#[derive(Clone, Copy)]
struct Request<'a> {
    stage: &'static str,
    source: &'a str,
    session_id: u64,
    call: Option<u64>,
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
    /// [`owed`] is matched by a `Misbehavior` naming the source's peer, the
    /// work's session as its origin and the reason the category calls for,
    /// which the reactor reported while it handled the call that submitted
    /// the operation: strictly between that call's `CallEnded` and `Handled`,
    /// in the tap's order. A `Misbehavior` from another completion, even
    /// against the same session, matches nothing. A penalty still unmatched
    /// when a settled run ends is a break. Unmatched at any other end, or
    /// before the run has ended, it's inconclusive. With nothing unmatched,
    /// a fault the reactor never took ([`untaken`]) leaves the rule
    /// inconclusive too, so a node that abandons such work can't pass it.
    pub fn penalized(&self, entries: &[Entry]) -> Acceptance {
        let (owed, untaken) = match scan(entries, &self.attributions) {
            Ok(scanned) => scanned,
            Err(why) => return Acceptance::Violated(why),
        };
        let events: Vec<&ReactorFact> = entries
            .iter()
            .filter_map(|entry| match &entry.obs {
                Observation::Reactor { event, .. } => Some(event),
                _ => None,
            })
            .collect();
        let penalized = |penalty: &OwedPenalty| {
            let Some(call) = penalty.call else {
                return false;
            };
            let Some(start) = events.iter().position(
                |event| matches!(event, ReactorFact::CallEnded { call: ended, .. } if *ended == call),
            ) else {
                return false;
            };
            events[start + 1..]
                .iter()
                .take_while(
                    |event| !matches!(event, ReactorFact::Handled { call: handled, .. } if *handled == call),
                )
                .any(|event| {
                    matches!(event, ReactorFact::Misbehavior { peer, reason, origin, .. }
                        if *peer == penalty.source
                            && reason == penalty.reason
                            && *origin == Some(penalty.session_id))
                })
        };
        let Some(first) = owed.iter().find(|penalty| !penalized(penalty)) else {
            return match untaken.first() {
                None => Acceptance::Holds,
                Some(op) => Acceptance::Inconclusive(format!(
                    "UntakenFault: operation {op} failed with a header-peer fault whose reply \
                     the reactor never took"
                )),
            };
        };
        let what = format!(
            "{} operation {} failed with a header-peer fault of {} in session {}, and the \
             reactor reported no {} Misbehavior from that session while it handled call {:?}",
            first.stage, first.op, first.source, first.session_id, first.reason, first.call
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

    /// Operation `id`: a prepare from `source` in `session_id` that reactor
    /// call `id` submitted, failing with `category` and taken by the
    /// reactor, as facts `id * 10` on.
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
                    call: Some(id),
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

    /// The reactor's `event`.
    fn reactor(event: ReactorFact) -> Entry {
        Entry {
            step: 0,
            obs: Observation::Reactor {
                instance: 0,
                seq: 0,
                storage_seq: 0,
                node: "node".to_owned(),
                event,
            },
        }
    }

    /// The reactor's `Misbehavior` against `peer` from session `origin`.
    fn misbehavior(peer: &str, origin: u64) -> Entry {
        reactor(ReactorFact::Misbehavior {
            peer: peer.to_owned(),
            reason: "invalid-header".to_owned(),
            origin: Some(origin),
            current: None,
        })
    }

    /// The reactor's handling of call `call`'s completion, during which it
    /// reported `during`.
    fn handled(call: u64, during: Vec<Entry>) -> Vec<Entry> {
        let mut entries = vec![reactor(ReactorFact::CallEnded {
            call,
            end: "replied".to_owned(),
            op: Some(call),
        })];
        entries.extend(during);
        entries.push(reactor(ReactorFact::Handled {
            call,
            panicked: false,
        }));
        entries
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
        // Through the storage source's projection: a typed failure of a
        // request naming source A that blames source B keeps B's digest, and
        // owes nothing.
        use zakura_header_chain::{Attribution, ErrorCategory, SourceId};
        use zakura_storage::header::evidence::ResultIdentity;

        let digest = |byte: u8| -> String { format!("{byte:02x}").repeat(32) };
        let failed = |blamed: u8| ResultIdentity::PrepareFailed {
            category: ErrorCategory::InvalidHeader,
            attribution: Attribution::HeaderPeer(SourceId::from_digest([blamed; 32])),
        };
        let log = failed_prepare(1, &digest(0xaa), 3, "invalid_header");
        let projected = FaultAttributions::default();
        projected.record_result(NODE, 1, &failed(0xbb));
        assert_eq!(projected.get(NODE, 1), Some(blame(&digest(0xbb))));
        assert_eq!(owed(&log, &projected).unwrap(), Vec::new());
        projected.record_result(NODE, 1, &failed(0xaa));
        assert_eq!(owed(&log, &projected).unwrap().len(), 1);

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
                call: Some(1),
            }]
        );
    }

    /// The penalty must come while the reactor handles the call that took
    /// the failed reply, against the work's session.
    #[test]
    fn the_penalty_comes_while_the_failing_call_is_handled() {
        let attributed = evidence(&[(1, blame("aa"))]);
        let broken = |log: &[Entry]| {
            matches!(attributed.penalized(log),
                Acceptance::Violated(why) if why.starts_with("UnpenalizedFault"))
        };
        let mut log = failed_prepare(1, "aa", 3, "invalid_header");
        // Another session's or peer's penalty during the handling.
        log.extend(handled(1, vec![misbehavior("aa", 4), misbehavior("bb", 3)]));
        assert!(broken(&log));
        // A matching penalty for a later completion, such as a malformed
        // page from the same session, isn't this failure's.
        log.extend(handled(2, vec![misbehavior("aa", 3)]));
        assert!(broken(&log));
        // A matching penalty before the call ended isn't either.
        let mut early = failed_prepare(1, "aa", 3, "invalid_header");
        early.push(misbehavior("aa", 3));
        early.extend(handled(1, Vec::new()));
        assert!(broken(&early));
        let mut log = failed_prepare(1, "aa", 3, "invalid_header");
        log.extend(handled(1, vec![misbehavior("aa", 3)]));
        assert_eq!(attributed.penalized(&log), Acceptance::Holds);
    }

    /// Each failure needs its own call's penalty.
    #[test]
    fn each_failure_needs_its_own_penalty() {
        let attributed = evidence(&[(1, blame("aa")), (2, blame("aa"))]);
        let mut log = failed_prepare(1, "aa", 3, "invalid_header");
        log.extend(failed_prepare(2, "aa", 3, "invalid_header"));
        log.extend(handled(1, vec![misbehavior("aa", 3)]));
        log.extend(handled(2, Vec::new()));
        assert!(matches!(
            attributed.penalized(&log),
            Acceptance::Violated(why) if why.contains("operation 2")
        ));
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

    /// A fault whose reply the reactor never took owes no penalty, and leaves
    /// the rule inconclusive rather than holding.
    #[test]
    fn an_untaken_fault_is_inconclusive() {
        let attributed = evidence(&[(1, blame("aa"))]);
        let mut log = failed_prepare(1, "aa", 3, "invalid_header");
        log.pop();
        assert_eq!(owed(&log, attributed.attributions()).unwrap(), Vec::new());
        assert_eq!(untaken(&log, attributed.attributions()).unwrap(), vec![1]);
        assert!(matches!(
            attributed.penalized(&log),
            Acceptance::Inconclusive(why) if why.starts_with("UntakenFault")
        ));
        // A failure that owes nothing isn't listed.
        let unblamed = evidence(&[(1, blame("bb"))]);
        assert_eq!(
            untaken(&log, unblamed.attributions()).unwrap(),
            Vec::<u64>::new()
        );
        assert_eq!(unblamed.penalized(&log), Acceptance::Holds);
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
