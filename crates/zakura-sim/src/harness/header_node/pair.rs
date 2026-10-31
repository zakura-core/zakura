//! Pairing a tapped reactor's port calls with its storage's receipts.
//!
//! A call that took a reply or timed out ends with `CallEnded` right after
//! its storage receipt, with no scheduling point between them. So the
//! receipt is exactly the fact at the event's `storage_seq`: `ReplyTaken`
//! for `replied` and `port-error`, and `CallerDropped` or `ReplyDropped` for
//! a locator or VCT context that `timed-out`. [`Pairer`] checks that the
//! receipt's operation is the call's own work, pairs the two one to one, and
//! records the operation in the event. A `not-submitted` or
//! `port-panicked` call pairs with nothing.
//!
//! Since the receipt and its `CallEnded` share a step, every receipt a
//! reactor call must have taken pairs in its own drain: every locator's,
//! and every VCT context's, prepare's or apply's whose work some dispatched
//! call has. Anything else fails the run's evidence. A `port-panicked` call
//! is reported instead, since no M4 run expects one and its receipt, if it
//! took one, stays unpaired.

use std::collections::{BTreeMap, BTreeSet};

use super::policy::OpKind;
use crate::observe::{ReactorCall, ReactorFact, StorageFact, StorageRequest, WorkOwner};

/// What a call and its operation must share: the kind, the owner of a VCT
/// context, prepare or apply, and the source of a prepare or apply.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Work {
    kind: OpKind,
    owner: Option<WorkOwner>,
    source: Option<String>,
}

impl Work {
    /// The work `request` asks storage for.
    fn of_request(request: &StorageRequest) -> Self {
        let (owner, source) = match request {
            StorageRequest::VctRepairContext { owner, .. } => (Some(owner.clone()), None),
            StorageRequest::PrepareHeaderTarget { owner, source, .. }
            | StorageRequest::ApplyHeaderTarget { owner, source, .. } => {
                (Some(owner.clone()), Some(source.clone()))
            }
            _ => (None, None),
        };
        Work {
            kind: OpKind::of(request).0,
            owner,
            source,
        }
    }

    /// The work a dispatched `call` asks storage for.
    fn of_call(call: &ReactorCall) -> Self {
        let (kind, owner, source) = match call {
            ReactorCall::Locator { .. } => (OpKind::Locator, None, None),
            ReactorCall::VctContext { owner, .. } => {
                (OpKind::VctContext, Some(owner.clone()), None)
            }
            ReactorCall::Prepare { owner, source, .. } => {
                (OpKind::Prepare, Some(owner.clone()), Some(source.clone()))
            }
            ReactorCall::Apply { owner, source, .. } => {
                (OpKind::Apply, Some(owner.clone()), Some(source.clone()))
            }
        };
        Work {
            kind,
            owner,
            source,
        }
    }
}

/// A fact that ends a caller's interest in an operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Receipt {
    /// `ReplyTaken`.
    ReplyTaken,
    /// `CallerDropped`.
    CallerDropped,
    /// `ReplyDropped`.
    ReplyDropped,
}

/// Why a drain doesn't pair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PairError {
    /// Call `call` ended `port-panicked`. It outranks every other failure of
    /// its drain, since the panic leaves its receipt unpaired.
    PortPanicked {
        /// The first such call in the drain.
        call: u64,
    },
    /// The drain's evidence doesn't pair, led by the failure's name.
    Evidence(String),
}

/// One reactor's calls and its storage's operations, paired drain by drain.
/// See the module doc.
#[derive(Debug, Default)]
pub(crate) struct Pairer {
    /// Each operation's work, from its submission.
    ops: BTreeMap<u64, Work>,
    /// Each receipt fact, by its sequence number, with its operation.
    receipts: BTreeMap<u64, (Receipt, u64)>,
    /// Each dispatched call's work.
    calls: BTreeMap<u64, Work>,
    /// The calls that have ended.
    ended: BTreeSet<u64>,
    /// The operations paired with a call.
    paired: BTreeSet<u64>,
    /// The receipts the current drain's facts hold, by sequence number.
    fresh: Vec<u64>,
}

impl Pairer {
    /// Reads one drain's storage facts, as `(seq, fact)` in order. Call it
    /// before [`Pairer::pair`] for the same drain.
    pub(crate) fn facts<'f>(&mut self, facts: impl IntoIterator<Item = (u64, &'f StorageFact)>) {
        for (seq, fact) in facts {
            let (receipt, id) = match fact {
                StorageFact::Submitted { id, request, .. } => {
                    self.ops.insert(*id, Work::of_request(request));
                    continue;
                }
                StorageFact::ReplyTaken { id } => (Receipt::ReplyTaken, *id),
                StorageFact::CallerDropped { id } => (Receipt::CallerDropped, *id),
                StorageFact::ReplyDropped { id } => (Receipt::ReplyDropped, *id),
                _ => continue,
            };
            self.receipts.insert(seq, (receipt, id));
            self.fresh.push(seq);
        }
    }

    /// Pairs one drain's reactor events, as `(storage_seq, event)` in order,
    /// recording each paired `CallEnded`'s operation in its `op`. Then
    /// requires every receipt of the drain that a call must have taken to
    /// be paired. Every event is paired even after a failure, so a panic
    /// later in the drain still outranks it.
    pub(crate) fn pair<'e>(
        &mut self,
        events: impl IntoIterator<Item = (u64, &'e mut ReactorFact)>,
    ) -> Result<(), PairError> {
        let mut failure = None;
        let mut panicked = None;
        for (storage_seq, event) in events {
            match event {
                ReactorFact::Dispatched { call, port } => {
                    self.calls.insert(*call, Work::of_call(port));
                }
                ReactorFact::CallEnded { call, end, op } => {
                    if end == "port-panicked" {
                        panicked.get_or_insert(*call);
                    }
                    match self.end(*call, end, storage_seq) {
                        Ok(paired) => *op = paired,
                        Err(detail) => {
                            failure.get_or_insert(detail);
                        }
                    }
                }
                _ => {}
            }
        }
        let fresh = std::mem::take(&mut self.fresh);
        if failure.is_none() {
            failure = fresh.into_iter().find_map(|seq| self.unpaired(seq));
        }
        match (panicked, failure) {
            (Some(call), _) => Err(PairError::PortPanicked { call }),
            (None, Some(detail)) => Err(PairError::Evidence(detail)),
            (None, None) => Ok(()),
        }
    }

    /// The operation call `call` pairs with, ending `end` with storage's
    /// last fact at `storage_seq`, or `None` for an end that pairs with
    /// nothing.
    fn end(&mut self, call: u64, end: &str, storage_seq: u64) -> Result<Option<u64>, String> {
        let Some(work) = self.calls.get(&call) else {
            return Err(format!("Mispaired: call {call} ended before its dispatch"));
        };
        if !self.ended.insert(call) {
            return Err(format!("PairedTwice: call {call} ended again"));
        }
        let receipts: &[Receipt] = match end {
            "replied" | "port-error" => &[Receipt::ReplyTaken],
            "timed-out" if matches!(work.kind, OpKind::Locator | OpKind::VctContext) => {
                &[Receipt::CallerDropped, Receipt::ReplyDropped]
            }
            "timed-out" => {
                return Err(format!(
                    "Mispaired: call {call}, a {:?} call, timed out, which only a locator or \
                     VCT context can",
                    work.kind
                ))
            }
            "not-submitted" | "port-panicked" => return Ok(None),
            other => return Err(format!("Mispaired: call {call} ended `{other}`")),
        };
        let Some(&(receipt, id)) = self.receipts.get(&storage_seq) else {
            return Err(format!(
                "Mispaired: call {call} ended `{end}` at storage fact {storage_seq}, which isn't \
                 a receipt"
            ));
        };
        if !receipts.contains(&receipt) {
            return Err(format!(
                "Mispaired: call {call} ended `{end}` at storage fact {storage_seq}, operation \
                 {id}'s {receipt:?}"
            ));
        }
        if self.ops.get(&id) != Some(work) {
            return Err(format!(
                "Mispaired: call {call}, a {:?} call, ended at operation {id}'s receipt, whose \
                 kind, owner or source differs",
                work.kind
            ));
        }
        if !self.paired.insert(id) {
            return Err(format!(
                "PairedTwice: call {call} ended at operation {id}'s receipt, already paired"
            ));
        }
        Ok(Some(id))
    }

    /// The failure for the receipt at `seq`, if a call must have taken it
    /// and none is paired with its operation.
    fn unpaired(&self, seq: u64) -> Option<String> {
        let (receipt, id) = self.receipts[&seq];
        if self.paired.contains(&id) {
            return None;
        }
        let work = self.ops.get(&id)?;
        // Scanned only for an unpaired receipt, which a healthy run never has.
        let reactors = match work.kind {
            OpKind::Locator => true,
            OpKind::VctContext | OpKind::Prepare | OpKind::Apply => {
                self.calls.values().any(|call| call == work)
            }
            _ => false,
        };
        reactors.then(|| {
            format!(
                "Unpaired: operation {id}'s {receipt:?} at storage fact {seq} has no call ending \
                 at it in its step"
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::{StorageBranch, WorkScope};

    /// A header owner in `session`.
    fn owner(session: u64) -> WorkOwner {
        WorkOwner {
            kind: "header".to_owned(),
            scope: WorkScope {
                header_generation: 1,
                branch: StorageBranch {
                    anchor: "00".to_owned(),
                    target: "11".to_owned(),
                },
            },
            verified_generation: None,
            body_work_epoch: None,
            session_id: session,
            request_id: 1,
        }
    }

    /// Operation `id`, an apply by `session` from `source`.
    fn apply_op(id: u64, session: u64, source: &str) -> StorageFact {
        StorageFact::Submitted {
            id,
            request: StorageRequest::ApplyHeaderTarget {
                owner: owner(session),
                source: source.to_owned(),
                parent: None,
                target: "11".to_owned(),
            },
            digest: String::new(),
        }
    }

    /// Operation `id`, a locator.
    fn locator_op(id: u64) -> StorageFact {
        StorageFact::Submitted {
            id,
            request: StorageRequest::ContinuationLocator,
            digest: String::new(),
        }
    }

    /// Call `call`, an apply by `session` from `source`.
    fn apply_call(call: u64, session: u64, source: &str) -> ReactorFact {
        ReactorFact::Dispatched {
            call,
            port: ReactorCall::Apply {
                peer: "01".to_owned(),
                source: source.to_owned(),
                owner: owner(session),
            },
        }
    }

    /// Call `call`, a locator.
    fn locator_call(call: u64) -> ReactorFact {
        ReactorFact::Dispatched {
            call,
            port: ReactorCall::Locator {
                peer: "01".to_owned(),
                session_id: 1,
                target: "11".to_owned(),
                scope: owner(1).scope,
            },
        }
    }

    /// Call `call` ended `end`, unpaired.
    fn ended(call: u64, end: &str) -> ReactorFact {
        ReactorFact::CallEnded {
            call,
            end: end.to_owned(),
            op: None,
        }
    }

    /// Pairs one drain of `facts` and `events` on `pairer`. Returns the
    /// result and each `CallEnded`'s `op`, in order.
    fn drain(
        pairer: &mut Pairer,
        facts: &[(u64, StorageFact)],
        mut events: Vec<(u64, ReactorFact)>,
    ) -> (Result<(), PairError>, Vec<Option<u64>>) {
        pairer.facts(facts.iter().map(|(seq, fact)| (*seq, fact)));
        let result = pairer.pair(events.iter_mut().map(|(seq, event)| (*seq, event)));
        let ops = events
            .iter()
            .filter_map(|(_, event)| match event {
                ReactorFact::CallEnded { op, .. } => Some(*op),
                _ => None,
            })
            .collect();
        (result, ops)
    }

    /// The failure name leading `result`'s evidence failure.
    fn evidence(result: Result<(), PairError>) -> String {
        match result {
            Err(PairError::Evidence(detail)) => detail,
            other => panic!("expected an evidence failure, got {other:?}"),
        }
    }

    #[test]
    fn replied_and_port_error_pair_with_reply_taken() {
        let mut pairer = Pairer::default();
        let (result, ops) = drain(
            &mut pairer,
            &[
                (1, apply_op(1, 1, "aa")),
                (2, locator_op(2)),
                (3, StorageFact::ReplyTaken { id: 1 }),
                (4, StorageFact::ReplyTaken { id: 2 }),
            ],
            vec![
                (0, apply_call(1, 1, "aa")),
                (0, locator_call(2)),
                (3, ended(1, "replied")),
                (4, ended(2, "port-error")),
            ],
        );
        assert_eq!(result, Ok(()));
        assert_eq!(ops, [Some(1), Some(2)]);
    }

    #[test]
    fn timed_out_pairs_with_caller_dropped_or_reply_dropped() {
        for dropped in [
            StorageFact::CallerDropped { id: 1 },
            StorageFact::ReplyDropped { id: 1 },
        ] {
            let mut pairer = Pairer::default();
            let (result, ops) = drain(
                &mut pairer,
                &[(1, locator_op(1)), (2, dropped)],
                vec![(0, locator_call(1)), (2, ended(1, "timed-out"))],
            );
            assert_eq!(result, Ok(()));
            assert_eq!(ops, [Some(1)]);
        }
    }

    #[test]
    fn a_timed_out_apply_or_a_wrong_receipt_kind_is_mispaired() {
        let mut pairer = Pairer::default();
        let (result, _) = drain(
            &mut pairer,
            &[
                (1, apply_op(1, 1, "aa")),
                (2, StorageFact::CallerDropped { id: 1 }),
            ],
            vec![(0, apply_call(1, 1, "aa")), (2, ended(1, "timed-out"))],
        );
        assert!(evidence(result).starts_with("Mispaired"));
        let mut pairer = Pairer::default();
        let (result, _) = drain(
            &mut pairer,
            &[
                (1, locator_op(1)),
                (2, StorageFact::CallerDropped { id: 1 }),
            ],
            vec![(0, locator_call(1)), (2, ended(1, "replied"))],
        );
        assert!(evidence(result).starts_with("Mispaired"));
    }

    #[test]
    fn not_submitted_stays_unpaired_beside_a_matching_reply() {
        let mut pairer = Pairer::default();
        let (result, ops) = drain(
            &mut pairer,
            &[
                (1, apply_op(1, 1, "aa")),
                (2, StorageFact::ReplyTaken { id: 1 }),
            ],
            vec![
                (0, apply_call(1, 1, "aa")),
                (0, apply_call(2, 1, "aa")),
                (2, ended(2, "not-submitted")),
                (2, ended(1, "replied")),
            ],
        );
        assert_eq!(result, Ok(()));
        assert_eq!(ops, [None, Some(1)]);
    }

    #[test]
    fn port_panicked_stays_unpaired_and_outranks_an_unpaired_receipt() {
        let mut pairer = Pairer::default();
        let (result, ops) = drain(
            &mut pairer,
            &[
                (1, apply_op(1, 1, "aa")),
                (2, StorageFact::ReplyTaken { id: 1 }),
            ],
            vec![(0, apply_call(1, 1, "aa")), (2, ended(1, "port-panicked"))],
        );
        assert_eq!(result, Err(PairError::PortPanicked { call: 1 }));
        assert_eq!(ops, [None]);
    }

    #[test]
    fn a_mismatched_kind_owner_or_source_is_mispaired() {
        let cases: [(StorageFact, ReactorFact); 3] = [
            (locator_op(1), apply_call(1, 1, "aa")),
            (apply_op(1, 2, "aa"), apply_call(1, 1, "aa")),
            (apply_op(1, 1, "bb"), apply_call(1, 1, "aa")),
        ];
        for (op, call) in cases {
            let mut pairer = Pairer::default();
            let (result, ops) = drain(
                &mut pairer,
                &[(1, op), (2, StorageFact::ReplyTaken { id: 1 })],
                vec![(0, call), (2, ended(1, "replied"))],
            );
            assert!(evidence(result).starts_with("Mispaired"));
            assert_eq!(ops, [None]);
        }
    }

    #[test]
    fn an_operation_or_a_call_pairs_once() {
        let mut pairer = Pairer::default();
        let (result, _) = drain(
            &mut pairer,
            &[(1, locator_op(1)), (2, StorageFact::ReplyTaken { id: 1 })],
            vec![
                (0, locator_call(1)),
                (0, locator_call(2)),
                (2, ended(1, "replied")),
                (2, ended(2, "replied")),
            ],
        );
        assert!(evidence(result).starts_with("PairedTwice"));
        let mut pairer = Pairer::default();
        let (result, _) = drain(
            &mut pairer,
            &[(1, locator_op(1)), (2, StorageFact::ReplyTaken { id: 1 })],
            vec![
                (0, locator_call(1)),
                (2, ended(1, "replied")),
                (2, ended(1, "replied")),
            ],
        );
        assert!(evidence(result).starts_with("PairedTwice"));
    }

    #[test]
    fn a_non_receipt_fact_at_storage_seq_is_mispaired() {
        let mut pairer = Pairer::default();
        let (result, _) = drain(
            &mut pairer,
            &[
                (1, locator_op(1)),
                (2, StorageFact::PermitDropped { id: 1 }),
            ],
            vec![(0, locator_call(1)), (2, ended(1, "replied"))],
        );
        assert!(evidence(result).starts_with("Mispaired"));
    }

    #[test]
    fn a_receipt_a_call_must_take_pairs_in_its_own_step() {
        // A locator's receipt, with no call ending at it.
        let mut pairer = Pairer::default();
        let (result, _) = drain(
            &mut pairer,
            &[(1, locator_op(1)), (2, StorageFact::ReplyTaken { id: 1 })],
            vec![(0, locator_call(1))],
        );
        assert!(evidence(result).starts_with("Unpaired"));

        // An apply whose work a dispatched call has.
        let mut pairer = Pairer::default();
        let (result, _) = drain(
            &mut pairer,
            &[
                (1, apply_op(1, 1, "aa")),
                (2, StorageFact::ReplyTaken { id: 1 }),
            ],
            vec![(0, apply_call(1, 1, "aa"))],
        );
        assert!(evidence(result).starts_with("Unpaired"));

        // An apply no dispatched call owns, such as another caller's.
        let mut pairer = Pairer::default();
        let (result, _) = drain(
            &mut pairer,
            &[
                (1, apply_op(1, 9, "aa")),
                (2, StorageFact::ReplyTaken { id: 1 }),
            ],
            vec![(0, apply_call(1, 1, "aa"))],
        );
        assert_eq!(result, Ok(()));
    }
}
