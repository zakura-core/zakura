//! Pairing a tapped block-sync node's events with its block storage's
//! operations.
//!
//! The sequencer reports a submission's `Submitted` holder before it sends
//! the `SubmitBlock` action, and block storage logs the `submit_block`
//! operation once its intake takes the action. So each such operation
//! follows the holder of its token, which stays retained, `Submitted` or
//! `Detached`, until the driver replies or the sequencer closes. Storage may
//! take the action off the channel early and defer it behind a query, which
//! it logs as a `Deferred` fact, so that fact must follow the holder too.
//! Serving reports a response's lease once it takes the execution slots,
//! before the response's task reads storage, and the release once the slots
//! return, after the task ends. So each `read_range` operation falls inside
//! a held lease on its first height, for at most the lease's count, since
//! serving truncates the range it reads. A lease's release names its
//! serving session, the peer's connection and routine generation, so it
//! ends that session's lease even when another session holds a lease on
//! the same range.
//!
//! [`Pairer`] merges one drain's facts and events in the order they
//! happened: an event with storage watermark `w` came after fact `w` and
//! before fact `w + 1`. It pairs each operation with one holder or lease.
//! An operation that doesn't pair is a [`PairingAnomaly`]: the node sent
//! storage something its owners don't account for, which a property judges.
//! A drain whose sequence numbers skip or go back is a harness error
//! instead, since the merge order is then unknown. Holders and leases may
//! end unpaired: a send that failed or timed out, or a response cancelled
//! before its read, never reaches storage.

use std::collections::BTreeMap;

use super::view::{PairingAnomaly, PairingKind};
use crate::observe::{BlockSyncFact, BlockSyncHolder, StorageFact, StorageRequest, WorkOwner};

/// A sequencer submission, by its token.
#[derive(Clone, Debug)]
struct Submission {
    height: u32,
    owner: WorkOwner,
    /// Whether a `Submitted` or `Detached` holder retains it.
    held: bool,
    /// The storage operation paired with it.
    op: Option<u64>,
}

/// A serving response's lease.
#[derive(Clone, Debug)]
struct Lease {
    peer: String,
    connection: u64,
    generation: u64,
    start: u32,
    count: u32,
    /// The storage read paired with it.
    read: Option<u64>,
}

/// One pairing [`Pairer::pair`] made.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Paired {
    /// Storage operation `op` took submission `token`.
    Submission {
        /// The submission's token.
        token: u64,
        /// The operation.
        op: u64,
    },
    /// Storage operation `op` read for a lease on height `start`.
    Read {
        /// The lease's first height.
        start: u32,
        /// The operation.
        op: u64,
    },
}

/// What [`Pairer::pair`] made of one drain.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Drain {
    /// Each pairing, in order.
    pub(crate) paired: Vec<Paired>,
    /// Each operation or deferral that didn't pair, in order.
    pub(crate) anomalies: Vec<PairingAnomaly>,
}

/// One node's submissions and serving leases, paired with its block
/// storage's operations drain by drain. See the module doc.
#[derive(Debug, Default)]
pub(crate) struct Pairer {
    /// Each submission the sequencer ever retained, by token. A paired
    /// submission stays, so a second operation on its token fails, even
    /// after a holder change.
    submissions: BTreeMap<u64, Submission>,
    /// Each lease held now, oldest first.
    leases: Vec<Lease>,
    /// Set once the sequencer closed, after which a queued action may reach
    /// storage with no holder left.
    closed: bool,
    /// The last fact handed over, 0 before any.
    last_fact: u64,
    /// The last event's storage watermark, 0 before any.
    watermark: u64,
}

impl Pairer {
    /// Pairs one drain's block storage facts, as `(seq, fact)` in order, with
    /// its events, as `(storage_seq, event)` in order. Returns every pairing
    /// and every anomaly, in order, or an error for a fact that doesn't
    /// follow the last one, or an event whose watermark goes back or names a
    /// fact never handed over. Every fact and event is processed even after
    /// an error, so the pairer stays coherent.
    pub(crate) fn pair<'a>(
        &mut self,
        facts: impl IntoIterator<Item = (u64, &'a StorageFact)>,
        events: impl IntoIterator<Item = (u64, &'a BlockSyncFact)>,
    ) -> Result<Drain, String> {
        let facts: Vec<_> = facts.into_iter().collect();
        let mut failure = None;
        for (seq, _) in &facts {
            if *seq != self.last_fact.saturating_add(1) {
                failure.get_or_insert(format!(
                    "LostFact: block storage fact {seq} follows fact {}",
                    self.last_fact
                ));
            }
            self.last_fact = *seq;
        }
        let mut facts = facts.into_iter().peekable();
        let mut drain = Drain::default();
        for (storage_seq, event) in events {
            if storage_seq < self.watermark || storage_seq > self.last_fact {
                failure.get_or_insert(format!(
                    "MisorderedEvent: a block-sync event saw storage fact {storage_seq}, after \
                     an event that saw fact {}, with fact {} the last handed over",
                    self.watermark, self.last_fact
                ));
            }
            self.watermark = storage_seq;
            while let Some((seq, fact)) = facts.next_if(|(seq, _)| *seq <= storage_seq) {
                drain.record(seq, self.fact(fact));
            }
            self.event(event);
        }
        for (seq, fact) in facts {
            drain.record(seq, self.fact(fact));
        }
        match failure {
            Some(detail) => Err(detail),
            None => Ok(drain),
        }
    }

    /// Pairs `fact`'s operation, if it's a submission or a read, and checks
    /// that each submission storage defers has a holder.
    fn fact(&mut self, fact: &StorageFact) -> Result<Option<Paired>, Unpaired> {
        if let StorageFact::Deferred { queue } = fact {
            return self.deferred(queue).map(|()| None);
        }
        let StorageFact::BlockSubmitted { id, request, .. } = fact else {
            return Ok(None);
        };
        match request {
            StorageRequest::SubmitBlock {
                token,
                owner,
                height,
                ..
            } => self.submit(*id, *token, owner, *height).map(Some),
            StorageRequest::ReadRange {
                start_height,
                count,
                ..
            } => self.read(*id, *start_height, *count).map(Some),
            _ => Ok(None),
        }
    }

    /// Pairs submission operation `op` with token `token`'s holder.
    fn submit(
        &mut self,
        op: u64,
        token: u64,
        owner: &WorkOwner,
        height: Option<u32>,
    ) -> Result<Paired, Unpaired> {
        let Some(submission) = self.submissions.get_mut(&token) else {
            return Err((
                PairingKind::Unpaired,
                format!(
                    "block operation {op} submits token {token}, which no sequencer submission \
                     retained before it"
                ),
            ));
        };
        if let Some(earlier) = submission.op {
            return Err((
                PairingKind::PairedTwice,
                format!(
                    "block operation {op} submits token {token}, which operation {earlier} \
                     already took"
                ),
            ));
        }
        if !submission.held && !self.closed {
            return Err((
                PairingKind::Mispaired,
                format!(
                    "block operation {op} submits token {token}, whose submission the sequencer \
                     released before storage took it"
                ),
            ));
        }
        if &submission.owner != owner || height.is_some_and(|height| height != submission.height) {
            return Err((
                PairingKind::Mispaired,
                format!(
                    "block operation {op} submits token {token}, whose submission holds another \
                     owner or height"
                ),
            ));
        }
        submission.op = Some(op);
        Ok(Paired::Submission { token, op })
    }

    /// Checks that a sequencer submission retained each submission in
    /// `queue`, the actions storage took off the channel and deferred.
    fn deferred(&self, queue: &[StorageRequest]) -> Result<(), Unpaired> {
        for request in queue {
            if let StorageRequest::SubmitBlock { token, .. } = request {
                if !self.submissions.contains_key(token) {
                    return Err((
                        PairingKind::Unpaired,
                        format!(
                            "block storage deferred a submission of token {token}, which no \
                             sequencer submission retained before it"
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Pairs read operation `op`, of `count` heights from `start`, with the
    /// oldest held lease that covers it.
    fn read(&mut self, op: u64, start: u32, count: u32) -> Result<Paired, Unpaired> {
        let Some(lease) = self
            .leases
            .iter_mut()
            .find(|lease| lease.read.is_none() && lease.start == start && count <= lease.count)
        else {
            return Err((
                PairingKind::Unpaired,
                format!(
                    "block operation {op} reads {count} heights from {start}, which no held \
                     serving lease covers"
                ),
            ));
        };
        lease.read = Some(op);
        Ok(Paired::Read { start, op })
    }

    /// Applies `event` to the submissions and leases.
    fn event(&mut self, event: &BlockSyncFact) {
        match event {
            BlockSyncFact::Holder {
                body,
                holder: BlockSyncHolder::Submitted { token } | BlockSyncHolder::Detached { token },
                change,
            } => {
                let retained = change == "retained";
                match self.submissions.get_mut(token) {
                    // A submission that detached: its `Detached` holder
                    // takes over from its `Submitted` one. A retry takes a
                    // new token, so this is the only second retain.
                    Some(submission) => submission.held = retained,
                    None if retained => {
                        self.submissions.insert(
                            *token,
                            Submission {
                                height: body.height,
                                owner: body.owner.clone(),
                                held: true,
                                op: None,
                            },
                        );
                    }
                    // Never retained, which the view's fold reports.
                    None => {}
                }
            }
            BlockSyncFact::SequencerClosed => self.closed = true,
            BlockSyncFact::ServingLease {
                peer,
                connection,
                generation,
                start,
                count,
                step,
            } if step == "acquired" => {
                self.leases.push(Lease {
                    peer: peer.clone(),
                    connection: *connection,
                    generation: *generation,
                    start: *start,
                    count: *count,
                    read: None,
                });
            }
            BlockSyncFact::ServingLease {
                peer,
                connection,
                generation,
                start,
                count,
                ..
            } => {
                // A lease never acquired, which the view's fold reports.
                if let Some(at) = self.leases.iter().position(|lease| {
                    &lease.peer == peer
                        && lease.connection == *connection
                        && lease.generation == *generation
                        && lease.start == *start
                        && lease.count == *count
                }) {
                    self.leases.remove(at);
                }
            }
            _ => {}
        }
    }
}

/// Why an operation didn't pair.
type Unpaired = (PairingKind, String);

impl Drain {
    /// Keeps fact `seq`'s pairing or anomaly.
    fn record(&mut self, seq: u64, result: Result<Option<Paired>, Unpaired>) {
        match result {
            Ok(pairing) => self.paired.extend(pairing),
            Err((kind, detail)) => self.anomalies.push(PairingAnomaly {
                kind,
                fact: seq,
                detail,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::{BlockSyncBody, StorageBranch, WorkScope};

    /// A body owner for request `request`.
    fn owner(request: u64) -> WorkOwner {
        WorkOwner {
            kind: "body".to_owned(),
            scope: WorkScope {
                header_generation: 1,
                branch: StorageBranch {
                    anchor: "00".to_owned(),
                    target: "11".to_owned(),
                },
            },
            verified_generation: Some(1),
            body_work_epoch: Some(1),
            session_id: 1,
            request_id: request,
        }
    }

    /// Token `token`'s holder for the body at `height`, retained or
    /// released.
    fn holder(token: u64, height: u32, change: &str) -> BlockSyncFact {
        BlockSyncFact::Holder {
            body: BlockSyncBody {
                height,
                owner: owner(1),
                bytes: 100,
            },
            holder: BlockSyncHolder::Submitted { token },
            change: change.to_owned(),
        }
    }

    /// Operation `id`, a submission of `token` at `height`.
    fn submit(id: u64, token: u64, height: u32) -> StorageFact {
        StorageFact::BlockSubmitted {
            id,
            request: StorageRequest::SubmitBlock {
                owner: owner(1),
                source: "01".to_owned(),
                token,
                height: Some(height),
                hash: "aa".to_owned(),
                parent: "bb".to_owned(),
                class: "full".to_owned(),
            },
            stage: Some("queued".to_owned()),
        }
    }

    /// Operation `id`, a read of `count` heights from `start`.
    fn read(id: u64, start: u32, count: u32) -> StorageFact {
        StorageFact::BlockSubmitted {
            id,
            request: StorageRequest::ReadRange {
                start_height: start,
                count,
                max_body_bytes: 1_000,
            },
            stage: Some("pending".to_owned()),
        }
    }

    /// A lease on `count` heights from `start`, acquired or released, by
    /// the serving session on connection 1 at generation 1.
    fn lease(start: u32, count: u32, step: &str) -> BlockSyncFact {
        session_lease((1, 1), start, count, step)
    }

    /// A lease on `count` heights from `start`, acquired or released, by
    /// the serving session on `(connection, generation)`.
    fn session_lease(
        (connection, generation): (u64, u64),
        start: u32,
        count: u32,
        step: &str,
    ) -> BlockSyncFact {
        BlockSyncFact::ServingLease {
            peer: "02".to_owned(),
            connection,
            generation,
            start,
            count,
            step: step.to_owned(),
        }
    }

    /// Pairs one drain on `pairer`, which must keep its sequence numbers.
    fn drain(
        pairer: &mut Pairer,
        facts: &[(u64, StorageFact)],
        events: &[(u64, BlockSyncFact)],
    ) -> Drain {
        pairer
            .pair(
                facts.iter().map(|(seq, fact)| (*seq, fact)),
                events.iter().map(|(seq, event)| (*seq, event)),
            )
            .unwrap_or_else(|detail| panic!("{detail}"))
    }

    /// A drain that pairs `paired` with no anomaly.
    fn clean(paired: Vec<Paired>) -> Drain {
        Drain {
            paired,
            anomalies: Vec::new(),
        }
    }

    /// The kinds of `drain`'s anomalies, with their facts.
    fn kinds(drain: &Drain) -> Vec<(PairingKind, u64)> {
        drain
            .anomalies
            .iter()
            .map(|anomaly| (anomaly.kind, anomaly.fact))
            .collect()
    }

    #[test]
    fn a_submission_pairs_with_its_earlier_holder() {
        let mut pairer = Pairer::default();
        let result = drain(
            &mut pairer,
            &[(1, submit(1, 7, 3))],
            &[(0, holder(7, 3, "retained"))],
        );
        assert_eq!(result, clean(vec![Paired::Submission { token: 7, op: 1 }]));
    }

    #[test]
    fn a_holder_retained_after_the_operation_leaves_it_unpaired() {
        // The holder's watermark is the operation's own fact, so it came
        // after the operation.
        let mut pairer = Pairer::default();
        let result = drain(
            &mut pairer,
            &[(1, submit(1, 7, 3))],
            &[(1, holder(7, 3, "retained"))],
        );
        assert_eq!(kinds(&result), [(PairingKind::Unpaired, 1)]);
        assert!(result.paired.is_empty());
    }

    /// Token `token`'s submission at `height`, as a deferred action.
    fn deferred(token: u64, height: u32) -> StorageFact {
        let StorageFact::BlockSubmitted { request, .. } = submit(0, token, height) else {
            unreachable!("`submit` makes a submission");
        };
        StorageFact::Deferred {
            queue: vec![request],
        }
    }

    #[test]
    fn a_deferred_submission_needs_an_earlier_holder() {
        let mut pairer = Pairer::default();
        let result = drain(
            &mut pairer,
            &[(1, deferred(7, 3)), (2, submit(2, 7, 3))],
            &[(0, holder(7, 3, "retained"))],
        );
        assert_eq!(result, clean(vec![Paired::Submission { token: 7, op: 2 }]));
        let mut pairer = Pairer::default();
        let result = drain(
            &mut pairer,
            &[(1, deferred(7, 3))],
            &[(1, holder(7, 3, "retained"))],
        );
        assert_eq!(kinds(&result), [(PairingKind::Unpaired, 1)]);
    }

    #[test]
    fn a_detached_submission_pairs_only_once() {
        let mut pairer = Pairer::default();
        let detached = BlockSyncFact::Holder {
            body: BlockSyncBody {
                height: 3,
                owner: owner(1),
                bytes: 100,
            },
            holder: BlockSyncHolder::Detached { token: 7 },
            change: "retained".to_owned(),
        };
        let result = drain(
            &mut pairer,
            &[(1, submit(1, 7, 3)), (2, submit(2, 7, 3))],
            &[
                (0, holder(7, 3, "retained")),
                (1, holder(7, 3, "released")),
                (1, detached),
            ],
        );
        assert_eq!(result.paired, [Paired::Submission { token: 7, op: 1 }]);
        assert_eq!(kinds(&result), [(PairingKind::PairedTwice, 2)]);
    }

    #[test]
    fn a_submission_pairs_across_drains_and_only_once() {
        let mut pairer = Pairer::default();
        assert_eq!(
            drain(&mut pairer, &[], &[(0, holder(7, 3, "retained"))]),
            clean(vec![])
        );
        assert_eq!(
            drain(&mut pairer, &[(1, submit(1, 7, 3))], &[]),
            clean(vec![Paired::Submission { token: 7, op: 1 }])
        );
        let again = drain(&mut pairer, &[(2, submit(2, 7, 3))], &[]);
        assert_eq!(kinds(&again), [(PairingKind::PairedTwice, 2)]);
    }

    /// An operation that doesn't pair leaves the pairer as it was, so the
    /// drain's later operations still pair.
    #[test]
    fn an_unpaired_operation_leaves_the_rest_of_the_drain_paired() {
        let mut pairer = Pairer::default();
        let result = drain(
            &mut pairer,
            &[(1, submit(1, 9, 3)), (2, submit(2, 7, 3))],
            &[(0, holder(7, 3, "retained"))],
        );
        assert_eq!(result.paired, [Paired::Submission { token: 7, op: 2 }]);
        assert_eq!(kinds(&result), [(PairingKind::Unpaired, 1)]);
        assert!(
            result.anomalies[0].detail.contains("token 9"),
            "{:?}",
            result.anomalies
        );
    }

    #[test]
    fn a_released_holder_pairs_only_after_the_sequencer_closed() {
        let mut pairer = Pairer::default();
        let result = drain(
            &mut pairer,
            &[(1, submit(1, 7, 3))],
            &[(0, holder(7, 3, "retained")), (0, holder(7, 3, "released"))],
        );
        assert_eq!(kinds(&result), [(PairingKind::Mispaired, 1)]);

        let mut pairer = Pairer::default();
        let result = drain(
            &mut pairer,
            &[(1, submit(1, 7, 3))],
            &[
                (0, holder(7, 3, "retained")),
                (0, BlockSyncFact::SequencerClosed),
                (0, holder(7, 3, "released")),
            ],
        );
        assert_eq!(result, clean(vec![Paired::Submission { token: 7, op: 1 }]));
    }

    #[test]
    fn a_submission_with_another_height_is_mispaired() {
        let mut pairer = Pairer::default();
        let result = drain(
            &mut pairer,
            &[(1, submit(1, 7, 4))],
            &[(0, holder(7, 3, "retained"))],
        );
        assert_eq!(kinds(&result), [(PairingKind::Mispaired, 1)]);
    }

    #[test]
    fn a_read_pairs_with_a_held_lease_that_covers_it() {
        let mut pairer = Pairer::default();
        let result = drain(
            &mut pairer,
            &[(1, read(1, 5, 2))],
            &[(0, lease(5, 4, "acquired")), (1, lease(5, 4, "released"))],
        );
        assert_eq!(result, clean(vec![Paired::Read { start: 5, op: 1 }]));
    }

    #[test]
    fn a_read_outside_any_lease_is_unpaired() {
        for (events, fact) in [
            // No lease at all.
            (vec![], read(1, 5, 2)),
            // A lease released before the read.
            (
                vec![(0, lease(5, 4, "acquired")), (0, lease(5, 4, "released"))],
                read(1, 5, 2),
            ),
            // A lease on another height.
            (vec![(0, lease(6, 4, "acquired"))], read(1, 5, 2)),
            // More heights than the lease asked for.
            (vec![(0, lease(5, 1, "acquired"))], read(1, 5, 2)),
        ] {
            let mut pairer = Pairer::default();
            let result = drain(&mut pairer, &[(1, fact)], &events);
            assert_eq!(kinds(&result), [(PairingKind::Unpaired, 1)]);
        }
    }

    #[test]
    fn a_lease_pairs_with_one_read_and_may_end_unread() {
        let mut pairer = Pairer::default();
        let result = drain(
            &mut pairer,
            &[(1, read(1, 5, 2)), (2, read(2, 5, 2))],
            &[(0, lease(5, 2, "acquired"))],
        );
        assert_eq!(kinds(&result), [(PairingKind::Unpaired, 2)]);

        let mut pairer = Pairer::default();
        let result = drain(
            &mut pairer,
            &[],
            &[(0, lease(5, 2, "acquired")), (0, lease(5, 2, "released"))],
        );
        assert_eq!(result, clean(vec![]));
    }

    /// Two serving sessions of one peer hold leases on the same range. The
    /// first lease takes the first read, and the second session's release
    /// ends the second lease, not the first. So a second read finds no
    /// unread lease. Matching a release on the peer alone would end the
    /// first, read lease and leave the released one to take the second
    /// read.
    #[test]
    fn a_release_ends_its_own_sessions_lease() {
        // A second connection, then a later generation on the same one.
        for other in [(2, 1), (1, 2)] {
            let mut pairer = Pairer::default();
            let result = drain(
                &mut pairer,
                &[(1, read(1, 5, 2)), (2, read(2, 5, 2))],
                &[
                    (0, session_lease((1, 1), 5, 2, "acquired")),
                    (0, session_lease(other, 5, 2, "acquired")),
                    (1, session_lease(other, 5, 2, "released")),
                ],
            );
            assert_eq!(
                result.paired,
                [Paired::Read { start: 5, op: 1 }],
                "{other:?}"
            );
            assert_eq!(kinds(&result), [(PairingKind::Unpaired, 2)], "{other:?}");
        }
    }

    /// A fact that skips or repeats a sequence number, or an event whose
    /// watermark goes back or names a fact never handed over, is a harness
    /// error, not an anomaly.
    #[test]
    fn a_lost_or_misordered_sequence_is_an_error() {
        let pair = |facts: &[(u64, StorageFact)], events: &[(u64, BlockSyncFact)]| {
            let mut pairer = Pairer::default();
            pairer.pair(
                facts.iter().map(|(seq, fact)| (*seq, fact)),
                events.iter().map(|(seq, event)| (*seq, event)),
            )
        };
        let retained = || holder(7, 3, "retained");
        for (facts, events, name) in [
            (vec![(2, read(2, 5, 2))], vec![], "LostFact"),
            (
                vec![(1, read(1, 5, 2)), (1, read(1, 5, 2))],
                vec![],
                "LostFact",
            ),
            (vec![], vec![(1, retained())], "MisorderedEvent"),
            (
                vec![(1, submit(1, 7, 3))],
                vec![(1, retained()), (0, retained())],
                "MisorderedEvent",
            ),
        ] {
            let result = pair(&facts, &events);
            assert!(
                result
                    .as_ref()
                    .is_err_and(|detail| detail.starts_with(name)),
                "{name}: {result:?}"
            );
        }
        // A later drain carries on from the last fact.
        let mut pairer = Pairer::default();
        drain(&mut pairer, &[(1, read(1, 5, 2))], &[]);
        let skipped = pairer.pair([(3, &read(3, 5, 2))], []);
        assert!(skipped.is_err_and(|detail| detail.starts_with("LostFact")));
    }
}
