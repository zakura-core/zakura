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
//! Serving
//! reports a response's lease once it takes the execution slots, before the
//! response's task reads storage, and the release once the slots return,
//! after the task ends. So each `read_range` operation falls inside a held
//! lease on its first height, for at most the lease's count, since serving
//! truncates the range it reads.
//!
//! [`Pairer`] merges one drain's facts and events in the order they
//! happened: an event with storage watermark `w` came after fact `w` and
//! before fact `w + 1`. It pairs each operation with one holder or lease,
//! and reports the first operation that has none. Holders and leases may
//! end unpaired: a send that failed or timed out, or a response cancelled
//! before its read, never reaches storage.

use std::collections::BTreeMap;

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

/// One node's submissions and serving leases, paired with its block
/// storage's operations drain by drain. See the module doc.
#[derive(Debug, Default)]
pub(crate) struct Pairer {
    /// Each submission the sequencer retained and storage hasn't finished
    /// pairing, by token.
    submissions: BTreeMap<u64, Submission>,
    /// Each lease held now, oldest first.
    leases: Vec<Lease>,
    /// Set once the sequencer closed, after which a queued action may reach
    /// storage with no holder left.
    closed: bool,
}

impl Pairer {
    /// Pairs one drain's block storage facts, as `(seq, fact)` in order, with
    /// its events, as `(storage_seq, event)` in order. Returns every pairing,
    /// in order, or the first operation or release that has no partner.
    /// Every fact and event is processed even after a failure, so the
    /// pairer stays coherent.
    pub(crate) fn pair<'a>(
        &mut self,
        facts: impl IntoIterator<Item = (u64, &'a StorageFact)>,
        events: impl IntoIterator<Item = (u64, &'a BlockSyncFact)>,
    ) -> Result<Vec<Paired>, String> {
        let mut facts = facts.into_iter().peekable();
        let mut paired = Vec::new();
        let mut failure = None;
        let mut record = |result: Result<Option<Paired>, String>| match result {
            Ok(pairing) => paired.extend(pairing),
            Err(detail) => {
                failure.get_or_insert(detail);
            }
        };
        for (storage_seq, event) in events {
            while let Some((_, fact)) = facts.next_if(|(seq, _)| *seq <= storage_seq) {
                record(self.fact(fact));
            }
            record(self.event(event).map(|()| None));
        }
        for (_, fact) in facts {
            record(self.fact(fact));
        }
        match failure {
            Some(detail) => Err(detail),
            None => Ok(paired),
        }
    }

    /// Pairs `fact`'s operation, if it's a submission or a read, and checks
    /// that each submission storage defers has a holder.
    fn fact(&mut self, fact: &StorageFact) -> Result<Option<Paired>, String> {
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
    ) -> Result<Paired, String> {
        let Some(submission) = self.submissions.get_mut(&token) else {
            return Err(format!(
                "Unpaired: block operation {op} submits token {token}, which no sequencer \
                 submission retained before it"
            ));
        };
        if let Some(earlier) = submission.op {
            return Err(format!(
                "PairedTwice: block operation {op} submits token {token}, which operation \
                 {earlier} already took"
            ));
        }
        if !submission.held && !self.closed {
            return Err(format!(
                "Mispaired: block operation {op} submits token {token}, whose submission the \
                 sequencer released before storage took it"
            ));
        }
        if &submission.owner != owner || height.is_some_and(|height| height != submission.height) {
            return Err(format!(
                "Mispaired: block operation {op} submits token {token}, whose submission holds \
                 another owner or height"
            ));
        }
        submission.op = Some(op);
        if !submission.held {
            self.submissions.remove(&token);
        }
        Ok(Paired::Submission { token, op })
    }

    /// Checks that a sequencer submission retained each submission in
    /// `queue`, the actions storage took off the channel and deferred.
    fn deferred(&self, queue: &[StorageRequest]) -> Result<(), String> {
        for request in queue {
            if let StorageRequest::SubmitBlock { token, .. } = request {
                if !self.submissions.contains_key(token) {
                    return Err(format!(
                        "Unpaired: block storage deferred a submission of token {token}, which \
                         no sequencer submission retained before it"
                    ));
                }
            }
        }
        Ok(())
    }

    /// Pairs read operation `op`, of `count` heights from `start`, with the
    /// oldest held lease that covers it.
    fn read(&mut self, op: u64, start: u32, count: u32) -> Result<Paired, String> {
        let Some(lease) = self
            .leases
            .iter_mut()
            .find(|lease| lease.read.is_none() && lease.start == start && count <= lease.count)
        else {
            return Err(format!(
                "Unpaired: block operation {op} reads {count} heights from {start}, which no held \
                 serving lease covers"
            ));
        };
        lease.read = Some(op);
        Ok(Paired::Read { start, op })
    }

    /// Applies `event` to the submissions and leases.
    fn event(&mut self, event: &BlockSyncFact) -> Result<(), String> {
        match event {
            BlockSyncFact::Holder {
                body,
                holder: BlockSyncHolder::Submitted { token } | BlockSyncHolder::Detached { token },
                change,
            } => {
                let retained = change == "retained";
                match self.submissions.get_mut(token) {
                    // A token sent again after a failed send, or a
                    // submission that detached.
                    Some(submission) if retained => submission.held = true,
                    Some(submission) => {
                        submission.held = false;
                        if submission.op.is_some() {
                            self.submissions.remove(token);
                        }
                    }
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
                    // Released once paired and gone, or never retained,
                    // which the holder fold reports.
                    None => {}
                }
                Ok(())
            }
            BlockSyncFact::SequencerClosed => {
                self.closed = true;
                Ok(())
            }
            BlockSyncFact::ServingLease {
                peer,
                start,
                count,
                step,
            } if step == "acquired" => {
                self.leases.push(Lease {
                    peer: peer.clone(),
                    start: *start,
                    count: *count,
                    read: None,
                });
                Ok(())
            }
            BlockSyncFact::ServingLease {
                peer, start, count, ..
            } => {
                let Some(at) = self.leases.iter().position(|lease| {
                    &lease.peer == peer && lease.start == *start && lease.count == *count
                }) else {
                    return Err(format!(
                        "Mispaired: serving released a lease on {count} heights from {start} \
                         that it never acquired"
                    ));
                };
                self.leases.remove(at);
                Ok(())
            }
            _ => Ok(()),
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

    /// A lease on `count` heights from `start`, acquired or released.
    fn lease(start: u32, count: u32, step: &str) -> BlockSyncFact {
        BlockSyncFact::ServingLease {
            peer: "02".to_owned(),
            start,
            count,
            step: step.to_owned(),
        }
    }

    /// Pairs one drain on `pairer`.
    fn drain(
        pairer: &mut Pairer,
        facts: &[(u64, StorageFact)],
        events: &[(u64, BlockSyncFact)],
    ) -> Result<Vec<Paired>, String> {
        pairer.pair(
            facts.iter().map(|(seq, fact)| (*seq, fact)),
            events.iter().map(|(seq, event)| (*seq, event)),
        )
    }

    #[test]
    fn a_submission_pairs_with_its_earlier_holder() {
        let mut pairer = Pairer::default();
        let result = drain(
            &mut pairer,
            &[(1, submit(1, 7, 3))],
            &[(0, holder(7, 3, "retained"))],
        );
        assert_eq!(result, Ok(vec![Paired::Submission { token: 7, op: 1 }]));
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
        assert!(result.unwrap_err().starts_with("Unpaired"));
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
        assert_eq!(result, Ok(vec![Paired::Submission { token: 7, op: 2 }]));
        let mut pairer = Pairer::default();
        let result = drain(
            &mut pairer,
            &[(1, deferred(7, 3))],
            &[(1, holder(7, 3, "retained"))],
        );
        assert!(result.unwrap_err().starts_with("Unpaired"));
    }

    #[test]
    fn a_submission_pairs_across_drains_and_only_once() {
        let mut pairer = Pairer::default();
        assert_eq!(
            drain(&mut pairer, &[], &[(0, holder(7, 3, "retained"))]),
            Ok(vec![])
        );
        assert_eq!(
            drain(&mut pairer, &[(1, submit(1, 7, 3))], &[]),
            Ok(vec![Paired::Submission { token: 7, op: 1 }])
        );
        let again = drain(&mut pairer, &[(2, submit(2, 7, 3))], &[]);
        assert!(again.unwrap_err().starts_with("PairedTwice"));
    }

    #[test]
    fn a_released_holder_pairs_only_after_the_sequencer_closed() {
        let mut pairer = Pairer::default();
        let result = drain(
            &mut pairer,
            &[(1, submit(1, 7, 3))],
            &[(0, holder(7, 3, "retained")), (0, holder(7, 3, "released"))],
        );
        assert!(result.unwrap_err().starts_with("Mispaired"));

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
        assert_eq!(result, Ok(vec![Paired::Submission { token: 7, op: 1 }]));
    }

    #[test]
    fn a_submission_with_another_height_is_mispaired() {
        let mut pairer = Pairer::default();
        let result = drain(
            &mut pairer,
            &[(1, submit(1, 7, 4))],
            &[(0, holder(7, 3, "retained"))],
        );
        assert!(result.unwrap_err().starts_with("Mispaired"));
    }

    #[test]
    fn a_read_pairs_with_a_held_lease_that_covers_it() {
        let mut pairer = Pairer::default();
        let result = drain(
            &mut pairer,
            &[(1, read(1, 5, 2))],
            &[(0, lease(5, 4, "acquired")), (1, lease(5, 4, "released"))],
        );
        assert_eq!(result, Ok(vec![Paired::Read { start: 5, op: 1 }]));
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
            assert!(result.unwrap_err().starts_with("Unpaired"));
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
        assert!(result.unwrap_err().starts_with("Unpaired"));

        let mut pairer = Pairer::default();
        let result = drain(
            &mut pairer,
            &[],
            &[(0, lease(5, 2, "acquired")), (0, lease(5, 2, "released"))],
        );
        assert_eq!(result, Ok(vec![]));
    }

    #[test]
    fn a_release_of_a_lease_never_acquired_is_mispaired() {
        let mut pairer = Pairer::default();
        let result = drain(&mut pairer, &[], &[(0, lease(5, 2, "released"))]);
        assert!(result.unwrap_err().starts_with("Mispaired"));
    }
}
