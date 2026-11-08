//! `header-sync/commit-only-authorized`'s submission rule: the request
//! storage receives for a prepare or apply call is the one the reactor's
//! `Dispatched` named.
//!
//! - **Prepare.** Storage's request has the dispatch's owner, source,
//!   common ancestor, target and header hashes, in order.
//! - **Apply.** Storage's request has the dispatch's owner, source and
//!   target, and the sealed insertion starts from the common ancestor of
//!   the prepare that handed the apply its headers and holds exactly that
//!   prepare's dispatched headers. An insertion storage couldn't open was
//!   sealed elsewhere, which breaks the rule too.
//!
//! Storage tags each submission with the reactor call that made it, so the
//! two sides join by call. The pairer ([`super::super::pair`]) still checks
//! that they're the same kind of call.

use std::collections::{HashMap, HashSet};

use zakura_network::zakura::observe::{HeaderWorkEnd, PortCall, ReactorEvent};

use crate::{
    observe::{StorageFact, StoragePoint, StorageRequest, WorkOwner},
    storage::{hash, hex, point, work_owner},
};

/// What a dispatched prepare or apply named, in storage's terms.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Sent {
    Prepare {
        owner: WorkOwner,
        source: String,
        ancestor: StoragePoint,
        target: StoragePoint,
        hashes: Vec<String>,
    },
    Apply {
        owner: WorkOwner,
        source: String,
        target: String,
    },
}

/// The submission rule's state.
#[derive(Debug, Default)]
pub(super) struct Submission {
    /// Each prepare or apply call's dispatch.
    dispatched: HashMap<u64, Sent>,
    /// Each call's submitted request not yet judged.
    submitted: HashMap<u64, StorageRequest>,
    /// Each apply call's prepare, by the prepare's handoff.
    prepare_of: HashMap<u64, u64>,
    judged: HashSet<u64>,
    breaks: Vec<String>,
}

impl Submission {
    /// Notes `event`, which the reactor's own task reported. Returns whether
    /// it judged a submission.
    pub(super) fn observe(&mut self, event: &ReactorEvent) -> bool {
        match event {
            ReactorEvent::Dispatched {
                call,
                op:
                    PortCall::Prepare {
                        source,
                        owner,
                        common_ancestor,
                        target,
                        hashes,
                        ..
                    },
            } => {
                self.dispatched.insert(
                    *call,
                    Sent::Prepare {
                        owner: work_owner(*owner),
                        source: hex(&source.digest()),
                        ancestor: point(*common_ancestor),
                        target: point(*target),
                        hashes: hashes.iter().map(|each| hash(*each)).collect(),
                    },
                );
                self.judge(*call)
            }
            ReactorEvent::Dispatched {
                call,
                op:
                    PortCall::Apply {
                        source,
                        owner,
                        target_tip_hash,
                        ..
                    },
            } => {
                self.dispatched.insert(
                    *call,
                    Sent::Apply {
                        owner: work_owner(*owner),
                        source: hex(&source.digest()),
                        target: hash(*target_tip_hash),
                    },
                );
                self.judge(*call)
            }
            ReactorEvent::HeadersEnded {
                call,
                end: HeaderWorkEnd::HandedToApply { apply },
                ..
            } => {
                self.prepare_of.insert(*apply, *call);
                self.judge(*apply)
            }
            _ => false,
        }
    }

    /// Notes storage's `facts`. Returns whether they judged a submission.
    pub(super) fn storage<'f>(&mut self, facts: impl IntoIterator<Item = &'f StorageFact>) -> bool {
        let mut judged = false;
        for fact in facts {
            if let StorageFact::Submitted {
                request:
                    request @ (StorageRequest::PrepareHeaderTarget { .. }
                    | StorageRequest::ApplyHeaderTarget { .. }),
                call: Some(call),
                ..
            } = fact
            {
                self.submitted.insert(*call, request.clone());
                judged |= self.judge(*call);
            }
        }
        judged
    }

    /// The breaks found since the last call.
    pub(super) fn take_breaks(&mut self) -> Vec<String> {
        std::mem::take(&mut self.breaks)
    }

    /// Judges call `call`'s submission once its dispatch, its request and,
    /// for an apply, its prepare are known. Returns whether it judged it.
    fn judge(&mut self, call: u64) -> bool {
        let (Some(sent), Some(request)) = (self.dispatched.get(&call), self.submitted.get(&call))
        else {
            return false;
        };
        if self.judged.contains(&call) {
            return false;
        }
        let mut found = Vec::new();
        match (sent, request) {
            (
                Sent::Prepare {
                    owner,
                    source,
                    ancestor,
                    target,
                    hashes,
                },
                StorageRequest::PrepareHeaderTarget {
                    owner: got_owner,
                    source: got_source,
                    common_ancestor: got_ancestor,
                    target: got_target,
                    hashes: got_hashes,
                    ..
                },
            ) => {
                compare(&mut found, "owner", owner, got_owner);
                compare(&mut found, "source", source, got_source);
                compare(&mut found, "ancestor", ancestor, got_ancestor);
                compare(&mut found, "target", target, got_target);
                compare(&mut found, "headers", hashes, got_hashes);
            }
            (
                Sent::Apply {
                    owner,
                    source,
                    target,
                },
                StorageRequest::ApplyHeaderTarget {
                    owner: got_owner,
                    source: got_source,
                    parent,
                    target: got_target,
                    hashes: got_hashes,
                },
            ) => {
                let Some(Sent::Prepare {
                    ancestor,
                    hashes: prepared,
                    ..
                }) = self
                    .prepare_of
                    .get(&call)
                    .and_then(|prepare| self.dispatched.get(prepare))
                else {
                    return false;
                };
                compare(&mut found, "owner", owner, got_owner);
                compare(&mut found, "source", source, got_source);
                compare(&mut found, "target", target, got_target);
                compare(
                    &mut found,
                    "sealed parent",
                    &Some(ancestor.hash.clone()),
                    parent,
                );
                compare(
                    &mut found,
                    "sealed headers",
                    &Some(prepared.clone()),
                    got_hashes,
                );
            }
            // The pairer reports a call whose operation is another kind.
            _ => {}
        }
        self.judged.insert(call);
        self.submitted.remove(&call);
        for detail in found {
            self.breaks.push(format!(
                "CommitOnlyAuthorized: call {call} reached storage with {detail}"
            ));
        }
        true
    }
}

/// Pushes to `found` that `field` reached storage as `got`, if that isn't
/// the `sent` one.
fn compare<T: PartialEq + std::fmt::Debug>(
    found: &mut Vec<String>,
    field: &str,
    sent: &T,
    got: &T,
) {
    if sent != got {
        found.push(format!("{field} {got:?}, not its dispatch's {sent:?}"));
    }
}
