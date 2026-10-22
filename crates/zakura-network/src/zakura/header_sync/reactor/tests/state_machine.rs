//! Machine tests for [`ReactorState`]'s session schedules and locator reads.
//!
//! Each step drives `ReactorState` the way the reactor does, applies the same
//! step to an independent model, and compares the two. The model never calls
//! `ReactorState`.

use std::collections::BTreeMap;

use proptest::prelude::*;

use super::*;

const PEERS: usize = 3;

fn peer_id(peer: usize) -> ZakuraPeerId {
    let byte = u8::try_from(peer).expect("the test uses few peers") + 1;
    ZakuraPeerId::new(vec![byte; 32]).expect("the test peer ID has the required length")
}

fn hash(domain: u8, value: u8) -> block::Hash {
    let mut hash = [0; 32];
    hash[0] = domain;
    hash[1] = value;
    block::Hash(hash)
}

fn status(tip: u8) -> Status {
    Status {
        work_anchor_height: block::Height(0),
        work_anchor_hash: hash(0xA0, 0),
        selected_tip_height: block::Height(u32::from(tip) + 1),
        selected_tip_hash: hash(0xA1, tip),
        suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(1_u8),
        oldest_retained_height: block::Height(0),
        max_headers_per_response: 16,
        max_subscriptions: 1,
        max_message_bytes: 1_000_000,
        tree_aux_schema_mask: 0,
    }
}

/// Header authority: a header generation, a finality anchor, and a target tip.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Authority {
    generation: u8,
    anchor: u8,
    tip: u8,
}

impl Authority {
    fn arbitrary() -> impl Strategy<Value = Self> {
        (0..2_u8, 0..2_u8, 0..3_u8).prop_map(|(generation, anchor, tip)| Self {
            generation,
            anchor,
            tip,
        })
    }

    fn scope(self) -> zakura_header_chain::HeaderWorkAuthority {
        zakura_header_chain::HeaderWorkAuthority {
            header_generation: zakura_header_chain::HeaderGeneration::new(u64::from(
                self.generation,
            )),
            branch: zakura_header_chain::BranchId {
                anchor_hash: hash(0xB0, self.anchor),
                target_tip_hash: status(self.tip).selected_tip_hash,
            },
        }
    }

    /// An awaiting target under this authority from session `session_id`.
    fn target(self, session_id: u64) -> AdvertisedHeaderTarget {
        AdvertisedHeaderTarget {
            scope: self.scope(),
            session_id,
            status: status(self.tip),
        }
    }
}

#[derive(Clone, Debug)]
enum Op {
    /// Admit a new session for `peer`, replacing any current one.
    Connect { peer: usize },
    /// Remove `peer`'s session.
    Disconnect { peer: usize },
    /// Record a `Status` from `peer`'s session.
    Status { peer: usize, tip: u8 },
    /// Stage `peer`'s offer and request a locator read under `authority`.
    /// IO has locator capacity when `capacity` holds.
    RequestLocator {
        peer: usize,
        authority: Authority,
        capacity: bool,
    },
    /// Settle the `read`th started read with `found`, while its peer awaits `awaiting`.
    Complete {
        read: usize,
        found: bool,
        awaiting: Option<Authority>,
    },
    /// The `read`th started read panics.
    Panic { read: usize },
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        2 => (0..PEERS).prop_map(|peer| Op::Connect { peer }),
        1 => (0..PEERS).prop_map(|peer| Op::Disconnect { peer }),
        2 => (0..PEERS, 0..3_u8).prop_map(|(peer, tip)| Op::Status { peer, tip }),
        3 => (0..PEERS, Authority::arbitrary(), any::<bool>()).prop_map(
            |(peer, authority, capacity)| Op::RequestLocator { peer, authority, capacity }
        ),
        3 => (any::<usize>(), any::<bool>(), proptest::option::of(Authority::arbitrary()))
            .prop_map(|(read, found, awaiting)| Op::Complete { read, found, awaiting }),
        1 => any::<usize>().prop_map(|read| Op::Panic { read }),
    ]
}

/// The model's view of one admitted session.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ModelSession {
    status: Option<u8>,
    spent: bool,
}

/// One pending read in the model, by its index in [`Harness::started`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct ModelRead {
    read: usize,
    authority: Authority,
}

#[derive(Debug, Default)]
struct Model {
    sessions: BTreeMap<usize, ModelSession>,
    pending: BTreeMap<usize, ModelRead>,
    revision: u64,
}

impl Model {
    /// The disposition the existing locator rules give a read under
    /// `query` for a peer awaiting `awaiting`.
    fn disposition(
        query: Authority,
        awaiting: Option<Authority>,
        found: bool,
    ) -> LocatorDisposition {
        let Some(awaiting) = awaiting else {
            return LocatorDisposition::Stale;
        };
        let authority_moved =
            awaiting.generation != query.generation || awaiting.anchor != query.anchor;
        if authority_moved || (!found && awaiting.tip != query.tip) {
            LocatorDisposition::Refetch
        } else {
            LocatorDisposition::Ready
        }
    }
}

/// The machine, the model, and every started read's key.
struct Harness {
    state: ReactorState,
    model: Model,
    started: Vec<(usize, LocatorKey)>,
}

impl Harness {
    fn new() -> Self {
        Self {
            state: ReactorState::new(Instant::now()),
            model: Model::default(),
            started: Vec::new(),
        }
    }

    fn apply(&mut self, op: Op) -> Result<(), TestCaseError> {
        match op {
            Op::Connect { peer } => {
                let replaced = self.state.admit_session(&peer_id(peer));
                let previous = self.model.sessions.insert(peer, ModelSession::default());
                self.model.revision += 1;
                prop_assert_eq!(replaced, previous.is_some());
            }
            Op::Disconnect { peer } => {
                self.state.remove_session(&peer_id(peer));
                if self.model.sessions.remove(&peer).is_some() {
                    self.model.revision += 1;
                }
            }
            Op::Status { peer, tip } => {
                self.state.record_status(&peer_id(peer), status(tip));
                if let Some(session) = self.model.sessions.get_mut(&peer) {
                    session.status = Some(tip);
                    session.spent = false;
                }
            }
            Op::RequestLocator {
                peer,
                authority,
                capacity,
            } => {
                // The reactor requests reads only for admitted sessions.
                let Some(session) = self.model.sessions.get_mut(&peer) else {
                    return Ok(());
                };
                // Staging spends the offer before dispatch.
                self.state.spend_status(&peer_id(peer));
                session.spent = true;
                let proposal = self
                    .state
                    .locators
                    .propose(&peer_id(peer), authority.scope());
                if self.model.pending.contains_key(&peer) {
                    prop_assert_eq!(proposal, LocatorProposal::Joined);
                    return Ok(());
                }
                let LocatorProposal::Start(key) = proposal else {
                    return Err(TestCaseError::fail("a peer without a read must start one"));
                };
                prop_assert_eq!(&key.peer, &peer_id(peer));
                if capacity {
                    self.model.pending.insert(
                        peer,
                        ModelRead {
                            read: self.started.len(),
                            authority,
                        },
                    );
                    self.started.push((peer, key));
                } else {
                    // A refusal for local capacity keeps the offer for a later pass.
                    self.state.refuse_locator(&key);
                    session.spent = false;
                    self.model.revision += 1;
                }
            }
            Op::Complete {
                read,
                found,
                awaiting,
            } => {
                let Some((peer, key)) = self.pick(read) else {
                    return Ok(());
                };
                let target = awaiting.map(|authority| authority.target(0));
                let disposition = self.state.locators.settle(&key, target.as_ref(), found);
                let expected = match self.model.pending.get(&peer) {
                    Some(pending) if pending.read == read % self.started.len() => {
                        let pending = self
                            .model
                            .pending
                            .remove(&peer)
                            .expect("the pending read was just found");
                        Model::disposition(pending.authority, awaiting, found)
                    }
                    // A late completion for a settled read leaves the current read pending.
                    _ => LocatorDisposition::Stale,
                };
                prop_assert_eq!(disposition, expected);
            }
            Op::Panic { read } => {
                let Some((peer, key)) = self.pick(read) else {
                    return Ok(());
                };
                let removed = self.state.locators.remove_exact(&key);
                let matches = self
                    .model
                    .pending
                    .get(&peer)
                    .is_some_and(|pending| pending.read == read % self.started.len());
                if matches {
                    self.model.pending.remove(&peer);
                }
                prop_assert_eq!(removed.is_some(), matches);
            }
        }
        self.check()
    }

    /// The started read that `read` selects, if any read started.
    fn pick(&self, read: usize) -> Option<(usize, LocatorKey)> {
        if self.started.is_empty() {
            return None;
        }
        Some(self.started[read % self.started.len()].clone())
    }

    /// Compare every observable fact of the machine with the model.
    fn check(&self) -> Result<(), TestCaseError> {
        prop_assert_eq!(self.state.scheduling_revision, self.model.revision);
        prop_assert_eq!(self.state.peers.len(), self.model.sessions.len());
        for peer in 0..PEERS {
            let id = peer_id(peer);
            let schedule = self.state.peers.get(&id).map(|schedule| {
                (
                    schedule
                        .last_status
                        .as_ref()
                        .map(|status| status.selected_tip_hash),
                    schedule.status_spent,
                    schedule.unproductive_requests,
                )
            });
            let expected = self.model.sessions.get(&peer).map(|session| {
                (
                    session.status.map(|tip| status(tip).selected_tip_hash),
                    session.spent,
                    0,
                )
            });
            prop_assert_eq!(schedule, expected, "peer {} schedule", peer);
            prop_assert_eq!(
                self.state.locators.contains(&id),
                self.model.pending.contains_key(&peer),
                "peer {} pending read",
                peer
            );
        }
        Ok(())
    }
}

/// Run `ops` from an empty state, checking the machine after every step.
fn run(ops: impl IntoIterator<Item = Op>) -> Result<Harness, TestCaseError> {
    let mut harness = Harness::new();
    for op in ops {
        harness.apply(op)?;
    }
    Ok(harness)
}

#[test]
fn late_locator_after_session_replacement_leaves_the_replacement_unchanged() {
    let authority = Authority {
        generation: 0,
        anchor: 0,
        tip: 1,
    };
    let mut harness = run([
        Op::Connect { peer: 0 },
        Op::Status { peer: 0, tip: 1 },
        Op::RequestLocator {
            peer: 0,
            authority,
            capacity: true,
        },
        // The replacement drops the first session's awaiting target.
        Op::Connect { peer: 0 },
        Op::Status { peer: 0, tip: 2 },
    ])
    .expect("the scenario follows the model");
    let replacement = harness.state.peers[&peer_id(0)].clone();
    let revision = harness.state.scheduling_revision;

    harness
        .apply(Op::Complete {
            read: 0,
            found: true,
            awaiting: None,
        })
        .expect("the late read settles as the model predicts");

    assert!(!harness.state.locators.contains(&peer_id(0)));
    assert_eq!(harness.state.peers[&peer_id(0)], replacement);
    assert_eq!(harness.state.scheduling_revision, revision);
}

#[test]
fn locator_requests_for_a_replacement_join_the_first_sessions_read() {
    let first = Authority {
        generation: 0,
        anchor: 0,
        tip: 1,
    };
    let replacement = Authority { tip: 2, ..first };
    let mut harness = run([
        Op::Connect { peer: 0 },
        Op::RequestLocator {
            peer: 0,
            authority: first,
            capacity: true,
        },
        Op::Connect { peer: 0 },
        // The replacement's request joins the read already in flight.
        Op::RequestLocator {
            peer: 0,
            authority: replacement,
            capacity: true,
        },
    ])
    .expect("the scenario follows the model");
    assert_eq!(harness.started.len(), 1);

    // The read follows the local selected path, so it serves the replacement's
    // target under the same generation and anchor.
    let (_, key) = harness.started[0].clone();
    let target = replacement.target(2);
    assert_eq!(
        harness.state.locators.settle(&key, Some(&target), true),
        LocatorDisposition::Ready
    );
}

#[test]
fn refused_locator_dispatch_keeps_the_offer() {
    let authority = Authority {
        generation: 0,
        anchor: 0,
        tip: 1,
    };
    let mut harness = run([Op::Connect { peer: 0 }, Op::Status { peer: 0, tip: 1 }])
        .expect("the scenario follows the model");
    let revision = harness.state.scheduling_revision;

    harness
        .apply(Op::RequestLocator {
            peer: 0,
            authority,
            capacity: false,
        })
        .expect("the refusal follows the model");

    let schedule = &harness.state.peers[&peer_id(0)];
    assert!(!schedule.status_spent, "a capacity refusal keeps the offer");
    assert!(schedule.last_status.is_some());
    assert!(!harness.state.locators.contains(&peer_id(0)));
    assert_eq!(harness.state.scheduling_revision, revision + 1);

    // The next request starts a fresh read.
    harness
        .apply(Op::RequestLocator {
            peer: 0,
            authority,
            capacity: true,
        })
        .expect("the retry follows the model");
    assert!(harness.state.locators.contains(&peer_id(0)));
    assert!(harness.state.peers[&peer_id(0)].status_spent);
}

#[test]
fn late_completion_keeps_a_newer_read_for_the_same_peer() {
    let authority = Authority {
        generation: 0,
        anchor: 0,
        tip: 1,
    };
    let mut harness = run([
        Op::Connect { peer: 0 },
        Op::RequestLocator {
            peer: 0,
            authority,
            capacity: true,
        },
        Op::Complete {
            read: 0,
            found: true,
            awaiting: Some(authority),
        },
        Op::RequestLocator {
            peer: 0,
            authority,
            capacity: true,
        },
    ])
    .expect("the scenario follows the model");

    let (_, settled) = harness.started[0].clone();
    assert_eq!(
        harness
            .state
            .locators
            .settle(&settled, Some(&authority.target(1)), true),
        LocatorDisposition::Stale
    );
    assert!(harness.state.locators.contains(&peer_id(0)));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn reactor_state_matches_the_model(ops in prop::collection::vec(op(), 1..64)) {
        run(ops)?;
    }
}
