//! A peer that reconnects while its block is in admission or after it
//! (#2758). Each test joins one `BlockTable` to the peers' live
//! `PeerState` sessions through the production routers in `route`, one
//! queued input at a time, and never moves the clock.

use std::collections::VecDeque;

use zakura_chain::block;
use zakura_quic::NodeId;
use zakura_rt::time::Instant;

use super::{
    blocks::{
        BlockConfig, BlockEvent, BlockInput, BlockOutput, BlockStep, BlockTable,
        HeaderAdmissionResult,
    },
    blocks_tests::{commitment_ok, header_result},
    codec::encode_body,
    peer::{PartAuth, PeerConfig, PeerInput, PeerOutput, PeerState},
    peer_tests::{have_with, recv_meta, tiny},
    profile::Profile,
    route::{
        pull_state, replay_admitted, route_block_output, route_peer_output, BlockRoute, PeerRoute,
    },
    test_support::{make_envelope, node_id, TEST_TIP_HASH},
    wire::{Bpv1Message, HaveFlags},
};

/// Inputs a test pumps before it calls the exchange a spin.
const STEP_CAP: usize = 1_000;

/// One node's table and the live session of each of its peers.
struct Node {
    now: Instant,
    table: BlockTable,
    sessions: Vec<PeerState>,
    to_session: VecDeque<(NodeId, PeerInput)>,
    to_table: VecDeque<BlockInput>,
    /// What each live session wrote to its peer, by peer.
    written: Vec<(NodeId, PeerOutput)>,
    /// Pull grants each live session took, by peer.
    grants: Vec<NodeId>,
    /// Grants the sessions refused.
    refusals: usize,
}

impl Node {
    fn new(peers: &[NodeId]) -> Self {
        Self {
            now: Instant::now(),
            table: BlockTable::new(BlockConfig::for_profile(Profile::TINY)),
            sessions: peers.iter().map(|&peer| session(peer, 1)).collect(),
            to_session: VecDeque::new(),
            to_table: VecDeque::new(),
            written: Vec::new(),
            grants: Vec::new(),
            refusals: 0,
        }
    }

    fn session(&mut self, peer: NodeId) -> &mut PeerState {
        self.sessions
            .iter_mut()
            .find(|s| s.remote_node() == peer)
            .expect("the peer has a live session")
    }

    fn session_gen(&mut self, peer: NodeId) -> u64 {
        self.session(peer).session_gen()
    }

    /// `peer`'s live session receives the block's `HeaderMeta`, and the
    /// table gets its copy, as the IO shell submits it.
    fn receive_meta(&mut self, peer: NodeId, block_id: block::Hash) -> BlockStep {
        let now = self.now;
        let session_gen = self.session_gen(peer);
        let step = self.session(peer).step(now, recv_meta(block_id));
        assert!(step
            .outputs
            .iter()
            .any(|o| matches!(o, PeerOutput::SubmitHeaderMeta { .. })));
        self.table.step(
            now,
            BlockInput::MetaReceived {
                envelope: make_envelope(block_id, tiny(), [0xA1; 32], node_id(1)),
                from_peer: peer,
                session_gen,
            },
        )
    }

    /// Ends `peer`'s live session and opens its next one, as the service
    /// does. A `replaced` session ends after its replacement owns the peer,
    /// so the table keeps the peer's node-wide entries. The new session
    /// learns every active admitted block.
    fn reconnect(&mut self, peer: NodeId, replaced: bool) {
        let now = self.now;
        let session_gen = self.session_gen(peer);
        let end = self.session(peer).step(now, PeerInput::SessionEnd);
        self.to_session.retain(|(to, _)| *to != peer);
        self.written.retain(|(to, _)| *to != peer);
        *self.session(peer) = session(peer, session_gen + 1);
        let states = self.peer_states();
        for output in end.outputs {
            if let PeerRoute::Table(input) = route_peer_output(peer, output, || states.clone()) {
                self.to_table.push_back(input);
            }
        }
        self.table
            .step(now, BlockInput::MetaSessionEnded { peer, session_gen });
        if !replaced {
            let peer_states = self.peer_states();
            self.table
                .step(now, BlockInput::SessionEnded { peer, peer_states });
        }
        for input in replay_admitted(&self.table) {
            self.to_session.push_back((peer, input));
        }
    }

    fn peer_states(&self) -> Vec<super::blocks::PullPeerState> {
        self.sessions
            .iter()
            .map(|s| pull_state(s, self.now))
            .collect()
    }

    /// Queues each of `outputs` for the session it names.
    fn route_table(&mut self, outputs: Vec<BlockOutput>) {
        for output in outputs {
            match route_block_output(output) {
                BlockRoute::Broadcast(broadcast) => {
                    for s in &self.sessions {
                        let peer = s.remote_node();
                        self.to_session
                            .push_back((peer, broadcast.input_for(&peer)));
                    }
                }
                BlockRoute::Peer {
                    peer,
                    session_gen,
                    input,
                } => {
                    let live = self.sessions.iter().any(|s| {
                        s.remote_node() == peer && session_gen.is_none_or(|g| g == s.session_gen())
                    });
                    if live {
                        self.to_session.push_back((peer, input));
                    }
                }
                BlockRoute::Shell(_) => {}
            }
        }
    }

    /// Delivers queued inputs, sessions first, until both queues are empty
    /// or [`STEP_CAP`] inputs ran. Returns the inputs it ran.
    fn pump(&mut self) -> usize {
        let now = self.now;
        let mut steps = 0;
        while steps < STEP_CAP {
            if let Some((peer, input)) = self.to_session.pop_front() {
                if matches!(input, PeerInput::PullGranted(_)) {
                    self.grants.push(peer);
                }
                let step = self.session(peer).step(now, input);
                let states = self.peer_states();
                for output in step.outputs {
                    if matches!(output, PeerOutput::ReleaseClaims { .. }) {
                        self.refusals += 1;
                    }
                    match route_peer_output(peer, output, || states.clone()) {
                        PeerRoute::Table(input) => self.to_table.push_back(input),
                        PeerRoute::Session(output) => self.written.push((peer, output)),
                    }
                }
            } else if let Some(input) = self.to_table.pop_front() {
                let step = self.table.step(now, input);
                self.route_table(step.outputs);
            } else {
                break;
            }
            steps += 1;
        }
        steps
    }

    /// The `HeaderMeta`s and `Want`s that `peer`'s live session wrote.
    fn wrote(&self, peer: NodeId) -> (usize, usize) {
        let control = |want: bool| {
            self.written
                .iter()
                .filter(|(to, o)| {
                    *to == peer
                        && matches!(o, PeerOutput::SendControl(msg) if matches!(
                            (want, &**msg),
                            (false, Bpv1Message::HeaderMeta { .. }) | (true, Bpv1Message::Want { .. })
                        ))
                })
                .count()
        };
        (control(false), control(true))
    }

    /// Delivers a plain `Have` of part 0 from `peer`, then pumps.
    fn have(&mut self, peer: NodeId, block_id: block::Hash) -> usize {
        self.to_session
            .push_back((peer, have_with(block_id, HaveFlags::Plain, vec![0])));
        self.pump()
    }

    fn grants_to(&self, peer: NodeId) -> usize {
        self.grants.iter().filter(|p| **p == peer).count()
    }
}

fn session(peer: NodeId, session_gen: u64) -> PeerState {
    PeerState::new(PeerConfig::for_profile(Profile::TINY), peer, session_gen)
}

/// Admits the header of the metadata that `received` started admitting.
fn admit_header(node: &mut Node, received: &BlockStep) -> BlockStep {
    let now = node.now;
    header_result(
        &mut node.table,
        now,
        received,
        HeaderAdmissionResult::Admitted {
            height: 1,
            parent_hash: TEST_TIP_HASH,
        },
    )
}

/// Passes the commitment check that `header` started and publishes the
/// admission to every live session.
fn admit(node: &mut Node, header: &BlockStep, block_id: block::Hash) {
    let now = node.now;
    let committed = commitment_ok(&mut node.table, now, header);
    assert!(committed
        .events
        .contains(&BlockEvent::BlockAdmitted { block_id }));
    node.route_table(committed.outputs);
}

/// #2758: the first sender of a block's `HeaderMeta` reconnects while the
/// block is in admission. Its new session never got the metadata, so it
/// gets it at admission and takes the table's first grant. A table that
/// matched the first sender by node stripped the metadata for every session
/// of the node, so the session refused each grant and the table granted it
/// again at once, until the step cap.
#[test]
fn first_sender_reconnecting_during_admission_gets_the_metadata() {
    for replaced in [false, true] {
        let block_id = block::Hash([0x5C; 32]);
        let p = node_id(2);
        let mut node = Node::new(&[p]);
        let received = node.receive_meta(p, block_id);
        let header = admit_header(&mut node, &received);
        node.reconnect(p, replaced);
        admit(&mut node, &header, block_id);

        // The new session sends no `HeaderMeta` of its own.
        let steps = node.have(p, block_id);
        assert!(
            steps < STEP_CAP,
            "grant spin: {} grants, {} refusals",
            node.grants.len(),
            node.refusals
        );
        assert_eq!(node.wrote(p), (1, 1), "one HeaderMeta, then one Want");
        assert_eq!((node.grants_to(p), node.refusals), (1, 0));
        node.session(p).assert_invariants();
        node.table.assert_invariants();
    }
}

/// #2758: the reconnected first sender also resends `HeaderMeta` on its new
/// session while the block is in admission, as an honest relay does. The
/// node records the new session as a later sender and sends no copy back.
#[test]
fn first_sender_resending_on_its_new_session_gets_no_copy_back() {
    let block_id = block::Hash([0x5D; 32]);
    let p = node_id(2);
    let mut node = Node::new(&[p]);
    let received = node.receive_meta(p, block_id);
    let header = admit_header(&mut node, &received);
    node.reconnect(p, true);
    node.receive_meta(p, block_id);
    admit(&mut node, &header, block_id);

    let steps = node.have(p, block_id);
    assert!(steps < STEP_CAP);
    assert_eq!(node.wrote(p), (0, 1), "no HeaderMeta back, then one Want");
    assert_eq!((node.grants_to(p), node.refusals), (1, 0));
    assert!(node.session(p).close_cause().is_none());
}

/// The first sender reconnects after admission. The old session got no
/// copy back, and the new session gets the metadata from the replay of
/// active blocks.
#[test]
fn first_sender_reconnecting_after_admission_gets_the_metadata() {
    let block_id = block::Hash([0x5E; 32]);
    let p = node_id(2);
    let mut node = Node::new(&[p]);
    let received = node.receive_meta(p, block_id);
    let header = admit_header(&mut node, &received);
    admit(&mut node, &header, block_id);
    node.pump();
    assert_eq!(node.wrote(p), (0, 0), "no HeaderMeta back to its sender");

    node.reconnect(p, true);
    let steps = node.have(p, block_id);
    assert!(steps < STEP_CAP);
    assert_eq!(node.wrote(p), (1, 1), "one HeaderMeta, then one Want");
    assert_eq!((node.grants_to(p), node.refusals), (1, 0));
}

/// A later sender reconnects while the block is in admission. The table
/// forgets its old session as a sender, its new session gets the metadata,
/// and the first sender's session gets no copy back.
#[test]
fn later_sender_reconnecting_during_admission_gets_the_metadata() {
    let block_id = block::Hash([0x5F; 32]);
    let (p, q) = (node_id(2), node_id(3));
    let mut node = Node::new(&[p, q]);
    let received = node.receive_meta(p, block_id);
    let header = admit_header(&mut node, &received);
    node.receive_meta(q, block_id);
    node.reconnect(q, false);
    admit(&mut node, &header, block_id);

    let steps = node.have(q, block_id);
    assert!(steps < STEP_CAP);
    assert_eq!(
        node.wrote(p),
        (0, 0),
        "no HeaderMeta back to the first sender"
    );
    assert_eq!(node.wrote(q), (1, 1), "one HeaderMeta, then one Want");
    assert_eq!((node.grants_to(q), node.refusals), (1, 0));
}

/// bpv1-63: a replaced session's stale proof result releases only its own
/// part charge. The replacement's charge for the same part survives, so
/// the replacement's verified copy is stored.
#[test]
fn a_replaced_sessions_stale_release_keeps_the_replacements_charge() {
    let encoded = encode_body(&Profile::TINY, &[0x63; 64], 2).unwrap();
    let block_id = block::Hash([0x63; 32]);
    let p = node_id(2);
    let mut node = Node::new(&[p]);
    let now = node.now;
    let received = node.table.step(
        now,
        BlockInput::MetaReceived {
            envelope: make_envelope(block_id, encoded.coding, encoded.part_root, node_id(1)),
            from_peer: p,
            session_gen: 1,
        },
    );
    let header = admit_header(&mut node, &received);
    admit(&mut node, &header, block_id);
    let part = encoded.parts[0].clone();
    let bytes = part.payload().len() as u64;

    // Session 1 accepted the part, and its proof is still running when
    // session 2 accepts the same part.
    for session_gen in [1, 2] {
        node.table
            .charge_part(now, &block_id, part.index(), p, session_gen, bytes)
            .unwrap();
    }
    // Session 1 ends, and its proof result releases its charge.
    node.table.step(
        now,
        BlockInput::ReleasePartCharge {
            block_id,
            index: part.index(),
            from_peer: p,
            session_gen: 1,
        },
    );
    // Session 2's proof passes.
    let inserted = node.table.step(
        now,
        BlockInput::InsertPart {
            block_id,
            part,
            from_peer: p,
            session_gen: 2,
            auth: PartAuth::Push,
            peer_states: Vec::new(),
        },
    );
    assert!(
        inserted
            .events
            .iter()
            .any(|e| matches!(e, BlockEvent::PartInserted { first: true, .. })),
        "the replacement's verified part is stored"
    );
    node.table.assert_invariants();
}
