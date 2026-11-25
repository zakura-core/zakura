//! Pure routing of machine outputs between [`PeerState`] sessions and the
//! [`BlockTable`]. The IO shell and the test machine mesh both route through
//! these functions, so each routing rule has one encoding.
//!
//! [`PeerState`]: super::peer::PeerState

use zakura_chain::block;
use zakura_quic::NodeId;
use zakura_rt::time::Instant;

use super::{
    blocks::{
        AdmittedBlock, BlockInput, BlockOutput, BlockTable, PartRefusal, PullPeerState,
        ReleaseReason,
    },
    peer::{
        BlockAdmitted, DropReason, PartToken, PeerInput, PeerOutput, PeerState, PullGranted, Retire,
    },
    profile::Coding,
    tree::VerifiedPart,
};

/// Where a session's [`PeerOutput`] goes.
#[derive(Debug)]
pub(crate) enum PeerRoute {
    /// To the node's [`BlockTable`].
    Table(BlockInput),
    /// To the session's own IO: writes, serving, proof work, queue edits,
    /// and closes.
    Session(PeerOutput),
}

/// Routes `output` of `peer`'s session. `peer_states` runs only for an
/// output that the table plans pulls from.
pub(crate) fn route_peer_output(
    peer: NodeId,
    output: PeerOutput,
    peer_states: impl FnOnce() -> Vec<PullPeerState>,
) -> PeerRoute {
    let input = match output {
        PeerOutput::RateSample(sample) => BlockInput::RateSample { peer, sample },
        PeerOutput::PeerBlockDone { block_id } => BlockInput::PeerBlockDone {
            peer,
            block_id,
            peer_states: peer_states(),
        },
        PeerOutput::PeerHas { block_id, indices } => BlockInput::PeerAnnounced {
            peer,
            block_id,
            indices,
            peer_states: peer_states(),
        },
        PeerOutput::InsertPart {
            block_id,
            part,
            auth,
        } => BlockInput::InsertPart {
            block_id,
            part,
            from_peer: peer,
            auth,
            peer_states: peer_states(),
        },
        PeerOutput::PushFlagged {
            block_id,
            indices,
            relay,
        } => BlockInput::PushFlagged {
            peer,
            block_id,
            indices,
            relay,
        },
        PeerOutput::ReleaseClaims {
            block_id,
            stripe,
            slots,
            reason,
        } => BlockInput::ReleaseClaims {
            peer,
            block_id,
            stripe,
            slots,
            reason,
            peer_states: peer_states(),
        },
        // A fully served `Want` plans the stripe's next pull; a short one
        // releases what it left unreceived.
        PeerOutput::WantCompleted {
            block_id,
            stripe,
            unreceived_slots,
            ..
        } if unreceived_slots.is_empty() => BlockInput::PlanPulls {
            block_id,
            stripe,
            peer_states: peer_states(),
        },
        PeerOutput::WantCompleted {
            block_id,
            stripe,
            unreceived_slots,
            ..
        } => BlockInput::ReleaseClaims {
            peer,
            block_id,
            stripe,
            slots: unreceived_slots,
            reason: ReleaseReason::Failed,
            peer_states: peer_states(),
        },
        other => return PeerRoute::Session(other),
    };
    PeerRoute::Table(input)
}

/// The pull capacity the table plans `state`'s peer with at `now`.
pub(crate) fn pull_state(state: &PeerState, now: Instant) -> PullPeerState {
    PullPeerState {
        peer: state.remote_node(),
        ahead_bytes: state.outstanding_bytes(),
        can_pull: state.can_pull(now),
        window_remaining: state.window_remaining(),
    }
}

/// The held parts that a `ServeWant` for `eligible_slots` of `stripe` sends,
/// in slot order.
pub(crate) fn held_parts(
    table: &BlockTable,
    block_id: &block::Hash,
    stripe: u32,
    eligible_slots: &[u32],
) -> Vec<VerifiedPart> {
    let Some((coding, _)) = table.admitted_descriptor(block_id) else {
        return Vec::new();
    };
    eligible_slots
        .iter()
        .filter_map(|&slot| coding.wire_index(stripe, slot))
        .filter_map(|index| table.get_part(block_id, index).cloned())
        .collect()
}

/// Charges the part buffer for `token`'s part of `bytes` before proof
///. On refusal, returns the input that clears the session's
/// attempt without faulting `peer`.
pub(crate) fn charge_part(
    table: &mut BlockTable,
    now: Instant,
    peer: NodeId,
    token: PartToken,
    bytes: u64,
) -> Result<(Coding, [u8; 32]), PeerInput> {
    table
        .charge_part(now, &token.block_id, token.index, peer, bytes)
        .map_err(|refusal| PeerInput::PartRefused {
            token,
            reason: match refusal {
                PartRefusal::LocalCapacity => DropReason::LocalCapacity,
                PartRefusal::NotAdmitted => DropReason::NotAdmitted,
            },
        })
}

/// Where a [`BlockTable`] output goes.
#[derive(Debug)]
pub(crate) enum BlockRoute {
    /// To `peer`'s session. With `session_gen`, only that generation takes it.
    Peer {
        peer: NodeId,
        session_gen: Option<u64>,
        input: PeerInput,
    },
    /// To every live session.
    Broadcast(Broadcast),
    /// Shell work: header admission, commitment checks, stripe jobs, the
    /// verifier, body claims, and closes.
    Shell(BlockOutput),
}

/// An input for every live session.
#[derive(Debug)]
pub(crate) struct Broadcast {
    input: PeerInput,
    /// The peer that sent the block's `HeaderMeta`, which gets none back.
    origin: Option<NodeId>,
    /// The block's `Have` initial group, whose sessions announce at once
    /// (decision-26).
    have_group: Vec<NodeId>,
}

impl Broadcast {
    /// The input `peer`'s session takes.
    pub(crate) fn input_for(&self, peer: &NodeId) -> PeerInput {
        let mut input = self.input.clone();
        if let PeerInput::BlockAdmitted(admitted) = &mut input {
            if self.origin == Some(*peer) {
                admitted.header_meta = None;
            }
            if self.have_group.contains(peer) {
                admitted.have_held_until = None;
            }
        }
        input
    }

    /// Whether this publishes a newly admitted block.
    pub(crate) fn admits(&self) -> bool {
        matches!(self.input, PeerInput::BlockAdmitted(_))
    }
}

/// Routes one [`BlockTable`] output.
pub(crate) fn route_block_output(output: BlockOutput) -> BlockRoute {
    let broadcast = |input| {
        BlockRoute::Broadcast(Broadcast {
            input,
            origin: None,
            have_group: Vec::new(),
        })
    };
    match output {
        BlockOutput::PublishAdmitted {
            mut admitted,
            from_peer,
        } => BlockRoute::Broadcast(Broadcast {
            input: PeerInput::BlockAdmitted(admitted_input(&admitted)),
            origin: from_peer,
            have_group: std::mem::take(&mut admitted.have_group),
        }),
        BlockOutput::GrantPull {
            peer,
            block_id,
            stripe,
            requested,
            arrival,
        } => BlockRoute::Peer {
            peer,
            session_gen: None,
            input: PeerInput::PullGranted(PullGranted {
                block_id,
                stripe,
                requested,
                arrival,
            }),
        },
        BlockOutput::MetaRejected {
            block_id,
            peer,
            session_gen,
        } => BlockRoute::Peer {
            peer,
            session_gen: Some(session_gen),
            input: PeerInput::MetaRejected { block_id },
        },
        BlockOutput::PartReady {
            block_id,
            indices,
            push,
        } => broadcast(PeerInput::Ready {
            block_id,
            indices,
            push,
        }),
        BlockOutput::NotifyStripeDone { block_id, stripe } => {
            broadcast(PeerInput::LocalStripeDone { block_id, stripe })
        }
        BlockOutput::NotifyBlockDone { block_id } => {
            broadcast(PeerInput::LocalBlockDone { block_id })
        }
        BlockOutput::PublishRetire {
            block_id,
            stripes,
            n,
        } => broadcast(PeerInput::Retire(Retire {
            block_id,
            stripes,
            n,
        })),
        BlockOutput::PublishEvictRetired { block_id } => {
            broadcast(PeerInput::EvictRetired { block_id })
        }
        shell => BlockRoute::Shell(shell),
    }
}

/// The release that returns a grant's claims to `peer`'s table entry when
/// no session took `input`, so slots never strand.
pub(crate) fn refused_grant(
    peer: NodeId,
    input: PeerInput,
    peer_states: impl FnOnce() -> Vec<PullPeerState>,
) -> Option<BlockInput> {
    let PeerInput::PullGranted(grant) = input else {
        return None;
    };
    let slots: Vec<u32> = grant.requested.iter().collect();
    (!slots.is_empty()).then(|| BlockInput::ReleaseClaims {
        peer,
        block_id: grant.block_id,
        stripe: grant.stripe,
        slots,
        reason: ReleaseReason::Refused,
        peer_states: peer_states(),
    })
}

/// The [`PeerInput::BlockAdmitted`] payload that publishes `admitted`.
pub(crate) fn admitted_input(admitted: &AdmittedBlock) -> BlockAdmitted {
    BlockAdmitted {
        block_id: admitted.block_id,
        coding: admitted.coding,
        proposer_node: admitted.proposer_node,
        header_meta: Some(admitted.header_meta.clone()),
        as_proposer: admitted.as_proposer,
        have_held_until: admitted.have_deadline,
    }
}
