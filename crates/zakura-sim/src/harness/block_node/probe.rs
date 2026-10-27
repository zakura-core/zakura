//! A scripted block-sync peer. The probe speaks the regulated stream-6 codec
//! to the node under test, sends the frames a `Control` step names, and logs
//! every message the node sends it.
//!
//! The probe judges nothing and answers nothing on its own: a scenario
//! scripts each of its frames, so it can send what no honest peer does, such
//! as an unsolicited body or an ending for a range nobody asked for.

use std::{cell::RefCell, rc::Rc};

use serde::{Deserialize, Serialize};
use zakura_chain::block;
use zakura_network::zakura::{BlockSyncMessage, BlockSyncStatus, FramedRecv};
use zakura_storage::block::fixture::FixtureBlock;

use crate::Sim;

/// One frame a probe sends, named by the fixture chain's heights.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProbeFrame {
    /// A status advertising `servable_low..=servable_high` of the chain,
    /// with the default serving limits.
    Status {
        /// The lowest servable height.
        servable_low: u32,
        /// The highest servable height, whose hash the status carries.
        servable_high: u32,
    },
    /// A request for `count` bodies from `start_height`.
    GetBlocks {
        /// The first height.
        start_height: u32,
        /// The number of bodies.
        count: u32,
    },
    /// The chain's body at `height`, which must be at least 1.
    Block {
        /// The body's height.
        height: u32,
    },
    /// An ending for a response.
    BlocksDone {
        /// The response's first height.
        start_height: u32,
        /// The bodies it returned.
        returned: u32,
    },
    /// A refusal to serve a range.
    RangeUnavailable {
        /// The first height.
        start_height: u32,
        /// The number of heights.
        count: u32,
    },
}

impl ProbeFrame {
    /// The message, with hashes and bodies from `chain`, the blocks above
    /// genesis whose hash is `genesis`.
    ///
    /// # Panics
    ///
    /// If a height the frame names is above the chain, which is a scenario
    /// bug.
    pub fn message(self, genesis: block::Hash, chain: &[FixtureBlock]) -> BlockSyncMessage {
        let at = |height: u32| -> &FixtureBlock {
            let index = usize::try_from(height)
                .ok()
                .and_then(|height| height.checked_sub(1))
                .expect("a probe body is above genesis");
            chain
                .get(index)
                .unwrap_or_else(|| panic!("the chain reaches height {height}"))
        };
        match self {
            ProbeFrame::Status {
                servable_low,
                servable_high,
            } => BlockSyncMessage::Status(BlockSyncStatus {
                servable_low: block::Height(servable_low),
                servable_high: block::Height(servable_high),
                tip_hash: if servable_high == 0 {
                    genesis
                } else {
                    at(servable_high).hash()
                },
                ..BlockSyncStatus::default()
            }),
            ProbeFrame::GetBlocks {
                start_height,
                count,
            } => BlockSyncMessage::GetBlocks {
                start_height: block::Height(start_height),
                count,
            },
            ProbeFrame::Block { height } => BlockSyncMessage::Block(at(height).block.clone()),
            ProbeFrame::BlocksDone {
                start_height,
                returned,
            } => BlockSyncMessage::BlocksDone {
                start_height: block::Height(start_height),
                returned,
            },
            ProbeFrame::RangeUnavailable {
                start_height,
                count,
            } => BlockSyncMessage::RangeUnavailable {
                start_height: block::Height(start_height),
                count,
            },
        }
    }
}

/// What the node sent a probe, in order. A frame the codec refuses is
/// logged as `None`.
#[derive(Clone, Debug, Default)]
pub struct ProbeLog(Rc<RefCell<Vec<Option<BlockSyncMessage>>>>);

impl ProbeLog {
    /// Every message received so far.
    pub fn received(&self) -> Vec<Option<BlockSyncMessage>> {
        self.0.borrow().clone()
    }

    /// Spawns the probe's reader, which logs every frame from the node
    /// until the stream closes. It always reads, so the node never blocks
    /// on a full link.
    pub(super) fn spawn_reader(&self, sim: &mut Sim, mut from_node: FramedRecv) {
        let log = self.0.clone();
        sim.scheduler().spawn_root("probe-reader", async move {
            while let Some(frame) = from_node.recv().await {
                log.borrow_mut()
                    .push(BlockSyncMessage::decode_frame(frame).ok());
            }
        });
    }
}
