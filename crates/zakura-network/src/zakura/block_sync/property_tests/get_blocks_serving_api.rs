//! Test-only adapter between the serving properties and the current reactor seam.
//!
//! The properties use these opaque values so request/session correlation can evolve
//! without rewriting the property logic that exposed a bug.

use std::sync::Arc;

use super::super::super::{BlockSyncAction, BlockSyncEvent};
use crate::zakura::ZakuraPeerId;
use zakura_chain::block;

#[derive(Clone, Debug)]
pub(super) struct ServingQuery {
    peer: ZakuraPeerId,
    start: block::Height,
    count: u32,
}

impl ServingQuery {
    pub(super) fn from_action(action: BlockSyncAction) -> Result<Option<Self>, BlockSyncAction> {
        match action {
            BlockSyncAction::QueryBlocksByHeightRange {
                peer, start, count, ..
            } => Ok(Some(Self { peer, start, count })),
            BlockSyncAction::QueryNeededBlocks { .. } => Ok(None),
            action => Err(action),
        }
    }

    pub(super) fn peer(&self) -> &ZakuraPeerId {
        &self.peer
    }

    pub(super) fn start(&self) -> block::Height {
        self.start
    }

    pub(super) fn count(&self) -> u32 {
        self.count
    }

    pub(super) fn with_start(&self, start: block::Height) -> Self {
        Self {
            peer: self.peer.clone(),
            start,
            count: self.count,
        }
    }

    pub(super) fn ready_event(
        &self,
        blocks: Vec<(block::Height, Arc<block::Block>, usize)>,
    ) -> BlockSyncEvent {
        BlockSyncEvent::BlockRangeResponseReady {
            peer: self.peer.clone(),
            start_height: self.start,
            requested_count: self.count,
            blocks,
        }
    }

    pub(super) fn finished_event(&self, returned_count: u32) -> BlockSyncEvent {
        BlockSyncEvent::BlockRangeResponseFinished {
            peer: self.peer.clone(),
            start_height: self.start,
            requested_count: self.count,
            returned_count,
        }
    }
}
