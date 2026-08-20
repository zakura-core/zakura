//! Block verifier request type.

use std::sync::Arc;

use zakura_chain::block::Block;
use zakura_state::BlockAdmission;

#[derive(Debug, Clone, PartialEq, Eq)]
/// A request to the chain or block verifier
pub enum Request {
    /// Performs semantic validation, then asks the state to perform contextual validation and commit the block
    Commit(Arc<Block>),
    /// Reuses prepared mining work when possible, then commits the solved block.
    CommitMined {
        /// The solved block.
        block: Arc<Block>,
        /// The template work ID supplied by the miner.
        work_id: Option<String>,
        /// State write-queue admission notification.
        admission: BlockAdmission,
    },
    /// Performs semantic validation but skips checking proof of work,
    /// then asks the state to perform contextual validation.
    /// Does not commit the block to the state.
    CheckProposal(Arc<Block>),
    /// Validates and caches a mining candidate without checking proof of work.
    Prepare {
        /// The unsolved candidate block.
        block: Arc<Block>,
        /// The template work ID, when one was assigned.
        work_id: Option<String>,
    },
}

impl Request {
    /// Returns inner block
    pub fn block(&self) -> Arc<Block> {
        Arc::clone(match self {
            Request::Commit(block) => block,
            Request::CommitMined { block, .. } => block,
            Request::CheckProposal(block) => block,
            Request::Prepare { block, .. } => block,
        })
    }

    /// Returns `true` if the request is a proposal
    pub fn is_proposal(&self) -> bool {
        match self {
            Request::Commit(_) | Request::CommitMined { .. } => false,
            Request::CheckProposal(_) | Request::Prepare { .. } => true,
        }
    }

    /// Returns true when a successful proposal should populate the prepared-candidate cache.
    pub fn should_cache(&self) -> bool {
        matches!(self, Request::Prepare { .. })
    }

    /// Returns the supplied mining work ID.
    pub fn work_id(&self) -> Option<&str> {
        match self {
            Request::CommitMined { work_id, .. } | Request::Prepare { work_id, .. } => {
                work_id.as_deref()
            }
            Request::Commit(_) | Request::CheckProposal(_) => None,
        }
    }

    /// Returns the state admission notification for a mined commit.
    pub fn admission(&self) -> Option<BlockAdmission> {
        match self {
            Request::CommitMined { admission, .. } => Some(admission.clone()),
            _ => None,
        }
    }

    /// Returns true for a mined-block commit.
    pub fn is_mined_commit(&self) -> bool {
        matches!(self, Request::CommitMined { .. })
    }
}
