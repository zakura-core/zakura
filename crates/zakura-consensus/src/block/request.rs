//! Block verifier request type.

use std::sync::Arc;

use zakura_chain::block::Block;
use zakura_state::BlockLifecycleReporter;

#[derive(Debug, Clone, PartialEq, Eq)]
/// A request to the chain or block verifier
pub enum Request {
    /// Performs semantic validation, then asks the state to perform contextual validation and commit the block
    Commit(Arc<Block>),
    /// Performs semantic validation and reports lifecycle milestones while committing the block.
    CommitWithLifecycle {
        /// The block received from a peer.
        block: Arc<Block>,
        /// Reports verification and state lifecycle milestones.
        lifecycle: BlockLifecycleReporter,
    },
    /// Reuses prepared mining work when possible, then commits the solved block.
    CommitMined {
        /// The solved block.
        block: Arc<Block>,
        /// The template work ID supplied by the miner.
        work_id: Option<String>,
        /// Reports verification and state lifecycle milestones.
        lifecycle: BlockLifecycleReporter,
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
            Request::CommitWithLifecycle { block, .. } => block,
            Request::CommitMined { block, .. } => block,
            Request::CheckProposal(block) => block,
            Request::Prepare { block, .. } => block,
        })
    }

    /// Returns `true` if the request is a proposal
    pub fn is_proposal(&self) -> bool {
        match self {
            Request::Commit(_)
            | Request::CommitWithLifecycle { .. }
            | Request::CommitMined { .. } => false,
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
            Request::Commit(_)
            | Request::CommitWithLifecycle { .. }
            | Request::CheckProposal(_) => None,
        }
    }

    /// Returns the lifecycle reporter for a tracked commit.
    pub fn lifecycle(&self) -> Option<BlockLifecycleReporter> {
        match self {
            Request::CommitWithLifecycle { lifecycle, .. }
            | Request::CommitMined { lifecycle, .. } => Some(lifecycle.clone()),
            _ => None,
        }
    }

    /// Returns true for a mined-block commit.
    pub fn is_mined_commit(&self) -> bool {
        matches!(self, Request::CommitMined { .. })
    }
}
