//! Block verifier request type.

use zakura_node_services::block_memory::BlockHandle;
use zakura_state::BlockAdmission;

/// Identifies who supplied a prepared mining candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedCandidateSource {
    /// The node's background `getblocktemplate` preparation.
    ServerTemplate,
    /// A client's proposal-mode `getblocktemplate` request.
    ClientProposal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A request to the chain or block verifier
pub enum Request {
    /// Performs semantic validation, then asks the state to perform contextual validation and commit the block
    Commit(BlockHandle),
    /// Reuses prepared mining work when possible, then commits the solved block.
    CommitMined {
        /// The solved block.
        block: BlockHandle,
        /// State write-queue admission notification.
        admission: BlockAdmission,
    },
    /// Performs semantic validation but skips checking proof of work,
    /// then asks the state to perform contextual validation.
    /// Does not commit the block to the state.
    CheckProposal(BlockHandle),
    /// Validates and caches a mining candidate without checking proof of work.
    Prepare {
        /// The unsolved candidate block.
        block: BlockHandle,
        /// The source that supplied the candidate.
        source: PreparedCandidateSource,
    },
}

impl Request {
    /// Returns inner block
    pub fn block(&self) -> BlockHandle {
        match self {
            Request::Commit(block) => block,
            Request::CommitMined { block, .. } => block,
            Request::CheckProposal(block) => block,
            Request::Prepare { block, .. } => block,
        }
        .clone()
    }

    /// Returns `true` if the request is a proposal
    pub fn is_proposal(&self) -> bool {
        match self {
            Request::Commit(_) | Request::CommitMined { .. } => false,
            Request::CheckProposal(_) | Request::Prepare { .. } => true,
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
