//! State contextual verification and storage code for the Zakura node.
//!
//! # Correctness
//!
//! Await UTXO and block commit requests should be wrapped in a timeout, because:
//! - await UTXO requests wait for a block containing that UTXO, and
//! - contextual verification and state updates wait for all previous blocks.
//!
//! Otherwise, verification of out-of-order and invalid blocks can hang indefinitely.

#![doc(html_favicon_url = "https://zakura.com/assets/rustdoc/zakura-favicon-128.png")]
#![doc(html_logo_url = "https://zakura.com/assets/rustdoc/zakura-icon.png")]
#![doc(html_root_url = "https://docs.rs/zakura_state")]
// Remove if possible if MSRV is increased
#![allow(unknown_lints)]
#![allow(clippy::manual_is_multiple_of)]
// Long Tower service and future types are routine in this crate, and factoring
// them into type aliases would not make the code clearer.
#![allow(clippy::type_complexity)]

#[macro_use]
extern crate tracing;

// TODO: only export the Config struct and a few other important methods
pub mod config;
// Most constants are exported by default
pub mod constants;
mod header_chain;

// Allow use in external tests
#[cfg(any(test, feature = "proptest-impl"))]
pub mod arbitrary;

mod error;
mod request;
mod response;
mod service;

#[cfg(test)]
mod tests;

pub use config::{
    check_and_delete_old_databases, check_and_delete_old_state_databases,
    database_format_version_on_disk, state_database_format_version_on_disk, Config, PruningConfig,
    StorageMode,
};
pub use constants::{
    state_database_format_version_in_code, MAX_BLOCK_REORG_HEIGHT,
    MAX_HISTORICAL_TREE_REPLAY_BLOCKS,
};
pub use error::{
    BoxError, CloneError, CommitBlockError, CommitCheckpointVerifiedError,
    CommitSemanticallyVerifiedError, DuplicateNullifierError, HistoricalSubtreeUnavailable,
    HistoricalSubtreeUnavailableReason, HistoricalTreeUnavailable, MissingSproutTipTree,
    StateInitError, ValidateContextError,
};
pub use header_chain::*;
pub use request::{
    CheckpointVerifiedBlock, CommitSemanticallyVerifiedBlockRequest, HashOrHeight,
    HeaderChainBodyEvidenceAuthority, MappedRequest, PreparedHeaderChainBodyEvidence,
    PreparedHeaderChainInsert, ReadRequest, Request, SemanticallyVerifiedBlock,
};

#[cfg(feature = "indexer")]
pub use request::Spend;

pub use response::{
    AnyTx, BlockSyncBodyMetadata, GetBlockTemplateChainInfo, KnownBlock, MinedTx,
    NonFinalizedBlocksListener, ReadResponse, Response,
};
#[cfg(any(test, feature = "header-fuzz"))]
pub use service::finalized_state::{replay_recovery_rows_bytes, RecoveryRowsReplaySummary};
pub use service::{
    chain_tip::{ChainTipBlock, ChainTipChange, ChainTipSender, LatestChainTip, TipAction},
    check,
    finalized_state::FinalizedState,
    init, init_read_only, init_with_header_chain_body_evidence,
    non_finalized_state::NonFinalizedState,
    spawn_init_read_only,
    watch_receiver::WatchReceiver,
    OutputLocation, ReadState, State, TransactionIndex, TransactionLocation,
};

// Allow use in the scanner and external tests
#[cfg(any(test, feature = "proptest-impl"))]
pub use service::finalized_state::{ReadDisk, TypedColumnFamily, WriteTypedBatch};

// Lets tests above this crate drive real RPC handlers over a fast-synced node's absent band.
#[cfg(any(test, feature = "proptest-impl"))]
pub use service::finalized_state::vct_fast_sync_fixture::{VctFastSyncedChain, VctFastSyncedNode};

#[cfg(feature = "internal-bench")]
pub use service::finalized_state::{
    benchmark_finality_witness, FinalityWitnessBenchmarkReport, FinalityWitnessBenchmarkSample,
};
pub use service::finalized_state::{
    derived_roots_in_display_order, inventory as vct_treestate_inventory,
    inventory_with_scans as vct_treestate_inventory_with_scans, measure_derivations, replay_inputs,
    verify_subtrees_against_stored, DerivationSample, ReplayInputs, SubtreeVerification,
    VctTreestateInventory,
};
pub use service::finalized_state::{
    export_frontier_grid_to, produce_release_treestate_artifacts, verify_subtree_artifact,
    FrontierArtifact, FrontierEntry, FrontierGridExport, FrontierGridExportError, GridSpacing,
    ReleaseTreestateArtifacts, ReleaseTreestateArtifactsError, SubtreeArtifact, SubtreeRecord,
    TreestateArtifactError, VerifiedSubtreeCounts,
};
pub use service::finalized_state::{
    preview_prune_finalized_state, prune_finalized_state, PruneFinalizedStateError,
    PruneFinalizedStateOptions, PruneFinalizedStateSummary,
};
pub use service::finalized_state::{
    preview_rollback_finalized_state, rollback_finalized_state, RollbackBackupSummary,
    RollbackFinalizedStateError, RollbackFinalizedStateOptions, RollbackFinalizedStateSummary,
};
pub use service::finalized_state::{
    produce_final_frontiers_bytes, produce_settled_final_frontiers_bytes,
    validate_final_frontiers_bytes, FinalFrontiersGenerationError, FinalFrontiersValidationError,
};
pub use service::read::{
    derive_historical_frontiers, ChainTipInfo, ChainTipStatus, DerivedFrontiers,
    HistoricalTreeCache, MAX_CACHED_FRONTIERS,
};
pub use service::{
    finalized_state::{DiskWriteBatch, FallibleDiskValue, FromDisk, IntoDisk, WriteDisk, ZakuraDb},
    ReadStateService, VctRootRepairState, VctRootRepairStatus,
};

// Allow use in external tests
#[cfg(any(test, feature = "proptest-impl"))]
pub use service::{
    arbitrary::{populated_state, CHAIN_TIP_UPDATE_WAIT_LIMIT},
    finalized_state::{RawBytes, KV, MAX_ON_DISK_HEIGHT},
    init_test, init_test_services,
};

#[cfg(any(test, feature = "proptest-impl"))]
pub use config::hidden::{
    write_database_format_version_to_disk, write_state_database_format_version_to_disk,
};

// Allow use only inside the crate in production
#[cfg(not(any(test, feature = "proptest-impl")))]
#[allow(unused_imports)]
pub(crate) use config::hidden::{
    write_database_format_version_to_disk, write_state_database_format_version_to_disk,
};

pub use request::ContextuallyVerifiedBlock;
