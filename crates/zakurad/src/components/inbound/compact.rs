//! Screens compact block relay requests from connections that negotiated compact block relay.
//!
//! Each check runs before the work it protects. A `cmpctblock` header must extend the best tip
//! before this node spends CPU on its proof of work, and must pass every header check before
//! this node reveals which of its transactions the mempool holds. A rebuilt block must match
//! its header's commitments before the connection hands it to the block downloader.
//!
//! Every failure is a plain error. The connection falls back to downloading the whole block,
//! and doesn't score the peer.

use std::sync::Arc;

use chrono::Utc;
use futures::FutureExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tower::{util::BoxCloneSyncService, ServiceExt};

use zakura_chain::{
    block::{self, merkle::AuthDataRoot, Block, MAX_BLOCK_BYTES},
    parameters::{Network, NetworkUpgrade},
    serialization::ZcashSerialize,
    transaction::UnminedTx,
};
use zakura_network::CompactBlock;
use zakura_node_services::mempool;
use zakura_state as zs;

use crate::BoxError;

use super::Mempool;

/// The most compact block checks that run at once.
///
/// A check that finds every permit taken fails at once, rather than queueing behind CPU work.
pub const MAX_CONCURRENT_COMPACT_CHECKS: usize = 3;

/// The read-only state service that compact block checks read from.
pub type CompactReadState = BoxCloneSyncService<zs::ReadRequest, zs::ReadResponse, zs::BoxError>;

/// Answers the compact block relay checks of connections that negotiated compact block relay.
///
/// Each check holds one of [`MAX_CONCURRENT_COMPACT_CHECKS`] permits until it finishes,
/// including any blocking task it started. The inbound service bounds each check's time with
/// its response timeout.
#[derive(Clone)]
pub struct CompactRelay {
    /// The network whose upgrades and proof of work rules the checks use.
    network: Network,

    /// The state the checks read the best tip and its context from.
    read_state: CompactReadState,

    /// The permits that bound concurrent checks.
    checks: Arc<Semaphore>,
}

impl CompactRelay {
    /// Returns a compact relay checker for `network` that reads from `read_state`.
    pub fn new(network: Network, read_state: CompactReadState) -> Self {
        Self {
            network,
            read_state,
            checks: Arc::new(Semaphore::new(MAX_CONCURRENT_COMPACT_CHECKS)),
        }
    }

    /// Checks that `compact`'s header extends the best tip at its coinbase height, that NU5 is
    /// active at that height, and that its proof of work, difficulty, and time are valid.
    pub async fn check_header(&self, compact: Arc<CompactBlock>) -> Result<(), BoxError> {
        let permit = self.acquire()?;
        self.screen_header(compact, permit).await
    }

    /// Screens `compact`'s header like [`Self::check_header`], then returns the transactions in
    /// `mempool` that match its short IDs.
    ///
    /// Fails if the matching transactions exceed the block size limit.
    pub async fn block_transactions(
        &self,
        compact: Arc<CompactBlock>,
        mempool: Mempool,
    ) -> Result<Vec<UnminedTx>, BoxError> {
        let permit = self.acquire()?;
        self.screen_header(compact.clone(), permit.clone()).await?;
        if compact.short_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mempool::Response::TransactionIds(mempool_ids) = mempool
            .clone()
            .oneshot(mempool::Request::TransactionIds)
            // Boxing erases the future's type, which a higher-ranked lifetime check on its
            // error type otherwise rejects.
            .boxed()
            .await?
        else {
            unreachable!("the mempool answers a transaction ID request with transaction IDs");
        };
        let ids = run_blocking(permit.clone(), move || compact.matching_ids(mempool_ids)).await?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mempool::Response::Transactions(transactions) = mempool
            .oneshot(mempool::Request::TransactionsById(ids))
            // Boxing erases the future's type, which a higher-ranked lifetime check on its
            // error type otherwise rejects.
            .boxed()
            .await?
        else {
            unreachable!("the mempool answers a transaction lookup with transactions");
        };
        let bytes = transactions.iter().fold(0usize, |bytes, transaction| {
            bytes.saturating_add(transaction.size())
        });
        if u64::try_from(bytes).unwrap_or(u64::MAX) > MAX_BLOCK_BYTES {
            return Err("compact block candidates exceed the block size limit".into());
        }

        drop(permit);
        Ok(transactions)
    }

    /// Checks that `block`, rebuilt from a `cmpctblock`, matches its header's commitments.
    ///
    /// The block passes if it fits the block size limit, its coinbase transaction comes first,
    /// NU5 is active at its height, its Merkle root and authorizing-data commitment match its
    /// header, and it extends the best tip with the expected difficulty.
    pub async fn check_block(&self, block: Arc<Block>) -> Result<(), BoxError> {
        let permit = self.acquire()?;
        let network = self.network.clone();
        let checked = block.clone();
        let auth_data_root =
            run_blocking(permit.clone(), move || check_block_body(&checked, &network)).await??;

        let commitment = zs::BlockCommitmentData {
            block: block.into(),
            auth_data_root: Some(auth_data_root),
        };
        let response = self
            .read_state
            .clone()
            .oneshot(zs::ReadRequest::CheckPreparedMinedRelayEligibility(
                commitment,
            ))
            .await?;
        drop(permit);

        match response {
            zs::ReadResponse::PreparedMinedRelayEligibility(
                zs::PreparedMinedRelayEligibility::Authorized,
            ) => Ok(()),
            zs::ReadResponse::PreparedMinedRelayEligibility(_) => {
                Err("rebuilt compact block is not an authenticated child of the best tip".into())
            }
            _ => unreachable!("the state answers a relay eligibility check with an eligibility"),
        }
    }

    /// Takes a check permit, or fails if every permit is taken.
    fn acquire(&self) -> Result<Arc<OwnedSemaphorePermit>, BoxError> {
        let permit = self
            .checks
            .clone()
            .try_acquire_owned()
            .map_err(|_| "compact block checks are busy")?;
        Ok(Arc::new(permit))
    }

    /// Screens `compact`'s header for [`Self::check_header`], holding `permit` throughout.
    async fn screen_header(
        &self,
        compact: Arc<CompactBlock>,
        permit: Arc<OwnedSemaphorePermit>,
    ) -> Result<(), BoxError> {
        // Reject a stale parent before spending CPU on proof of work.
        let zs::ReadResponse::Tip(tip) = self
            .read_state
            .clone()
            .oneshot(zs::ReadRequest::Tip)
            .await?
        else {
            unreachable!("the state answers a tip request with a tip");
        };
        if tip.map(|(_, hash)| hash) != Some(compact.header.previous_block_hash) {
            return Err("compact block does not extend the best tip".into());
        }

        let network = self.network.clone();
        let checked = compact.clone();
        let height = run_blocking(permit.clone(), move || {
            check_header_proof(&checked, &network)
        })
        .await??;

        let request = zs::ReadRequest::CheckBestTipHeader {
            header: compact.header.clone(),
            height,
        };
        let zs::ReadResponse::ValidBestTipHeader = self.read_state.clone().oneshot(request).await?
        else {
            unreachable!("the state answers a best-tip header check with a valid header");
        };

        drop(permit);
        Ok(())
    }
}

/// Runs `work` on a blocking task that holds `permit` until `work` returns, even if the caller
/// stops waiting.
async fn run_blocking<T: Send + 'static>(
    permit: Arc<OwnedSemaphorePermit>,
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, BoxError> {
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await?;
    Ok(result)
}

/// Checks the parts of `compact`'s header that need no chain context, and returns the height
/// its coinbase transaction claims.
///
/// Checks the message's limits, that NU5 is active at the coinbase height, that the header time
/// isn't too far in the future, and the header's proof of work. Callers run it on a blocking
/// task, because it checks the Equihash solution.
fn check_header_proof(
    compact: &CompactBlock,
    network: &Network,
) -> Result<block::Height, BoxError> {
    compact.validate()?;
    let height = compact
        .coinbase_height()
        .ok_or("compact block coinbase transaction has no height")?;
    if NetworkUpgrade::current(network, height) < NetworkUpgrade::Nu5 {
        return Err("compact block relay needs NU5 block commitments".into());
    }

    let hash = compact.block_hash();
    zakura_consensus::time_is_valid_at(&compact.header, Utc::now(), &height, &hash)?;
    if !zakura_consensus::proof_of_work_is_valid(&compact.header, network, &height, &hash) {
        return Err("compact block header fails proof of work".into());
    }

    Ok(height)
}

/// Checks `block`'s size, coinbase position, network upgrade, and Merkle root, and returns its
/// authorizing-data root.
///
/// Callers run it on a blocking task, because it hashes every transaction.
fn check_block_body(block: &Block, network: &Network) -> Result<AuthDataRoot, BoxError> {
    if u64::try_from(block.zcash_serialized_size()).unwrap_or(u64::MAX) > MAX_BLOCK_BYTES {
        return Err("rebuilt compact block exceeds the block size limit".into());
    }
    zakura_consensus::coinbase_position_is_valid(block)?;
    let height = block
        .coinbase_height()
        .ok_or("rebuilt compact block coinbase transaction has no height")?;
    if NetworkUpgrade::current(network, height) < NetworkUpgrade::Nu5 {
        return Err("compact block relay needs NU5 block commitments".into());
    }
    zakura_consensus::merkle_root_validity(network, block, &block.transaction_hashes())?;

    Ok(block.auth_data_root())
}
