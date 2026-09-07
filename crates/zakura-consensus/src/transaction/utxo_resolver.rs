//! One cancellable UTXO resolution shared by transactions in a block.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use futures::{
    future::{BoxFuture, Shared},
    FutureExt, StreamExt,
};
use tower::{Service, ServiceExt};

use zakura_chain::{block, transparent};
use zakura_state as zs;

use crate::{error::TransactionError, BoxError};

/// Maximum outstanding batches per block, including missing-output waits.
/// The state separately bounds running database reads across all blocks.
const MAX_IN_FLIGHT_UTXO_BATCHES: usize = 4;

type ResolvedUtxos = Arc<HashMap<transparent::OutPoint, transparent::Utxo>>;

/// A block-owned lookup shared by its transactions.
///
/// The resolver caches only informational outputs for this verification attempt.
/// Contextual validation must still check spends against the block's actual chain.
/// Dropping every clone cancels unresolved requests, but not already-running disk reads.
#[derive(Clone)]
pub struct BlockUtxos(Arc<Shared<BoxFuture<'static, Result<ResolvedUtxos, TransactionError>>>>);

impl fmt::Debug for BlockUtxos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockUtxos").finish_non_exhaustive()
    }
}

impl PartialEq for BlockUtxos {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for BlockUtxos {}

impl BlockUtxos {
    pub(crate) fn for_block<S>(
        block: &block::Block,
        known_utxos: &HashMap<transparent::OutPoint, transparent::OrderedUtxo>,
        state: S,
    ) -> Option<Self>
    where
        S: Service<zs::Request, Response = zs::Response, Error = BoxError> + Send + Clone + 'static,
        S::Future: Send + 'static,
    {
        let mut seen = HashSet::new();
        let outpoints: Vec<_> = block
            .transactions
            .iter()
            .flat_map(|tx| tx.spent_outpoints())
            .filter(|outpoint| !known_utxos.contains_key(outpoint) && seen.insert(*outpoint))
            .collect();
        if outpoints.is_empty() {
            return None;
        }

        let lookup = async move {
            let resolve = async move {
                let mut batches = futures::stream::iter(outpoints)
                    .chunks(zs::constants::MAX_UTXO_BATCH_SIZE)
                    .map(move |outpoints| {
                        let state = state.clone();
                        async move {
                            let response = state
                                .oneshot(zs::Request::AwaitUtxos(outpoints.clone()))
                                .await
                                .map_err(|error| {
                                    match error.downcast::<tower::timeout::error::Elapsed>() {
                                        Ok(_) => TransactionError::TransparentInputNotFound,
                                        Err(error) => TransactionError::from(error),
                                    }
                                })?;
                            let zs::Response::Utxos(utxos) = response else {
                                unreachable!("AwaitUtxos returns Utxos")
                            };
                            // Never let an incomplete state response omit a sighash input.
                            if utxos.len() != outpoints.len()
                                || outpoints
                                    .iter()
                                    .any(|outpoint| !utxos.contains_key(outpoint))
                            {
                                return Err(TransactionError::TransparentInputNotFound);
                            }
                            Ok(utxos)
                        }
                    })
                    .buffer_unordered(MAX_IN_FLIGHT_UTXO_BATCHES);
                let mut resolved = HashMap::new();
                while let Some(batch) = batches.next().await {
                    resolved.extend(batch?);
                }
                Ok(Arc::new(resolved))
            };
            // One deadline covers admission, reads, and dependency waits for this block.
            // Unlike serial per-input deadlines, later batches do not get extra time.
            tokio::time::timeout(super::UTXO_LOOKUP_TIMEOUT, resolve)
                .await
                .map_err(|_| TransactionError::TransparentInputNotFound)?
        };
        Some(Self(Arc::new(lookup.boxed().shared())))
    }

    pub(super) async fn resolve(&self) -> Result<ResolvedUtxos, TransactionError> {
        self.0.as_ref().clone().await
    }
}
