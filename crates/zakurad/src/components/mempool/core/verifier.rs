//! The core's verifications, and the transaction verifier's pool handle.
//!
//! [`ConsensusVerifier`] is today's download-and-verify task body without the
//! fetch: the size check, the already-mined lookup, then the transaction
//! verifier. It turns the answer into the machine's [`Outcome`], with
//! today's charge for a failure.
//!
//! The transaction verifier looks up outputs a transaction spends from the
//! pool through its own mempool handle. Today's pool makes it wait up to a
//! minute for an output to arrive. In core mode [`VerifierPool`] answers at
//! once from the pool the core published before it started the job, and the
//! machine parks a transaction whose input is missing, so nothing waits
//! inside a verification.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
};

use futures::FutureExt;
use tower::{timeout::Timeout, Service, ServiceExt};

use zakura_chain::{
    block::Height,
    parameters::{Network, NetworkUpgrade},
    transaction::UnminedTx,
    transparent::OutPoint,
};
use zakura_consensus::{error::TransactionError, transaction as tx};
use zakura_mempool::{
    io::Verifier,
    machine::Outcome,
    storage::{ExactTipRejectionError, NonStandardTransactionError, RejectionError},
};
use zakura_node_services::mempool::{
    port::{ChainTip, Charge, LocalFault, PoolHandle, PoolSnapshot},
    OutputNotInPool, Request, Response,
};
use zakura_state as zs;

use crate::BoxError;

use super::super::{
    downloads::TransactionDownloadVerifyError, failure_bans, failure_cools_down,
    upgrade_grace_period, ReadState, TxVerifier,
};

/// The core's verifications, through the node's transaction verifier.
#[derive(Clone)]
pub(crate) struct ConsensusVerifier {
    tx_verifier: Timeout<TxVerifier>,
    /// The read state service is not `Sync`, so each verification clones it
    /// under this lock.
    read_state: Arc<Mutex<ReadState>>,
    handle: PoolHandle,
    max_transaction_bytes: u64,
    network: Network,
    /// Whether the node is current enough to start cooldowns. Today's pool
    /// starts them only then.
    current: Arc<AtomicBool>,
}

impl ConsensusVerifier {
    /// A verifier that calls `tx_verifier`, looks transactions up in
    /// `read_state`, and reads the pool through `handle`.
    pub fn new(
        tx_verifier: Timeout<TxVerifier>,
        read_state: ReadState,
        handle: PoolHandle,
        max_transaction_bytes: u64,
        network: Network,
        current: Arc<AtomicBool>,
    ) -> Self {
        Self {
            tx_verifier,
            read_state: Arc::new(Mutex::new(read_state)),
            handle,
            max_transaction_bytes,
            network,
            current,
        }
    }

    /// Verifies `tx` for the block after `tip`. `start` is the pool the core
    /// published before it started this job.
    async fn check(
        self,
        tx: UnminedTx,
        tip: ChainTip,
        revalidation: bool,
        start: Option<Arc<PoolSnapshot>>,
    ) -> Outcome<TransactionError> {
        if usize::try_from(self.max_transaction_bytes)
            .is_ok_and(|max_transaction_bytes| tx.size() > max_transaction_bytes)
        {
            return Outcome::Fail {
                error: RejectionError::ExactTip(ExactTipRejectionError::FailedStandard(
                    NonStandardTransactionError::TransactionTooLarge {
                        actual_bytes: tx.size(),
                        max_bytes: self.max_transaction_bytes,
                    },
                )),
                charge: Charge::None,
            };
        }

        let read_state = self
            .read_state
            .lock()
            .expect("no code panics while holding the read state lock")
            .clone();
        // Boxing the service futures works around a compiler limitation:
        // without it, the boxed verification cannot be proven `Send`.
        let lookup: futures::future::BoxFuture<'static, Result<zs::ReadResponse, BoxError>> =
            read_state
                .oneshot(zs::ReadRequest::Transaction(tx.id().mined_id()))
                .boxed();
        match lookup.await {
            Ok(zs::ReadResponse::Transaction(None)) => {}
            Ok(zs::ReadResponse::Transaction(Some(_))) => {
                return Outcome::AlreadyMined(Arc::new(TransactionDownloadVerifyError::InState))
            }
            Ok(_) => unreachable!("a transaction lookup is answered with a transaction"),
            Err(_) => return Outcome::LocalFault(LocalFault::StateUnavailable),
        }

        let next_height = (tip.height + 1).unwrap_or(Height::MAX);
        let transaction_version = tx.transaction().version();
        let transaction_network_upgrade = tx.transaction().network_upgrade();
        let spends: Vec<OutPoint> = tx.transaction().spent_outpoints().collect();
        let verification: futures::future::BoxFuture<'static, Result<tx::Response, BoxError>> =
            self.tx_verifier
                .clone()
                .oneshot(tx::Request::Mempool {
                    transaction: tx,
                    height: next_height,
                })
                .boxed();
        let result = verification.await;

        let error = match result {
            Ok(tx::Response::Mempool {
                transaction,
                spent_mempool_outpoints,
            }) => {
                metrics::counter!(
                    "mempool.verified.transactions.total",
                    "version" => format!("{}", transaction.transaction.transaction().version()),
                )
                .increment(1);
                return Outcome::Pass {
                    tx: transaction,
                    pool_inputs: spent_mempool_outpoints,
                };
            }
            Ok(_) => unreachable!("a mempool request is answered with a mempool response"),
            Err(error) if error.is::<tokio::time::error::Elapsed>() => {
                return Outcome::LocalFault(LocalFault::VerifyTimeout)
            }
            Err(error) => TransactionError::from(error),
        };

        match error {
            // The inputs the start pool did not offer. Spends of chain
            // outputs are among them, which only parks the transaction on
            // parents that never arrive. Using the pool from the start of
            // the job is what lets the machine see a parent that arrived
            // while it ran.
            TransactionError::TransparentInputNotFound => Outcome::MissingInputs {
                missing: spends
                    .into_iter()
                    .filter(|outpoint| {
                        start
                            .as_ref()
                            .is_none_or(|start| start.created_output(outpoint).is_none())
                    })
                    .collect(),
                error: TransactionError::TransparentInputNotFound,
            },
            TransactionError::StateUnavailable(_) => {
                Outcome::LocalFault(LocalFault::StateUnavailable)
            }
            error => {
                let charge = if revalidation {
                    // The machine charges nobody for a transaction that
                    // passed once.
                    Charge::None
                } else if failure_bans(&error, transaction_version) {
                    Charge::Ban
                } else if self.current.load(Ordering::Relaxed)
                    && failure_cools_down(
                        &error,
                        transaction_version,
                        transaction_network_upgrade,
                        NetworkUpgrade::current(&self.network, next_height),
                        upgrade_grace_period(&self.network, next_height),
                    )
                {
                    Charge::Cooldown
                } else {
                    Charge::None
                };
                Outcome::Fail {
                    error: RejectionError::ExactTip(ExactTipRejectionError::FailedVerification(
                        error,
                    )),
                    charge,
                }
            }
        }
    }
}

impl Verifier<TransactionError> for ConsensusVerifier {
    fn verify(
        &self,
        tx: UnminedTx,
        tip: ChainTip,
        revalidation: bool,
    ) -> Pin<Box<dyn Future<Output = Outcome<TransactionError>> + Send>> {
        // The core calls this after it published the step that starts the
        // job, so this is the pool the job started from.
        let start = self.handle.snapshot();
        Box::pin(self.clone().check(tx, tip, revalidation, start))
    }
}

/// The transaction verifier's pool handle in core mode. It answers
/// `AwaitOutput` at once, from the latest published pool, with the output
/// or [`OutputNotInPool`].
#[derive(Clone, Debug)]
pub(super) struct VerifierPool {
    handle: PoolHandle,
}

impl VerifierPool {
    /// A handle that reads the pool through `handle`.
    pub fn new(handle: PoolHandle) -> Self {
        Self { handle }
    }
}

impl Service<Request> for VerifierPool {
    type Response = Response;
    type Error = BoxError;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let answer = match request {
            Request::AwaitOutput(outpoint) => self
                .handle
                .snapshot()
                .and_then(|snapshot| snapshot.created_output(&outpoint).cloned())
                .map(Response::UnspentOutput)
                .ok_or_else(|| BoxError::from(OutputNotInPool)),
            _ => Err("the verifier's pool handle only answers output lookups".into()),
        };
        async move { answer }.boxed()
    }
}
