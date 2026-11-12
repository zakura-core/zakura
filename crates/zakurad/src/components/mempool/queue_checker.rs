//! Zebra Mempool queue checker.
//!
//! The queue checker sends a request to the mempool whenever a transaction
//! verification finishes, and periodically as a fallback, so that newly
//! verified transactions are added to the mempool, and gossiped to peers.
//!
//! It also sends a request whenever the best chain tip changes. The mempool
//! only learns about a new tip when it is polled, and a tip it sees after
//! missing the one before it forces a full reset instead of removing the
//! mined transactions. With no request between two blocks, every pair of
//! blocks closer together than [`RATE_LIMIT_DELAY`] caused such a reset.
//!
//! The mempool performs these actions on every request,
//! but we can't guarantee that requests will arrive from peers
//! on a regular basis.
//!
//! Crawler queue requests are also too infrequent,
//! and they only happen if peers respond within the timeout.

use std::{sync::Arc, time::Duration};

use tokio::{sync::Notify, task::JoinHandle, time::sleep};
use tower::{BoxError, Service, ServiceExt};
use tracing::Instrument;
use zakura_chain::chain_tip::ChainTip;
use zakura_node_services::task::spawn_erased;

use crate::components::mempool;

/// The longest delay between queue check events.
///
/// This interval is chosen so that there are a significant number of
/// queue checks in each target block interval.
///
/// This allows transactions to propagate across the network for each block,
/// even if some peers are poorly connected.
const RATE_LIMIT_DELAY: Duration = Duration::from_secs(5);

/// The mempool queue checker.
///
/// The queue checker relies on the mempool to ignore requests when the mempool is inactive.
pub struct QueueChecker<Mempool, Tip> {
    /// The mempool service that receives crawled transaction IDs.
    mempool: Mempool,

    /// Signalled after each mempool transaction verification finishes.
    verification_finished: Arc<Notify>,

    /// Signals each best chain tip change.
    latest_chain_tip: Tip,
}

impl<Mempool, Tip> QueueChecker<Mempool, Tip>
where
    Mempool:
        Service<mempool::Request, Response = mempool::Response, Error = BoxError> + Send + 'static,
    Mempool::Future: Send,
    Tip: ChainTip + Send + 'static,
{
    /// Spawn an asynchronous task to run the mempool queue checker.
    ///
    /// `verification_finished` must be the signal returned by the mempool's
    /// `verification_finished` method. `latest_chain_tip` must follow the same
    /// state as the mempool.
    pub fn spawn(
        mempool: Mempool,
        verification_finished: Arc<Notify>,
        latest_chain_tip: Tip,
    ) -> JoinHandle<Result<(), BoxError>> {
        let queue_checker = QueueChecker {
            mempool,
            verification_finished,
            latest_chain_tip,
        };

        spawn_erased(queue_checker.run().in_current_span())
    }

    /// Send a request to the mempool after each transaction verification
    /// finishes, after each best chain tip change, or after
    /// [`RATE_LIMIT_DELAY`] at the latest.
    ///
    /// Runs until the mempool or the chain tip returns an error,
    /// which happens when Zebra is shutting down.
    pub async fn run(mut self) -> Result<(), BoxError> {
        info!("initializing mempool queue checker task");

        loop {
            // `Notify` keeps one permit, so signals sent during a check
            // start one more check rather than being lost.
            tokio::select! {
                _ = self.verification_finished.notified() => {}
                tip_changed = self.latest_chain_tip.best_tip_changed() => tip_changed?,
                _ = sleep(RATE_LIMIT_DELAY) => {}
            }
            self.check_queue().await?;
        }
    }

    /// Check if the mempool has newly verified transactions.
    async fn check_queue(&mut self) -> Result<(), BoxError> {
        debug!("checking for newly verified mempool transactions");

        // Since this is an internal request, we don't expect any errors.
        // So we propagate any unexpected errors to the task that spawned us.
        let response = self
            .mempool
            .ready()
            .await?
            .call(mempool::Request::CheckForVerifiedTransactions)
            .await?;

        match response {
            mempool::Response::CheckedForVerifiedTransactions => {}
            _ => {
                unreachable!("mempool did not respond with checked queue to mempool queue checker")
            }
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use zakura_chain::{block::Height, chain_tip::mock::MockChainTip};
    use zakura_test::mock_service::MockService;

    use super::*;

    /// A finished verification starts a queue check well before the periodic
    /// fallback, so `sendrawtransaction` callers don't wait for it.
    #[tokio::test]
    async fn verification_finished_starts_queue_check() {
        let _init_guard = zakura_test::init();

        let mut mempool = MockService::build().for_unit_tests();
        let verification_finished = Arc::new(Notify::new());
        let (latest_chain_tip, _chain_tip_sender) = MockChainTip::new();
        let _queue_checker = QueueChecker::spawn(
            mempool.clone(),
            verification_finished.clone(),
            latest_chain_tip,
        );

        mempool.expect_no_requests().await;
        verification_finished.notify_one();

        // The mock waits far less than `RATE_LIMIT_DELAY` for this request.
        mempool
            .expect_request(mempool::Request::CheckForVerifiedTransactions)
            .await
            .respond(mempool::Response::CheckedForVerifiedTransactions);
    }

    /// Each best chain tip change starts a queue check well before the
    /// periodic fallback, so the mempool sees every tip and two blocks close
    /// together don't look like skipped blocks.
    #[tokio::test]
    async fn tip_change_starts_queue_check() {
        let _init_guard = zakura_test::init();

        let mut mempool = MockService::build().for_unit_tests();
        let (latest_chain_tip, chain_tip_sender) = MockChainTip::new();
        let _queue_checker =
            QueueChecker::spawn(mempool.clone(), Arc::new(Notify::new()), latest_chain_tip);

        mempool.expect_no_requests().await;

        for height in [Height(1), Height(2)] {
            chain_tip_sender.send_best_tip_height(height);

            // The mock waits far less than `RATE_LIMIT_DELAY` for this request.
            mempool
                .expect_request(mempool::Request::CheckForVerifiedTransactions)
                .await
                .respond(mempool::Response::CheckedForVerifiedTransactions);
        }
    }
}
