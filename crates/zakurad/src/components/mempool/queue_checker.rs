//! Zebra Mempool queue checker.
//!
//! The queue checker sends a request to the mempool whenever a transaction
//! verification finishes, and periodically as a fallback, so that newly
//! verified transactions are added to the mempool, and gossiped to peers.
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
pub struct QueueChecker<Mempool> {
    /// The mempool service that receives crawled transaction IDs.
    mempool: Mempool,

    /// Signalled after each mempool transaction verification finishes.
    verification_finished: Arc<Notify>,
}

impl<Mempool> QueueChecker<Mempool>
where
    Mempool:
        Service<mempool::Request, Response = mempool::Response, Error = BoxError> + Send + 'static,
    Mempool::Future: Send,
{
    /// Spawn an asynchronous task to run the mempool queue checker.
    ///
    /// `verification_finished` must be the signal returned by the mempool's
    /// `verification_finished` method.
    pub fn spawn(
        mempool: Mempool,
        verification_finished: Arc<Notify>,
    ) -> JoinHandle<Result<(), BoxError>> {
        let queue_checker = QueueChecker {
            mempool,
            verification_finished,
        };

        spawn_erased(queue_checker.run().in_current_span())
    }

    /// Check if the mempool has newly verified transactions, after each
    /// verification finishes, or after [`RATE_LIMIT_DELAY`] at the latest.
    ///
    /// Runs until the mempool returns an error,
    /// which happens when Zebra is shutting down.
    pub async fn run(mut self) -> Result<(), BoxError> {
        info!("initializing mempool queue checker task");

        loop {
            // `Notify` keeps one permit, so signals sent during a check
            // start one more check rather than being lost.
            tokio::select! {
                _ = self.verification_finished.notified() => {}
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
    use zakura_test::mock_service::MockService;

    use super::*;

    /// A finished verification starts a queue check well before the periodic
    /// fallback, so `sendrawtransaction` callers don't wait for it.
    #[tokio::test]
    async fn verification_finished_starts_queue_check() {
        let _init_guard = zakura_test::init();

        let mut mempool = MockService::build().for_unit_tests();
        let verification_finished = Arc::new(Notify::new());
        let _queue_checker = QueueChecker::spawn(mempool.clone(), verification_finished.clone());

        mempool.expect_no_requests().await;
        verification_finished.notify_one();

        // The mock waits far less than `RATE_LIMIT_DELAY` for this request.
        mempool
            .expect_request(mempool::Request::CheckForVerifiedTransactions)
            .await
            .respond(mempool::Response::CheckedForVerifiedTransactions);
    }
}
