//! The trusted broadcast's readiness, commitment, failure, and deadline boundaries.
use super::*;
use futures::{future::BoxFuture, FutureExt};
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::sync::oneshot;
use zakura_chain::serialization::ZcashSerialize;
use zakura_rpc::PendingBlockRegistry;

struct HeldNetwork {
    ready: Option<oneshot::Receiver<()>>,
    response: Option<oneshot::Receiver<zn::Response>>,
    calls: Arc<AtomicUsize>,
}
impl Service<zn::Request> for HeldNetwork {
    type Response = zn::Response;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        if let Some(ready) = self.ready.as_mut() {
            std::task::ready!(std::future::Future::poll(Pin::new(ready), cx)).unwrap();
            self.ready = None;
        }
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: zn::Request) -> Self::Future {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let response = self.response.take().unwrap();
        async move { Ok(response.await?) }.boxed()
    }
}

#[tokio::test(start_paused = true)]
async fn resolution_before_readiness_never_dispatches_trusted_inventory() {
    for committed in [false, true] {
        let registry = PendingBlockRegistry::default();
        let block = zakura_chain::block::genesis::regtest_genesis_block();
        let registration = registry.insert(block.clone()).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let (_ready, waiting) = oneshot::channel();
        let (_response, received) = oneshot::channel();
        let future = broadcast_trusted_with_timeout(
            HeldNetwork {
                ready: Some(waiting),
                response: Some(received),
                calls: calls.clone(),
            },
            zn::Request::AdvertiseMinedBlock(zn::AdvertisedBlock::new(
                block.hash(),
                block.zcash_serialized_size(),
            )),
            registration.signal(),
        );
        tokio::pin!(future);
        assert!(future.as_mut().now_or_never().is_none());
        registration.resolve(if committed { Ok(block) } else { Err(()) });
        assert!(!future.await);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test(start_paused = true)]
async fn commit_keeps_dispatched_trusted_work_but_rejection_and_timeout_cancel_it() {
    for outcome in ["commit", "rejection", "timeout", "relay failure"] {
        let registry = PendingBlockRegistry::default();
        let block = zakura_chain::block::genesis::regtest_genesis_block();
        let registration = registry.insert(block.clone()).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let (response, received) = oneshot::channel();
        let future = broadcast_trusted_with_timeout(
            HeldNetwork {
                ready: None,
                response: Some(received),
                calls: calls.clone(),
            },
            zn::Request::AdvertiseMinedBlock(zn::AdvertisedBlock::new(
                block.hash(),
                block.zcash_serialized_size(),
            )),
            registration.signal(),
        );
        tokio::pin!(future);
        assert!(future.as_mut().now_or_never().is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(registration.signal().was_dispatched());
        match outcome {
            "commit" => {
                registration.resolve(Ok(block));
                assert!(future.as_mut().now_or_never().is_none());
                response.send(zn::Response::Nil).unwrap();
                assert!(future.await);
            }
            "rejection" => {
                registration.resolve(Err(()));
                assert!(!future.await);
                assert!(response.is_closed());
            }
            "timeout" => {
                tokio::time::advance(BLOCK_GOSSIP_TIMEOUT).await;
                assert!(!future.await);
                assert!(response.is_closed());
            }
            _ => {
                drop(response);
                assert!(!future.await);
            }
        }
    }
}
