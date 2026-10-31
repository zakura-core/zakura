//! Owns the peer set and drives connection maintenance independently of requests.

use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::{
    future::{poll_fn, BoxFuture},
    FutureExt,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::{CancellationToken, PollSender};
use tower::{discover::Discover, util::BoxCloneService, Service};
use zakura_chain::chain_tip::ChainTip;

use super::PeerSet;
use crate::{
    constants::PEERSET_BUFFER_SIZE,
    peer::{block_gossip::BlockAdvertiser, LoadTrackedClient},
    BoxError, PeerSocketAddr, Request, Response,
};

#[derive(Clone, Debug, thiserror::Error)]
#[error("peer set stopped: {0}")]
struct DriverError(#[source] Arc<dyn std::error::Error + Send + Sync>);

type Reply = oneshot::Sender<BoxFuture<'static, Result<Response, BoxError>>>;

struct Dispatch {
    request: Request,
    reply: Reply,
    span: tracing::Span,
}

#[derive(Clone)]
struct Client {
    sender: PollSender<Dispatch>,
    error: Arc<Mutex<Option<DriverError>>>,
}

impl Client {
    fn closed(&self) -> BoxError {
        self.error
            .lock()
            .expect("driver error lock is never poisoned")
            .clone()
            .map(|error| Box::new(error) as BoxError)
            .unwrap_or_else(|| "peer set driver closed".into())
    }
}

impl Service<Request> for Client {
    type Response = Response;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Response, BoxError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        self.sender.poll_reserve(cx).map_err(|_| self.closed())
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let (reply, response) = oneshot::channel();
        if self
            .sender
            .send_item(Dispatch {
                request,
                reply,
                span: tracing::Span::current(),
            })
            .is_err()
        {
            return futures::future::ready(Err(self.closed())).boxed();
        }
        let error = self.error.clone();
        async move {
            let future = response.await.map_err(|_| {
                error
                    .lock()
                    .expect("driver error lock is never poisoned")
                    .clone()
                    .map(|error| Box::new(error) as BoxError)
                    .unwrap_or_else(|| "peer set driver closed".into())
            })?;
            future.await
        }
        .boxed()
    }
}

/// Starts the sole peer-set owner. Snapshot requests are serviced independently of ordinary
/// readiness, after processing discovery. Response futures remain owned by their callers.
pub(super) fn spawn<D, C>(
    mut peers: PeerSet<D, C>,
    shutdown: CancellationToken,
) -> (
    BoxCloneService<Request, Response, BoxError>,
    BlockAdvertiser<C>,
)
where
    D: Discover<Key = PeerSocketAddr, Service = LoadTrackedClient> + Unpin + Send + 'static,
    D::Error: Into<BoxError>,
    C: ChainTip + Clone + Send + Sync + 'static,
{
    let (requests, mut incoming) = mpsc::channel::<Dispatch>(PEERSET_BUFFER_SIZE);
    let (refresh, mut refreshes) = mpsc::channel::<oneshot::Sender<()>>(PEERSET_BUFFER_SIZE);
    let mut advertiser = peers.block_advertiser();
    advertiser.refresh = Some(refresh);
    let (bans, mut tip) = peers.authorization();
    let mut bans_changed = bans.subscribe();
    let error = Arc::new(Mutex::new(None));
    let client = Client {
        sender: PollSender::new(requests),
        error: error.clone(),
    };
    tokio::spawn(async move {
        let mut pending: Option<Dispatch> = None;
        let result: Result<(), BoxError> = async {
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    changed = bans_changed.changed() => { changed?; }
                    changed = tip.best_tip_changed() => { changed?; }
                    running = poll_fn(|cx| {
                        peers.maintain(cx)?;
                        // Bounded channel polling participates in Tokio's cooperative budget.
                        while let Poll::Ready(Some(reply)) = refreshes.poll_recv(cx) {
                            let _ = reply.send(());
                        }
                        if pending.is_none() {
                            match incoming.poll_recv(cx) {
                                Poll::Ready(Some(request)) => pending = Some(request),
                                Poll::Ready(None) => return Poll::Ready(Ok(false)),
                                Poll::Pending => return Poll::Pending,
                            }
                        }
                        let request = pending.as_mut().expect("a pending request was just installed");
                        if Pin::new(&mut request.reply).poll_closed(cx).is_ready() {
                            pending.take();
                            return Poll::Ready(Ok(true));
                        }
                        futures::ready!(peers.poll_ready(cx))?;
                        let request = pending.take().expect("readiness does not remove the request");
                        let response = request.span.in_scope(|| peers.call(request.request));
                        let _ = request.reply.send(response);
                        Poll::Ready(Ok(true))
                    }) => { if !running? { break; } }
                }
            }
            Ok(())
        }.await;
        let cause = result.err().unwrap_or_else(|| "peer set shut down".into());
        *error.lock().expect("driver error lock is never poisoned") =
            Some(DriverError(Arc::from(cause)));
        // Closing queues and dropping the peer set cancels peers and wakes all waiters.
    });
    (BoxCloneService::new(client), advertiser)
}
