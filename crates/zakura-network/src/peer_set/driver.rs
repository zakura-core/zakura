//! Owns the peer set and drives connection maintenance independently of requests.

use std::{
    sync::Arc,
    task::{Context, Poll},
};

use futures::{
    future::{poll_fn, BoxFuture},
    FutureExt,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::{CancellationToken, PollSender};
use tower::{discover::Discover, util::BoxCloneSyncService, Service};
use tracing::debug;
use zakura_chain::chain_tip::ChainTip;

use super::PeerSet;
use crate::{
    constants::PEERSET_BUFFER_SIZE,
    peer::{block_gossip::BlockAdvertiser, LoadTrackedClient, MinimumPeerVersion},
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
    error: watch::Receiver<Option<DriverError>>,
}

impl Client {
    fn closed(&self) -> BoxError {
        self.error
            .borrow()
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
        if self.error.borrow().is_some() {
            return Poll::Ready(Err(self.closed()));
        }
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
        let mut error = self.error.clone();
        async move {
            let closed = |error: &watch::Receiver<Option<DriverError>>| -> BoxError {
                error
                    .borrow()
                    .clone()
                    .map(|error| Box::new(error) as BoxError)
                    .unwrap_or_else(|| "peer set driver closed".into())
            };
            if error.borrow().is_some() {
                return Err(closed(&error));
            }
            // A reserved permit may send after the receiver closes. Watch terminal state as
            // well as the reply so that such a request cannot wait on an abandoned queue.
            let future = tokio::select! {
                biased;
                _ = error.changed() => return Err(closed(&error)),
                response = response => response.map_err(|_| closed(&error))?,
            };
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
    BoxCloneSyncService<Request, Response, BoxError>,
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
    let (bans, minimum) = peers.authorization();
    let mut bans_changed = bans.subscribe();
    let mut version_changed = watch_minimum_version(minimum, shutdown.clone());
    // Once the version watcher ends, the last minimum version stays in force.
    let mut version_watch_open = true;
    let (failed, error) = watch::channel(None);
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
                    changed = version_changed.changed(), if version_watch_open => {
                        version_watch_open = changed.is_ok();
                    }
                    running = poll_fn(|cx| {
                        peers.maintain(cx)?;
                        // Bounded channel polling participates in Tokio's cooperative budget.
                        while let Poll::Ready(Some(reply)) = refreshes.poll_recv(cx) {
                            // Discovery may have arrived between the first maintenance pass and
                            // this refresh. Process it after receiving the barrier, before acking.
                            peers.maintain(cx)?;
                            peers.demand_peers_if_none_advertisable();
                            let _ = reply.send(());
                        }
                        if pending.is_none() {
                            match incoming.poll_recv(cx) {
                                Poll::Ready(Some(request)) => pending = Some(request),
                                Poll::Ready(None) => return Poll::Ready(Ok::<_, BoxError>(false)),
                                Poll::Pending => return Poll::Pending,
                            }
                        }
                        let request = pending.as_mut().expect("a pending request was just installed");
                        if request.reply.poll_closed(cx).is_ready() {
                            pending.take();
                            return Poll::Ready(Ok(true));
                        }
                        // Maintenance already ran in this poll, so only check routing readiness.
                        futures::ready!(peers.poll_routing_readiness(cx))?;
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
        failed.send_replace(Some(DriverError(Arc::from(cause))));
        // Closing queues and dropping the peer set cancels peers and wakes all waiters.
    });
    (BoxCloneSyncService::new(client), advertiser)
}

/// Reports minimum peer version changes on the returned channel, from a task of its own.
///
/// Every block changes the chain tip, but maintenance only needs to rerun when the minimum peer
/// version changes, so the owner must not wake on each block. The task ends on shutdown, or once
/// the chain tip can no longer change, which leaves the last minimum version in force.
fn watch_minimum_version<C>(
    mut minimum: MinimumPeerVersion<C>,
    shutdown: CancellationToken,
) -> watch::Receiver<()>
where
    C: ChainTip + Clone + Send + Sync + 'static,
{
    let (changed_tx, changed_rx) = watch::channel(());
    // A cloned tracker reports the current version as a change; mark it seen so that only later
    // version changes are reported.
    let _ = minimum.changed();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                _ = changed_tx.closed() => break,
                changed = minimum.wait_for_change() => match changed {
                    Ok(version) => {
                        debug!(?version, "minimum peer version changed");
                        changed_tx.send_replace(());
                    }
                    Err(error) => {
                        debug!(%error, "chain tip watch closed, keeping the last minimum peer version");
                        break;
                    }
                },
            }
        }
    });
    changed_rx
}
