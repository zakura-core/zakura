//! A node that runs one layout, two siblings, and a real handler over QUIC.

use std::sync::{Arc, Mutex, PoisonError};

use tokio_util::task::AbortOnDropHandle;
use zakura_chain::parameters::Network;
use zakura_quic::{NodeAddr, QuicEndpoint};

use super::{
    service::LayoutShared, LayoutService, LayoutSession, SiblingService, StreamConformance,
    UpdateOp, CONFORMANCE_DEADLINE, SIBLINGS,
};
use crate::{
    zakura::{
        handler::serve_native_dial_connection, testkit::LocalEndpointFactory, Service,
        ServiceRegistry, Stream, ZakuraEndpoint, ZakuraHandshakeConfig, ZakuraLocalLimits,
        ZakuraPeerId, ZakuraProtocolHandler, ZakuraSupervisorHandle,
    },
    BoxError, Config,
};

/// A node with the layout service and two siblings.
#[derive(Debug)]
pub(crate) struct LayoutNode<A: StreamConformance> {
    endpoint: QuicEndpoint,
    zakura: ZakuraEndpoint,
    pub(crate) service: Arc<LayoutService<A>>,
    pub(crate) siblings: [Arc<SiblingService>; 2],
    pub(crate) limits: ZakuraLocalLimits,
    dials: Mutex<Vec<AbortOnDropHandle<()>>>,
}

/// Production limits.
pub(crate) fn production_limits() -> ZakuraLocalLimits {
    ZakuraLocalLimits::from_config(&Config::default())
}

impl<A: StreamConformance> LayoutNode<A> {
    /// A node for `layout` with production limits.
    pub(crate) async fn spawn(seed: u64, layout: &'static [Stream]) -> Result<Self, BoxError> {
        Self::spawn_with_limits(seed, layout, production_limits()).await
    }

    /// A node for `layout` with `limits`.
    pub(crate) async fn spawn_with_limits(
        seed: u64,
        layout: &'static [Stream],
        limits: ZakuraLocalLimits,
    ) -> Result<Self, BoxError> {
        let endpoint = LocalEndpointFactory::with_limits(&limits)
            .endpoint(seed)
            .await?;
        let supervisor = ZakuraSupervisorHandle::new(16);
        let service = Arc::new(LayoutService::<A>::new(layout));
        let siblings = SIBLINGS.map(|stream| Arc::new(SiblingService::new(stream)));
        let services: Vec<Arc<dyn Service>> =
            vec![service.clone(), siblings[0].clone(), siblings[1].clone()];
        let handler = ZakuraProtocolHandler::new_with_registry(
            supervisor.clone(),
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            limits.clone(),
            Arc::new(ServiceRegistry::new(services)?),
        )
        .with_local_node_id(endpoint.local_id());
        endpoint.serve(handler.clone())?;
        Ok(Self {
            endpoint: endpoint.clone(),
            zakura: ZakuraEndpoint::from_parts(endpoint, supervisor, handler),
            service,
            siblings,
            limits,
            dials: Mutex::new(Vec::new()),
        })
    }

    /// This node's identity.
    pub(crate) fn id(&self) -> ZakuraPeerId {
        ZakuraPeerId::new(self.endpoint.local_id().as_bytes().to_vec())
            .expect("an endpoint id is a valid peer id")
    }

    /// This node's address.
    pub(crate) async fn addr(&self) -> NodeAddr {
        LocalEndpointFactory::node_addr(&self.endpoint).await
    }

    /// The layout service's shared state.
    pub(crate) fn shared(&self) -> &LayoutShared<A> {
        &self.service.shared
    }

    /// Dial `other` once, then wait until both nodes hold a layout session and
    /// both sibling sessions with each other.
    pub(crate) async fn connect<B: StreamConformance>(
        &self,
        other: &LayoutNode<B>,
    ) -> Result<(), BoxError> {
        let addr = other.addr().await;
        let zakura = self.zakura.clone();
        let limits = self.limits.clone();
        let dial = tokio::spawn(async move {
            let _ = serve_native_dial_connection(&zakura, addr, &limits).await;
        });
        self.dials
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(AbortOnDropHandle::new(dial));
        self.wait_session(&other.id()).await?;
        other.wait_session(&self.id()).await?;
        for (mine, theirs) in self.siblings.iter().zip(&other.siblings) {
            mine.wait_session(&other.id()).await?;
            theirs.wait_session(&self.id()).await?;
        }
        Ok(())
    }

    /// Wait until this node holds a layout session with `peer`.
    pub(crate) async fn wait_session(
        &self,
        peer: &ZakuraPeerId,
    ) -> Result<Arc<LayoutSession<A>>, BoxError> {
        self.wait_session_after(peer, None).await
    }

    /// Wait until this node holds a layout session with `peer` other than
    /// `old`.
    pub(crate) async fn wait_session_after(
        &self,
        peer: &ZakuraPeerId,
        old: Option<u64>,
    ) -> Result<Arc<LayoutSession<A>>, BoxError> {
        let table = &self.shared().table;
        let mut changed = table.subscribe();
        tokio::time::timeout(CONFORMANCE_DEADLINE, async {
            loop {
                changed.borrow_and_update();
                if let Some((key, session)) = table.get(peer) {
                    if Some(key.session_id) != old {
                        return Ok(session);
                    }
                }
                changed.changed().await?;
            }
        })
        .await
        .map_err(|_| "timed out waiting for a layout session")?
    }

    /// Download `exchanges` of the layout's first request row and `pages`
    /// pages of a subscription on its first subscription row from `peer`, at
    /// once.
    pub(crate) async fn exchange(
        &self,
        peer: &ZakuraPeerId,
        exchanges: impl IntoIterator<Item = u32>,
        pages: u32,
    ) -> Result<(), BoxError> {
        tokio::try_join!(self.download(peer, exchanges), self.subscribe(peer, pages)).map(drop)
    }

    /// Download `exchanges` of the layout's first request row from `peer`,
    /// keeping at most `max_in_flight` open and sending the next request as
    /// soon as an ending arrives. Does nothing if the layout has no request
    /// row.
    pub(crate) async fn download(
        &self,
        peer: &ZakuraPeerId,
        exchanges: impl IntoIterator<Item = u32>,
    ) -> Result<(), BoxError> {
        let Some(&plan) = self.shared().plan.requests.first() else {
            return Ok(());
        };
        let session = self.wait_session(peer).await?;
        let limit = usize::try_from(plan.max_in_flight).expect("a small limit fits usize");
        let mut ended = session.ended.subscribe();
        let before = ended.borrow().len();
        let mut sent = 0usize;
        for exchange in exchanges {
            let needed = (sent + 1).saturating_sub(limit);
            tokio::time::timeout(
                CONFORMANCE_DEADLINE,
                ended.wait_for(|ended| ended.len() - before >= needed),
            )
            .await
            .map_err(|_| "timed out waiting for an ending")??;
            self.service
                .request(peer, plan.row.message_type, exchange)
                .await?;
            sent += 1;
        }
        tokio::time::timeout(
            CONFORMANCE_DEADLINE,
            ended.wait_for(|ended| ended.len() - before >= sent),
        )
        .await
        .map_err(|_| "timed out waiting for the last endings")??;
        Ok(())
    }

    /// Subscribe to `peer` on the layout's first subscription row with the
    /// row's whole window, and accept `pages` pages, renewing credit as it
    /// frees. Then close the subscription and wait for its ending. Does
    /// nothing if the layout has no subscription row.
    ///
    /// See [`LayoutSession::subscription_progress`] for when a grant is due.
    pub(crate) async fn subscribe(&self, peer: &ZakuraPeerId, pages: u32) -> Result<(), BoxError> {
        let shared = self.shared();
        let Some(&plan) = shared.plan.subscriptions.first() else {
            return Ok(());
        };
        let session = self.wait_session(peer).await?;
        let row = plan.row.message_type;
        let key = session.next_key();
        let mut accepted = session.pages.subscribe();
        let mut ended = session.subscriptions_ended.subscribe();
        let before = ended.borrow().len();
        self.service
            .subscribe(peer, row, key, plan.limits.credit)
            .await?;
        loop {
            // Mark the pages seen before reading the progress, so a page that
            // arrives after the read wakes the wait below.
            accepted.borrow_and_update();
            let (received, due) = session
                .subscription_progress(shared, row, key)
                .ok_or("the subscription ended before its pages")?;
            if received >= pages {
                break;
            }
            if let Some(added) = due {
                self.service
                    .update(peer, row, key, UpdateOp::Grant, |subscriptions| {
                        subscriptions.grant(&key, added)
                    })
                    .await?;
            }
            tokio::time::timeout(CONFORMANCE_DEADLINE, accepted.changed())
                .await
                .map_err(|_| "timed out waiting for a page")??;
        }
        self.service
            .update(peer, row, key, UpdateOp::Close, |subscriptions| {
                subscriptions.close(&key)
            })
            .await?;
        tokio::time::timeout(
            CONFORMANCE_DEADLINE,
            ended.wait_for(|ended| ended[before..].contains(&(row, key))),
        )
        .await
        .map_err(|_| "timed out waiting for the subscription's ending")??;
        Ok(())
    }

    /// Whether this node still has a connection registered for `peer`.
    pub(crate) fn connected(&self, peer: &ZakuraPeerId) -> bool {
        self.zakura.supervisor().subscribe().borrow().contains(peer)
    }

    /// Wait until this node has no connection registered for `peer`.
    pub(crate) async fn wait_disconnected(&self, peer: &ZakuraPeerId) -> Result<(), BoxError> {
        let mut peers = self.zakura.supervisor().subscribe();
        tokio::time::timeout(
            CONFORMANCE_DEADLINE,
            peers.wait_for(|peers| !peers.contains(peer)),
        )
        .await
        .map_err(|_| "timed out waiting for the disconnect")??;
        Ok(())
    }

    /// Shut the node down.
    pub(crate) async fn shutdown(self) {
        self.dials
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        self.zakura.shutdown().await;
    }
}
