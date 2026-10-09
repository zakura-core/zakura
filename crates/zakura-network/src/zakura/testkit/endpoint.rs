//! Loopback `zakura-quic` endpoints for Zakura tests.

use std::net::{Ipv4Addr, SocketAddr};

use blake2b_simd::Params as Blake2bParams;
use zakura_quic::{NodeAddr, NodeSecretKey, QuicBindConfig, QuicConfig, QuicEndpoint};

use crate::{zakura::ZakuraLocalLimits, BoxError};

/// Bidirectional stream limit for endpoints built without Zakura limits.
const DEFAULT_TEST_MAX_BIDI_STREAMS: u32 = 100;

/// Factory for deterministic loopback endpoints with production transport
/// settings (zakura-quic API-8).
#[derive(Clone, Debug)]
pub struct LocalEndpointFactory {
    quic: QuicConfig,
    max_bidi_streams: u32,
    max_connections: usize,
    max_inbound_connections: usize,
    max_draining_connections: usize,
}

impl LocalEndpointFactory {
    /// Create a factory with the default transport settings.
    pub fn new() -> Self {
        Self::with_limits(&ZakuraLocalLimits::from_config(&crate::Config::default()))
            .max_bidi_streams(DEFAULT_TEST_MAX_BIDI_STREAMS)
    }

    /// Create a factory with the transport settings a node with `limits` uses.
    pub fn with_limits(limits: &ZakuraLocalLimits) -> Self {
        let bind = limits.quic_bind_config(Vec::new());
        Self {
            quic: limits.quic.clone(),
            max_bidi_streams: u32::from(limits.max_open_streams),
            max_connections: limits.max_connections,
            max_inbound_connections: bind.max_inbound_connections,
            max_draining_connections: bind.max_draining_connections,
        }
    }

    /// Override the transport settings.
    pub fn quic_config(mut self, quic: QuicConfig) -> Self {
        self.quic = quic;
        self
    }

    /// Override the peer's bidirectional stream limit.
    pub fn max_bidi_streams(mut self, max_bidi_streams: u32) -> Self {
        self.max_bidi_streams = max_bidi_streams;
        self
    }

    /// Deterministically derive a node secret key from a small seed.
    pub fn secret_key(seed: u64) -> NodeSecretKey {
        let mut seed_bytes = [0; 8];
        seed_bytes.copy_from_slice(&seed.to_le_bytes());
        let digest = Blake2bParams::new()
            .hash_length(32)
            .personal(b"zakura-test-key")
            .to_state()
            .update(&seed_bytes)
            .finalize();
        let mut key_bytes = [0; 32];
        key_bytes.copy_from_slice(digest.as_bytes());
        NodeSecretKey::from_bytes(&key_bytes)
    }

    /// Bind an endpoint to an OS-assigned loopback port.
    pub async fn endpoint(self, seed: u64) -> Result<QuicEndpoint, BoxError> {
        let bind = QuicBindConfig {
            addrs: vec![SocketAddr::from((Ipv4Addr::LOCALHOST, 0))],
            max_bidi_streams: self.max_bidi_streams,
            max_connections: self.max_connections,
            max_inbound_connections: self.max_inbound_connections,
            max_draining_connections: self.max_draining_connections,
        };
        Ok(QuicEndpoint::bind(
            Self::secret_key(seed),
            &bind,
            &self.quic,
        )?)
    }

    /// Return the endpoint's direct node address.
    pub async fn node_addr(endpoint: &QuicEndpoint) -> NodeAddr {
        NodeAddr::with_addrs(endpoint.local_id(), endpoint.local_addrs())
    }
}

impl Default for LocalEndpointFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use futures::future::BoxFuture;
    use zakura_quic::{Acceptor, Admit, Conn, IncomingInfo};

    use super::*;

    const ALPN: &[u8] = b"/zakura/testkit/noop/0";

    #[derive(Debug, Clone)]
    struct Noop;

    impl Acceptor for Noop {
        fn admit(&self, _incoming: &IncomingInfo) -> Admit {
            Admit::Accept
        }

        fn alpns(&self) -> Vec<Vec<u8>> {
            vec![ALPN.to_vec()]
        }

        fn handle(&self, _conn: Conn) -> BoxFuture<'static, ()> {
            Box::pin(async {})
        }
    }

    #[tokio::test]
    async fn endpoint_factory_is_deterministic_and_loopback_only() -> Result<(), BoxError> {
        let key_a = LocalEndpointFactory::secret_key(1);
        let key_b = LocalEndpointFactory::secret_key(1);
        assert_eq!(key_a.public(), key_b.public());

        let endpoint = LocalEndpointFactory::new().endpoint(1).await?;
        let node_addr = LocalEndpointFactory::node_addr(&endpoint).await;

        assert_eq!(node_addr.id, endpoint.local_id());
        assert_eq!(node_addr.direct.len(), 1);
        assert!(node_addr.direct[0].ip().is_loopback());

        endpoint.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn factory_endpoints_connect_over_loopback() -> Result<(), BoxError> {
        let server = LocalEndpointFactory::new().endpoint(10).await?;
        server.serve(Noop)?;
        let client = LocalEndpointFactory::new().endpoint(11).await?;
        let server_addr = LocalEndpointFactory::node_addr(&server).await;

        let connection = client.connect(server_addr, ALPN).await?;
        assert_eq!(connection.remote_id(), server.local_id());

        connection.close(0u32.into(), b"done");
        client.shutdown().await;
        server.shutdown().await;
        Ok(())
    }
}
