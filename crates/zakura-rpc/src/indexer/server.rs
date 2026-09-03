//! A tonic RPC server for Zebra's indexer API.

use std::{
    fs, io,
    net::SocketAddr,
    path::Path,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures::{stream, Stream, StreamExt};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
};
use tonic::transport::{
    server::{Connected, TcpConnectInfo, TcpIncoming},
    Certificate, Identity, Server, ServerTlsConfig,
};
use tower::BoxError;
use zakura_chain::chain_tip::ChainTip;
use zakura_node_services::mempool::MempoolTxSubscriber;
use zakura_state::ReadState;

use crate::{
    config::rpc::IndexerTlsConfig, indexer::indexer_server::IndexerServer,
    server::OPENED_RPC_ENDPOINT_MSG,
};

type ServerTask = JoinHandle<Result<(), BoxError>>;

/// Maximum number of TCP connections accepted by the indexer RPC server.
const MAX_CONNECTIONS: usize = 64;

/// Maximum number of concurrent HTTP/2 streams on each connection.
const MAX_CONCURRENT_STREAMS_PER_CONNECTION: u32 = 64;

/// Maximum time an unauthenticated connection may spend negotiating TLS.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// A TCP connection that retains one server-wide connection permit.
#[derive(Debug)]
struct LimitedTcpStream {
    inner: TcpStream,
    _permit: OwnedSemaphorePermit,
}

impl AsyncRead for LimitedTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for LimitedTcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl Connected for LimitedTcpStream {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.inner.connect_info()
    }
}

/// Indexer RPC service.
pub struct IndexerRPC<ReadStateService, Tip>
where
    ReadStateService: ReadState,
    Tip: ChainTip + Clone + Send + Sync + 'static,
{
    pub(super) read_state: ReadStateService,
    pub(super) chain_tip_change: Tip,
    pub(super) mempool_change: MempoolTxSubscriber,
}

/// Initializes the indexer RPC server
#[tracing::instrument(skip_all)]
pub async fn init<ReadStateService, Tip>(
    listen_addr: SocketAddr,
    tls: Option<IndexerTlsConfig>,
    read_state: ReadStateService,
    chain_tip_change: Tip,
    mempool_change: MempoolTxSubscriber,
) -> Result<(ServerTask, SocketAddr), BoxError>
where
    ReadStateService: ReadState,
    Tip: ChainTip + Clone + Send + Sync + 'static,
{
    validate_transport(listen_addr, tls.as_ref())?;

    let tls = tls.map(load_mtls_config).transpose()?;
    let indexer_service = IndexerRPC {
        read_state,
        chain_tip_change,
        mempool_change,
    };

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(crate::indexer::FILE_DESCRIPTOR_SET)
        .build_v1()
        .unwrap();

    let request_limit = usize::try_from(MAX_CONCURRENT_STREAMS_PER_CONNECTION)
        .expect("the small HTTP/2 stream limit fits in usize");
    let mut server = Server::builder()
        .concurrency_limit_per_connection(request_limit)
        .max_concurrent_streams(MAX_CONCURRENT_STREAMS_PER_CONNECTION);
    if let Some(tls) = tls {
        server = server.tls_config(tls)?;
    }

    tracing::info!("Trying to open indexer RPC endpoint at {}...", listen_addr,);

    let tcp_listener = tokio::net::TcpListener::bind(listen_addr).await?;

    let listen_addr = tcp_listener.local_addr()?;
    tracing::info!("{OPENED_RPC_ENDPOINT_MSG}{}", listen_addr);

    let incoming = limited_tcp_incoming(tcp_listener, MAX_CONNECTIONS);
    let server_task: JoinHandle<Result<(), BoxError>> = tokio::spawn(async move {
        server
            .add_service(reflection_service)
            .add_service(IndexerServer::new(indexer_service))
            .serve_with_incoming(incoming)
            .await?;

        Ok(())
    });

    Ok((server_task, listen_addr))
}

/// Limits accepted TCP connections until an earlier connection closes.
fn limited_tcp_incoming(
    listener: TcpListener,
    max_connections: usize,
) -> impl Stream<Item = io::Result<LimitedTcpStream>> {
    let incoming = TcpIncoming::from(listener);
    let connection_permits = Arc::new(Semaphore::new(max_connections));

    stream::unfold(
        (incoming, connection_permits),
        |(mut incoming, connection_permits)| async move {
            let permit = connection_permits
                .clone()
                .acquire_owned()
                .await
                .expect("the connection semaphore is never closed");
            let connection = incoming.next().await?;
            let connection = connection.map(|inner| LimitedTcpStream {
                inner,
                _permit: permit,
            });

            Some((connection, (incoming, connection_permits)))
        },
    )
}

/// Rejects remotely reachable plaintext indexer listeners.
fn validate_transport(
    listen_addr: SocketAddr,
    tls: Option<&IndexerTlsConfig>,
) -> Result<(), BoxError> {
    if !listen_addr.ip().is_loopback() && tls.is_none() {
        return Err(
            "plaintext indexer RPC listeners are restricted to loopback addresses; configure \
             rpc.indexer_tls for a non-loopback address"
                .into(),
        );
    }

    Ok(())
}

/// Loads the server identity and client trust root for indexer mTLS.
fn load_mtls_config(tls: IndexerTlsConfig) -> Result<ServerTlsConfig, BoxError> {
    install_tls_crypto_provider();

    let cert = read_tls_file(&tls.cert_file, "server certificate")?;
    let key = read_tls_file(&tls.key_file, "server private key")?;
    let client_ca = read_tls_file(&tls.client_ca_file, "client CA certificate")?;

    Ok(ServerTlsConfig::new()
        .identity(Identity::from_pem(cert, key))
        .client_ca_root(Certificate::from_pem(client_ca))
        .timeout(TLS_HANDSHAKE_TIMEOUT))
}

/// Ensures tonic TLS connections have a rustls crypto provider.
pub(crate) fn install_tls_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Reads an indexer TLS file with its role included in any error.
pub(crate) fn read_tls_file(path: &Path, role: &str) -> Result<Vec<u8>, BoxError> {
    fs::read(path).map_err(|error| {
        std::io::Error::new(
            error.kind(),
            format!(
                "could not read indexer RPC TLS {role} file {}: {error}",
                path.display()
            ),
        )
        .into()
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use tokio::net::{TcpListener, TcpStream};

    use super::{limited_tcp_incoming, validate_transport};

    #[tokio::test]
    async fn limits_accepted_connections() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let listen_addr = listener
            .local_addr()
            .expect("bound listener should have an address");
        let mut incoming = Box::pin(limited_tcp_incoming(listener, 1));

        let _first_client = TcpStream::connect(listen_addr)
            .await
            .expect("first test client should connect");
        let first_server = incoming
            .next()
            .await
            .expect("listener should remain open")
            .expect("listener should accept the first connection");

        let _second_client = TcpStream::connect(listen_addr)
            .await
            .expect("second test client should connect to the socket backlog");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), incoming.next())
                .await
                .is_err(),
            "a second connection was accepted before a permit was available"
        );

        drop(first_server);
        tokio::time::timeout(Duration::from_secs(5), incoming.next())
            .await
            .expect("a permit should become available")
            .expect("listener should remain open")
            .expect("listener should accept the second connection");
    }

    #[test]
    fn rejects_non_loopback_plaintext_listener() {
        let address = "0.0.0.0:8230"
            .parse()
            .expect("hard-coded socket address should parse");

        let error = validate_transport(address, None)
            .expect_err("non-loopback plaintext listeners must be rejected");

        assert!(error.to_string().contains("restricted to loopback"));
    }
}
