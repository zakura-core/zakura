//! A tonic RPC server for Zebra's indexer API.

use std::{fs, net::SocketAddr, path::Path};

use tokio::task::JoinHandle;
use tonic::transport::{server::TcpIncoming, Certificate, Identity, Server, ServerTlsConfig};
use tower::BoxError;
use zakura_chain::chain_tip::ChainTip;
use zakura_node_services::mempool::MempoolTxSubscriber;
use zakura_state::ReadState;

use crate::{
    config::rpc::IndexerTlsConfig, indexer::indexer_server::IndexerServer,
    server::OPENED_RPC_ENDPOINT_MSG,
};

type ServerTask = JoinHandle<Result<(), BoxError>>;

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

    let mut server = Server::builder();
    if let Some(tls) = tls {
        server = server.tls_config(tls)?;
    }

    tracing::info!("Trying to open indexer RPC endpoint at {}...", listen_addr,);

    let tcp_listener = tokio::net::TcpListener::bind(listen_addr).await?;

    let listen_addr = tcp_listener.local_addr()?;
    tracing::info!("{OPENED_RPC_ENDPOINT_MSG}{}", listen_addr);

    let server_task: JoinHandle<Result<(), BoxError>> = tokio::spawn(async move {
        server
            .add_service(reflection_service)
            .add_service(IndexerServer::new(indexer_service))
            .serve_with_incoming(TcpIncoming::from(tcp_listener))
            .await?;

        Ok(())
    });

    Ok((server_task, listen_addr))
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
        .client_ca_root(Certificate::from_pem(client_ca)))
}

/// Ensures tonic TLS connections have a rustls crypto provider.
pub(crate) fn install_tls_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
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
    use super::validate_transport;

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
