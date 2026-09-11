//! A JSON-RPC 1.0 & 2.0 endpoint for Zebra.
//!
//! This endpoint is compatible with clients that incorrectly send
//! `"jsonrpc" = 1.0` fields in JSON-RPC 1.0 requests,
//! such as `lightwalletd`.
//!
//! See the full list of
//! [Differences between JSON-RPC 1.0 and 2.0.](https://www.simple-is-better.org/rpc/#differences-between-1-0-and-2-0)

use std::{collections::BTreeSet, fmt, fs::File, io::Read, panic, path::Path, sync::Arc};

use chrono::{TimeZone, Utc};
use cookie::Cookie;
use der::{asn1::GeneralizedTime, Decode, Header, Reader, SliceReader, Tag};
use jsonrpsee::server::{
    middleware::rpc::RpcServiceBuilder, serve_with_graceful_shutdown, stop_channel, Server,
    ServerHandle,
};
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_rustls::{rustls::ServerConfig as RustlsServerConfig, TlsAcceptor};
use tracing::*;

use zakura_chain::{
    block::MAX_BLOCK_BYTES, chain_sync_status::ChainSyncStatus, chain_tip::ChainTip,
    parameters::Network,
};
use zakura_consensus::router::service_trait::BlockVerifierService;
use zakura_network::AddressBookPeers;
use zakura_node_services::mempool::MempoolService;
use zakura_state::{ReadState as ReadStateService, State as StateService};

use crate::{
    config,
    methods::{
        rpc_method_access, RpcAccess, RpcImpl, RpcServer as _, RpcSurface, RPC_METHOD_ACCESS,
    },
    server::{
        http_request_compatibility::HttpRequestMiddlewareLayer,
        rpc_call_compatibility::FixRpcResponseMiddleware, rpc_metrics::RpcMetricsMiddleware,
        rpc_tracing::RpcTracingMiddleware,
    },
};

pub mod cookie;
pub mod error;
pub mod http_request_compatibility;
pub mod rpc_call_compatibility;
pub mod rpc_metrics;
pub mod rpc_tracing;

#[cfg(test)]
mod tests;

/// Zebra RPC Server
#[derive(Clone)]
pub struct RpcServer {
    /// The RPC config.
    config: config::rpc::Config,

    /// The configured network.
    network: Network,

    /// Zebra's application version, with build metadata.
    build_version: String,

    /// A server handle used to shuts down the RPC server.
    close_handle: ServerHandle,
}

impl fmt::Debug for RpcServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcServer")
            .field("config", &self.config)
            .field("network", &self.network)
            .field("build_version", &self.build_version)
            .field(
                "close_handle",
                // TODO: when it stabilises, use std::any::type_name_of_val(&self.close_handle)
                &"ServerHandle",
            )
            .finish()
    }
}

/// The message to log when logging the RPC server's listen address
pub const OPENED_RPC_ENDPOINT_MSG: &str = "Opened RPC endpoint at ";

/// The message to log when logging the admin RPC server's listen address.
pub const OPENED_ADMIN_RPC_ENDPOINT_MSG: &str = "Opened admin RPC endpoint at ";

type ServerTask = JoinHandle<Result<(), tower::BoxError>>;

impl RpcServer {
    /// Starts the primary RPC server.
    ///
    /// Authenticated listeners and Regtest listeners expose the full method
    /// set. Unauthenticated Mainnet and Testnet listeners expose only the
    /// restricted compatibility method set.
    ///
    /// # Panics
    ///
    /// - If [`Config::listen_addr`](config::rpc::Config::listen_addr) is `None`.
    //
    // TODO:
    // - replace VersionString with semver::Version, and update the tests to provide valid versions
    #[allow(clippy::too_many_arguments)]
    pub async fn start<
        Mempool,
        State,
        ReadState,
        Tip,
        BlockVerifierRouter,
        SyncStatus,
        AddressBook,
    >(
        rpc: RpcImpl<Mempool, State, ReadState, Tip, AddressBook, BlockVerifierRouter, SyncStatus>,
        conf: config::rpc::Config,
    ) -> Result<ServerTask, tower::BoxError>
    where
        Mempool: MempoolService,
        State: StateService,
        ReadState: ReadStateService,
        Tip: ChainTip + Clone + Send + Sync + 'static,
        AddressBook: AddressBookPeers + Clone + Send + Sync + 'static,
        BlockVerifierRouter: BlockVerifierService,
        SyncStatus: ChainSyncStatus + Clone + Send + Sync + 'static,
    {
        conf.validate().map_err(std::io::Error::other)?;

        let surface = primary_rpc_surface(rpc.network(), conf.enable_cookie_auth);

        Self::start_inner(rpc, conf, surface, OPENED_RPC_ENDPOINT_MSG).await
    }

    /// Starts the loopback-only, cookie-authenticated admin RPC server.
    ///
    /// # Panics
    ///
    /// - If [`Config::admin_listen_addr`](config::rpc::Config::admin_listen_addr) is `None`.
    pub async fn start_admin<
        Mempool,
        State,
        ReadState,
        Tip,
        BlockVerifierRouter,
        SyncStatus,
        AddressBook,
    >(
        rpc: RpcImpl<Mempool, State, ReadState, Tip, AddressBook, BlockVerifierRouter, SyncStatus>,
        mut conf: config::rpc::Config,
    ) -> Result<ServerTask, tower::BoxError>
    where
        Mempool: MempoolService,
        State: StateService,
        ReadState: ReadStateService,
        Tip: ChainTip + Clone + Send + Sync + 'static,
        AddressBook: AddressBookPeers + Clone + Send + Sync + 'static,
        BlockVerifierRouter: BlockVerifierService,
        SyncStatus: ChainSyncStatus + Clone + Send + Sync + 'static,
    {
        conf.validate().map_err(std::io::Error::other)?;

        conf.listen_addr = Some(
            conf.admin_listen_addr
                .expect("caller should make sure admin_listen_addr is set"),
        );
        conf.admin_listen_addr = None;
        conf.enable_cookie_auth = true;
        conf.tls = None;

        Self::start_inner(rpc, conf, RpcSurface::Full, OPENED_ADMIN_RPC_ENDPOINT_MSG).await
    }

    /// Starts one RPC listener with the selected method surface.
    async fn start_inner<
        Mempool,
        State,
        ReadState,
        Tip,
        BlockVerifierRouter,
        SyncStatus,
        AddressBook,
    >(
        rpc: RpcImpl<Mempool, State, ReadState, Tip, AddressBook, BlockVerifierRouter, SyncStatus>,
        conf: config::rpc::Config,
        surface: RpcSurface,
        opened_endpoint_msg: &'static str,
    ) -> Result<ServerTask, tower::BoxError>
    where
        Mempool: MempoolService,
        State: StateService,
        ReadState: ReadStateService,
        Tip: ChainTip + Clone + Send + Sync + 'static,
        AddressBook: AddressBookPeers + Clone + Send + Sync + 'static,
        BlockVerifierRouter: BlockVerifierService,
        SyncStatus: ChainSyncStatus + Clone + Send + Sync + 'static,
    {
        let listen_addr = conf
            .listen_addr
            .expect("caller should make sure listen_addr is set");

        let rpc = rpc.with_rpc_surface(surface);
        let mut methods = rpc.into_rpc();
        configure_rpc_methods(&mut methods, surface)?;

        // The largest RPC request is submitblock, which sends a full block
        // as a hex string (2x MAX_BLOCK_BYTES) plus a small JSON-RPC wrapper.
        let max_request_body_size = (MAX_BLOCK_BYTES as usize) * 2 + 1024;

        let http_middleware_layer = if conf.enable_cookie_auth {
            let cookie = Cookie::default();
            cookie::write_to_disk(&cookie, &conf.cookie_dir, Some(&conf.cookie_file_name))
                .expect("Zakura must be able to write the auth cookie to the disk");
            HttpRequestMiddlewareLayer::new(Some(cookie), max_request_body_size)
        } else {
            HttpRequestMiddlewareLayer::new(None, max_request_body_size)
        };

        let http_middleware = tower::ServiceBuilder::new().layer(http_middleware_layer);

        let rpc_middleware = RpcServiceBuilder::new()
            .rpc_logger(1024)
            .layer_fn(FixRpcResponseMiddleware::new)
            .layer_fn(RpcMetricsMiddleware::new)
            .layer_fn(RpcTracingMiddleware::new);

        if let Some(tls) = conf.tls.clone() {
            let tls_config = load_tls_config(&tls)?;
            let listener = TcpListener::bind(listen_addr).await?;
            let local_addr = listener.local_addr()?;
            let acceptor = TlsAcceptor::from(tls_config);
            let service_builder = Server::builder()
                .http_only()
                .set_http_middleware(http_middleware)
                .set_rpc_middleware(rpc_middleware)
                .max_response_body_size(
                    conf.max_response_body_size
                        .try_into()
                        .expect("should be valid"),
                )
                .to_service_builder();
            let (stop_handle, server_handle) = stop_channel();

            info!("{opened_endpoint_msg}{local_addr}");

            return Ok(tokio::spawn(async move {
                loop {
                    let (socket, remote_addr) = tokio::select! {
                        result = listener.accept() => match result {
                            Ok(connection) => connection,
                            Err(error) => return Err(error.into()),
                        },
                        _ = stop_handle.clone().shutdown() => break,
                    };

                    let acceptor = acceptor.clone();
                    let service = service_builder
                        .clone()
                        .build(methods.clone(), stop_handle.clone());
                    let stopped = stop_handle.clone().shutdown();

                    tokio::spawn(async move {
                        match acceptor.accept(socket).await {
                            Ok(stream) => {
                                if let Err(error) =
                                    serve_with_graceful_shutdown(stream, service, stopped).await
                                {
                                    warn!(
                                        ?error,
                                        %remote_addr,
                                        "TLS RPC connection terminated with an error"
                                    );
                                }
                            }
                            Err(error) => {
                                warn!(
                                    ?error,
                                    %remote_addr,
                                    "TLS RPC handshake failed"
                                );
                            }
                        }
                    });
                }

                drop(server_handle);
                Ok(())
            }));
        }

        let server = Server::builder()
            .http_only()
            .set_http_middleware(http_middleware)
            .set_rpc_middleware(rpc_middleware)
            .max_response_body_size(
                conf.max_response_body_size
                    .try_into()
                    .expect("should be valid"),
            )
            .build(listen_addr)
            .await?;

        info!("{opened_endpoint_msg}{}", server.local_addr()?);

        Ok(tokio::spawn(async move {
            server.start(methods).stopped().await;
            Ok(())
        }))
    }

    /// Shut down this RPC server, blocking the current thread.
    ///
    /// This method can be called from within a tokio executor without panicking.
    /// But it is blocking, so `shutdown()` should be used instead.
    pub fn shutdown_blocking(&self) {
        Self::shutdown_blocking_inner(self.close_handle.clone(), self.config.clone())
    }

    /// Shut down this RPC server asynchronously.
    /// Returns a task that completes when the server is shut down.
    pub fn shutdown(&self) -> JoinHandle<()> {
        let close_handle = self.close_handle.clone();
        let config = self.config.clone();
        let span = Span::current();

        tokio::task::spawn_blocking(move || {
            span.in_scope(|| Self::shutdown_blocking_inner(close_handle, config))
        })
    }

    /// Shuts down this RPC server using its `close_handle`.
    ///
    /// See `shutdown_blocking()` for details.
    fn shutdown_blocking_inner(close_handle: ServerHandle, config: config::rpc::Config) {
        // The server is a blocking task, so it can't run inside a tokio thread.
        // See the note at wait_on_server.
        let span = Span::current();
        let wait_on_shutdown = move || {
            span.in_scope(|| {
                if config.enable_cookie_auth {
                    if let Err(err) =
                        cookie::remove_from_disk(&config.cookie_dir, Some(&config.cookie_file_name))
                    {
                        warn!(
                            ?err,
                            "unexpectedly could not remove the rpc auth cookie from the disk"
                        )
                    }
                }

                info!("Stopping RPC server");
                let _ = close_handle.stop();
                debug!("Stopped RPC server");
            })
        };

        let span = Span::current();
        let thread_handle = std::thread::spawn(wait_on_shutdown);

        // Propagate panics from the inner std::thread to the outer tokio blocking task
        span.in_scope(|| match thread_handle.join() {
            Ok(()) => (),
            Err(panic_object) => panic::resume_unwind(panic_object),
        })
    }
}

/// Validates the RPC method classification and removes methods that are not
/// available on restricted unauthenticated listeners.
fn configure_rpc_methods<Context>(
    methods: &mut jsonrpsee::RpcModule<Context>,
    surface: RpcSurface,
) -> Result<(), std::io::Error>
where
    Context: Send + Sync + 'static,
{
    let registered: BTreeSet<_> = methods.method_names().collect();
    let classified: BTreeSet<_> = RPC_METHOD_ACCESS.iter().map(|(name, _)| *name).collect();

    if classified.len() != RPC_METHOD_ACCESS.len() {
        return Err(std::io::Error::other(
            "RPC method access classifications contain duplicate method names",
        ));
    }

    if registered != classified {
        let unclassified: Vec<_> = registered.difference(&classified).copied().collect();
        let unregistered: Vec<_> = classified.difference(&registered).copied().collect();

        return Err(std::io::Error::other(format!(
            "RPC method access classification mismatch; unclassified registered methods: {unclassified:?}; classified but unregistered methods: {unregistered:?}"
        )));
    }

    if surface == RpcSurface::Restricted {
        for method_name in registered {
            if rpc_method_access(method_name) != Some(RpcAccess::Unauthenticated) {
                methods
                    .remove_method(method_name)
                    .expect("method exists because it came from method_names");
            }
        }
    }

    Ok(())
}

/// Selects the primary listener's method surface.
fn primary_rpc_surface(network: &Network, enable_cookie_auth: bool) -> RpcSurface {
    if enable_cookie_auth || network.is_regtest() {
        RpcSurface::Full
    } else {
        RpcSurface::Restricted
    }
}

fn load_tls_config(
    tls: &config::rpc::TlsConfig,
) -> Result<Arc<RustlsServerConfig>, tower::BoxError> {
    let cert_file = File::open(&tls.cert_file).map_err(|error| {
        std::io::Error::new(
            error.kind(),
            format!(
                "could not open RPC TLS certificate file {}: {error}",
                tls.cert_file.display()
            ),
        )
    })?;
    let key_file = File::open(&tls.key_file).map_err(|error| {
        std::io::Error::new(
            error.kind(),
            format!(
                "could not open RPC TLS private key file {}: {error}",
                tls.key_file.display()
            ),
        )
    })?;

    let cert_chain = parse_tls_cert_chain(cert_file)?;
    if cert_chain.is_empty() {
        return Err(format!(
            "RPC TLS certificate file {} did not contain any certificates",
            tls.cert_file.display()
        )
        .into());
    }

    warn_if_certificates_are_not_current(&cert_chain, &tls.cert_file);

    let private_key = parse_tls_private_key(key_file)?.ok_or_else(|| {
        format!(
            "RPC TLS private key file {} did not contain a usable private key",
            tls.key_file.display()
        )
    })?;

    let crypto_provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = RustlsServerConfig::builder_with_provider(crypto_provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| format!("could not configure RPC TLS protocol versions: {error}"))?
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)
        .map_err(|error| format!("could not build RPC TLS server config: {error}"))?;

    Ok(Arc::new(config))
}

fn parse_tls_cert_chain(
    cert_reader: impl Read,
) -> Result<Vec<CertificateDer<'static>>, rustls::pki_types::pem::Error> {
    CertificateDer::pem_reader_iter(cert_reader).collect()
}

fn parse_tls_private_key(
    key_reader: impl Read,
) -> Result<Option<PrivateKeyDer<'static>>, rustls::pki_types::pem::Error> {
    PrivateKeyDer::pem_reader_iter(key_reader)
        .next()
        .transpose()
}

/// Whether a certificate is inside its validity window.
#[derive(Debug, Eq, PartialEq)]
enum CertificateValidity {
    /// The certificate can be used now.
    Current,

    /// The certificate's `notBefore` field is in the future.
    NotYetValid {
        /// The `notBefore` field, as Unix seconds.
        not_before: i64,
    },

    /// The certificate's `notAfter` field is in the past.
    Expired {
        /// The `notAfter` field, as Unix seconds.
        not_after: i64,
    },
}

/// Logs a warning for every certificate in `cert_chain` that is outside its
/// validity window and may cause clients to reject the TLS handshake.
///
/// This warns and keeps running, rather than refusing to start:
/// - an RPC certificate that expired while the node was down must not stop the node from
///   verifying blocks when it comes back up, and
/// - a `notBefore` in the future is usually an unsynchronised clock, which resolves itself
///   without a restart.
///
/// Certificates whose dates can't be read are ignored because validity checks
/// are best-effort diagnostics; TLS peers remain responsible for validation.
fn warn_if_certificates_are_not_current(cert_chain: &[CertificateDer<'static>], cert_file: &Path) {
    let now = Utc::now().timestamp();

    for (position, certificate) in cert_chain.iter().enumerate() {
        match certificate_validity(certificate, now) {
            Ok(CertificateValidity::Current) => {}
            Ok(CertificateValidity::NotYetValid { not_before }) => warn!(
                cert_file = %cert_file.display(),
                position,
                not_before,
                now,
                "RPC TLS certificate is not valid yet, \
                 clients may reject the TLS handshake until its notBefore date",
            ),
            Ok(CertificateValidity::Expired { not_after }) => warn!(
                cert_file = %cert_file.display(),
                position,
                not_after,
                now,
                "RPC TLS certificate has expired, clients may reject the TLS handshake",
            ),
            Err(error) => debug!(
                ?error,
                cert_file = %cert_file.display(),
                position,
                "could not read the validity dates of an RPC TLS certificate",
            ),
        }
    }
}

/// Returns whether `certificate` is inside its validity window at `now`.
///
/// Walks the DER encoding as far as the `validity` field of the `TBSCertificate`
/// (RFC 5280 section 4.1):
/// ```text
/// Certificate  ::= SEQUENCE { tbsCertificate TBSCertificate, ... }
/// TBSCertificate ::= SEQUENCE {
///     version         [0] EXPLICIT Version DEFAULT v1,
///     serialNumber        CertificateSerialNumber,
///     signature           AlgorithmIdentifier,
///     issuer              Name,
///     validity            Validity,
///     ... }
/// Validity ::= SEQUENCE { notBefore Time, notAfter Time }
/// ```
fn certificate_validity(
    certificate: &CertificateDer<'_>,
    now: i64,
) -> Result<CertificateValidity, der::Error> {
    let mut reader = SliceReader::new(certificate.as_ref())?;
    let (tag, certificate) = read_der_value(&mut reader)?;
    tag.assert_eq(Tag::Sequence)?;

    let mut reader = SliceReader::new(certificate)?;
    let (tag, tbs_certificate) = read_der_value(&mut reader)?;
    tag.assert_eq(Tag::Sequence)?;

    let mut reader = SliceReader::new(tbs_certificate)?;
    if reader.peek_header()?.tag.is_context_specific() {
        // `version` is only encoded when it isn't the default.
        read_der_value(&mut reader)?;
    }
    read_der_value(&mut reader)?; // serialNumber
    read_der_value(&mut reader)?; // signature
    read_der_value(&mut reader)?; // issuer

    let (tag, validity) = read_der_value(&mut reader)?;
    tag.assert_eq(Tag::Sequence)?;

    let mut reader = SliceReader::new(validity)?;
    let not_before = read_x509_time(&mut reader)?;
    let not_after = read_x509_time(&mut reader)?;

    Ok(if now < not_before {
        CertificateValidity::NotYetValid { not_before }
    } else if now > not_after {
        CertificateValidity::Expired { not_after }
    } else {
        CertificateValidity::Current
    })
}

/// Reads the next DER tag-length-value item, returning its tag and the bytes of its value.
fn read_der_value<'a>(reader: &mut SliceReader<'a>) -> Result<(Tag, &'a [u8]), der::Error> {
    let header = Header::decode(reader)?;
    let value = reader.read_slice(header.length)?;

    Ok((header.tag, value))
}

/// Reads an X.509 `Time` as Unix seconds.
///
/// RFC 5280 section 4.1.2.5 encodes years from 1950 through 2049 as
/// `UTCTime`, and later years as `GeneralizedTime`.
fn read_x509_time(reader: &mut SliceReader<'_>) -> Result<i64, der::Error> {
    match reader.peek_header()?.tag {
        Tag::UtcTime => read_utc_time(reader),
        Tag::GeneralizedTime => i64::try_from(
            GeneralizedTime::decode(reader)?
                .to_unix_duration()
                .as_secs(),
        )
        .map_err(|_| Tag::GeneralizedTime.value_error()),
        tag => Err(tag.value_error()),
    }
}

/// Reads an RFC 5280 `UTCTime`, including dates before the Unix epoch.
///
/// The [`der::asn1::UtcTime`] decoder only supports years from 1970 onward,
/// while RFC 5280 requires support for the full 1950–2049 range.
fn read_utc_time(reader: &mut SliceReader<'_>) -> Result<i64, der::Error> {
    let (tag, value) = read_der_value(reader)?;
    tag.assert_eq(Tag::UtcTime)?;

    let Some(digits) = value.strip_suffix(b"Z").filter(|digits| digits.len() == 12) else {
        return Err(Tag::UtcTime.value_error());
    };

    let short_year = decode_two_digits_at(Tag::UtcTime, digits, 0)?;
    let year = if short_year >= 50 {
        1900 + i32::from(short_year)
    } else {
        2000 + i32::from(short_year)
    };
    let month = decode_two_digits_at(Tag::UtcTime, digits, 2)?;
    let day = decode_two_digits_at(Tag::UtcTime, digits, 4)?;
    let hour = decode_two_digits_at(Tag::UtcTime, digits, 6)?;
    let minute = decode_two_digits_at(Tag::UtcTime, digits, 8)?;
    let second = decode_two_digits_at(Tag::UtcTime, digits, 10)?;

    Utc.with_ymd_and_hms(
        year,
        u32::from(month),
        u32::from(day),
        u32::from(hour),
        u32::from(minute),
        u32::from(second),
    )
    .single()
    .map(|date_time| date_time.timestamp())
    .ok_or_else(|| Tag::UtcTime.value_error())
}

/// Decodes two ASCII decimal digits at `offset`.
fn decode_two_digits_at(tag: Tag, value: &[u8], offset: usize) -> Result<u8, der::Error> {
    let Some(&[tens, ones]) = value.get(offset..offset.saturating_add(2)) else {
        return Err(tag.value_error());
    };

    decode_two_digits(tag, tens, ones)
}

/// Decodes two validated ASCII decimal digits.
fn decode_two_digits(tag: Tag, tens: u8, ones: u8) -> Result<u8, der::Error> {
    if !tens.is_ascii_digit() || !ones.is_ascii_digit() {
        return Err(tag.value_error());
    }

    // Each operand is at most 9, so the result fits in a `u8`.
    Ok((tens - b'0') * 10 + (ones - b'0'))
}

impl Drop for RpcServer {
    fn drop(&mut self) {
        // Block on shutting down, propagating panics.
        // This can take around 150 seconds.
        //
        // Without this shutdown, Zebra's RPC unit tests sometimes crashed with memory errors.
        self.shutdown_blocking();
    }
}
