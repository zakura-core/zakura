//! Tests for TLS PEM parsing and listener connection lifetimes.

use std::{io::ErrorKind, net::SocketAddr, sync::Arc, time::Duration};

use jsonrpsee::{
    server::{serve_with_graceful_shutdown, stop_channel, Server, ServerHandle},
    RpcModule,
};
use rustls::pki_types::{pem::PemObject, CertificateDer};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};
use tokio_rustls::TlsAcceptor;

use super::super::{
    certificate_validity, parse_tls_cert_chain, parse_tls_private_key, run_tls_listener,
    CertificateValidity, MAX_PENDING_TLS_HANDSHAKES, TLS_HANDSHAKE_TIMEOUT,
};

// This self-signed localhost certificate and key are exclusively test fixtures.
const HANDSHAKE_CERT: &str = include_str!("handshake-cert.pem");
const HANDSHAKE_KEY: &str = include_str!("handshake-key.pem");
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

struct TestTlsListener {
    address: SocketAddr,
    handle: ServerHandle,
    task: JoinHandle<Result<(), std::io::Error>>,
}

impl TestTlsListener {
    async fn start(max_pending: usize, handshake_timeout: Duration) -> Self {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                parse_tls_cert_chain(HANDSHAKE_CERT.as_bytes()).unwrap(),
                parse_tls_private_key(HANDSHAKE_KEY.as_bytes())
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop_handle, handle) = stop_channel();
        let mut methods = RpcModule::new(());
        methods.register_method("ping", |_, _, _| "pong").unwrap();
        let services = Server::builder().http_only().to_service_builder();
        let task = tokio::spawn(run_tls_listener(
            listener,
            TlsAcceptor::from(Arc::new(config)),
            stop_handle,
            handle.clone(),
            max_pending,
            handshake_timeout,
            move |stream, _, stop_handle| {
                let service = services.clone().build(methods.clone(), stop_handle.clone());
                async move {
                    let _ =
                        serve_with_graceful_shutdown(stream, service, stop_handle.shutdown()).await;
                }
            },
        ));
        Self {
            address,
            handle,
            task,
        }
    }

    async fn ping(&self) -> reqwest::Client {
        crate::indexer::server::install_tls_crypto_provider();
        let client = reqwest::Client::builder()
            .tls_certs_only([reqwest::Certificate::from_pem(HANDSHAKE_CERT.as_bytes()).unwrap()])
            .resolve("localhost", self.address)
            .no_proxy()
            .timeout(TEST_TIMEOUT)
            .build()
            .unwrap();
        let response = client
            .post(format!("https://localhost:{}/", self.address.port()))
            .header("content-type", "application/json")
            .body(r#"{"jsonrpc":"2.0","method":"ping","id":1}"#)
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        let body: serde_json::Value =
            serde_json::from_str(&response.text().await.unwrap()).unwrap();
        assert_eq!(body["result"], "pong");
        client
    }

    async fn stop(self) {
        self.handle.stop().unwrap();
        timeout(TEST_TIMEOUT, self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

async fn assert_socket_closed(socket: &mut TcpStream) {
    let result = timeout(TEST_TIMEOUT, socket.read(&mut [0])).await.unwrap();
    match result {
        Ok(bytes) => assert_eq!(bytes, 0),
        Err(error) => assert!(matches!(
            error.kind(),
            ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted
        )),
    }
}

#[tokio::test]
async fn silent_tls_handshake_times_out_and_releases_capacity() {
    // Shorten only the deadline; exercise the same listener as production.
    let listener = TestTlsListener::start(1, Duration::from_secs(1)).await;
    let mut silent = TcpStream::connect(listener.address).await.unwrap();
    assert_socket_closed(&mut silent).await;
    listener.ping().await;
    listener.stop().await;
}

#[tokio::test]
async fn tls_handshake_admission_is_bounded_and_recovers_after_failure() {
    let listener = TestTlsListener::start(MAX_PENDING_TLS_HANDSHAKES, TLS_HANDSHAKE_TIMEOUT).await;
    let mut pending = Vec::new();
    for _ in 0..MAX_PENDING_TLS_HANDSHAKES {
        pending.push(TcpStream::connect(listener.address).await.unwrap());
    }
    let mut excess = TcpStream::connect(listener.address).await.unwrap();
    assert_socket_closed(&mut excess).await;

    // The first admitted socket is still pending, rather than being rejected.
    assert!(
        timeout(Duration::from_millis(20), pending[0].read(&mut [0]))
            .await
            .is_err()
    );
    pending[0].shutdown().await.unwrap();
    assert_socket_closed(&mut pending[0]).await;
    listener.ping().await;
    listener.stop().await;
    for socket in &mut pending {
        assert_socket_closed(socket).await;
    }
}

#[tokio::test]
async fn completed_tls_handshake_releases_capacity_with_http_connection_open() {
    let listener = TestTlsListener::start(1, TLS_HANDSHAKE_TIMEOUT).await;
    // Keep the first client's HTTP connection pooled while another handshakes.
    let client = listener.ping().await;
    listener.ping().await;
    listener.stop().await;
    drop(client);
}

#[tokio::test]
async fn tls_listener_shutdown_cancels_silent_handshakes() {
    let listener = TestTlsListener::start(MAX_PENDING_TLS_HANDSHAKES, TLS_HANDSHAKE_TIMEOUT).await;
    let mut silent = TcpStream::connect(listener.address).await.unwrap();
    let client = listener.ping().await;
    listener.stop().await;
    assert_socket_closed(&mut silent).await;
    drop(client);
}

#[tokio::test]
async fn aborting_tls_listener_cancels_owned_connections() {
    let listener = TestTlsListener::start(MAX_PENDING_TLS_HANDSHAKES, TLS_HANDSHAKE_TIMEOUT).await;
    let mut silent = TcpStream::connect(listener.address).await.unwrap();
    // Ensure the listener accepted the socket before aborting its parent task.
    listener.ping().await;
    listener.task.abort();
    let error = timeout(TEST_TIMEOUT, listener.task)
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.is_cancelled());
    assert_socket_closed(&mut silent).await;
}

/// A self-signed test certificate whose `notBefore` and `notAfter` are both before 2050, so
/// both are encoded as `UTCTime`: 2025-01-01 00:00:00 UTC to 2025-02-01 00:00:00 UTC.
const UTC_TIME_CERT: &str = "\
-----BEGIN CERTIFICATE-----
MIIBXzCCAQWgAwIBAgIUOqpDeLLE9L/b+m+mfxMBjnCetbswCgYIKoZIzj0EAwIw
HjEcMBoGA1UEAwwTemFrdXJhLXJwYy10bHMtdGVzdDAeFw0yNTAxMDEwMDAwMDBa
Fw0yNTAyMDEwMDAwMDBaMB4xHDAaBgNVBAMME3pha3VyYS1ycGMtdGxzLXRlc3Qw
WTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAQ+wHoX5FRv42/6K6QTK/S4bo9He6nb
Er3si2BfQwWbPWAsEokgpCoP3GBM3xbK5u2GuL6w5kVrIz1XDI1cAnnNoyEwHzAd
BgNVHQ4EFgQUVqQ+oR1le2DmrIi5cNKCaXdiBF4wCgYIKoZIzj0EAwIDSAAwRQIh
ALWt669Gzty6dj95pi6imo8sIFtBzUBYMy97W/vHGC9dAiAtdr3+QKUEAuPw7DfK
Q4y1hbo0mHmIo4gHWo93MnAJ1A==
-----END CERTIFICATE-----
";

/// The same certificate, but valid until 2060-01-01 00:00:00 UTC, so its `notAfter` is encoded
/// as a `GeneralizedTime`.
const GENERALIZED_TIME_CERT: &str = "\
-----BEGIN CERTIFICATE-----
MIIBYjCCAQegAwIBAgIUKl5vhHmfrnymJ7fL+vr+XmAmt/gwCgYIKoZIzj0EAwIw
HjEcMBoGA1UEAwwTemFrdXJhLXJwYy10bHMtdGVzdDAgFw0yNTAxMDEwMDAwMDBa
GA8yMDYwMDEwMTAwMDAwMFowHjEcMBoGA1UEAwwTemFrdXJhLXJwYy10bHMtdGVz
dDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABD7AehfkVG/jb/orpBMr9Lhuj0d7
qdsSveyLYF9DBZs9YCwSiSCkKg/cYEzfFsrm7Ya4vrDmRWsjPVcMjVwCec2jITAf
MB0GA1UdDgQWBBRWpD6hHWV7YOasiLlw0oJpd2IEXjAKBggqhkjOPQQDAgNJADBG
AiEA+dmdhqGXyRHuEuNg0iJzhFM/Axx+gHq/U63Hw1rijdUCIQDjmsEOZr2vEZQv
EVH9mWqX5H2mIDsHB02Nw+Iebwao5A==
-----END CERTIFICATE-----
";

/// 2025-01-01 00:00:00 UTC, the `notBefore` of both test certificates.
const NOT_BEFORE: i64 = 1_735_689_600;
/// 2025-02-01 00:00:00 UTC, the `notAfter` of [`UTC_TIME_CERT`].
const UTC_TIME_NOT_AFTER: i64 = 1_738_368_000;
/// 2060-01-01 00:00:00 UTC, the `notAfter` of [`GENERALIZED_TIME_CERT`].
const GENERALIZED_TIME_NOT_AFTER: i64 = 2_840_140_800;
/// 1960-01-01 00:00:00 UTC, before the Unix epoch but valid in an X.509 `UTCTime`.
const PRE_UNIX_EPOCH_NOT_BEFORE: i64 = -315_619_200;

fn certificate(pem: &str) -> CertificateDer<'static> {
    CertificateDer::from_pem_slice(pem.as_bytes()).expect("test certificate should be valid PEM")
}

/// Replaces the test certificate's `notBefore` without re-signing it.
///
/// [`certificate_validity`] only parses the validity fields, so the unchanged
/// signature is irrelevant to this focused test.
fn certificate_with_not_before(not_before: &[u8; 13]) -> CertificateDer<'static> {
    let mut certificate = certificate(UTC_TIME_CERT).as_ref().to_vec();
    let original_not_before = b"250101000000Z";
    let offset = certificate
        .windows(original_not_before.len())
        .position(|window| window == original_not_before)
        .expect("test certificate should contain its notBefore value");

    certificate[offset..offset + not_before.len()].copy_from_slice(not_before);
    CertificateDer::from(certificate)
}

#[test]
fn parses_certificate_chain_and_first_private_key() {
    let cert_pem = b"\
-----BEGIN CERTIFICATE-----\n\
AQID\n\
-----END CERTIFICATE-----\n\
-----BEGIN CERTIFICATE-----\n\
BAUG\n\
-----END CERTIFICATE-----\n";
    let key_pem = b"\
-----BEGIN CERTIFICATE-----\n\
AQID\n\
-----END CERTIFICATE-----\n\
-----BEGIN PRIVATE KEY-----\n\
BwgJ\n\
-----END PRIVATE KEY-----\n\
-----BEGIN PRIVATE KEY-----\n\
CgsM\n\
-----END PRIVATE KEY-----\n";

    let cert_chain =
        parse_tls_cert_chain(cert_pem.as_slice()).expect("valid certificate PEM sections");
    let private_key =
        parse_tls_private_key(key_pem.as_slice()).expect("valid private key PEM sections");

    assert_eq!(cert_chain.len(), 2);
    assert_eq!(cert_chain[0].as_ref(), [1, 2, 3]);
    assert_eq!(cert_chain[1].as_ref(), [4, 5, 6]);
    assert_eq!(
        private_key
            .expect("private key section should be loaded")
            .secret_der(),
        [7, 8, 9]
    );
}

#[test]
fn reads_utc_time_validity_dates() {
    let certificate = certificate(UTC_TIME_CERT);

    assert_eq!(
        certificate_validity(&certificate, NOT_BEFORE - 1),
        Ok(CertificateValidity::NotYetValid {
            not_before: NOT_BEFORE
        }),
    );
    assert_eq!(
        certificate_validity(&certificate, NOT_BEFORE),
        Ok(CertificateValidity::Current),
    );
    assert_eq!(
        certificate_validity(&certificate, UTC_TIME_NOT_AFTER),
        Ok(CertificateValidity::Current),
    );
    assert_eq!(
        certificate_validity(&certificate, UTC_TIME_NOT_AFTER + 1),
        Ok(CertificateValidity::Expired {
            not_after: UTC_TIME_NOT_AFTER
        }),
    );
}

#[test]
fn reads_generalized_time_validity_dates() {
    let certificate = certificate(GENERALIZED_TIME_CERT);

    assert_eq!(
        certificate_validity(&certificate, NOT_BEFORE),
        Ok(CertificateValidity::Current),
    );
    assert_eq!(
        certificate_validity(&certificate, GENERALIZED_TIME_NOT_AFTER + 1),
        Ok(CertificateValidity::Expired {
            not_after: GENERALIZED_TIME_NOT_AFTER
        }),
    );
}

#[test]
fn reads_pre_unix_epoch_utc_time() {
    let certificate = certificate_with_not_before(b"600101000000Z");

    assert_eq!(
        certificate_validity(&certificate, PRE_UNIX_EPOCH_NOT_BEFORE - 1),
        Ok(CertificateValidity::NotYetValid {
            not_before: PRE_UNIX_EPOCH_NOT_BEFORE,
        }),
    );
    assert_eq!(
        certificate_validity(&certificate, 0),
        Ok(CertificateValidity::Current),
    );
    assert_eq!(
        certificate_validity(&certificate, UTC_TIME_NOT_AFTER + 1),
        Ok(CertificateValidity::Expired {
            not_after: UTC_TIME_NOT_AFTER,
        }),
    );
}

#[test]
fn ignores_certificates_that_are_not_valid_der() {
    // The same placeholder bytes that `parses_certificate_chain_and_first_private_key` loads:
    // rustls accepts them at config time, so reading their dates must fail without panicking.
    let certificate = CertificateDer::from(vec![1, 2, 3]);

    assert!(certificate_validity(&certificate, NOT_BEFORE).is_err());
}
