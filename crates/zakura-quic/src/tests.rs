//! Endpoint conformance tests (SPEC §16).

use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::future::BoxFuture;
use tokio::sync::mpsc;

use crate::{
    Acceptor, Admit, Conn, ConnectError, IncomingInfo, NodeAddr, NodeSecretKey, QuicBindConfig,
    QuicConfig, QuicEndpoint, VarInt,
};

const ALPN: &[u8] = b"p2p-v2/2";
const TEST_TIMEOUT: Duration = Duration::from_secs(20);

struct TestAcceptor {
    decide: Box<dyn Fn(&IncomingInfo) -> Admit + Send + Sync>,
    alpns: Vec<Vec<u8>>,
    seen: Arc<Mutex<Vec<IncomingInfo>>>,
    banned: Arc<Mutex<HashSet<IpAddr>>>,
    handled: mpsc::UnboundedSender<Conn>,
}

impl Acceptor for TestAcceptor {
    fn admit(&self, incoming: &IncomingInfo) -> Admit {
        self.seen.lock().unwrap().push(*incoming);
        (self.decide)(incoming)
    }

    fn alpns(&self) -> Vec<Vec<u8>> {
        self.alpns.clone()
    }

    fn handle(&self, conn: Conn) -> BoxFuture<'static, ()> {
        let handled = self.handled.clone();
        Box::pin(async move {
            // Echo every stream until the connection closes.
            let echo = conn.clone();
            let _ = handled.send(conn);
            while let Ok((mut send, mut recv)) = echo.accept_bi().await {
                tokio::spawn(async move {
                    let data = recv.read_to_end(64 * 1024 * 1024).await.unwrap_or_default();
                    let _ = send.write_all(&data).await;
                    let _ = send.finish();
                    let _ = send.stopped().await;
                });
            }
        })
    }

    fn is_banned(&self, ip: IpAddr) -> bool {
        self.banned.lock().unwrap().contains(&ip)
    }
}

struct Server {
    endpoint: QuicEndpoint,
    seen: Arc<Mutex<Vec<IncomingInfo>>>,
    banned: Arc<Mutex<HashSet<IpAddr>>>,
    handled: mpsc::UnboundedReceiver<Conn>,
}

impl Server {
    fn addr(&self) -> NodeAddr {
        NodeAddr::with_addrs(self.endpoint.local_id(), self.endpoint.local_addrs())
    }
}

fn loopback() -> QuicBindConfig {
    QuicBindConfig {
        addrs: vec![SocketAddr::from((Ipv4Addr::LOCALHOST, 0))],
        max_bidi_streams: 64,
    }
}

fn test_config() -> QuicConfig {
    QuicConfig {
        // Keep CI hosts with small rmem_max quiet.
        recv_buffer_bytes: 256 * 1024,
        send_buffer_bytes: 256 * 1024,
        handshake_timeout_secs: Some(3),
        ..QuicConfig::default()
    }
}

fn server_with(
    config: &QuicConfig,
    bind: &QuicBindConfig,
    alpns: &[&[u8]],
    decide: impl Fn(&IncomingInfo) -> Admit + Send + Sync + 'static,
) -> Server {
    let endpoint = QuicEndpoint::bind(NodeSecretKey::generate(), bind, config).unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let banned = Arc::new(Mutex::new(HashSet::new()));
    let (handled_tx, handled) = mpsc::unbounded_channel();
    endpoint
        .serve(TestAcceptor {
            decide: Box::new(decide),
            alpns: alpns.iter().map(|alpn| alpn.to_vec()).collect(),
            seen: seen.clone(),
            banned: banned.clone(),
            handled: handled_tx,
        })
        .unwrap();
    Server {
        endpoint,
        seen,
        banned,
        handled,
    }
}

fn server() -> Server {
    server_with(&test_config(), &loopback(), &[ALPN], |_| Admit::Accept)
}

fn client() -> QuicEndpoint {
    QuicEndpoint::bind(NodeSecretKey::generate(), &loopback(), &test_config()).unwrap()
}

async fn echo(conn: &Conn, payload: &[u8]) -> Vec<u8> {
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    send.write_all(payload).await.unwrap();
    send.finish().unwrap();
    recv.read_to_end(payload.len() + 1).await.unwrap()
}

#[tokio::test]
async fn dial_authenticates_both_sides_and_echoes() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut server = server();
        let client = client();
        let conn = client.connect(server.addr(), ALPN).await.unwrap();
        assert_eq!(conn.remote_id(), server.endpoint.local_id());
        assert_eq!(conn.alpn(), ALPN);
        assert_eq!(conn.admitted_ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        // WIRE-3: multipath negotiated, as Iroh 1.1 does.
        assert!(conn.noq().is_multipath_enabled());

        let payload = vec![7u8; 1024 * 1024];
        assert_eq!(echo(&conn, &payload).await, payload);

        let inbound = server.handled.recv().await.unwrap();
        assert_eq!(inbound.remote_id(), client.local_id());
        let seen = server.seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].remote.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert!(!seen[0].validated);
        assert_eq!(seen[0].pending_total, 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn self_dial_is_refused_before_any_packet() {
    let client = client();
    let own = NodeAddr::with_addrs(client.local_id(), client.local_addrs());
    assert!(matches!(
        client.connect(own, ALPN).await,
        Err(ConnectError::SelfDial)
    ));
}

#[tokio::test]
async fn dial_without_a_matching_family_fails_fast() {
    let server = server();
    let client = client();
    let v6_only = NodeAddr::with_addrs(server.endpoint.local_id(), ["[::1]:8234".parse().unwrap()]);
    assert!(matches!(
        client.connect(v6_only, ALPN).await,
        Err(ConnectError::NoUsableAddress)
    ));
}

#[tokio::test]
async fn wrong_identity_is_refused() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let server = server();
        let client = client();
        let impostor = NodeAddr::with_addrs(
            NodeSecretKey::generate().public(),
            server.endpoint.local_addrs(),
        );
        assert!(matches!(
            client.connect(impostor, ALPN).await,
            Err(ConnectError::WrongIdentity)
        ));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn alpn_mismatch_is_classified() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let server = server();
        let client = client();
        assert!(matches!(
            client.connect(server.addr(), b"p2p-v2/3").await,
            Err(ConnectError::AlpnMismatch)
        ));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn server_prefers_its_own_alpn_order() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let server = server_with(&test_config(), &loopback(), &[b"b", b"a"], |_| {
            Admit::Accept
        });
        let client = client();
        let conn = client.connect(server.addr(), b"a").await.unwrap();
        assert_eq!(conn.alpn(), b"a");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn refused_attempt_creates_no_connection() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut server = server_with(&test_config(), &loopback(), &[ALPN], |_| Admit::Refuse);
        let client = client();
        assert!(matches!(
            client.connect(server.addr(), ALPN).await,
            Err(ConnectError::Refused)
        ));
        // SEC-3: the acceptor never saw a connection.
        assert!(server.handled.try_recv().is_err());
        assert_eq!(server.seen.lock().unwrap().len(), 1);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ignored_attempt_times_out_on_the_dialer() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut server = server_with(&test_config(), &loopback(), &[ALPN], |_| Admit::Ignore);
        let client = client();
        assert!(matches!(
            client.connect(server.addr(), ALPN).await,
            Err(ConnectError::HandshakeTimeout)
        ));
        assert!(server.handled.try_recv().is_err());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn retry_threshold_validates_before_accepting() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let config = QuicConfig {
            retry_threshold: Some(0),
            ..test_config()
        };
        let mut server = server_with(&config, &loopback(), &[ALPN], |_| Admit::Accept);
        let client = client();
        client.connect(server.addr(), ALPN).await.unwrap();
        server.handled.recv().await.unwrap();
        let seen = server.seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 2, "one Retry, then the validated attempt");
        assert!(!seen[0].validated);
        assert!(seen[1].validated);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn max_pending_per_ip_refuses_concurrent_handshakes() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let config = QuicConfig {
            max_pending_per_ip: Some(1),
            ..test_config()
        };
        let server = server_with(&config, &loopback(), &[ALPN], |_| Admit::Accept);
        // Two clients from the same IP racing their handshakes: at least one
        // succeeds, and any refusal is a clean `Refused`.
        let (a, b) = (client(), client());
        let (first, second) = tokio::join!(
            a.connect(server.addr(), ALPN),
            b.connect(server.addr(), ALPN)
        );
        assert!(first.is_ok() || second.is_ok());
        for result in [first, second] {
            if let Err(error) = result {
                assert!(matches!(error, ConnectError::Refused), "{error:?}");
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn stalled_handshake_releases_its_pending_slot() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let server = server_with(&test_config(), &loopback(), &[ALPN], |_| Admit::Accept);
        // A raw noq client sends one Initial, then its endpoint disappears.
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let raw = noq::Endpoint::new(
            noq::EndpointConfig::default(),
            None,
            socket,
            Arc::new(noq::TokioRuntime),
        )
        .unwrap();
        let client_tls = crate::tls::TlsConfig::new(&NodeSecretKey::generate())
            .client_config(server.endpoint.local_id(), ALPN)
            .unwrap();
        let connecting = raw
            .connect_with(
                noq::ClientConfig::new(Arc::new(client_tls)),
                server.endpoint.local_addrs()[0],
                crate::tls::UNSENT_SERVER_NAME,
            )
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(connecting);
        drop(raw);

        // ADM-6/ADM-7: the 3 s deadline frees the slot. A later attempt sees none pending.
        tokio::time::sleep(Duration::from_secs(4)).await;
        let client = client();
        client.connect(server.addr(), ALPN).await.unwrap();
        let seen = server.seen.lock().unwrap().clone();
        assert_eq!(seen.last().unwrap().pending_total, 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn happy_eyeballs_skips_a_black_hole() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let config = QuicConfig {
            dial_stagger_ms: 100,
            ..test_config()
        };
        let server = server_with(&config, &loopback(), &[ALPN], |_| Admit::Accept);
        let client = QuicEndpoint::bind(
            NodeSecretKey::generate(),
            &QuicBindConfig {
                addrs: vec!["0.0.0.0:0".parse().unwrap()],
                max_bidi_streams: 64,
            },
            &config,
        )
        .unwrap();
        // 192.0.2.1 (TEST-NET-1) never answers; the loopback address does.
        let mut direct = vec![
            "192.0.2.1:8234".parse().unwrap(),
            "[::ffff:192.0.2.1]:8234".parse().unwrap(),
        ];
        direct.extend(server.endpoint.local_addrs());
        let conn = client
            .connect(
                NodeAddr::with_addrs(server.endpoint.local_id(), direct),
                ALPN,
            )
            .await
            .unwrap();
        // DIAL-6: the winning attempt's address is admitted.
        assert_eq!(conn.admitted_ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn peer_opened_path_from_a_banned_ip_closes() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut server = server();
        let client = QuicEndpoint::bind(
            NodeSecretKey::generate(),
            &QuicBindConfig {
                addrs: vec!["0.0.0.0:0".parse().unwrap()],
                max_bidi_streams: 64,
            },
            &test_config(),
        )
        .unwrap();
        let conn = client.connect(server.addr(), ALPN).await.unwrap();
        let inbound = server.handled.recv().await.unwrap();
        let banned_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
        server.banned.lock().unwrap().insert(banned_ip);

        // The client opens extra paths once the server has issued connection IDs.
        async fn open_from(conn: &Conn, server: SocketAddr, local: IpAddr) -> noq::Path {
            let tuple = noq::FourTuple::new(server, Some(local));
            for _ in 0..50 {
                if let Ok(path) = conn
                    .noq()
                    .open_path(tuple, noq::PathStatus::Available)
                    .await
                {
                    return path;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            panic!("the server never accepted a path from {local}");
        }
        let server_addr = server.endpoint.local_addrs()[0];

        // Control: a path from an IP that isn't banned stays open.
        let allowed = open_from(&conn, server_addr, IpAddr::V4(Ipv4Addr::new(127, 0, 0, 3))).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(allowed.status().is_ok(), "an allowed path closed");

        let path = open_from(&conn, server_addr, banned_ip).await;
        // PATH-2: the server closes it; the client sees it go away.
        for _ in 0..50 {
            if path.status().is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(path.status().is_err(), "the banned path is still open");
        // SEC-5: path 0 and the admitted IP stay.
        assert!(inbound.close_reason().is_none());
        assert_eq!(inbound.admitted_ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        let payload = vec![1u8; 4096];
        assert_eq!(echo(&conn, &payload).await, payload);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn shutdown_closes_connections_within_the_bound() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut server = server();
        let client = client();
        let conn = client.connect(server.addr(), ALPN).await.unwrap();
        let _inbound = server.handled.recv().await.unwrap();
        let started = std::time::Instant::now();
        server.endpoint.shutdown().await;
        assert!(started.elapsed() < Duration::from_secs(4));
        let reason = conn.closed().await;
        assert!(
            matches!(&reason, noq::ConnectionError::ApplicationClosed(close) if close.error_code == VarInt::from_u32(0)),
            "{reason:?}"
        );
        // A shut-down endpoint dials nothing.
        assert!(server
            .endpoint
            .connect(NodeAddr::with_addrs(client.local_id(), client.local_addrs()), ALPN)
            .await
            .is_err());
    })
    .await
    .unwrap();
}

/// An embedder that drops its node without calling `shutdown` must still get
/// its port back.
#[tokio::test]
async fn dropping_the_last_handle_closes_connections_and_frees_the_port() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut server = server();
        let port = server.endpoint.local_addrs()[0];
        let client = client();
        let conn = client.connect(server.addr(), ALPN).await.unwrap();
        let inbound = server.handled.recv().await.unwrap();

        drop(server);
        conn.closed().await;
        drop(inbound);
        loop {
            if std::net::UdpSocket::bind(port).is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn buffers_are_read_back() {
    let client = client();
    let stats = client.socket_stats();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].buffers.recv_requested, 256 * 1024);
    assert!(stats[0].buffers.recv_effective > 0);
    assert!(stats[0].buffers.send_effective > 0);
}

#[tokio::test]
async fn dropping_the_last_conn_closes_the_connection() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut server = server();
        let client = client();
        let conn = client.connect(server.addr(), ALPN).await.unwrap();
        let inbound = server.handled.recv().await.unwrap();
        drop(conn);
        let reason = inbound.closed().await;
        assert!(
            matches!(reason, noq::ConnectionError::ApplicationClosed(_)),
            "{reason:?}"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn congestion_controller_is_cubic_by_default() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let server = server();
        let client = client();
        let conn = client.connect(server.addr(), ALPN).await.unwrap();
        let controller = conn
            .noq()
            .congestion_state(noq::PathId::ZERO)
            .expect("path 0 is open");
        let name = format!("{controller:?}");
        assert!(name.contains("Cubic"), "{name}");
    })
    .await
    .unwrap();
}

/// Dials `server` with a hand-built rustls client config (SEC-2).
async fn dial_raw(
    server: &Server,
    crypto: rustls::ClientConfig,
) -> Result<(), noq::ConnectionError> {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let raw = noq::Endpoint::new(
        noq::EndpointConfig::default(),
        None,
        socket,
        Arc::new(noq::TokioRuntime),
    )
    .unwrap();
    let quic = noq::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap();
    let connecting = raw
        .connect_with(
            noq::ClientConfig::new(Arc::new(quic)),
            server.endpoint.local_addrs()[0],
            crate::tls::UNSENT_SERVER_NAME,
        )
        .unwrap();
    let connection = connecting.await?;
    // A server that accepted the handshake may still reject the client's
    // certificate right after; give it a moment to close.
    tokio::time::timeout(Duration::from_secs(2), connection.closed())
        .await
        .map_or(Ok(()), Err)
}

fn raw_client_builder(
    server: &Server,
) -> rustls::ConfigBuilder<rustls::ClientConfig, rustls::client::WantsClientCert> {
    rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_protocol_versions(crate::tls::verifier::PROTOCOL_VERSIONS)
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(
            crate::tls::verifier::ServerCertificateVerifier::new(server.endpoint.local_id()),
        ))
}

#[tokio::test]
async fn client_without_a_key_is_refused() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut server = server();
        let mut crypto = raw_client_builder(&server).with_no_client_auth();
        crypto.alpn_protocols = vec![ALPN.to_vec()];
        assert!(dial_raw(&server, crypto).await.is_err());
        assert!(server.handled.try_recv().is_err());
    })
    .await
    .unwrap();
}

/// Presents the Ed25519 key without negotiating RFC 7250 raw public keys.
#[derive(Debug)]
struct NotRawPublicKey(Arc<rustls::sign::CertifiedKey>);

impl rustls::client::ResolvesClientCert for NotRawPublicKey {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[rustls::SignatureScheme],
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        Some(self.0.clone())
    }

    fn only_raw_public_keys(&self) -> bool {
        false
    }

    fn has_certs(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn client_with_an_x509_style_certificate_is_refused() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut server = server();
        // Borrow the RPK resolver's key, but present it as an X.509 chain.
        let rpk = crate::tls::resolver::ResolveRawPublicKeyCert::new(&NodeSecretKey::generate());
        let key = rustls::client::ResolvesClientCert::resolve(&rpk, &[], &[]).unwrap();
        let mut crypto =
            raw_client_builder(&server).with_client_cert_resolver(Arc::new(NotRawPublicKey(key)));
        crypto.alpn_protocols = vec![ALPN.to_vec()];
        assert!(dial_raw(&server, crypto).await.is_err());
        assert!(server.handled.try_recv().is_err());
    })
    .await
    .unwrap();
}

#[test]
fn profile_is_tls13_only() {
    // TLS-1: QUIC needs TLS 1.3, and the profile offers nothing else.
    assert_eq!(crate::tls::verifier::PROTOCOL_VERSIONS.len(), 1);
    assert_eq!(
        crate::tls::verifier::PROTOCOL_VERSIONS[0].version,
        rustls::ProtocolVersion::TLSv1_3
    );
}

#[tokio::test]
async fn raw_client_with_the_profile_is_accepted() {
    // Control for the refusal tests: the same raw dial with an RPK key succeeds.
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut server = server();
        let rpk = crate::tls::resolver::ResolveRawPublicKeyCert::new(&NodeSecretKey::generate());
        let mut crypto = raw_client_builder(&server).with_client_cert_resolver(Arc::new(rpk));
        crypto.alpn_protocols = vec![ALPN.to_vec()];
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let raw = noq::Endpoint::new(
            noq::EndpointConfig::default(),
            None,
            socket,
            Arc::new(noq::TokioRuntime),
        )
        .unwrap();
        let quic = noq::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap();
        let _connection = raw
            .connect_with(
                noq::ClientConfig::new(Arc::new(quic)),
                server.endpoint.local_addrs()[0],
                crate::tls::UNSENT_SERVER_NAME,
            )
            .unwrap()
            .await
            .unwrap();
        server.handled.recv().await.unwrap();
    })
    .await
    .unwrap();
}
