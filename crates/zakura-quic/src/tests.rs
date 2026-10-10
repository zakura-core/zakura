//! Endpoint conformance tests (SPEC §16).

use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::future::BoxFuture;
use quinn_proto::crypto::rustls::QuicClientConfig;
use tokio::sync::mpsc;

use crate::{
    endpoint::watch_interfaces, Acceptor, Admit, Conn, ConnectError, ConnectionError, IncomingInfo,
    NodeAddr, NodeSecretKey, QuicBindConfig, QuicConfig, QuicEndpoint, VarInt,
};

const ALPN: &[u8] = b"p2p-v2/3";
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
            // Echo every stream until the connection closes. `conn` stays
            // alive here, so finished streams still deliver their data.
            let _ = handled.send(conn.clone());
            while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                tokio::spawn(async move {
                    let data = recv.read_to_end(64 * 1024 * 1024).await.unwrap_or_default();
                    let _ = send.write_all(&data).await;
                    let _ = send.finish();
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

fn wildcard() -> QuicBindConfig {
    QuicBindConfig {
        addrs: vec![SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))],
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

/// A handshake that never finishes: one client Initial sent from a socket
/// nobody reads. The server's replies, including any Retry, go nowhere. This
/// is what a spoofed source looks like to the server.
struct StalledHandshake {
    _socket: std::net::UdpSocket,
}

fn stalled_handshake(server: &Server) -> StalledHandshake {
    let target = server.endpoint.local_addrs()[0];
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut endpoint = quinn_proto::Endpoint::new(
        Arc::new(quinn_proto::EndpointConfig::default()),
        None,
        false,
        None,
    );
    let tls = crate::tls::TlsConfig::new(&NodeSecretKey::generate())
        .client_config(server.endpoint.local_id(), ALPN)
        .unwrap();
    let now = std::time::Instant::now();
    let (_, mut conn) = endpoint
        .connect(
            now,
            quinn_proto::ClientConfig::new(Arc::new(tls)),
            target,
            crate::tls::UNSENT_SERVER_NAME,
        )
        .unwrap();
    let mut buf = Vec::new();
    let transmit = conn
        .poll_transmit(now, 1, &mut buf)
        .expect("a new connection sends its Initial at once");
    socket.send_to(&buf[..transmit.size], target).unwrap();
    StalledHandshake { _socket: socket }
}

async fn wait_for_attempts(server: &Server, count: usize) {
    while server.seen.lock().unwrap().len() < count {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_until_bindable(addr: SocketAddr) {
    while std::net::UdpSocket::bind(addr).is_err() {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
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

        // Larger than the stream receive window, so flow control has to
        // extend credit as the reader consumes.
        let payload: Vec<u8> = (0..4 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
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
async fn many_concurrent_streams_keep_their_data_apart() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let server = server();
        let client = client();
        let conn = client.connect(server.addr(), ALPN).await.unwrap();
        let echoes = (0..32u8).map(|i| {
            let conn = conn.clone();
            async move {
                let payload = vec![i; 64 * 1024 + usize::from(i)];
                assert_eq!(echo(&conn, &payload).await, payload);
            }
        });
        futures::future::join_all(echoes).await;
    })
    .await
    .unwrap();
}

/// API-5: a cancelled read loses nothing. The next call waits for the reply
/// of the cancelled one first.
#[tokio::test]
async fn cancelled_reads_lose_no_data() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let server = server();
        let client = client();
        let conn = client.connect(server.addr(), ALPN).await.unwrap();
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        send.write_all(b"hello").await.unwrap();
        // The echo replies only after the stream finishes, so this read
        // waits and is cancelled.
        let mut buf = [0u8; 16];
        assert!(
            tokio::time::timeout(Duration::from_millis(100), recv.read(&mut buf))
                .await
                .is_err()
        );
        send.finish().unwrap();
        assert_eq!(recv.read_to_end(16).await.unwrap(), b"hello");
    })
    .await
    .unwrap();
}

/// API-5: a cancelled write keeps its place. The next write waits for it, so
/// the peer receives both, in order.
#[tokio::test]
async fn cancelled_writes_keep_their_order() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let server = server();
        let client = client();
        let conn = client.connect(server.addr(), ALPN).await.unwrap();
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        // Far more than the flow-control windows, so the write blocks until
        // the echo reads.
        let first = vec![1u8; 32 * 1024 * 1024];
        let _ = tokio::time::timeout(Duration::from_millis(1), send.write_all(&first)).await;
        send.write_all(b"tail").await.unwrap();
        send.finish().unwrap();
        let echoed = recv.read_to_end(64 * 1024 * 1024).await.unwrap();
        assert!(echoed.ends_with(b"tail"));
        let body = &echoed[..echoed.len() - 4];
        assert!(body.is_empty() || body == first, "partial first write");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn stream_errors_carry_the_peer_code() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut server = server();
        let client = client();
        let conn = client.connect(server.addr(), ALPN).await.unwrap();
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        send.write_all(b"x").await.unwrap();
        let inbound = server.handled.recv().await.unwrap();
        drop(inbound);
        // Reset our send side and stop our receive side.
        send.reset(VarInt::from_u32(7)).unwrap();
        assert!(send.write_all(b"y").await.is_err());
        recv.stop(VarInt::from_u32(9)).unwrap();
        assert!(recv.read(&mut [0u8; 4]).await.is_err());
    })
    .await
    .unwrap();
}

// SOCK-12: an interface change reaches every connection once, and a
// connection keeps working after it forgets its pinned local address.
#[tokio::test]
async fn interface_change_notifies_connections_and_keeps_them() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let server = server();
        let client =
            QuicEndpoint::bind(NodeSecretKey::generate(), &wildcard(), &test_config()).unwrap();
        let conn = client.connect(server.addr(), ALPN).await.unwrap();
        assert_eq!(echo(&conn, b"before").await, b"before");

        let ips = Arc::new(Mutex::new(vec![IpAddr::from([192, 0, 2, 1])]));
        let list_ips = {
            let ips = ips.clone();
            move || ips.lock().unwrap().clone()
        };
        let watcher = tokio::spawn(watch_interfaces(
            Arc::downgrade(client.shared()),
            list_ips,
            Duration::from_millis(20),
        ));

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(client.network_changes(), 0, "no change, no notice");

        ips.lock().unwrap().push(IpAddr::from([198, 51, 100, 1]));
        while client.network_changes() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(echo(&conn, b"after").await, b"after");
        assert_eq!(client.network_changes(), 1);

        client.shutdown().await;
        watcher.await.unwrap();
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
            client.connect(server.addr(), b"p2p-v2/2").await,
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

/// V12 F-305590: without Retry, spoofed Initials held every pending slot until
/// the idle timeout, and an acceptor that refuses at its budget then refused
/// every honest peer. The default threshold sends unvalidated sources a Retry
/// once 8 handshakes are pending, so spoofed sources hold at most 8 slots.
#[tokio::test]
async fn spoofed_handshakes_cannot_fill_the_pending_budget() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut server = server_with(&test_config(), &loopback(), &[ALPN], |info| {
            if info.pending_total >= 32 {
                Admit::Refuse
            } else {
                Admit::Accept
            }
        });
        let stalled: Vec<_> = (0..40).map(|_| stalled_handshake(&server)).collect();
        wait_for_attempts(&server, 40).await;
        let most_pending = server
            .seen
            .lock()
            .unwrap()
            .iter()
            .map(|info| info.pending_total)
            .max();
        assert_eq!(most_pending, Some(8));

        let client = client();
        client.connect(server.addr(), ALPN).await.unwrap();
        server.handled.recv().await.unwrap();
        assert!(server.seen.lock().unwrap().last().unwrap().validated);
        drop(stalled);
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
        let stalled = stalled_handshake(&server);
        wait_for_attempts(&server, 1).await;
        drop(stalled);

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
        let client = QuicEndpoint::bind(NodeSecretKey::generate(), &wildcard(), &config).unwrap();
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

/// A UDP relay that rebinds its server-facing socket on demand, the way a NAT
/// does. The server then sees the client migrate to the relay's new address.
struct Rebinder {
    addr: SocketAddr,
    rebind: tokio::sync::watch::Sender<SocketAddr>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Rebinder {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn rebinder(server: SocketAddr) -> Rebinder {
    let front = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = front.local_addr().unwrap();
    let (rebind, mut source) = tokio::sync::watch::channel(SocketAddr::from(([127, 0, 0, 1], 0)));
    let task = tokio::spawn(async move {
        let first = *source.borrow_and_update();
        let mut back = tokio::net::UdpSocket::bind(first).await.unwrap();
        let mut client = None;
        let (mut up, mut down) = (vec![0; 65_536], vec![0; 65_536]);
        loop {
            tokio::select! {
                Ok(()) = source.changed() => {
                    let next = *source.borrow_and_update();
                    back = tokio::net::UdpSocket::bind(next).await.unwrap();
                }
                Ok((len, from)) = front.recv_from(&mut up) => {
                    client = Some(from);
                    let _ = back.send_to(&up[..len], server).await;
                }
                Ok((len, _)) = back.recv_from(&mut down) => {
                    if let Some(client) = client {
                        let _ = front.send_to(&down[..len], client).await;
                    }
                }
            }
        }
    });
    Rebinder { addr, rebind, task }
}

/// PATH-3: a connection that migrates to a banned IP closes. Linux routes all
/// of 127.0.0.0/8 to loopback, so the relay can move to 127.0.0.2.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn migration_to_a_banned_ip_closes_the_connection() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut server = server();
        let relay = rebinder(server.endpoint.local_addrs()[0]).await;
        let client = client();
        let conn = client
            .connect(
                NodeAddr::with_addrs(server.endpoint.local_id(), [relay.addr]),
                ALPN,
            )
            .await
            .unwrap();
        let inbound = server.handled.recv().await.unwrap();
        assert_eq!(echo(&conn, b"before").await, b"before");

        let banned_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
        server.banned.lock().unwrap().insert(banned_ip);
        relay
            .rebind
            .send_replace(SocketAddr::from((banned_ip, 0)));
        // Traffic from the new address makes the server migrate.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(echo(&conn, b"after").await, b"after");

        // The next 10 s sample sees the banned IP.
        inbound.closed().await;
        let reason = conn.closed().await;
        assert!(
            matches!(&reason, ConnectionError::ApplicationClosed(close) if &close.reason[..] == b"banned path"),
            "{reason:?}"
        );
    })
    .await
    .unwrap();
}

/// PATH-3 control: migrating to an IP that isn't banned keeps the connection.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn migration_to_an_allowed_ip_keeps_the_connection() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut server = server();
        let relay = rebinder(server.endpoint.local_addrs()[0]).await;
        let client = client();
        let conn = client
            .connect(
                NodeAddr::with_addrs(server.endpoint.local_id(), [relay.addr]),
                ALPN,
            )
            .await
            .unwrap();
        let inbound = server.handled.recv().await.unwrap();
        relay
            .rebind
            .send_replace(SocketAddr::from(([127, 0, 0, 3], 0)));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(echo(&conn, b"after").await, b"after");
        assert!(inbound.close_reason().is_none());
        // The admitted address doesn't follow the migration (PATH-4).
        assert_eq!(inbound.admitted_ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
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
            matches!(&reason, ConnectionError::ApplicationClosed(close) if close.error_code == VarInt::from_u32(0)),
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

/// V12 F-305596: API-7 step 4. `shutdown` releases the sockets even while
/// other handles to the endpoint live.
#[tokio::test]
async fn shutdown_releases_the_socket_while_handles_live() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut server = server();
        let port = server.endpoint.local_addrs()[0];
        let client = client();
        let conn = client.connect(server.addr(), ALPN).await.unwrap();
        let inbound = server.handled.recv().await.unwrap();
        let handle = server.endpoint.clone();

        server.endpoint.shutdown().await;
        conn.closed().await;
        // The inbound `Conn` still lives; the socket must not wait for it.
        wait_until_bindable(port).await;
        drop(inbound);
        assert!(handle
            .connect(
                NodeAddr::with_addrs(client.local_id(), client.local_addrs()),
                ALPN
            )
            .await
            .is_err());
    })
    .await
    .unwrap();
}

/// V12 F-305588: a handshake task used to hold a strong endpoint handle, so a
/// peer that stalled its handshake kept a dropped endpoint and its port alive.
#[tokio::test]
async fn a_stalled_handshake_does_not_keep_a_dropped_endpoint_alive() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        // A deadline longer than the test, so only the fix can free the port.
        let config = QuicConfig {
            handshake_timeout_secs: Some(60),
            ..test_config()
        };
        let server = server_with(&config, &loopback(), &[ALPN], |_| Admit::Accept);
        let port = server.endpoint.local_addrs()[0];
        let _stalled = stalled_handshake(&server);
        wait_for_attempts(&server, 1).await;

        drop(server);
        wait_until_bindable(port).await;
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
        wait_until_bindable(port).await;
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
            matches!(reason, ConnectionError::ApplicationClosed(_)),
            "{reason:?}"
        );
    })
    .await
    .unwrap();
}

/// OBS-12: the instrumented controller and the driver report what they see.
#[tokio::test]
async fn stats_report_congestion_and_driver_counters() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let server = server();
        let client = client();
        let conn = client.connect(server.addr(), ALPN).await.unwrap();
        let payload = vec![3u8; 2 * 1024 * 1024];
        assert_eq!(echo(&conn, &payload).await, payload);
        let stats = conn.stats().await.expect("the connection is open");
        assert!(stats.congestion.window > 0);
        assert!(stats.congestion.acked_bytes >= payload.len() as u64);
        assert!(stats.driver.transmits > 0);
        assert!(stats.driver.datagrams_received > 0);
        assert!(stats.connection.udp_tx.bytes >= payload.len() as u64);
        assert_eq!(stats.driver.queued_send_bytes, 0);
    })
    .await
    .unwrap();
}

/// Dials `server` with a hand-built rustls client config (SEC-2). Returns
/// `Ok` only when the connection survives 2 s after the handshake.
async fn dial_raw(server: &Server, crypto: rustls::ClientConfig) -> Result<(), ConnectError> {
    let client = client();
    let quic = QuicClientConfig::try_from(crypto).unwrap();
    let (conn, _) = client
        .dial_with_crypto(quic, server.endpoint.local_addrs()[0])
        .await?;
    // A server that accepted the handshake may still reject the client's
    // certificate right after; give it a moment to close.
    let mut closed = conn.shared.closed.subscribe();
    let survived = tokio::time::timeout(Duration::from_secs(2), closed.wait_for(Option::is_some))
        .await
        .is_err();
    if survived {
        Ok(())
    } else {
        Err(ConnectError::Transport(conn.shared.close_reason()))
    }
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
        dial_raw(&server, crypto).await.unwrap();
        server.handled.recv().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn socket_counters_track_received_datagrams() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let server = server();
        let client = client();
        let conn = client.connect(server.addr(), ALPN).await.unwrap();
        assert_eq!(echo(&conn, &[5u8; 64 * 1024]).await, vec![5u8; 64 * 1024]);
        // The endpoint task flushes its counters every 10 s.
        loop {
            let stats = server.endpoint.socket_stats();
            if stats[0].datagrams_received > 0 {
                assert!(stats[0].recv_calls > 0);
                assert!(stats[0].datagrams_received >= stats[0].recv_calls);
                assert_eq!(stats[0].queue_drops, 0);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
}
