//! Endpoint conformance tests (SPEC §16).

use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use futures::future::BoxFuture;
use quinn_proto::crypto::rustls::QuicClientConfig;
use tokio::sync::mpsc;

use crate::{
    endpoint::watch_interfaces,
    sim_net::{LinkModel, NetEvent, SimNet},
    Acceptor, Admit, Conn, ConnectError, ConnectionError, IncomingInfo, NodeAddr, NodeSecretKey,
    QuicBindConfig, QuicConfig, QuicEndpoint, SocketBuffers, SocketStats, VarInt,
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
    #[cfg(target_os = "linux")]
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
        max_connections: 1024,
        dial_reserve: 0,
        max_closing_connections: 1024,
    }
}

fn wildcard() -> QuicBindConfig {
    QuicBindConfig {
        addrs: vec![SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))],
        max_bidi_streams: 64,
        max_connections: 1024,
        dial_reserve: 0,
        max_closing_connections: 1024,
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
    serve_test_acceptor(endpoint, alpns, decide)
}

/// Serves a [`TestAcceptor`] on `endpoint`.
fn serve_test_acceptor(
    endpoint: QuicEndpoint,
    alpns: &[&[u8]],
    decide: impl Fn(&IncomingInfo) -> Admit + Send + Sync + 'static,
) -> Server {
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
        #[cfg(target_os = "linux")]
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
        .client_config(server.endpoint.local_id(), &[ALPN])
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
        let conn = client.connect(server.addr(), &[ALPN]).await.unwrap();
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
        let conn = client.connect(server.addr(), &[ALPN]).await.unwrap();
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
        let conn = client.connect(server.addr(), &[ALPN]).await.unwrap();
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

/// API-5 and DRV-5: a write cancelled while flow control blocks it keeps
/// its place. The next write waits for it, so the peer receives both, in
/// order. The transfer is larger than both the stream and the connection
/// receive windows, so it completes only through window updates (DRV-6).
#[tokio::test]
async fn cancelled_writes_keep_their_order() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (_server, _client, conn, inbound) = held_pair_with(&small_windows(), 8).await;
        let (mut send, _recv) = conn.open_bi().await.unwrap();
        // 1 MiB against a 64 KiB stream window: the peer isn't reading yet,
        // so this write blocks and the timeout cancels it.
        // Truncation gives a repeating pattern, so a reorder changes bytes.
        let first: Vec<u8> = (0..1 << 20).map(|i: u32| i as u8).collect();
        assert!(
            tokio::time::timeout(Duration::from_millis(200), send.write_all(&first))
                .await
                .is_err(),
            "the write must block on flow control"
        );
        // The tail write waits for the cancelled one, which needs the peer
        // to read, so the peer reads on its own task.
        let (_peer_send, mut recv) = inbound.accept_bi().await.unwrap();
        let reader = tokio::spawn(async move { recv.read_to_end(2 << 20).await.unwrap() });
        send.write_all(b"tail").await.unwrap();
        send.finish().unwrap();
        let echoed = reader.await.unwrap();
        assert_eq!(echoed.len(), first.len() + 4);
        assert!(
            echoed[..first.len()] == first[..],
            "first write reordered or lost"
        );
        assert_eq!(&echoed[first.len()..], b"tail");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn stream_errors_carry_the_peer_code() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut server = server();
        let client = client();
        let conn = client.connect(server.addr(), &[ALPN]).await.unwrap();
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

/// DRV-9: a stream the peer stops before this node accepts it still reports
/// the peer's code to the later writer.
#[tokio::test]
async fn stop_before_accept_keeps_the_peer_code() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut server = server();
        let client = client();
        let conn = client.connect(server.addr(), &[ALPN]).await.unwrap();
        let inbound = server.handled.recv().await.unwrap();
        let (mut send, mut recv) = inbound.open_bi().await.unwrap();
        send.write_all(b"x").await.unwrap();
        recv.stop(VarInt::from_u32(9)).unwrap();
        // Let STOP_SENDING reach the client before it accepts the stream.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let (mut accepted, _accepted_recv) = conn.accept_bi().await.unwrap();
        assert_eq!(
            write_until_error(&mut accepted).await,
            crate::WriteError::Stopped(VarInt::from_u32(9))
        );
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
        let conn = client.connect(server.addr(), &[ALPN]).await.unwrap();
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
        let retired = |conn: &Conn| {
            let conn = conn.clone();
            async move {
                conn.stats()
                    .await
                    .unwrap()
                    .connection
                    .frame_tx
                    .retire_connection_id
            }
        };
        assert_eq!(retired(&conn).await, 0);

        ips.lock().unwrap().push(IpAddr::from([198, 51, 100, 1]));
        while client.network_changes() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // quinn-proto answers `local_address_changed` by moving to a fresh
        // remote connection ID, so a retired ID shows the connection got the
        // notice.
        while retired(&conn).await == 0 {
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
        client.connect(own, &[ALPN]).await,
        Err(ConnectError::SelfDial)
    ));
}

#[tokio::test]
async fn dial_without_a_matching_family_fails_fast() {
    let server = server();
    let client = client();
    let v6_only = NodeAddr::with_addrs(server.endpoint.local_id(), ["[::1]:8234".parse().unwrap()]);
    assert!(matches!(
        client.connect(v6_only, &[ALPN]).await,
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
            client.connect(impostor, &[ALPN]).await,
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
            client.connect(server.addr(), &[b"p2p-v2/2"]).await,
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
        // The client prefers `a`; the server's order wins.
        let conn = client.connect(server.addr(), &[b"a", b"b"]).await.unwrap();
        assert_eq!(conn.alpn(), b"b");
    })
    .await
    .unwrap();
}

/// A node that offers a newer and an older protocol version reaches a peer
/// that knows only the older one, in one handshake.
#[tokio::test]
async fn offering_several_alpns_reaches_an_older_peer() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let server = server();
        let client = client();
        let conn = client
            .connect(server.addr(), &[b"p2p-v2/4", ALPN])
            .await
            .unwrap();
        assert_eq!(conn.alpn(), ALPN);
        assert!(matches!(
            client.connect(server.addr(), &[]).await,
            Err(ConnectError::AlpnMismatch)
        ));
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
            client.connect(server.addr(), &[ALPN]).await,
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
            client.connect(server.addr(), &[ALPN]).await,
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
        client.connect(server.addr(), &[ALPN]).await.unwrap();
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
        client.connect(server.addr(), &[ALPN]).await.unwrap();
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
            a.connect(server.addr(), &[ALPN]),
            b.connect(server.addr(), &[ALPN])
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
        client.connect(server.addr(), &[ALPN]).await.unwrap();
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
                &[ALPN],
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
#[cfg(target_os = "linux")]
struct Rebinder {
    addr: SocketAddr,
    rebind: tokio::sync::watch::Sender<SocketAddr>,
    task: tokio::task::JoinHandle<()>,
}

#[cfg(target_os = "linux")]
impl Drop for Rebinder {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Starts a relay whose server-facing address can change during migration tests.
#[cfg(target_os = "linux")]
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
                &[ALPN],
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
        // Traffic from the new address makes the server migrate, and the
        // server checks the new IP as soon as it handles that traffic.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let started = std::time::Instant::now();
        if let Ok((mut send, _recv)) = conn.open_bi().await {
            let _ = send.write_all(b"after").await;
        }
        inbound.closed().await;
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the close waited for the 10 s sample"
        );
        let reason = conn.closed().await;
        assert!(
            matches!(&reason, ConnectionError::ApplicationClosed(close) if &close.reason[..] == b"banned path"),
            "{reason:?}"
        );
    })
    .await
    .unwrap();
}

/// PATH-3: a ban that lands after the peer moved still closes the
/// connection, at the next 10 s sample.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn ban_after_migration_closes_at_the_next_sample() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut server = server();
        let relay = rebinder(server.endpoint.local_addrs()[0]).await;
        let client = client();
        let conn = client
            .connect(
                NodeAddr::with_addrs(server.endpoint.local_id(), [relay.addr]),
                &[ALPN],
            )
            .await
            .unwrap();
        let inbound = server.handled.recv().await.unwrap();
        let moved_ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 4));
        relay.rebind.send_replace(SocketAddr::from((moved_ip, 0)));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(echo(&conn, b"after").await, b"after");
        // The snapshot follows the path within LOAD_INTERVAL, even with the
        // connection idle.
        while inbound.load().remote.ip() != moved_ip {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        server.banned.lock().unwrap().insert(moved_ip);
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
                &[ALPN],
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
        let conn = client.connect(server.addr(), &[ALPN]).await.unwrap();
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
            .connect(NodeAddr::with_addrs(client.local_id(), client.local_addrs()), &[ALPN])
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
        let conn = client.connect(server.addr(), &[ALPN]).await.unwrap();
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
                &[ALPN]
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
        let conn = client.connect(server.addr(), &[ALPN]).await.unwrap();
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
        let conn = client.connect(server.addr(), &[ALPN]).await.unwrap();
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
        let conn = client.connect(server.addr(), &[ALPN]).await.unwrap();
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
        let conn = client.connect(server.addr(), &[ALPN]).await.unwrap();
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

#[test]
fn oversized_datagrams_are_dropped_before_any_copy() {
    // One GRO batch of two 1,472-byte datagrams, then a lone 65,000-byte one.
    let batch = vec![7u8; 1472 * 2];
    let kept: Vec<_> = crate::driver::endpoint::split_segments(&batch, 1472, 1472).collect();
    assert!(kept.iter().all(Option::is_some));
    let jumbo = vec![7u8; 65_000];
    let dropped: Vec<_> = crate::driver::endpoint::split_segments(&jumbo, 65_000, 1472).collect();
    assert_eq!(dropped.len(), 1);
    assert!(
        dropped[0].is_none(),
        "a datagram above the advertised limit is dropped"
    );
}

/// SPEC §6a: the endpoint drops a datagram above its advertised
/// `max_udp_payload_size` before quinn-proto routes it.
#[tokio::test]
async fn the_endpoint_counts_oversized_datagrams_as_dropped() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let server = server();
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        // Exceed the endpoint limit without exceeding the platform's UDP send limit.
        let max_payload = test_config().endpoint_config().get_max_udp_payload_size();
        let oversized_len = usize::try_from(max_payload).unwrap() + 1;
        sender
            .send_to(&vec![0x40; oversized_len], server.endpoint.local_addrs()[0])
            .unwrap();
        while server.endpoint.socket_stats()[0].oversize_drops == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(server.endpoint.socket_stats()[0].oversize_drops, 1);
    })
    .await
    .unwrap();
}

#[test]
fn gro_segments_do_not_share_an_allocation() {
    let batch = vec![7u8; 1200 * 3 + 500];
    let mut segments: Vec<bytes::Bytes> =
        crate::driver::endpoint::split_segments(&batch, 1200, 1472)
            .map(|segment| segment.unwrap().freeze())
            .collect();
    assert_eq!(
        segments.iter().map(bytes::Bytes::len).collect::<Vec<_>>(),
        [1200, 1200, 1200, 500]
    );
    // quinn-proto buffers stream data as a slice of its datagram. Keeping one
    // slice must not keep the other datagrams' memory alive.
    // The other datagrams stay alive, as other connections' buffered chunks
    // would.
    let kept = segments.remove(0).slice(10..20);
    assert!(
        kept.is_unique(),
        "a datagram shares its allocation with others"
    );
    drop(segments);
}

/// Hands each connection over without accepting any stream, so a test drives
/// `accept_bi` itself.
struct HoldAcceptor {
    handled: mpsc::UnboundedSender<Conn>,
}

impl Acceptor for HoldAcceptor {
    fn admit(&self, _incoming: &IncomingInfo) -> Admit {
        Admit::Accept
    }

    fn alpns(&self) -> Vec<Vec<u8>> {
        vec![ALPN.to_vec()]
    }

    fn handle(&self, conn: Conn) -> BoxFuture<'static, ()> {
        let handled = self.handled.clone();
        Box::pin(async move {
            let _ = handled.send(conn.clone());
            conn.closed().await;
        })
    }
}

/// A connected pair whose server allows `max_bidi_streams` client streams and
/// accepts none on its own.
async fn held_pair(max_bidi_streams: u32) -> (QuicEndpoint, QuicEndpoint, Conn, Conn) {
    held_pair_with(&test_config(), max_bidi_streams).await
}

/// [`held_pair`] with `config` on both endpoints.
async fn held_pair_with(
    config: &QuicConfig,
    max_bidi_streams: u32,
) -> (QuicEndpoint, QuicEndpoint, Conn, Conn) {
    let bind = QuicBindConfig {
        max_bidi_streams,
        ..loopback()
    };
    let server = QuicEndpoint::bind(NodeSecretKey::generate(), &bind, config).unwrap();
    let (handled, mut inbound) = mpsc::unbounded_channel();
    server.serve(HoldAcceptor { handled }).unwrap();
    let client = QuicEndpoint::bind(NodeSecretKey::generate(), &loopback(), config).unwrap();
    let addr = NodeAddr::with_addrs(server.local_id(), server.local_addrs());
    let conn = client.connect(addr, &[ALPN]).await.unwrap();
    let inbound = inbound.recv().await.unwrap();
    (server, client, conn, inbound)
}

/// API-5: an `accept_bi` dropped after the stream arrived (a losing
/// `select!` arm) hands the stream to the next call instead of losing it and
/// its stream credit.
#[tokio::test]
async fn cancelled_accept_hands_the_stream_to_the_next_call() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (_server, _client, conn, inbound) = held_pair(2).await;
        for round in 0..3u8 {
            let mut accept = Box::pin(inbound.accept_bi());
            assert!(futures::poll!(&mut accept).is_pending());
            let (mut send, _recv) = conn.open_bi().await.unwrap();
            send.write_all(&[round]).await.unwrap();
            send.finish().unwrap();
            // Let the reply reach the dropped call's channel.
            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(accept);
            let (_send, mut recv) =
                tokio::time::timeout(Duration::from_secs(2), inbound.accept_bi())
                    .await
                    .expect("the stream reaches the next accept")
                    .unwrap();
            // Reading to the end frees the stream, so with two streams
            // allowed a third round only runs if the credit came back.
            assert_eq!(recv.read_to_end(8).await.unwrap(), [round]);
        }
    })
    .await
    .unwrap();
}

/// RFC 9000 §3.5: a sender answers STOP_SENDING with RESET_STREAM, even when
/// it later finishes or drops the stream. Without the reset the receiver never
/// frees the stream and the sender runs out of stream credit.
#[tokio::test]
async fn a_stopped_stream_returns_its_credit() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (_server, _client, conn, inbound) = held_pair(2).await;
        for round in 0..4u8 {
            let (mut send, recv) = tokio::time::timeout(Duration::from_secs(2), conn.open_bi())
                .await
                .expect("stream credit came back")
                .unwrap();
            send.write_all(&[round]).await.unwrap();
            // The server drops both halves: STOP_SENDING plus a FIN.
            drop(inbound.accept_bi().await.unwrap());
            // Wait until the stop arrives, then finish as an unaware sender
            // would.
            while send.write_all(&[round]).await.is_ok() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let _ = send.finish();
            drop(recv);
        }
    })
    .await
    .unwrap();
}

/// API-9: raising the stream limit at runtime lets a blocked opener through,
/// and the load snapshot reports the new limit.
#[tokio::test]
async fn set_limits_raises_the_stream_limit() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (_server, _client, conn, inbound) = held_pair(1).await;
        let (mut first, _first_recv) = conn.open_bi().await.unwrap();
        first.write_all(b"1").await.unwrap();
        let mut second = Box::pin(conn.open_bi());
        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut second)
                .await
                .is_err(),
            "the peer allows one stream"
        );
        inbound
            .set_limits(crate::ConnLimits::default().with_max_bidi_streams(2))
            .unwrap();
        let (mut second, _second_recv) = tokio::time::timeout(Duration::from_secs(2), second)
            .await
            .expect("the raised limit admits a second stream")
            .unwrap();
        second.write_all(b"2").await.unwrap();
        while inbound.load().max_bidi_streams != 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

/// API-9: the load snapshot counts delivered stream bytes and the peer's
/// open streams without a round trip to the connection task.
#[tokio::test]
async fn load_reports_delivered_bytes_and_open_streams() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (_server, _client, conn, inbound) = held_pair(8).await;
        let (mut send, _recv) = conn.open_bi().await.unwrap();
        send.write_all(&[5; 100_000]).await.unwrap();
        send.finish().unwrap();
        let (_send, mut recv) = inbound.accept_bi().await.unwrap();
        assert_eq!(recv.read_to_end(200_000).await.unwrap().len(), 100_000);
        loop {
            let load = inbound.load();
            if load.delivered_bytes == 100_000 && load.remote_open_streams == 1 {
                assert!(load.udp_rx.datagrams > 0);
                assert!(load.frame_rx.stream > 0);
                assert_eq!(load.queue_drops, 0);
                assert_eq!(load.queued_datagram_bytes, 0);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn set_priority_needs_an_open_stream() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (_server, _client, conn, _inbound) = held_pair(8).await;
        let (mut send, _recv) = conn.open_bi().await.unwrap();
        send.set_priority(5).unwrap();
        send.write_all(b"x").await.unwrap();
        send.finish().unwrap();
        assert!(send.set_priority(1).is_err());
    })
    .await
    .unwrap();
}

/// OBS-3: the final sample of a connection this node closed names why, not
/// just "closed".
#[tokio::test]
async fn final_sample_names_the_local_close_cause() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (server, _client, conn, inbound) = held_pair(8).await;
        let (samples, mut sampled) = mpsc::unbounded_channel();
        server.set_conn_observer(Arc::new(move |sample: &crate::ConnSample| {
            let _ = samples.send(sample.clone());
        }));
        let inbound_id = inbound.stable_id();
        inbound.close(VarInt::from_u32(3), b"too slow");
        let sample = loop {
            let sample = sampled.recv().await.unwrap();
            if sample.close_reason.is_some() {
                break sample;
            }
        };
        let reason = sample.close_reason.unwrap();
        assert!(
            reason.contains("code 3") && reason.contains("too slow"),
            "{reason}"
        );
        // The sample joins to the handle that owns the connection.
        assert_eq!(sample.stable_id, inbound_id);
        assert!(!sample.outbound, "the server accepted this connection");
        drop(conn);
    })
    .await
    .unwrap();
}

/// The smallest windows `QuicConfig` accepts, so a few writes reach flow control.
fn small_windows() -> QuicConfig {
    QuicConfig {
        stream_receive_window_bytes: 64 * 1024,
        receive_window_bytes: 128 * 1024,
        send_window_bytes: 64 * 1024,
        ..test_config()
    }
}

/// DRV-5: a read cancelled after its request left keeps the reply, even when
/// the reply already carries data.
#[tokio::test]
async fn cancelled_read_keeps_a_reply_that_carries_data() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (_server, _client, conn, inbound) = held_pair(64).await;
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        send.write_all(b"x").await.unwrap();
        let (mut peer_send, _peer_recv) = inbound.accept_bi().await.unwrap();
        peer_send.write_all(b"hello").await.unwrap();
        peer_send.finish().unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let mut buf = [0u8; 16];
        // One poll sends the request; dropping the future cancels the wait.
        assert!(futures::FutureExt::now_or_never(recv.read(&mut buf)).is_none());
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(recv.read_to_end(16).await.unwrap(), b"hello");
    })
    .await
    .unwrap();
}

/// DRV-4 and OBS-12: a write waits once more than 64 KiB is queued, and the
/// driver reports the queued bytes and the time spent blocked.
#[tokio::test]
async fn write_ahead_is_bounded_and_reported() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (_server, _client, conn, _inbound) = held_pair_with(&small_windows(), 64).await;
        let (mut send, _recv) = conn.open_bi().await.unwrap();
        assert!(
            tokio::time::timeout(
                Duration::from_millis(500),
                send.write_all(&vec![0; 1 << 20])
            )
            .await
            .is_err(),
            "1 MiB can't pass a 64 KiB window plus 64 KiB of write-ahead"
        );
        let stats = conn.stats().await.unwrap();
        assert!(stats.driver.queued_send_bytes >= 512 * 1024, "{stats:?}");
        assert!(stats.driver.send_blocked > Duration::ZERO, "{stats:?}");
    })
    .await
    .unwrap();
}

/// DRV-4: an earlier write's failure surfaces on the next write, even if the
/// call that waited for it was cancelled.
#[tokio::test]
async fn earlier_write_error_surfaces_after_a_cancelled_wait() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (_server, _client, conn, inbound) = held_pair_with(&small_windows(), 64).await;
        let (mut send, _recv) = conn.open_bi().await.unwrap();
        send.write_all(b"x").await.unwrap();
        let (_peer_send, mut peer_recv) = inbound.accept_bi().await.unwrap();
        assert!(tokio::time::timeout(
            Duration::from_millis(300),
            send.write_all(&vec![0; 1 << 20])
        )
        .await
        .is_err());
        peer_recv.stop(VarInt::from_u32(9)).unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            send.write_all(b"y").await,
            Err(crate::WriteError::Stopped(VarInt::from_u32(9)))
        );
    })
    .await
    .unwrap();
}

/// A write blocked on flow control fails with the peer's code when the peer
/// stops the stream; it doesn't wait forever.
#[tokio::test]
async fn blocked_write_fails_when_the_peer_stops() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (_server, _client, conn, inbound) = held_pair_with(&small_windows(), 64).await;
        let (mut send, _recv) = conn.open_bi().await.unwrap();
        send.write_all(b"x").await.unwrap();
        let (_peer_send, mut peer_recv) = inbound.accept_bi().await.unwrap();
        let writer = tokio::spawn(async move { send.write_all(&vec![0; 1 << 20]).await });
        tokio::time::sleep(Duration::from_millis(300)).await;
        peer_recv.stop(VarInt::from_u32(9)).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), writer)
                .await
                .expect("a stopped stream fails its blocked write")
                .unwrap(),
            Err(crate::WriteError::Stopped(VarInt::from_u32(9)))
        );
    })
    .await
    .unwrap();
}

/// `finish` after writes that wait on flow control finishes after them.
#[tokio::test]
async fn finish_waits_for_writes_queued_behind_flow_control() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (_server, _client, conn, inbound) = held_pair_with(&small_windows(), 64).await;
        let (mut send, _recv) = conn.open_bi().await.unwrap();
        send.write_all(&vec![1; 64 * 1024]).await.unwrap();
        send.write_all(b"tail").await.unwrap();
        send.finish().unwrap();
        let (_peer_send, mut peer_recv) = inbound.accept_bi().await.unwrap();
        let data = tokio::time::timeout(Duration::from_secs(5), peer_recv.read_to_end(1 << 20))
            .await
            .expect("the stream finishes")
            .unwrap();
        assert_eq!(data.len(), 64 * 1024 + 4);
        assert!(data.ends_with(b"tail"));
    })
    .await
    .unwrap();
}

/// Dropping a `SendStream` finishes it; dropping a `RecvStream` stops it.
#[tokio::test]
async fn dropped_stream_halves_finish_and_stop() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (_server, _client, conn, inbound) = held_pair(64).await;
        let (mut send, recv) = conn.open_bi().await.unwrap();
        send.write_all(b"bye").await.unwrap();
        drop(send);
        drop(recv);
        let (mut peer_send, mut peer_recv) = inbound.accept_bi().await.unwrap();
        let data = tokio::time::timeout(Duration::from_secs(5), peer_recv.read_to_end(16))
            .await
            .expect("dropping the send half finishes the stream")
            .unwrap();
        assert_eq!(data, b"bye");
        let stopped =
            tokio::time::timeout(Duration::from_secs(5), write_until_error(&mut peer_send))
                .await
                .expect("dropping the receive half stops the stream");
        assert_eq!(stopped, crate::WriteError::Stopped(VarInt::from_u32(0)));
    })
    .await
    .unwrap();
}

/// `reset` and `stop` reach the peer with their codes.
#[tokio::test]
async fn reset_and_stop_carry_their_codes_to_the_peer() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (_server, _client, conn, inbound) = held_pair(64).await;
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        send.write_all(b"x").await.unwrap();
        let (mut peer_send, mut peer_recv) = inbound.accept_bi().await.unwrap();
        send.reset(VarInt::from_u32(7)).unwrap();
        assert_eq!(
            peer_recv.read_to_end(16).await,
            Err(crate::ReadToEndError::Read(crate::ReadError::Reset(
                VarInt::from_u32(7)
            )))
        );
        recv.stop(VarInt::from_u32(9)).unwrap();
        let stopped = write_until_error(&mut peer_send).await;
        assert_eq!(stopped, crate::WriteError::Stopped(VarInt::from_u32(9)));
    })
    .await
    .unwrap();
}

/// ADM-3 rule 3, deterministic: one stalled handshake from 127.0.0.1 fills
/// `max_pending_per_ip = 1`, so the next attempt from that IP is refused.
#[tokio::test]
async fn max_pending_per_ip_refuses_the_second_handshake() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let config = QuicConfig {
            max_pending_per_ip: Some(1),
            ..test_config()
        };
        let server = server_with(&config, &loopback(), &[ALPN], |_| Admit::Accept);
        let _stalled = stalled_handshake(&server);
        wait_for_attempts(&server, 1).await;
        let client = client();
        assert!(matches!(
            client.connect(server.addr(), &[ALPN]).await,
            Err(ConnectError::Refused)
        ));
        assert_eq!(server.seen.lock().unwrap()[1].pending_from_ip, 1);
    })
    .await
    .unwrap();
}

/// ADM-11: once every connection drained, shutdown doesn't wait out the
/// 3 s drain bound.
#[tokio::test]
async fn drained_connections_leave_the_table() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut server = server();
        let client = client();
        let conn = client.connect(server.addr(), &[ALPN]).await.unwrap();
        let inbound = server.handled.recv().await.unwrap();
        conn.close(VarInt::from_u32(0), b"done");
        inbound.closed().await;
        drop(inbound);
        // Draining lasts three PTOs, a few milliseconds on loopback.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let started = std::time::Instant::now();
        server.endpoint.shutdown().await;
        assert!(
            started.elapsed() < Duration::from_millis(1500),
            "{:?}",
            started.elapsed()
        );
    })
    .await
    .unwrap();
}

/// DIAL-3: a later attempt starts only after the stagger, so a fast first
/// address keeps the second from seeing any packet.
#[tokio::test]
async fn staggered_dial_skips_later_addresses_after_a_fast_win() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let config = QuicConfig {
            dial_stagger_ms: 1_000,
            ..test_config()
        };
        let server = server_with(&config, &loopback(), &[ALPN], |_| Admit::Accept);
        let decoy = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        decoy.set_nonblocking(true).unwrap();
        let client = QuicEndpoint::bind(NodeSecretKey::generate(), &loopback(), &config).unwrap();
        let mut direct = server.endpoint.local_addrs();
        direct.push(decoy.local_addr().unwrap());
        client
            .connect(
                NodeAddr::with_addrs(server.endpoint.local_id(), direct),
                &[ALPN],
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            decoy.recv_from(&mut [0u8; 2048]).is_err(),
            "no Initial reached the decoy"
        );
    })
    .await
    .unwrap();
}

/// OBS-12: the driver's counters agree with quinn-proto's UDP stats.
#[tokio::test]
async fn driver_send_counters_match_quinn_proto() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let server = server();
        let client = client();
        let conn = client.connect(server.addr(), &[ALPN]).await.unwrap();
        let payload = vec![3u8; 2 * 1024 * 1024];
        assert_eq!(echo(&conn, &payload).await, payload);
        let stats = conn.stats().await.unwrap();
        assert_eq!(
            stats.driver.transmits, stats.connection.udp_tx.ios,
            "{stats:?}"
        );
        assert_eq!(
            stats.driver.datagrams_sent, stats.connection.udp_tx.datagrams,
            "{stats:?}"
        );
    })
    .await
    .unwrap();
}

/// A key that presents a victim's public key but signs with another key.
#[derive(Debug)]
struct Impostor(NodeSecretKey);

impl rustls::sign::SigningKey for Impostor {
    fn choose_scheme(
        &self,
        offered: &[rustls::SignatureScheme],
    ) -> Option<Box<dyn rustls::sign::Signer>> {
        offered
            .contains(&rustls::SignatureScheme::ED25519)
            .then(|| Box::new(ImpostorSigner(self.0.clone())) as Box<dyn rustls::sign::Signer>)
    }

    fn algorithm(&self) -> rustls::SignatureAlgorithm {
        rustls::SignatureAlgorithm::ED25519
    }
}

#[derive(Debug)]
struct ImpostorSigner(NodeSecretKey);

impl rustls::sign::Signer for ImpostorSigner {
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>, rustls::Error> {
        Ok(self.0.sign(message).to_vec())
    }

    fn scheme(&self) -> rustls::SignatureScheme {
        rustls::SignatureScheme::ED25519
    }
}

#[derive(Debug)]
struct ImpostorResolver(Arc<rustls::sign::CertifiedKey>);

impl rustls::client::ResolvesClientCert for ImpostorResolver {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[rustls::SignatureScheme],
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        Some(self.0.clone())
    }

    fn only_raw_public_keys(&self) -> bool {
        true
    }

    fn has_certs(&self) -> bool {
        true
    }
}

/// TLS-8, SEC-2: a client that presents another node's key without its
/// secret is refused, so `remote_id()` can't name the victim.
#[tokio::test]
async fn client_presenting_a_key_it_cannot_sign_for_is_refused() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut server = server();
        let victim = NodeSecretKey::generate().public();
        let cert =
            rustls_pki_types::CertificateDer::from(crate::tls::ed25519_spki(&victim).to_vec());
        let key = Arc::new(rustls::sign::CertifiedKey::new(
            vec![cert],
            Arc::new(Impostor(NodeSecretKey::generate())),
        ));
        let mut crypto =
            raw_client_builder(&server).with_client_cert_resolver(Arc::new(ImpostorResolver(key)));
        crypto.alpn_protocols = vec![ALPN.to_vec()];
        assert!(dial_raw(&server, crypto).await.is_err());
        assert!(server.handled.try_recv().is_err());
    })
    .await
    .unwrap();
}

/// Writes 1 KiB chunks until a write fails, and returns that error.
///
/// Gives up after 16 MiB, so a driver that never blocks or fails a write
/// fails the test instead of growing without bound.
async fn write_until_error(send: &mut crate::SendStream) -> crate::WriteError {
    for _ in 0..16 * 1024 {
        if let Err(error) = send.write_all(&[0; 1024]).await {
            return error;
        }
    }
    panic!("16 MiB of writes never failed");
}

/// SOCK-6, G7: a send that hits `WouldBlock` is kept and sent later, so the
/// sender loses no packet. The hook only touches the client's socket.
#[tokio::test]
async fn hooktest_would_block_sends_are_kept() {
    use std::sync::atomic::Ordering;
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut server = server();
        let client = client();
        let conn = client.connect(server.addr(), &[ALPN]).await.unwrap();
        let inbound = server.handled.recv().await.unwrap();
        crate::socket::test_hooks::SEND_WOULD_BLOCK.store(200, Ordering::Relaxed);
        crate::socket::test_hooks::SEND_WOULD_BLOCK_PORT
            .store(client.local_addrs()[0].port(), Ordering::Relaxed);
        let payload = vec![7u8; 1 << 20];
        assert_eq!(echo(&conn, &payload).await, payload);
        assert_eq!(
            crate::socket::test_hooks::SEND_WOULD_BLOCK.load(Ordering::Relaxed),
            0
        );
        // The driver counts a transmit once the socket takes it, and
        // quinn-proto counts it when it builds it. A dropped blocked transmit
        // would leave the driver behind for good. (Packet loss can't show
        // this: loopback drops packets on a loaded host.)
        let caught_up = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let stats = conn.stats().await.unwrap();
                if stats.driver.transmits == stats.connection.udp_tx.ios {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(caught_up.is_ok(), "{:?}", conn.stats().await.unwrap());
        drop(inbound);
    })
    .await
    .unwrap();
}

/// SOCK-9: the first fatal receive error rebinds the socket on the same port
/// and connections keep working; a second within 60 s stops the endpoint.
#[tokio::test]
async fn hooktest_socket_failure_rebinds_once_then_stops() {
    use std::sync::atomic::Ordering;
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut server = server();
        let port = server.endpoint.local_addrs()[0].port();
        let client = client();
        let conn = client.connect(server.addr(), &[ALPN]).await.unwrap();
        let _inbound = server.handled.recv().await.unwrap();
        crate::socket::test_hooks::RECV_FAIL_PORT.store(port, Ordering::Relaxed);
        assert_eq!(echo(&conn, b"one").await, b"one");
        while server.endpoint.socket_stats()[0].rebinds == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(server.endpoint.local_addrs()[0].port(), port);
        assert_eq!(echo(&conn, b"two").await, b"two");

        crate::socket::test_hooks::RECV_FAIL_PORT.store(port, Ordering::Relaxed);
        // Traffic makes the server read and hit the second failure.
        if let Ok((mut send, _recv)) = conn.open_bi().await {
            let _ = send.write_all(b"three").await;
            let _ = send.finish();
        }
        let reason = tokio::time::timeout(Duration::from_secs(10), conn.closed())
            .await
            .expect("a second failure within 60 s stops the endpoint");
        assert!(
            matches!(&reason, ConnectionError::ApplicationClosed(close) if close.error_code == VarInt::from_u32(0)),
            "{reason:?}"
        );
        assert_eq!(server.endpoint.socket_stats()[0].rebinds, 1);
    })
    .await
    .unwrap();
}

/// Records the endpoint's pending-handshake count when `handle` runs.
struct PendingProbe {
    pending: crate::endpoint::PendingTable,
    seen: mpsc::UnboundedSender<usize>,
}

impl Acceptor for PendingProbe {
    fn admit(&self, _incoming: &IncomingInfo) -> Admit {
        Admit::Accept
    }

    fn alpns(&self) -> Vec<Vec<u8>> {
        vec![ALPN.to_vec()]
    }

    fn handle(&self, conn: Conn) -> BoxFuture<'static, ()> {
        let _ = self.seen.send(self.pending.total());
        Box::pin(async move {
            conn.closed().await;
        })
    }
}

/// ADM-7: `handle` runs while the attempt still counts as pending, so an
/// acceptor can take over the accounting with no window in which the IP
/// counts nowhere.
#[tokio::test]
async fn handle_runs_before_the_pending_count_drops() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let server =
            QuicEndpoint::bind(NodeSecretKey::generate(), &loopback(), &test_config()).unwrap();
        let (seen, mut pending_at_handle) = mpsc::unbounded_channel();
        server
            .serve(PendingProbe {
                pending: server.shared().pending.clone(),
                seen,
            })
            .unwrap();
        let client = client();
        let addr = NodeAddr::with_addrs(server.local_id(), server.local_addrs());
        let _conn = client.connect(addr, &[ALPN]).await.unwrap();
        assert_eq!(pending_at_handle.recv().await, Some(1));
    })
    .await
    .unwrap();
}

// The three stream-lifecycle regressions from the PR 1354 audit
// (art/inbox/iroh-deps/quinn-re-write), with a one-stream limit so a lost
// stream shows up as a stall.

/// DRV-10: an `open_bi` cancelled after its reply was sent hands the stream
/// to the next call.
#[tokio::test]
async fn cancel_after_open_reply_preserves_stream_capacity() {
    let (_server, _client, outbound, _inbound) = held_pair(1).await;
    let mut opening = Box::pin(outbound.open_bi());
    assert!(futures::poll!(&mut opening).is_pending());
    // Stats follows OpenBi in the command queue, so its reply was sent.
    outbound.stats().await.unwrap();
    drop(opening);
    tokio::time::timeout(Duration::from_secs(2), outbound.open_bi())
        .await
        .expect("cancelling an undelivered stream must preserve its capacity")
        .unwrap();
}

/// DRV-10: an `accept_bi` cancelled after its reply was sent hands the
/// stream, with its data, to the next call.
#[tokio::test]
async fn cancel_after_accept_reply_preserves_incoming_stream() {
    let (_server, _client, outbound, inbound) = held_pair(1).await;
    let mut accepting = Box::pin(inbound.accept_bi());
    assert!(futures::poll!(&mut accepting).is_pending());
    let (mut send, _recv) = outbound.open_bi().await.unwrap();
    send.write_all(b"x").await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while inbound.stats().await.unwrap().connection.frame_rx.stream == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // The driver handles stream events before it answers the next Stats.
    inbound.stats().await.unwrap();
    drop(accepting);
    let (_send, mut recv) = tokio::time::timeout(Duration::from_secs(2), inbound.accept_bi())
        .await
        .expect("cancelling an undelivered accept must preserve the stream")
        .unwrap();
    let data = tokio::time::timeout(Duration::from_secs(2), recv.read_chunk(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(data.as_deref(), Some(&b"x"[..]));
}

/// DRV-9: dropping a sender after the peer stopped it sends RESET_STREAM.
#[tokio::test]
async fn stopped_send_resets_when_dropped() {
    let (_server, _client, outbound, inbound) = held_pair(1).await;
    let (mut send, _recv) = outbound.open_bi().await.unwrap();
    send.write_all(b"x").await.unwrap();
    let (_reply, mut received) = inbound.accept_bi().await.unwrap();
    received.stop(VarInt::from_u32(7)).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while outbound
            .stats()
            .await
            .unwrap()
            .connection
            .frame_rx
            .stop_sending
            == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(send);
    outbound.stats().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while inbound
            .stats()
            .await
            .unwrap()
            .connection
            .frame_rx
            .reset_stream
            == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping a stopped sender must send RESET_STREAM");
}

/// DRV-4: a read takes at most what it asked for out of quinn-proto, so a
/// header read leaves the body under the peer's flow control.
#[tokio::test]
async fn a_header_read_takes_only_the_header_from_quinn() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (_server, _client, conn, inbound) = held_pair(8).await;
        let (mut send, _recv) = conn.open_bi().await.unwrap();
        send.write_all(&[5; 100_000]).await.unwrap();
        send.finish().unwrap();
        let (_send, mut recv) = inbound.accept_bi().await.unwrap();
        // Wait until the whole body sits in quinn-proto.
        while inbound.stats().await.unwrap().connection.udp_rx.bytes < 100_000 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut header = [0; 8];
        recv.read_exact(&mut header).await.unwrap();
        tokio::time::sleep(crate::LOAD_INTERVAL * 4).await;
        let delivered = inbound.load().delivered_bytes;
        // usize fits in u64 on supported targets.
        assert!(
            (8..=crate::stream::READ_AHEAD_BYTES as u64).contains(&delivered),
            "{delivered}"
        );
        assert!(recv.read_chunk(usize::MAX).await.unwrap().is_some());
    })
    .await
    .unwrap();
}

/// Takes the first stream's data and then rejects the connection, the way
/// Zakura closes a peer that fails its control handshake.
struct ChurnAcceptor;

impl Acceptor for ChurnAcceptor {
    fn admit(&self, _incoming: &IncomingInfo) -> Admit {
        Admit::Accept
    }

    fn alpns(&self) -> Vec<Vec<u8>> {
        vec![ALPN.to_vec()]
    }

    fn handle(&self, conn: Conn) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let _ = tokio::time::timeout(Duration::from_millis(50), conn.accept_bi()).await;
            conn.close(VarInt::from_u32(0), b"control handshake");
        })
    }
}

/// ADM-12, ADM-13: peers that fill a stream, get rejected and reconnect at
/// once never make the endpoint hold more live connections than its inbound
/// cap or more closed ones than its closing allowance, and every slot comes
/// back once they stop.
#[tokio::test]
async fn churned_connections_stay_within_both_allowances() {
    const CAP: usize = 6;
    const RESERVE: usize = 2;
    const CLOSING: usize = 3;
    tokio::time::timeout(TEST_TIMEOUT, async {
        let bind = QuicBindConfig {
            max_connections: CAP,
            dial_reserve: RESERVE,
            max_closing_connections: CLOSING,
            ..loopback()
        };
        let server = QuicEndpoint::bind(NodeSecretKey::generate(), &bind, &test_config()).unwrap();
        server.serve(ChurnAcceptor).unwrap();
        let addr = NodeAddr::with_addrs(server.local_id(), server.local_addrs());

        let stop = Arc::new(AtomicBool::new(false));
        let peaks = Arc::new((AtomicUsize::new(0), AtomicUsize::new(0)));
        let sampler = {
            let (server, stop, peaks) = (server.clone(), stop.clone(), peaks.clone());
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    peaks
                        .0
                        .fetch_max(server.connection_count(), Ordering::Relaxed);
                    peaks
                        .1
                        .fetch_max(server.closing_connection_count(), Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
            })
        };

        let mut churners = Vec::new();
        for _ in 0..12 {
            let addr = addr.clone();
            churners.push(tokio::spawn(async move {
                let client = client();
                for _ in 0..8 {
                    if let Ok(conn) = client.connect(addr.clone(), &[ALPN]).await {
                        if let Ok((mut send, _recv)) = conn.open_bi().await {
                            let _ = send.write_all(&[7; 256 * 1024]).await;
                        }
                        conn.closed().await;
                    }
                }
            }));
        }
        for churner in churners {
            churner.await.unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        sampler.await.unwrap();

        // Inbound attempts stop short of the dial reserve.
        let peak_live = peaks.0.load(Ordering::Relaxed);
        assert!((1..=CAP - RESERVE).contains(&peak_live), "{peak_live}");
        assert!(peaks.1.load(Ordering::Relaxed) <= CLOSING);
        tokio::time::timeout(Duration::from_secs(5), async {
            while server.connection_count() + server.closing_connection_count() > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("every slot frees once its connection drains");
    })
    .await
    .unwrap();
}

/// ADM-12: inbound connections can't take the dial reserve, and a dial at the
/// cap fails locally.
#[tokio::test]
async fn the_dial_reserve_keeps_dials_open_while_inbound_is_full() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let bind = QuicBindConfig {
            max_connections: 2,
            dial_reserve: 1,
            ..loopback()
        };
        let mut node = server_with(&test_config(), &bind, &[ALPN], |_| Admit::Accept);
        let first = client();
        let _held = first.connect(node.addr(), &[ALPN]).await.unwrap();
        node.handled.recv().await.unwrap();
        assert_eq!(node.endpoint.connection_count(), 1);

        let second = client();
        assert!(matches!(
            second.connect(node.addr(), &[ALPN]).await,
            Err(ConnectError::Refused)
        ));

        let peer = server();
        assert!(node.endpoint.has_dial_capacity());
        let _dialed = node.endpoint.connect(peer.addr(), &[ALPN]).await.unwrap();
        assert_eq!(node.endpoint.connection_count(), 2);
        assert!(!node.endpoint.has_dial_capacity());
        assert!(matches!(
            node.endpoint.connect(peer.addr(), &[ALPN]).await,
            Err(ConnectError::ConnectionLimit)
        ));
    })
    .await
    .unwrap();
}

/// ADM-12: a dial claims a slot for every address before its first attempt,
/// so with room for fewer right now it fails before the peer sees anything. A
/// cap hit partway would mix a local limit with the peer's real results.
#[tokio::test]
async fn a_dial_without_a_slot_for_every_address_contacts_no_one() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let other = server();
        let server = server();
        let silent = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = NodeAddr::with_addrs(
            server.endpoint.local_id(),
            vec![
                server.endpoint.local_addrs()[0],
                silent.local_addr().unwrap(),
            ],
        );

        // Two slots, one held by a connection to another peer.
        let two_slots = QuicBindConfig {
            max_connections: 2,
            ..loopback()
        };
        let client =
            QuicEndpoint::bind(NodeSecretKey::generate(), &two_slots, &test_config()).unwrap();
        let held = client.connect(other.addr(), &[ALPN]).await.unwrap();
        assert!(matches!(
            client.connect(addr.clone(), &[ALPN]).await,
            Err(ConnectError::ConnectionLimit)
        ));
        assert_eq!(client.connection_count(), 1);
        // The first address would have answered at once; give it time to show.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(server.seen.lock().unwrap().is_empty());

        // Once the slot frees, the same dial connects.
        drop(held);
        while client.connection_count() > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        client.connect(addr, &[ALPN]).await.unwrap();
    })
    .await
    .unwrap();
}

/// ADM-12: an endpoint with no connection slot could neither dial nor
/// accept, so `bind` refuses it.
#[tokio::test]
async fn bind_refuses_a_zero_connection_cap() {
    let bind = QuicBindConfig {
        max_connections: 0,
        ..loopback()
    };
    assert!(matches!(
        QuicEndpoint::bind(NodeSecretKey::generate(), &bind, &test_config()),
        Err(crate::BindError::NoConnectionSlots)
    ));
}

/// ADM-12: a dial tries at most as many addresses as the table can ever
/// hold, so a record with more addresses than slots still dials instead of
/// failing with `ConnectionLimit` forever.
#[tokio::test]
async fn a_dial_tries_no_more_addresses_than_the_table_holds() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let server = server();
        let silent = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = NodeAddr::with_addrs(
            server.endpoint.local_id(),
            vec![
                server.endpoint.local_addrs()[0],
                silent.local_addr().unwrap(),
            ],
        );
        let one_slot = QuicBindConfig {
            max_connections: 1,
            ..loopback()
        };
        let client =
            QuicEndpoint::bind(NodeSecretKey::generate(), &one_slot, &test_config()).unwrap();
        client.connect(addr, &[ALPN]).await.unwrap();
    })
    .await
    .unwrap();
}

/// A UDP relay that delays every datagram by `delay` in each direction.
struct DelayRelay {
    addr: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for DelayRelay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Starts a relay to `server` that adds `delay` each way.
async fn delay_relay(server: SocketAddr, delay: Duration) -> DelayRelay {
    let front = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let back = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let addr = front.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut client = None;
        let (mut up, mut down) = (vec![0; 65_536], vec![0; 65_536]);
        loop {
            let (data, to, socket) = tokio::select! {
                Ok((len, from)) = front.recv_from(&mut up) => {
                    client = Some(from);
                    (up[..len].to_vec(), server, back.clone())
                }
                Ok((len, _)) = back.recv_from(&mut down) => {
                    let Some(client) = client else { continue };
                    (down[..len].to_vec(), client, front.clone())
                }
            };
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let _ = socket.send_to(&data, to).await;
            });
        }
    });
    DelayRelay { addr, task }
}

/// Connects to `server` through a relay that makes three PTOs last longer
/// than `bound`, so a closed connection visibly stays in its table, and
/// returns the server's side of the connection.
async fn slow_closing_conn(
    server: &mut Server,
    client: &QuicEndpoint,
    bound: Duration,
) -> (DelayRelay, Conn, Conn) {
    let relay = delay_relay(server.endpoint.local_addrs()[0], Duration::from_millis(750)).await;
    let via_relay = NodeAddr::with_addrs(server.endpoint.local_id(), vec![relay.addr]);
    let conn = client.connect(via_relay, &[ALPN]).await.unwrap();
    let inbound = server.handled.recv().await.unwrap();
    let rtt = inbound.stats().await.unwrap().connection.path.rtt;
    assert!(rtt * 3 > bound, "{rtt:?}");
    (relay, conn, inbound)
}

/// ADM-13: a closed connection still draining moves to the closing allowance,
/// so a new inbound peer gets the live slot it held.
#[tokio::test]
async fn a_draining_connection_does_not_block_inbound() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let bind = QuicBindConfig {
            max_connections: 1,
            max_closing_connections: 4,
            ..loopback()
        };
        let mut server = server_with(&test_config(), &bind, &[ALPN], |_| Admit::Accept);
        let slow = client();
        let (_relay, _conn, inbound) =
            slow_closing_conn(&mut server, &slow, Duration::from_secs(4)).await;
        assert_eq!(server.endpoint.connection_count(), 1);

        inbound.close(VarInt::from_u32(0), b"done");
        while server.endpoint.closing_connection_count() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(server.endpoint.connection_count(), 0);

        let fresh = client();
        fresh.connect(server.addr(), &[ALPN]).await.unwrap();
        // The first connection is still draining.
        assert_eq!(server.endpoint.closing_connection_count(), 1);
    })
    .await
    .unwrap();
}

/// ADM-13: when the closing allowance is full, a connection that closes drops
/// its state at once instead of draining.
#[tokio::test]
async fn a_full_closing_allowance_drops_state_at_once() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let bind = QuicBindConfig {
            max_closing_connections: 0,
            ..loopback()
        };
        let mut server = server_with(&test_config(), &bind, &[ALPN], |_| Admit::Accept);
        let slow = client();
        let bound = Duration::from_secs(1);
        let (_relay, _conn, inbound) = slow_closing_conn(&mut server, &slow, bound).await;

        inbound.close(VarInt::from_u32(0), b"done");
        tokio::time::timeout(bound, async {
            while server.endpoint.connection_count() > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the connection skips its drain");
        assert_eq!(server.endpoint.closing_connection_count(), 0);
    })
    .await
    .unwrap();
}

/// Silent multi-address discovery attempts leave a live slot for an inbound
/// peer, while maintained dials can still use the transport's dial reserve.
#[tokio::test]
async fn discovery_dials_leave_room_for_an_inbound_peer() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let bind = QuicBindConfig {
            max_connections: 6,
            dial_reserve: 2,
            ..loopback()
        };
        let mut config = test_config();
        config.handshake_timeout_secs = Some(10);
        let mut node = server_with(&config, &bind, &[ALPN], |_| Admit::Accept);
        let peer = server();
        let _maintained = node.endpoint.connect(peer.addr(), &[ALPN]).await.unwrap();

        let silent: Vec<_> = (0..2)
            .map(|_| std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
            .collect();
        let target = NodeAddr::with_addrs(
            NodeSecretKey::generate().public(),
            silent.iter().map(|socket| socket.local_addr().unwrap()),
        );
        let quic = node.endpoint.clone();
        let discovery = tokio::spawn(async move { quic.connect_discovery(target, &[ALPN]).await });
        while node.endpoint.connection_count() < 3 {
            tokio::task::yield_now().await;
        }
        assert!(!node.endpoint.has_discovery_capacity());
        assert!(node.endpoint.has_dial_capacity());
        assert!(matches!(
            node.endpoint.connect_discovery(peer.addr(), &[ALPN]).await,
            Err(ConnectError::ConnectionLimit)
        ));
        assert_eq!(node.endpoint.connection_count(), 3);

        let inbound = client();
        let _accepted = inbound.connect(node.addr(), &[ALPN]).await.unwrap();
        node.handled.recv().await.unwrap();
        assert_eq!(node.endpoint.connection_count(), 4);

        let _reserved = node.endpoint.connect(peer.addr(), &[ALPN]).await.unwrap();
        assert_eq!(node.endpoint.connection_count(), 5);
        discovery.abort();
        let _ = discovery.await;
    })
    .await
    .unwrap();
}

/// Discovery caps its address list to its own ceiling, and a ceiling with no
/// room returns a local limit without contacting the peer or panicking.
#[tokio::test]
async fn discovery_handles_small_connection_limits() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        for (cap, reserve, can_discover) in [(1, 0, false), (2, 1, false), (3, 1, true)] {
            let bind = QuicBindConfig {
                max_connections: cap,
                dial_reserve: reserve,
                ..loopback()
            };
            let node =
                QuicEndpoint::bind(NodeSecretKey::generate(), &bind, &test_config()).unwrap();
            let peer = server();
            let silent = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let target = NodeAddr::with_addrs(
                peer.endpoint.local_id(),
                vec![peer.endpoint.local_addrs()[0], silent.local_addr().unwrap()],
            );
            assert_eq!(node.has_discovery_capacity(), can_discover);
            let result = node.connect_discovery(target, &[ALPN]).await;
            if can_discover {
                let held = result.unwrap();
                assert_eq!(node.connection_count(), 1);
                assert!(!node.has_discovery_capacity());
                drop(held);
            } else {
                assert!(matches!(result, Err(ConnectError::ConnectionLimit)));
                assert_eq!(node.connection_count(), 0);
                assert!(peer.seen.lock().unwrap().is_empty());
                // A discovery ceiling of zero doesn't disable maintained dials.
                node.connect(peer.addr(), &[ALPN]).await.unwrap();
            }
        }
    })
    .await
    .unwrap();
}

/// Cancelling discovery returns unused reservations and drains started
/// attempts, both before and after the staggered second attempt starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_discovery_dials_restore_every_slot() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        for started in [1, 2] {
            let bind = QuicBindConfig {
                max_connections: 5,
                dial_reserve: 1,
                max_closing_connections: 5,
                ..loopback()
            };
            let config = QuicConfig {
                dial_stagger_ms: if started == 1 { 5000 } else { 25 },
                handshake_timeout_secs: Some(10),
                ..test_config()
            };
            let node = QuicEndpoint::bind(NodeSecretKey::generate(), &bind, &config).unwrap();
            let peer = server();
            let retained = node.connect(peer.addr(), &[ALPN]).await.unwrap();
            let first = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let second = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let target = NodeAddr::with_addrs(
                NodeSecretKey::generate().public(),
                [first.local_addr().unwrap(), second.local_addr().unwrap()],
            );
            let quic = node.clone();
            let dial = tokio::spawn(async move { quic.connect_discovery(target, &[ALPN]).await });
            let mut packet = [0; 65_536];
            first.recv_from(&mut packet).await.unwrap();
            if started == 2 {
                second.recv_from(&mut packet).await.unwrap();
            }
            // Both addresses reserve slots before either sends an Initial.
            assert_eq!(node.connection_count(), 3);
            assert!(!node.has_discovery_capacity());
            dial.abort();
            assert!(dial.await.unwrap_err().is_cancelled());

            tokio::time::timeout(Duration::from_secs(5), async {
                while node.connection_count() != 1 || node.closing_connection_count() != 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("cancelled attempts release every slot while the retained peer stays live");
            assert!(node.has_discovery_capacity());
            assert!(retained.stats().await.is_some());
            if started == 1 {
                assert!(matches!(
                    second.try_recv_from(&mut packet),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
                ));
            }

            // This dial needs both refunded slots, even though its first
            // address answers before the second address needs to finish.
            let replacement = NodeAddr::with_addrs(
                peer.endpoint.local_id(),
                [peer.endpoint.local_addrs()[0], second.local_addr().unwrap()],
            );
            let _connected = node.connect_discovery(replacement, &[ALPN]).await.unwrap();
        }
    })
    .await
    .unwrap();
}

/// An endpoint on one simulated socket at `addr`.
fn sim_endpoint(net: &SimNet, addr: SocketAddr) -> QuicEndpoint {
    let bind = QuicBindConfig {
        addrs: vec![addr],
        ..loopback()
    };
    QuicEndpoint::bind_with_factory(
        NodeSecretKey::generate(),
        &bind,
        &test_config(),
        net.clone(),
    )
    .unwrap()
}

/// A simulated network whose links take 1 to 5 ms and may reorder.
fn sim_net() -> SimNet {
    SimNet::new(LinkModel {
        min_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(5),
        reorder: true,
        loss_budget: 0,
    })
}

/// Delivers every eligible datagram once per simulated millisecond. Links
/// from [`sim_net`] take at least 1 ms, so each datagram arrives inside its
/// window, and the assertion checks that time never passed a deadline.
async fn pump(net: SimNet) {
    loop {
        while let Some(id) = net.deliverable().first().copied() {
            net.deliver(id).unwrap();
        }
        assert!(net.can_advance(), "a datagram passed its deadline");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Bounds a simulation test in virtual time. It leaves room for the 91 s the
/// rebind test sleeps on purpose, and a hang still fails within seconds of
/// wall time, because paused time jumps ahead while the test waits.
const SIM_TEST_TIMEOUT: Duration = Duration::from_secs(120);

/// `10.0.0.n:4000`.
fn sim_addr(n: u8) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::new(10, 0, 0, n), 4000))
}

/// Tasks alive on the current runtime.
fn alive_tasks() -> usize {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks()
}

/// M2.6: the real driver handshakes and echoes over `SimNet`. Simulated
/// sockets hold no OS handle, and binding one starts only its endpoint task:
/// no kernel drop poller and no interface watcher. A production bind does
/// start them.
#[tokio::test(start_paused = true)]
async fn the_driver_runs_over_simnet_without_os_sockets_or_pollers() {
    tokio::time::timeout(SIM_TEST_TIMEOUT, async {
        let net = sim_net();
        let pump = tokio::spawn(pump(net.clone()));
        let before = alive_tasks();
        let server_endpoint = sim_endpoint(&net, sim_addr(1));
        assert_eq!(alive_tasks(), before + 1, "only the endpoint task");
        let mut server = serve_test_acceptor(server_endpoint, &[ALPN], |_| Admit::Accept);
        let client = sim_endpoint(&net, sim_addr(2));

        let conn = client.connect(server.addr(), &[ALPN]).await.unwrap();
        assert_eq!(conn.remote_id(), server.endpoint.local_id());
        assert_eq!(echo(&conn, b"over the sim").await, b"over the sim");
        let inbound = server.handled.recv().await.unwrap();
        assert_eq!(inbound.remote_id(), client.local_id());
        assert_eq!(inbound.admitted_ip(), sim_addr(2).ip());

        let stats = &server.endpoint.socket_stats()[0];
        assert_eq!(stats.buffers, SocketBuffers::default());
        assert!(!stats.gso);
        drop((conn, inbound));
        client.shutdown().await;
        server.endpoint.shutdown().await;
        pump.abort();

        let events = net.events();
        assert!(events
            .iter()
            .any(|event| matches!(event, NetEvent::Delivered { .. })));
        assert!(!events
            .iter()
            .any(|event| matches!(event, NetEvent::Dropped { .. })));
        assert!(events.contains(&NetEvent::Unbound {
            addr: sim_addr(1),
            unread: 0
        }));

        // A production wildcard bind adds the interface watcher, and on
        // Linux the kernel drop poller, beside its endpoint task.
        let before = alive_tasks();
        let _os =
            QuicEndpoint::bind(NodeSecretKey::generate(), &wildcard(), &test_config()).unwrap();
        assert!(alive_tasks() >= before + 2);
    })
    .await
    .expect("the simulation finishes within its virtual-time bound");
}

/// Injects a fatal receive error into `addr` and waits until the endpoint
/// has rebound or stopped.
async fn fail_and_wait(net: &SimNet, endpoint: &QuicEndpoint, addr: SocketAddr) -> SocketStats {
    let rebinds = endpoint.socket_stats()[0].rebinds;
    assert!(net.fail_next_recv(addr, std::io::ErrorKind::Other));
    loop {
        tokio::time::sleep(Duration::from_millis(1)).await;
        let stats = endpoint.socket_stats()[0];
        if stats.rebinds > rebinds || stats.stopped {
            return stats;
        }
    }
}

/// M2.5: a SOCK-9 rebind comes from the injected factory and measures its
/// 60 s window in virtual time. Under `std::time::Instant` the 61 s case
/// would see almost no elapsed time and stop the socket.
#[tokio::test(start_paused = true)]
async fn simnet_rebinds_use_the_factory_and_virtual_time() {
    tokio::time::timeout(SIM_TEST_TIMEOUT, async {
        let net = sim_net();
        let pump = tokio::spawn(pump(net.clone()));
        let mut server =
            serve_test_acceptor(sim_endpoint(&net, sim_addr(1)), &[ALPN], |_| Admit::Accept);
        let client = sim_endpoint(&net, sim_addr(2));
        let conn = client.connect(server.addr(), &[ALPN]).await.unwrap();
        let _inbound = server.handled.recv().await.unwrap();

        let stats = fail_and_wait(&net, &server.endpoint, sim_addr(1)).await;
        assert_eq!((stats.rebinds, stats.stopped), (1, false));
        let rebind = [
            NetEvent::Unbound {
                addr: sim_addr(1),
                unread: 0,
            },
            NetEvent::Bound { addr: sim_addr(1) },
        ];
        assert!(net.events().windows(2).any(|pair| pair == rebind));
        // The connection lives through the rebind.
        assert_eq!(echo(&conn, b"after the rebind").await, b"after the rebind");

        tokio::time::sleep(Duration::from_secs(61)).await;
        let stats = fail_and_wait(&net, &server.endpoint, sim_addr(1)).await;
        assert_eq!((stats.rebinds, stats.stopped), (2, false));

        tokio::time::sleep(Duration::from_secs(30)).await;
        let stats = fail_and_wait(&net, &server.endpoint, sim_addr(1)).await;
        assert_eq!((stats.rebinds, stats.stopped), (2, true));

        pump.abort();
    })
    .await
    .expect("the simulation finishes within its virtual-time bound");
}
