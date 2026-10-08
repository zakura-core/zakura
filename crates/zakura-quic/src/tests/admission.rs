//! Exercise admission through real sockets, including state after application close.

use super::*;

fn limited_bind(total: usize, inbound: usize) -> QuicBindConfig {
    QuicBindConfig {
        max_connections: total,
        max_inbound_connections: inbound,
        ..loopback()
    }
}

fn open_transports(endpoint: &QuicEndpoint) -> usize {
    endpoint
        .noq_endpoints()
        .iter()
        .map(noq::Endpoint::open_connections)
        .sum()
}

async fn wait_for_transports(endpoint: &QuicEndpoint, count: usize) {
    while open_transports(endpoint) != count {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unread_stream_holds_capacity_after_transport_drain() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let server = server();
        let client = QuicEndpoint::bind(
            NodeSecretKey::generate(),
            &limited_bind(1, 1),
            &test_config(),
        )
        .unwrap();
        let conn = client.connect(server.addr(), ALPN).await.unwrap();
        let weak = conn.noq().weak_handle();
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        send.write_all(&vec![9; 16 * 1024]).await.unwrap();
        send.finish().unwrap();
        // Wait for actual response bytes, leaving the rest unread.
        recv.read_exact(&mut [0u8; 1]).await.unwrap();
        conn.close(0u32.into(), b"retain unread response");
        drop((conn, send));
        wait_for_transports(&client, 0).await;
        assert!(weak.is_alive());
        assert!(matches!(
            client.connect(server.addr(), ALPN).await,
            Err(ConnectError::Capacity)
        ));
        drop(recv);
        while weak.is_alive() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Permit reclamation polls weak ownership without keeping the transport alive.
        let recovered = loop {
            match client.connect(server.addr(), ALPN).await {
                Err(ConnectError::Capacity) => tokio::time::sleep(Duration::from_millis(10)).await,
                result => break result.unwrap(),
            }
        };
        drop(recovered);
        client.shutdown().await;
        server.endpoint.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_inbound_handshake_stays_charged_through_drain() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let config = QuicConfig {
            handshake_timeout_secs: Some(2),
            retry_threshold: None,
            ..test_config()
        };
        let server = server_with(&config, &limited_bind(1, 1), &[ALPN], |_| Admit::Accept);
        let stalled = stalled_handshake(&server).await;
        wait_for_attempts(&server, 1).await;
        wait_for_transports(&server.endpoint, 1).await;
        tokio::time::sleep(Duration::from_millis(2100)).await;
        assert_eq!(open_transports(&server.endpoint), 1);
        let client = client();
        assert!(matches!(
            client.connect(server.addr(), ALPN).await,
            Err(ConnectError::Refused)
        ));
        // The pending handshake ended, but its transport table entry still blocks reuse.
        assert_eq!(server.seen.lock().unwrap().last().unwrap().pending_total, 0);
        wait_for_transports(&server.endpoint, 0).await;
        // The owner permit follows once the handshake task sees the state freed.
        while server.endpoint.held_owners() > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let conn = client.connect(server.addr(), ALPN).await.unwrap();
        drop((conn, stalled));
        client.shutdown().await;
        server.endpoint.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_and_timed_out_address_races_keep_transport_capacity() {
    tokio::time::timeout(Duration::from_secs(40), async {
        for cancel in [true, false] {
            let config = QuicConfig {
                handshake_timeout_secs: Some(2),
                dial_stagger_ms: 0,
                ..test_config()
            };
            let client =
                QuicEndpoint::bind(NodeSecretKey::generate(), &limited_bind(2, 1), &config)
                    .unwrap();
            let blackholes = [
                tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap(),
                tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap(),
            ];
            let target = NodeAddr::with_addrs(
                NodeSecretKey::generate().public(),
                blackholes.iter().map(|s| s.local_addr().unwrap()),
            );
            let dialer = client.clone();
            let dial = tokio::spawn(async move { dialer.connect(target, ALPN).await });
            wait_for_transports(&client, 2).await;
            if cancel {
                dial.abort();
                assert!(dial.await.unwrap_err().is_cancelled());
            } else {
                assert!(matches!(
                    dial.await.unwrap(),
                    Err(ConnectError::HandshakeTimeout)
                ));
            }
            assert_eq!(open_transports(&client), 2);
            let server = server();
            assert!(matches!(
                client.connect(server.addr(), ALPN).await,
                Err(ConnectError::Capacity)
            ));
            wait_for_transports(&client, 0).await;
            let recovered = client.connect(server.addr(), ALPN).await.unwrap();
            drop((recovered, blackholes));
            client.shutdown().await;
            server.endpoint.shutdown().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_sockets_preserve_an_outbound_slot() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut bind = limited_bind(4, 3);
        bind.addrs.push("[::1]:0".parse().unwrap());
        let server = server_with(&test_config(), &bind, &[ALPN], |_| Admit::Accept);
        let clients: Vec<_> = (0..12)
            .map(|_| {
                let mut bind = loopback();
                bind.addrs.push("[::1]:0".parse().unwrap());
                QuicEndpoint::bind(NodeSecretKey::generate(), &bind, &test_config()).unwrap()
            })
            .collect();
        let addresses = server.endpoint.local_addrs();
        let results = futures::future::join_all(clients.iter().enumerate().map(|(i, client)| {
            client.connect(
                NodeAddr::with_addrs(server.endpoint.local_id(), [addresses[i % 2]]),
                ALPN,
            )
        }))
        .await;
        let accepted: Vec<_> = results.into_iter().filter_map(Result::ok).collect();
        assert_eq!(accepted.len(), 3);
        assert_eq!(open_transports(&server.endpoint), 3);
        let outbound = super::server();
        let outgoing = server
            .endpoint
            .connect(outbound.addr(), ALPN)
            .await
            .unwrap();
        assert_eq!(open_transports(&server.endpoint), 4);
        assert!(matches!(
            server.endpoint.connect(outbound.addr(), ALPN).await,
            Err(ConnectError::Capacity)
        ));
        drop((accepted, outgoing));
        for client in clients {
            client.shutdown().await;
        }
        server.endpoint.shutdown().await;
        outbound.endpoint.shutdown().await;
    })
    .await
    .unwrap();
}

/// A failed handshake keeps its IP charge and owner permit until noq frees the
/// attempt, and no longer: well before the 52 s worst-case drain (ADM-7).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_handshake_charge_ends_when_noq_frees_it() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let config = QuicConfig {
            handshake_timeout_secs: Some(2),
            retry_threshold: None,
            ..test_config()
        };
        let server = server_with(&config, &loopback(), &[ALPN], |_| Admit::Accept);
        let ip = server.endpoint.local_addrs()[0].ip();
        let stalled = stalled_handshake(&server).await;
        wait_for_attempts(&server, 1).await;
        while server.endpoint.pending(ip).0 > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let failed = std::time::Instant::now();
        loop {
            // The owner permit drops after the IP charge, so read it first.
            let owners = server.endpoint.held_owners();
            if server.endpoint.pending(ip).1 == 0 {
                break;
            }
            assert_eq!(owners, 1, "a charged failed attempt keeps its owner permit");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(open_transports(&server.endpoint), 0);
        assert!(failed.elapsed() < Duration::from_secs(15));
        while server.endpoint.held_owners() > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drop(stalled);
        server.endpoint.shutdown().await;
    })
    .await
    .unwrap();
}

/// An IP stays charged for its failed and closed attempts while their state may
/// remain, so it cannot fill the inbound share; another IP still connects
/// (ADM-7, ADM-8).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_and_closed_attempts_stay_charged_to_their_ip() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let config = QuicConfig {
            handshake_timeout_secs: Some(2),
            retry_threshold: None,
            ..test_config()
        };
        let mut bind = limited_bind(4, 3);
        bind.addrs.push("[::1]:0".parse().unwrap());
        // Zakura's acceptor applies its per-IP limit to this count (ADM-3 rule 2).
        let mut server = server_with(&config, &bind, &[ALPN], |info| {
            if info.pending_from_ip >= 2 {
                Admit::Refuse
            } else {
                Admit::Accept
            }
        });
        let id = server.endpoint.local_id();
        let addrs = server.endpoint.local_addrs();
        let v4 = *addrs.iter().find(|addr| addr.is_ipv4()).unwrap();
        let v6 = *addrs.iter().find(|addr| addr.is_ipv6()).unwrap();
        // This handshake fails at its deadline, after noq accepted it.
        let stalled = stalled_handshake(&server).await;
        wait_for_attempts(&server, 1).await;
        tokio::time::sleep(Duration::from_millis(2100)).await;
        while server.endpoint.pending(v4.ip()).0 > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(server.endpoint.pending(v4.ip()), (0, 1));
        let attacker = client();
        let conn = attacker
            .connect(NodeAddr::with_addrs(id, [v4]), ALPN)
            .await
            .unwrap();
        // The server's retained handle keeps the closed connection's state alive.
        let retained = server.handled.recv().await.unwrap();
        conn.close(0u32.into(), b"churn");
        retained.closed().await;
        while server.endpoint.pending(v4.ip()).1 < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(matches!(
            attacker.connect(NodeAddr::with_addrs(id, [v4]), ALPN).await,
            Err(ConnectError::Refused)
        ));
        let seen = *server.seen.lock().unwrap().last().unwrap();
        assert_eq!((seen.pending_total, seen.pending_from_ip), (0, 2));

        let honest = QuicEndpoint::bind(
            NodeSecretKey::generate(),
            &QuicBindConfig {
                addrs: vec![SocketAddr::new(v6.ip(), 0)],
                ..loopback()
            },
            &test_config(),
        )
        .unwrap();
        let honest_conn = honest
            .connect(NodeAddr::with_addrs(id, [v6]), ALPN)
            .await
            .unwrap();
        drop((honest_conn, conn, retained, stalled));
        honest.shutdown().await;
        attacker.shutdown().await;
        server.endpoint.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outbound_connections_can_use_entire_budget() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let server = server();
        let client = QuicEndpoint::bind(
            NodeSecretKey::generate(),
            &limited_bind(4, 3),
            &test_config(),
        )
        .unwrap();
        let mut connections = Vec::new();
        for _ in 0..4 {
            connections.push(client.connect(server.addr(), ALPN).await.unwrap());
        }
        assert_eq!(open_transports(&client), 4);
        assert!(matches!(
            client.connect(server.addr(), ALPN).await,
            Err(ConnectError::Capacity)
        ));
        drop(connections);
        client.shutdown().await;
        server.endpoint.shutdown().await;
    })
    .await
    .unwrap();
}
