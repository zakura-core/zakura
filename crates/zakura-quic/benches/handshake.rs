//! What the post-quantum handshake costs a node (TLS-4, TLS-9, ID-13).
//!
//! - `handshake/loopback`: elapsed time from calling `connect` until the
//!   dialer returns an authenticated connection over UDP loopback. The
//!   listener runs in the same process and may still be verifying the dialer
//!   when the timer stops. Loopback does not model a wide-area network delay.
//! - `key/load`: loading a node key, which derives its ML-DSA key pair from
//!   the seed once.
//!
//! The bench uses only the crate's public API, so the same file measures any
//! release for comparison. `docs/design/pq-handshake-parameters.md` records
//! the results.

use std::{
    net::{Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};

use criterion::{criterion_group, criterion_main, Criterion};
use futures::future::BoxFuture;
use zakura_quic::{
    Acceptor, Admit, Conn, IncomingInfo, NodeAddr, NodeSecretKey, QuicBindConfig, QuicConfig,
    QuicEndpoint, VarInt,
};

const ALPN: &[u8] = b"zakura-bench/1";

/// Accepts every dial and holds each connection until its dialer closes it.
struct AcceptAll;

impl Acceptor for AcceptAll {
    fn admit(&self, _: &IncomingInfo) -> Admit {
        Admit::Accept
    }

    fn alpns(&self) -> Vec<Vec<u8>> {
        vec![ALPN.to_vec()]
    }

    fn handle(&self, conn: Conn) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            conn.closed().await;
        })
    }
}

/// A loopback endpoint with room for every connection a run opens and closes.
fn endpoint() -> QuicEndpoint {
    let bind = QuicBindConfig {
        addrs: vec![SocketAddr::from((Ipv4Addr::LOCALHOST, 0))],
        max_bidi_streams: 8,
        max_connections: 8_192,
        dial_reserve: 0,
        max_inbound_connections: None,
        max_closing_connections: 8_192,
    };
    QuicEndpoint::bind(NodeSecretKey::generate(), &bind, &QuicConfig::default())
        .expect("a loopback endpoint binds")
}

fn handshake(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("the bench runtime builds");
    let (server, client) = runtime.block_on(async {
        let server = endpoint();
        server.serve(AcceptAll).expect("the listener serves");
        (server, endpoint())
    });
    let target = NodeAddr::with_addrs(server.local_id(), server.local_addrs());

    let mut group = c.benchmark_group("handshake");
    group.measurement_time(Duration::from_secs(10));
    group.bench_function("loopback", |b| {
        b.iter_custom(|iterations| {
            runtime.block_on(async {
                let mut total = Duration::ZERO;
                for _ in 0..iterations {
                    let start = Instant::now();
                    let conn = client
                        .connect(target.clone(), &[ALPN])
                        .await
                        .expect("a loopback dial completes");
                    total += start.elapsed();
                    conn.close(VarInt::from_u32(0), b"bench");
                }
                total
            })
        })
    });
    group.finish();
}

fn key_load(c: &mut Criterion) {
    c.bench_function("key/load", |b| {
        b.iter(|| NodeSecretKey::from_bytes(std::hint::black_box(&[7; 32])))
    });
}

criterion_group!(benches, handshake, key_load);
criterion_main!(benches);
