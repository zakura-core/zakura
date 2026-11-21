use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use tokio::sync::mpsc;

const ALPN: &[u8] = b"zakura/compat/1";
const TEST_TIMEOUT: Duration = Duration::from_secs(20);
const PAYLOAD_LEN: usize = 256 * 1024;

/// The same small endpoint API over one version of the crate. Each version's
/// types are distinct, so node ids cross between them as bytes.
macro_rules! endpoint_api {
    ($name:ident, $quic:ident) => {
        mod $name {
            use std::net::SocketAddr;

            use futures::future::BoxFuture;
            use tokio::sync::mpsc;

            use super::{loopback, ALPN, PAYLOAD_LEN};

            pub struct Endpoint(pub $quic::QuicEndpoint);

            /// Accepts every attempt and echoes every stream.
            struct Echo {
                handled: mpsc::UnboundedSender<[u8; 32]>,
            }

            impl $quic::Acceptor for Echo {
                fn admit(&self, _incoming: &$quic::IncomingInfo) -> $quic::Admit {
                    $quic::Admit::Accept
                }

                fn alpns(&self) -> Vec<Vec<u8>> {
                    vec![ALPN.to_vec()]
                }

                fn handle(&self, conn: $quic::Conn) -> BoxFuture<'static, ()> {
                    let _ = self.handled.send(*conn.remote_id().as_bytes());
                    Box::pin(async move {
                        while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                            tokio::spawn(async move {
                                let data = recv.read_to_end(PAYLOAD_LEN).await.unwrap_or_default();
                                let _ = send.write_all(&data).await;
                                let _ = send.finish();
                            });
                        }
                    })
                }
            }

            fn bind() -> $quic::QuicEndpoint {
                let config = $quic::QuicConfig {
                    recv_buffer_bytes: 256 * 1024,
                    send_buffer_bytes: 256 * 1024,
                    handshake_timeout_secs: Some(5),
                    ..$quic::QuicConfig::default()
                };
                let bind = $quic::QuicBindConfig {
                    addrs: vec![loopback()],
                    max_bidi_streams: 16,
                    max_connections: 64,
                    dial_reserve: 0,
                    max_inbound_connections: None,
                    max_closing_connections: 64,
                };
                $quic::QuicEndpoint::bind($quic::NodeSecretKey::generate(), &bind, &config)
                    .expect("loopback has a free port")
            }

            /// A listener, and the ids of the nodes it served.
            pub fn listener() -> (Endpoint, mpsc::UnboundedReceiver<[u8; 32]>) {
                let endpoint = bind();
                let (handled, served) = mpsc::unbounded_channel();
                endpoint
                    .serve(Echo { handled })
                    .expect("the endpoint serves once");
                (Endpoint(endpoint), served)
            }

            pub fn dialer() -> Endpoint {
                Endpoint(bind())
            }

            impl Endpoint {
                pub fn id(&self) -> [u8; 32] {
                    *self.0.local_id().as_bytes()
                }

                pub fn addr(&self) -> SocketAddr {
                    self.0.local_addrs()[0]
                }

                /// Dials `id` at `addr`, checks who answered, and echoes
                /// `payload` over one stream.
                pub async fn dial_and_echo(&self, id: [u8; 32], addr: SocketAddr, payload: &[u8]) {
                    let remote = $quic::NodeId::from_bytes(&id).expect("ids come from real keys");
                    let conn = self
                        .0
                        .connect($quic::NodeAddr::with_addrs(remote, vec![addr]), &[ALPN])
                        .await
                        .expect("the handshake completes");
                    assert_eq!(
                        *conn.remote_id().as_bytes(),
                        id,
                        "the listener is authenticated"
                    );
                    assert_eq!(conn.alpn(), ALPN);
                    let (mut send, mut recv) = conn.open_bi().await.expect("a stream opens");
                    send.write_all(payload).await.expect("the payload is sent");
                    send.finish().expect("the stream finishes");
                    let echoed = recv
                        .read_to_end(PAYLOAD_LEN)
                        .await
                        .expect("the echo arrives");
                    assert_eq!(echoed, payload);
                }
            }
        }
    };
}

endpoint_api!(current, zakura_quic);
endpoint_api!(released, zakura_quic_released);

fn loopback() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

fn payload() -> Vec<u8> {
    // Several packets' worth, so the stream outlives the handshake.
    (0..PAYLOAD_LEN).map(|i| (i % 251) as u8).collect()
}

/// Waits for the listener's acceptor to report the dialer, which shows the
/// listener authenticated it too.
async fn served_by(served: &mut mpsc::UnboundedReceiver<[u8; 32]>) -> [u8; 32] {
    served.recv().await.expect("the listener keeps serving")
}

#[tokio::test]
async fn a_current_dialer_reaches_a_released_listener() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (listener, mut served) = released::listener();
        let dialer = current::dialer();
        dialer
            .dial_and_echo(listener.id(), listener.addr(), &payload())
            .await;
        assert_eq!(served_by(&mut served).await, dialer.id());
    })
    .await
    .expect("the test finishes in time");
}

#[tokio::test]
async fn a_released_dialer_reaches_a_current_listener() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (listener, mut served) = current::listener();
        let dialer = released::dialer();
        dialer
            .dial_and_echo(listener.id(), listener.addr(), &payload())
            .await;
        assert_eq!(served_by(&mut served).await, dialer.id());
    })
    .await
    .expect("the test finishes in time");
}

/// The control pairs, so a failure above points at the version mix rather
/// than at this harness.
#[tokio::test]
async fn same_version_pairs_connect() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (listener, mut served) = current::listener();
        let dialer = current::dialer();
        dialer
            .dial_and_echo(listener.id(), listener.addr(), &payload())
            .await;
        assert_eq!(served_by(&mut served).await, dialer.id());

        let (listener, mut served) = released::listener();
        let dialer = released::dialer();
        dialer
            .dial_and_echo(listener.id(), listener.addr(), &payload())
            .await;
        assert_eq!(served_by(&mut served).await, dialer.id());
    })
    .await
    .expect("the test finishes in time");
}

/// Repeated dials in both directions on the same endpoints: each dial starts
/// a fresh handshake and none falls back to a cached outcome.
#[tokio::test]
async fn repeated_cross_version_dials_keep_connecting() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (old_listener, mut old_served) = released::listener();
        let (new_listener, mut new_served) = current::listener();
        let new_dialer = current::dialer();
        let old_dialer = released::dialer();
        let payload = payload();
        for _ in 0..5 {
            new_dialer
                .dial_and_echo(old_listener.id(), old_listener.addr(), &payload)
                .await;
            assert_eq!(served_by(&mut old_served).await, new_dialer.id());
            old_dialer
                .dial_and_echo(new_listener.id(), new_listener.addr(), &payload)
                .await;
            assert_eq!(served_by(&mut new_served).await, old_dialer.id());
        }
    })
    .await
    .expect("the test finishes in time");
}
