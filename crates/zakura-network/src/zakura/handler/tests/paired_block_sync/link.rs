//! Bounded UDP proxy for the real-QUIC acceptance tests.

use super::*;
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::net::UdpSocket;

const ONE_WAY_DELAY: Duration = Duration::from_millis(25);
const MAX_PENDING_PACKETS: usize = 4096;

pub(super) struct ImpairedLink {
    pub(super) address: SocketAddr,
    response_bytes: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
    _task: AbortOnDropHandle<()>,
}

impl ImpairedLink {
    pub(super) async fn new(server: SocketAddr) -> Result<Self, BoxError> {
        let front = UdpSocket::bind("127.0.0.1:0").await?;
        let back = UdpSocket::bind("127.0.0.1:0").await?;
        let address = front.local_addr()?;
        let response_bytes = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let forwarded = response_bytes.clone();
        let lost = dropped.clone();
        let task = tokio::spawn(async move {
            let mut client = None;
            let mut from_client = [0; 65_536];
            let mut from_server = [0; 65_536];
            let mut queue: VecDeque<(Instant, bool, Vec<u8>)> = VecDeque::new();
            let mut client_packets = 0u64;
            let mut server_packets = 0u64;
            loop {
                let deadline = queue
                    .front()
                    .map_or_else(|| Instant::now() + DEADLINE, |packet| packet.0);
                tokio::select! {
                    biased;
                    () = tokio::time::sleep_until(deadline), if !queue.is_empty() => {
                        let (_, response, bytes) = queue.pop_front().unwrap();
                        if response {
                            if let Some(client) = client {
                                timeout(DEADLINE, front.send_to(&bytes, client)).await.unwrap().unwrap();
                                forwarded.fetch_add(u64::try_from(bytes.len()).unwrap(), Ordering::Relaxed);
                            }
                        } else {
                            timeout(DEADLINE, back.send_to(&bytes, server)).await.unwrap().unwrap();
                        }
                    }
                    received = front.recv_from(&mut from_client), if queue.len() < MAX_PENDING_PACKETS => {
                        let (len, source) = received.unwrap();
                        client = Some(source);
                        client_packets += 1;
                        // Deterministic 1% loss separately in each direction.
                        if client_packets.is_multiple_of(100) {
                            lost.fetch_add(1, Ordering::Relaxed);
                        } else {
                            queue.push_back((Instant::now() + ONE_WAY_DELAY, false, from_client[..len].to_vec()));
                        }
                    }
                    received = back.recv_from(&mut from_server), if queue.len() < MAX_PENDING_PACKETS => {
                        let (len, source) = received.unwrap();
                        assert_eq!(source, server);
                        server_packets += 1;
                        if server_packets.is_multiple_of(100) {
                            lost.fetch_add(1, Ordering::Relaxed);
                        } else {
                            queue.push_back((Instant::now() + ONE_WAY_DELAY, true, from_server[..len].to_vec()));
                        }
                    }
                }
            }
        });
        Ok(Self {
            address,
            response_bytes,
            dropped,
            _task: AbortOnDropHandle::new(task),
        })
    }

    pub(super) fn verify_path(&self, connection: &Connection, useful_bytes: u64) {
        let paths = connection.paths();
        let selected = paths
            .iter()
            .find(|path| path.is_selected())
            .expect("the active download connection has a selected path");
        assert_eq!(
            selected.remote_addr(),
            &iroh::TransportAddr::Ip(self.address),
            "iroh must not migrate around the impaired link"
        );
        assert!(
            self.response_bytes.load(Ordering::Relaxed) >= useful_bytes,
            "all useful response bytes crossed the proxy"
        );
        assert!(
            self.dropped.load(Ordering::Relaxed) > 0,
            "the test exercised packet loss"
        );
    }

    pub(super) fn counters(&self) -> (u64, u64) {
        (
            self.response_bytes.load(Ordering::Relaxed),
            self.dropped.load(Ordering::Relaxed),
        )
    }
}
