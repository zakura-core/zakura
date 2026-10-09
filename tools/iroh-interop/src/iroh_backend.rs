//! Iroh backend for the probe agent.
//!
//! The main workspace compiles this file against `zakura-iroh` 1.1.0-rc.1 (the
//! Iroh backend of the previous Zakura release). `upstream/` compiles the same
//! file against the newest released `iroh` 1.x.

use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context as _, Result};
use futures::{future::BoxFuture, FutureExt as _, StreamExt as _};
use iroh::{
    endpoint::{presets, Connection, PathEvent, QuicTransportConfig, VarInt},
    Endpoint, EndpointAddr, PublicKey, RelayMode, SecretKey, TransportAddr,
};

use crate::proto::{emit, to_hex, BoxRecv, BoxSend, NodeConn, NodeEndpoint};

/// Zakura's QUIC limits (crates/zakura-network/src/zakura/handler.rs).
const STREAM_RECEIVE_WINDOW: u32 = 16 * 1024 * 1024;
const RECEIVE_WINDOW: u32 = 32 * 1024 * 1024;
const SEND_WINDOW: u64 = 32 * 1024 * 1024;
const IDLE_TIMEOUT: Duration = Duration::from_secs(150);
const KEEP_ALIVE: Duration = Duration::from_secs(10);
const MAX_OPEN_STREAMS: u32 = 1024;

/// How the Iroh endpoint is configured.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Profile {
    /// Exactly like Zakura's `direct_endpoint_builder` plus
    /// `ZakuraLocalLimits::transport_config` with `nat_traversal = false`.
    Zakura,
    /// Iroh's own transport defaults, relay off, address lookup off.
    IrohDefaults,
}

fn transport_config(profile: Profile, qlog_dir: Option<&PathBuf>) -> QuicTransportConfig {
    let mut builder = QuicTransportConfig::builder();
    if profile == Profile::Zakura {
        builder = builder
            .max_remote_nat_traversal_addresses(0)
            .max_concurrent_bidi_streams(VarInt::from_u32(MAX_OPEN_STREAMS))
            .max_concurrent_uni_streams(VarInt::from_u32(0))
            .stream_receive_window(VarInt::from_u32(STREAM_RECEIVE_WINDOW))
            .receive_window(VarInt::from_u32(RECEIVE_WINDOW))
            .send_window(SEND_WINDOW)
            .max_idle_timeout(Some(IDLE_TIMEOUT.try_into().expect("valid idle timeout")))
            .keep_alive_interval(KEEP_ALIVE)
            .datagram_receive_buffer_size(None)
            .datagram_send_buffer_size(0);
    } else {
        // The probe opens bidirectional streams in both directions.
        builder = builder.max_concurrent_bidi_streams(VarInt::from_u32(MAX_OPEN_STREAMS));
    }
    if let Some(dir) = qlog_dir {
        builder = builder.qlog_from_path(dir, "iroh");
    }
    builder.build()
}

pub struct IrohNode {
    endpoint: Endpoint,
}

impl IrohNode {
    pub async fn bind(
        secret: [u8; 32],
        addrs: &[SocketAddr],
        alpn: Vec<u8>,
        profile: Profile,
        qlog_dir: Option<PathBuf>,
    ) -> Result<Self> {
        let mut builder = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .clear_address_lookup()
            .clear_ip_transports()
            .secret_key(SecretKey::from_bytes(&secret))
            .transport_config(transport_config(profile, qlog_dir.as_ref()))
            .alpns(vec![alpn]);
        // Zakura's bind_native_endpoint: clear, then bind each explicit address.
        builder = builder.clear_ip_transports();
        for addr in addrs {
            builder = builder.bind_addr(*addr)?;
        }
        let endpoint = builder.bind().await.context("bind iroh endpoint")?;
        Ok(Self { endpoint })
    }
}

impl NodeEndpoint for IrohNode {
    fn id(&self) -> String {
        to_hex(self.endpoint.id().as_bytes())
    }

    fn addrs(&self) -> Vec<SocketAddr> {
        self.endpoint.bound_sockets()
    }

    fn dial(
        &self,
        id_hex: String,
        addrs: Vec<SocketAddr>,
        alpn: Vec<u8>,
    ) -> BoxFuture<'_, Result<Arc<dyn NodeConn>>> {
        async move {
            let id = PublicKey::from_bytes(&crate::proto::id_bytes(&id_hex)?)?;
            let mut addr = EndpointAddr::new(id);
            for direct in addrs {
                addr = addr.with_ip_addr(direct);
            }
            let conn = self
                .endpoint
                .connect(addr, &alpn)
                .await
                .map_err(|error| anyhow::anyhow!("connect: {error:?}"))?;
            Ok(IrohConn::wrap(conn) as Arc<dyn NodeConn>)
        }
        .boxed()
    }

    fn accept(&self) -> BoxFuture<'_, Option<Result<Arc<dyn NodeConn>>>> {
        async move {
            let incoming = self.endpoint.accept().await?;
            let result = async {
                let accepting = incoming
                    .accept()
                    .map_err(|error| anyhow::anyhow!("accept: {error:?}"))?;
                let conn = accepting
                    .await
                    .map_err(|error| anyhow::anyhow!("handshake: {error:?}"))?;
                Ok(IrohConn::wrap(conn) as Arc<dyn NodeConn>)
            }
            .await;
            Some(result)
        }
        .boxed()
    }
}

#[derive(Default)]
struct PathLog {
    opened: Vec<String>,
    closed: Vec<String>,
    selected: Vec<String>,
}

pub struct IrohConn {
    conn: Connection,
    log: Arc<Mutex<PathLog>>,
}

fn addr_str(addr: &TransportAddr) -> String {
    match addr {
        TransportAddr::Ip(ip) => ip.to_string(),
        other => format!("{other:?}").replace(' ', ""),
    }
}

impl IrohConn {
    fn wrap(conn: Connection) -> Arc<Self> {
        let log = Arc::new(Mutex::new(PathLog::default()));
        let mut events = conn.path_events();
        let task_log = log.clone();
        let stable = conn.stable_id();
        tokio::spawn(async move {
            while let Some(event) = events.next().await {
                let mut log = task_log.lock().unwrap();
                let line = match &event {
                    PathEvent::Opened {
                        id, remote_addr, ..
                    } => {
                        let entry = format!("{id}@{}", addr_str(remote_addr));
                        log.opened.push(entry.clone());
                        format!("opened {entry}")
                    }
                    PathEvent::Closed {
                        id, remote_addr, ..
                    } => {
                        let entry = format!("{id}@{}", addr_str(remote_addr));
                        log.closed.push(entry.clone());
                        format!("closed {entry}")
                    }
                    PathEvent::Selected {
                        id, remote_addr, ..
                    } => {
                        let entry = format!("{id}@{}", addr_str(remote_addr));
                        log.selected.push(entry.clone());
                        format!("selected {entry}")
                    }
                    other => format!("{other:?}").replace(' ', ""),
                };
                emit(&format!("EVENT iroh_path stable={stable} {line}"));
            }
        });
        Arc::new(Self { conn, log })
    }
}

impl NodeConn for IrohConn {
    fn open_bi(&self) -> BoxFuture<'_, Result<(BoxSend, BoxRecv)>> {
        async move {
            let (send, recv) = self.conn.open_bi().await?;
            Ok((Box::new(send) as BoxSend, Box::new(recv) as BoxRecv))
        }
        .boxed()
    }

    fn accept_bi(&self) -> BoxFuture<'_, Result<(BoxSend, BoxRecv)>> {
        async move {
            let (send, recv) = self.conn.accept_bi().await?;
            Ok((Box::new(send) as BoxSend, Box::new(recv) as BoxRecv))
        }
        .boxed()
    }

    fn remote_id(&self) -> String {
        to_hex(self.conn.remote_id().as_bytes())
    }

    fn alpn(&self) -> String {
        String::from_utf8_lossy(self.conn.alpn()).into_owned()
    }

    fn info(&self) -> String {
        let paths = self.conn.paths();
        let open: Vec<String> = paths
            .iter()
            .map(|path| {
                format!(
                    "{}@{}{}rtt{}ms",
                    path.id(),
                    addr_str(path.remote_addr()),
                    if path.is_selected() { "*" } else { "" },
                    path.rtt().as_millis()
                )
            })
            .collect();
        let log = self.log.lock().unwrap();
        let stats = self.conn.stats();
        format!(
            "side=iroh multipath=n/a max_datagram={:?} paths_open={} paths=[{}] \
             path_opened=[{}] path_closed=[{}] path_selected=[{}] lost_packets={}",
            self.conn.max_datagram_size(),
            paths.len(),
            open.join(","),
            log.opened.join(","),
            log.closed.join(","),
            log.selected.join(","),
            stats.lost_packets,
        )
    }

    fn closed(&self) -> BoxFuture<'static, String> {
        let conn = self.conn.clone();
        async move { conn.closed().await.to_string() }.boxed()
    }

    fn close_reason(&self) -> Option<String> {
        self.conn.close_reason().map(|reason| reason.to_string())
    }
}
