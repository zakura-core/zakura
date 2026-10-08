//! zakura-quic backend for the probe agent.

use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Result};
use futures::{future::BoxFuture, FutureExt as _, StreamExt as _};
use noq::PathEvent;
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use zakura_quic::{
    Acceptor, Admit, Conn, IncomingInfo, NodeAddr, NodeId, NodeSecretKey, PathId, QuicBindConfig,
    QuicConfig, QuicEndpoint,
};

use crate::proto::{emit, to_hex, BoxRecv, BoxSend, NodeConn, NodeEndpoint};

struct ProbeAcceptor {
    alpns: Vec<Vec<u8>>,
    handled: mpsc::UnboundedSender<Conn>,
}

impl Acceptor for ProbeAcceptor {
    fn admit(&self, _incoming: &IncomingInfo) -> Admit {
        Admit::Accept
    }

    fn alpns(&self) -> Vec<Vec<u8>> {
        self.alpns.clone()
    }

    fn handle(&self, conn: Conn) -> BoxFuture<'static, ()> {
        let _ = self.handled.send(conn);
        async {}.boxed()
    }
}

pub struct QuicNode {
    endpoint: QuicEndpoint,
    handled: AsyncMutex<mpsc::UnboundedReceiver<Conn>>,
}

impl QuicNode {
    pub fn bind(
        secret: [u8; 32],
        addrs: &[SocketAddr],
        alpn: Vec<u8>,
        qlog_dir: Option<PathBuf>,
    ) -> Result<Self> {
        let config = QuicConfig {
            qlog_dir,
            ..QuicConfig::default()
        };
        let bind = QuicBindConfig {
            addrs: addrs.to_vec(),
            max_bidi_streams: 1024,
            max_connections: 256,
            max_inbound_connections: 224,
            max_draining_connections: 512,
        };
        let endpoint = QuicEndpoint::bind(NodeSecretKey::from_bytes(&secret), &bind, &config)?;
        let (tx, rx) = mpsc::unbounded_channel();
        endpoint.serve(ProbeAcceptor {
            alpns: vec![alpn],
            handled: tx,
        })?;
        Ok(Self {
            endpoint,
            handled: AsyncMutex::new(rx),
        })
    }
}

impl NodeEndpoint for QuicNode {
    fn id(&self) -> String {
        to_hex(self.endpoint.local_id().as_bytes())
    }

    fn addrs(&self) -> Vec<SocketAddr> {
        self.endpoint.local_addrs()
    }

    fn dial(
        &self,
        id_hex: String,
        addrs: Vec<SocketAddr>,
        alpn: Vec<u8>,
    ) -> BoxFuture<'_, Result<Arc<dyn NodeConn>>> {
        async move {
            let id = NodeId::from_bytes(&crate::proto::id_bytes(&id_hex)?)?;
            let conn = self
                .endpoint
                .connect(NodeAddr::with_addrs(id, addrs), &alpn)
                .await
                .map_err(|error| anyhow!("connect: {error} ({error:?})"))?;
            Ok(QuicConn::wrap(conn) as Arc<dyn NodeConn>)
        }
        .boxed()
    }

    fn accept(&self) -> BoxFuture<'_, Option<Result<Arc<dyn NodeConn>>>> {
        async move {
            let conn = self.handled.lock().await.recv().await?;
            Some(Ok(QuicConn::wrap(conn) as Arc<dyn NodeConn>))
        }
        .boxed()
    }
}

#[derive(Default)]
struct PathLog {
    established: Vec<String>,
    abandoned: Vec<String>,
    discarded: Vec<String>,
}

pub struct QuicConn {
    conn: Conn,
    log: Arc<Mutex<PathLog>>,
}

fn path_addr(conn: &noq::Connection, id: PathId) -> String {
    conn.path(id)
        .and_then(|path| path.remote_address().ok())
        .map_or_else(|| "?".into(), |addr| addr.to_string())
}

impl QuicConn {
    fn wrap(conn: Conn) -> Arc<Self> {
        let log = Arc::new(Mutex::new(PathLog::default()));
        let mut events = conn.noq().path_events();
        let task_log = log.clone();
        let weak = conn.noq().weak_handle();
        let stable = conn.stable_id();
        tokio::spawn(async move {
            while let Some(event) = events.next().await {
                let Ok(event) = event else {
                    emit(&format!("EVENT quic_path stable={stable} lagged"));
                    continue;
                };
                let addr = |id| {
                    weak.upgrade()
                        .map_or_else(|| "?".into(), |conn| path_addr(&conn, id))
                };
                let mut log = task_log.lock().unwrap();
                let line = match &event {
                    PathEvent::Established { id, .. } => {
                        let entry = format!("{id}@{}", addr(*id));
                        log.established.push(entry.clone());
                        format!("established {entry}")
                    }
                    PathEvent::Abandoned { id, reason, .. } => {
                        let entry = format!("{id}:{reason:?}");
                        log.abandoned.push(entry.clone());
                        format!("abandoned {entry}")
                    }
                    PathEvent::Discarded { id, .. } => {
                        log.discarded.push(id.to_string());
                        format!("discarded {id}")
                    }
                    other => format!("{other:?}").replace(' ', ""),
                };
                emit(&format!("EVENT quic_path stable={stable} {line}"));
            }
        });
        Arc::new(Self { conn, log })
    }
}

impl NodeConn for QuicConn {
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
        let noq = self.conn.noq();
        let stats = self.conn.stats();
        let open: Vec<String> = stats
            .paths
            .iter()
            .map(|(id, path)| format!("{id}@{}rtt{}ms", path_addr(noq, *id), path.rtt.as_millis()))
            .collect();
        let nat_local = match noq.get_local_nat_traversal_addresses() {
            Ok(addrs) => format!("Ok({})", addrs.len()),
            Err(error) => format!("Err({error})").replace(' ', "_"),
        };
        let nat_remote = match noq.get_remote_nat_traversal_addresses() {
            Ok(addrs) => format!("Ok({})", addrs.len()),
            Err(error) => format!("Err({error})").replace(' ', "_"),
        };
        let log = self.log.lock().unwrap();
        format!(
            "side=quic multipath={} max_datagram={:?} nat_local={nat_local} nat_remote={nat_remote} \
             paths_open={} paths=[{}] path_established=[{}] path_abandoned=[{}] \
             path_discarded=[{}] lost_packets={}",
            noq.is_multipath_enabled(),
            noq.max_datagram_size(),
            stats.paths.len(),
            open.join(","),
            log.established.join(","),
            log.abandoned.join(","),
            log.discarded.join(","),
            stats.connection.lost_packets,
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
