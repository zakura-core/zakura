//! A structured trace of block announcements and block serving on legacy peer connections.
//!
//! The legacy peer set already records each block it downloads (`legacy_peer_request.jsonl`).
//! This table records the two ends of that exchange that happen on a connection: a peer
//! telling us about a block, and us telling a peer about a block, hearing its request and
//! sending it. Together with the same rows on another node they give the time a block took
//! from one node to the next.
//!
//! Rows never hold a peer's address. A connection has a process-local number, and a peer whose
//! IP address the operator listed in the `ZAKURA_TRACE_PEER_NAMES` environment variable also
//! has its name.
//!
//! The list is an environment variable, like the node's own trace label, and not a config
//! setting: a config rejects settings it does not know, so a list written there would stop an
//! older build from starting after a rollback.

use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use serde::Serialize;
use zakura_chain::block;
use zakura_jsonl_trace::{
    JsonlDisplay, JsonlEventEmitter, JsonlTraceEvent, JsonlTraceTable, JsonlTracer,
};

use super::ConnectedAddr;
use crate::{protocol::external::canonical_ip, Config};

const TABLE: JsonlTraceTable = JsonlTraceTable::new("legacy_gossip", "legacy_gossip.jsonl");

/// Names for legacy peers in the gossip trace, as `name=ip` pairs separated by commas, for
/// example `us-0=203.0.113.7,asia-0=198.51.100.9`.
///
/// List only nodes you operate: the name tells a reader of the trace files which peer it was.
const PEER_NAMES_ENV: &str = "ZAKURA_TRACE_PEER_NAMES";

#[derive(Serialize)]
struct GossipEvent<'a> {
    /// What happened: `block_inv_received`, `block_inv_sent`, `block_getdata_received`,
    /// `block_sent` or `block_unavailable`.
    event: &'static str,
    hash: JsonlDisplay<'a, block::Hash>,
    connection_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    peer_name: Option<&'a str>,
}

impl JsonlTraceEvent for GossipEvent<'_> {
    const TABLE: JsonlTraceTable = TABLE;
}

/// Writes the block gossip trace for every legacy connection of this node.
#[derive(Clone, Debug)]
pub(crate) struct LegacyGossipTrace {
    emitter: JsonlEventEmitter,
    /// The operator's names for peers, by canonical IP address.
    peer_names: Arc<HashMap<IpAddr, Arc<str>>>,
    next_connection_id: Arc<AtomicU64>,
}

impl LegacyGossipTrace {
    /// Returns the trace `config` asks for: rows go to `network.zakura.trace_dir`, and
    /// nothing is written when that is unset. Peers are named from `ZAKURA_TRACE_PEER_NAMES`.
    pub(crate) fn new(config: &Config) -> Self {
        let Some(trace_dir) = config.zakura.trace_dir.clone() else {
            return Self::with_emitter(JsonlEventEmitter::noop(), []);
        };

        let listed = std::env::var(PEER_NAMES_ENV).unwrap_or_default();
        let (peer_names, rejected) = parse_peer_names(&listed);
        if !rejected.is_empty() {
            tracing::warn!(
                ?rejected,
                "ignoring {PEER_NAMES_ENV} entries that are not `name=ip address`"
            );
        }

        Self::with_emitter(
            JsonlEventEmitter::new(JsonlTracer::spawn(trace_dir), zakura_jsonl_trace::node_id()),
            peer_names,
        )
    }

    fn with_emitter<'a>(
        emitter: JsonlEventEmitter,
        peer_names: impl IntoIterator<Item = (IpAddr, &'a str)>,
    ) -> Self {
        Self {
            emitter,
            peer_names: Arc::new(
                peer_names
                    .into_iter()
                    .map(|(ip, name)| (canonical_ip(ip), Arc::from(name)))
                    .collect(),
            ),
            next_connection_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Returns the trace for one new connection to the peer at `connected_addr`.
    ///
    /// The peer is named only when the transport shows its own address; a proxied
    /// connection shows the proxy's.
    pub(crate) fn for_peer(&self, connected_addr: &ConnectedAddr) -> PeerGossipTrace {
        let peer_name = connected_addr
            .diagnostic_remote_addr()
            .and_then(|addr| self.peer_names.get(&canonical_ip(addr.ip())))
            .cloned();

        PeerGossipTrace {
            emitter: self.emitter.clone(),
            // Wrap-around is unreachable for a process-local u64 counter.
            connection_id: self.next_connection_id.fetch_add(1, Ordering::Relaxed),
            peer_name,
        }
    }
}

/// Splits a `name=ip,name=ip` list into the pairs it names and the entries it cannot read.
fn parse_peer_names(listed: &str) -> (Vec<(IpAddr, &str)>, Vec<&str>) {
    let mut peer_names = Vec::new();
    let mut rejected = Vec::new();

    for entry in listed.split(',').map(str::trim) {
        if entry.is_empty() {
            continue;
        }
        let named = entry.split_once('=').and_then(|(name, ip)| {
            let name = name.trim();
            let ip = ip.trim().parse().ok()?;
            (!name.is_empty()).then_some((ip, name))
        });
        match named {
            Some(pair) => peer_names.push(pair),
            None => rejected.push(entry),
        }
    }

    (peer_names, rejected)
}

/// Writes the block gossip trace of one legacy connection.
///
/// The default value writes nothing.
#[derive(Clone, Debug, Default)]
pub(crate) struct PeerGossipTrace {
    emitter: JsonlEventEmitter,
    connection_id: u64,
    peer_name: Option<Arc<str>>,
}

impl PeerGossipTrace {
    /// The peer announced the block `hash` to us.
    pub(crate) fn block_inv_received(&self, hash: block::Hash) {
        self.emit("block_inv_received", hash);
    }

    /// Our announcement of the block `hash` was written to the peer.
    pub(crate) fn block_inv_sent(&self, hash: block::Hash) {
        self.emit("block_inv_sent", hash);
    }

    /// The peer asked us for the block `hash`.
    pub(crate) fn block_getdata_received(&self, hash: block::Hash) {
        self.emit("block_getdata_received", hash);
    }

    /// The whole block `hash` was written to the peer.
    pub(crate) fn block_sent(&self, hash: block::Hash) {
        self.emit("block_sent", hash);
    }

    /// The peer asked for the block `hash` and we did not have it.
    pub(crate) fn block_unavailable(&self, hash: block::Hash) {
        self.emit("block_unavailable", hash);
    }

    fn emit(&self, event: &'static str, hash: block::Hash) {
        self.emitter.emit_event(|| GossipEvent {
            event,
            hash: JsonlDisplay(&hash),
            connection_id: self.connection_id,
            peer_name: self.peer_name.as_deref(),
        });
    }
}

#[cfg(test)]
pub(super) mod tests {
    use serde_json::Value;
    use tokio::sync::mpsc;
    use zakura_jsonl_trace::JsonlWriteEvent;

    use super::*;
    use crate::PeerSocketAddr;

    /// The rows a test trace wrote, oldest first.
    pub(in crate::peer) struct WrittenRows(mpsc::Receiver<JsonlWriteEvent>);

    impl WrittenRows {
        /// Takes the rows written so far, checking each went to the gossip table's file.
        pub(in crate::peer) fn take(&mut self) -> Vec<Value> {
            let mut rows = Vec::new();
            while let Ok(written) = self.0.try_recv() {
                assert_eq!(written.file_name, "legacy_gossip.jsonl");
                rows.push(serde_json::from_slice(&written.line).expect("a row is one JSON object"));
            }
            rows
        }
    }

    /// Returns a trace that names the peers in `peer_names`, and the rows it writes.
    pub(in crate::peer) fn test_trace(
        peer_names: &[(&str, &str)],
    ) -> (LegacyGossipTrace, WrittenRows) {
        let (tx, rx) = mpsc::channel(64);
        let trace = LegacyGossipTrace::with_emitter(
            JsonlEventEmitter::new(JsonlTracer::new(tx), "test-node"),
            peer_names
                .iter()
                .map(|(ip, name)| (ip.parse().expect("valid IP address"), *name)),
        );
        (trace, WrittenRows(rx))
    }

    fn hash(byte: u8) -> block::Hash {
        block::Hash([byte; 32])
    }

    fn direct(addr: &str) -> ConnectedAddr {
        let addr: PeerSocketAddr = addr.parse().expect("valid peer address");
        ConnectedAddr::new_inbound_direct(addr)
    }

    #[test]
    fn a_listed_peer_is_named_and_no_row_holds_an_address() {
        let (trace, mut written) = test_trace(&[("203.0.113.7", "us-0")]);

        // An IPv4-mapped address is the same peer as its IPv4 form.
        let named = trace.for_peer(&direct("[::ffff:203.0.113.7]:50123"));
        let unnamed = trace.for_peer(&direct("198.51.100.9:8233"));

        named.block_inv_received(hash(1));
        unnamed.block_inv_sent(hash(1));
        unnamed.block_getdata_received(hash(1));
        unnamed.block_sent(hash(1));
        named.block_unavailable(hash(2));

        let rows = written.take();
        let events: Vec<_> = rows.iter().map(|row| row["event"].as_str()).collect();
        assert_eq!(
            events,
            [
                Some("block_inv_received"),
                Some("block_inv_sent"),
                Some("block_getdata_received"),
                Some("block_sent"),
                Some("block_unavailable"),
            ]
        );

        assert_eq!(rows[0]["peer_name"], "us-0");
        assert_eq!(rows[0]["hash"], hash(1).to_string());
        assert_eq!(rows[0]["node"], "test-node");
        assert!(rows[0]["ts"].is_u64());
        assert!(rows[1].get("peer_name").is_none());
        assert_eq!(rows[4]["hash"], hash(2).to_string());

        // Each connection keeps its own number across its rows.
        assert_eq!(rows[0]["connection_id"], rows[4]["connection_id"]);
        assert_eq!(rows[1]["connection_id"], rows[3]["connection_id"]);
        assert_ne!(rows[0]["connection_id"], rows[1]["connection_id"]);

        for row in &rows {
            let text = row.to_string();
            assert!(!text.contains("203.0.113.7"), "{text}");
            assert!(!text.contains("198.51.100.9"), "{text}");
        }
    }

    #[test]
    fn the_peer_list_is_read_as_name_and_address_pairs() {
        let (peer_names, rejected) = parse_peer_names(
            " us-0=203.0.113.7 , asia-0 = 2001:db8::9,,no-address,=198.51.100.1,x=not-an-ip",
        );

        assert_eq!(
            peer_names,
            [
                ("203.0.113.7".parse().unwrap(), "us-0"),
                ("2001:db8::9".parse().unwrap(), "asia-0"),
            ]
        );
        assert_eq!(rejected, ["no-address", "=198.51.100.1", "x=not-an-ip"]);
        assert_eq!(parse_peer_names(""), (Vec::new(), Vec::new()));
    }

    #[test]
    fn a_proxied_connection_is_never_named() {
        let (trace, _written) = test_trace(&[("127.0.0.1", "local-proxy")]);

        let proxied = trace.for_peer(&ConnectedAddr::new_inbound_proxy(
            "127.0.0.1:9050".parse().expect("valid socket address"),
        ));

        assert_eq!(proxied.peer_name, None);
    }

    #[test]
    fn the_default_trace_writes_nothing() {
        let trace = PeerGossipTrace::default();
        assert!(!trace.emitter.is_enabled());
        trace.block_sent(hash(3));
    }
}
