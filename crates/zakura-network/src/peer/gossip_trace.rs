//! A structured trace of block announcements and block serving on legacy peer connections.
//!
//! The legacy peer set already records each block it downloads (`legacy_peer_request.jsonl`).
//! This table records the two ends of that exchange that happen on a connection: a peer
//! telling us about a block, and us telling a peer about a block, hearing its request and
//! sending it. Together with the same rows on another node they give the time a block took
//! from one node to the next.
//!
//! What a peer sends cannot make this table grow without bound. The peer's request for a
//! block, and the block being sent or found missing, are written only for a block this node
//! announced to that peer, once for each announcement. A peer downloading old blocks from us,
//! or asking for blocks we never announced to it, writes nothing. The peer's own announcements
//! are written up to a fixed number a minute for each connection.
//!
//! Rows never hold a peer's address. A connection has a process-local number, and a peer whose
//! IP address the operator listed in `network.zakura.trace_peer_names` also has its name.

use std::{
    collections::{HashMap, VecDeque},
    net::IpAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use serde::Serialize;
use tokio::time::Instant;
use zakura_chain::block;
use zakura_jsonl_trace::{
    JsonlDisplay, JsonlEventEmitter, JsonlTraceEvent, JsonlTraceTable, JsonlTracer,
};

use super::ConnectedAddr;
use crate::{protocol::external::canonical_ip, Config};

const TABLE: JsonlTraceTable = JsonlTraceTable::new("legacy_gossip", "legacy_gossip.jsonl");

/// How many of the blocks we last announced to a peer are remembered, so that the peer's
/// request for one of them is traced. A node announces one block at a time, and a peer asks
/// for it within seconds.
const REMEMBERED_ANNOUNCEMENTS: usize = 8;

/// How many of one peer's block announcements are traced in each [`ANNOUNCEMENT_WINDOW`].
///
/// A peer announces a block about once every 75 seconds. This leaves room for a burst after
/// a reorganisation, and bounds what a peer that announces without pause can make us write.
const TRACED_ANNOUNCEMENTS_PER_WINDOW: u32 = 16;

/// The time over which [`TRACED_ANNOUNCEMENTS_PER_WINDOW`] is counted.
const ANNOUNCEMENT_WINDOW: Duration = Duration::from_secs(60);

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
    /// nothing is written when that is unset. Peer names come from the same typed config.
    pub(crate) fn new(config: &Config) -> Self {
        let Some(trace_dir) = config.zakura.trace_dir.clone() else {
            return Self::with_emitter(JsonlEventEmitter::noop(), []);
        };

        Self::with_emitter(
            JsonlEventEmitter::new(JsonlTracer::spawn(trace_dir), zakura_jsonl_trace::node_id()),
            config
                .zakura
                .trace_peer_names
                .iter()
                .map(|(ip, name)| (*ip, name.as_str())),
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
            exchanges: Default::default(),
        }
    }
}

/// A block we announced to a peer, until the peer's request for it has been answered.
#[derive(Debug)]
struct AnnouncedBlock {
    hash: block::Hash,
    /// Whether the peer's request for the block has been traced.
    requested: bool,
}

/// What one connection's trace remembers, to decide which of the peer's messages to write.
#[derive(Debug, Default)]
struct PeerExchanges {
    /// The blocks we last announced to the peer and have not answered a request for, oldest
    /// first. Holds at most [`REMEMBERED_ANNOUNCEMENTS`].
    announced: VecDeque<AnnouncedBlock>,
    /// When the current [`ANNOUNCEMENT_WINDOW`] began, once the peer has announced a block.
    window_started: Option<Instant>,
    /// How many of the peer's announcements were traced in the current window.
    traced_in_window: u32,
}

impl PeerExchanges {
    /// Remembers that we announced `hash` to the peer, in place of the oldest announcement
    /// when [`REMEMBERED_ANNOUNCEMENTS`] are already remembered.
    fn announce(&mut self, hash: block::Hash) {
        self.announced.retain(|block| block.hash != hash);
        if self.announced.len() == REMEMBERED_ANNOUNCEMENTS {
            self.announced.pop_front();
        }
        self.announced.push_back(AnnouncedBlock {
            hash,
            requested: false,
        });
    }

    /// Returns true for the peer's first request for a block we announced to it.
    fn request(&mut self, hash: block::Hash) -> bool {
        match self.announced.iter_mut().find(|block| block.hash == hash) {
            Some(block) if !block.requested => {
                block.requested = true;
                true
            }
            _ => false,
        }
    }

    /// Returns true when the peer's request for `hash` was traced, and forgets the block:
    /// the request has now been answered.
    fn answer(&mut self, hash: block::Hash) -> bool {
        let requested = self
            .announced
            .iter()
            .position(|block| block.hash == hash && block.requested);
        if let Some(index) = requested {
            self.announced.remove(index);
        }
        requested.is_some()
    }

    /// Returns true while the peer has announced no more than
    /// [`TRACED_ANNOUNCEMENTS_PER_WINDOW`] blocks in the [`ANNOUNCEMENT_WINDOW`] that holds
    /// `now`.
    fn admit_announcement(&mut self, now: Instant) -> bool {
        let window_is_over = self
            .window_started
            .is_none_or(|started| now.duration_since(started) >= ANNOUNCEMENT_WINDOW);
        if window_is_over {
            self.window_started = Some(now);
            self.traced_in_window = 0;
        }
        if self.traced_in_window == TRACED_ANNOUNCEMENTS_PER_WINDOW {
            return false;
        }
        self.traced_in_window += 1;
        true
    }
}

/// Writes the block gossip trace of one legacy connection.
///
/// Copies of one connection's trace share what they remember about the connection. The
/// default value writes nothing.
#[derive(Clone, Debug, Default)]
pub(crate) struct PeerGossipTrace {
    emitter: JsonlEventEmitter,
    connection_id: u64,
    peer_name: Option<Arc<str>>,
    exchanges: Arc<Mutex<PeerExchanges>>,
}

impl PeerGossipTrace {
    /// This connection's process-local number, shared by every trace row about it.
    pub(crate) fn connection_id(&self) -> u64 {
        self.connection_id
    }

    /// The peer announced the block `hash` to us.
    ///
    /// Written for the first [`TRACED_ANNOUNCEMENTS_PER_WINDOW`] announcements of each
    /// [`ANNOUNCEMENT_WINDOW`].
    pub(crate) fn block_inv_received(&self, hash: block::Hash) {
        if self.remembered(|exchanges| exchanges.admit_announcement(Instant::now())) {
            self.emit("block_inv_received", hash);
        }
    }

    /// Our announcement of the block `hash` was written to the peer.
    pub(crate) fn block_inv_sent(&self, hash: block::Hash) {
        let announced = self.remembered(|exchanges| {
            exchanges.announce(hash);
            true
        });
        if announced {
            self.emit("block_inv_sent", hash);
        }
    }

    /// The peer asked us for the block `hash`.
    ///
    /// Written for the peer's first request for a block we announced to it.
    pub(crate) fn block_getdata_received(&self, hash: block::Hash) {
        if self.remembered(|exchanges| exchanges.request(hash)) {
            self.emit("block_getdata_received", hash);
        }
    }

    /// The whole block `hash` was written to the peer.
    ///
    /// Written when the peer's request for the block was.
    pub(crate) fn block_sent(&self, hash: block::Hash) {
        if self.remembered(|exchanges| exchanges.answer(hash)) {
            self.emit("block_sent", hash);
        }
    }

    /// The peer asked for the block `hash` and we did not have it.
    ///
    /// Written when the peer's request for the block was.
    pub(crate) fn block_unavailable(&self, hash: block::Hash) {
        if self.remembered(|exchanges| exchanges.answer(hash)) {
            self.emit("block_unavailable", hash);
        }
    }

    /// Updates what this connection remembers and returns what `update` decided, or false
    /// without remembering anything when the node writes no trace files.
    fn remembered(&self, update: impl FnOnce(&mut PeerExchanges) -> bool) -> bool {
        self.emitter.is_enabled()
            && update(
                &mut self
                    .exchanges
                    .lock()
                    .expect("no code panics while holding the gossip trace mutex"),
            )
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

    /// The `event` of each row, in order.
    fn events(rows: &[Value]) -> Vec<&str> {
        rows.iter()
            .map(|row| row["event"].as_str().expect("every row names its event"))
            .collect()
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
        named.block_inv_sent(hash(2));
        named.block_getdata_received(hash(2));
        named.block_unavailable(hash(2));

        let rows = written.take();
        assert_eq!(
            events(&rows),
            [
                "block_inv_received",
                "block_inv_sent",
                "block_getdata_received",
                "block_sent",
                "block_inv_sent",
                "block_getdata_received",
                "block_unavailable",
            ]
        );

        assert_eq!(rows[0]["peer_name"], "us-0");
        assert_eq!(rows[0]["hash"], hash(1).to_string());
        assert_eq!(rows[0]["node"], "test-node");
        assert!(rows[0]["ts"].is_u64());
        assert!(rows[1].get("peer_name").is_none());
        assert_eq!(rows[6]["hash"], hash(2).to_string());

        // Each connection keeps its own number across its rows.
        assert_eq!(rows[0]["connection_id"], rows[6]["connection_id"]);
        assert_eq!(rows[1]["connection_id"], rows[3]["connection_id"]);
        assert_ne!(rows[0]["connection_id"], rows[1]["connection_id"]);

        for row in &rows {
            let text = row.to_string();
            assert!(!text.contains("203.0.113.7"), "{text}");
            assert!(!text.contains("198.51.100.9"), "{text}");
        }
    }

    #[test]
    fn a_peer_s_request_is_traced_once_and_only_for_a_block_we_announced_to_it() {
        let (trace, mut written) = test_trace(&[]);
        let peer = trace.for_peer(&direct("203.0.113.7:8233"));
        let other_peer = trace.for_peer(&direct("198.51.100.9:8233"));

        peer.block_inv_sent(hash(1));
        assert_eq!(events(&written.take()), ["block_inv_sent"]);

        // A block we never announced: a peer downloading old blocks, or probing.
        peer.block_getdata_received(hash(2));
        peer.block_sent(hash(2));
        peer.block_unavailable(hash(2));
        // A block we announced, but on another connection.
        other_peer.block_getdata_received(hash(1));
        other_peer.block_sent(hash(1));
        other_peer.block_unavailable(hash(1));
        // A block sent without the peer having asked for it.
        peer.block_sent(hash(1));
        assert_eq!(written.take(), Vec::<Value>::new());

        // The request and its answer are each written once, however often they repeat.
        peer.block_getdata_received(hash(1));
        peer.block_getdata_received(hash(1));
        peer.block_sent(hash(1));
        peer.block_getdata_received(hash(1));
        peer.block_sent(hash(1));
        peer.block_unavailable(hash(1));
        assert_eq!(
            events(&written.take()),
            ["block_getdata_received", "block_sent"]
        );
    }

    #[test]
    fn only_the_blocks_we_last_announced_are_remembered() {
        let (trace, mut written) = test_trace(&[]);
        let peer = trace.for_peer(&direct("203.0.113.7:8233"));
        let last = u8::try_from(REMEMBERED_ANNOUNCEMENTS).expect("a small constant") + 1;

        for block in 1..=last {
            peer.block_inv_sent(hash(block));
        }
        written.take();

        // The oldest announcement was dropped to remember the newest.
        peer.block_getdata_received(hash(1));
        peer.block_getdata_received(hash(2));
        peer.block_getdata_received(hash(last));

        let rows = written.take();
        assert_eq!(
            events(&rows),
            ["block_getdata_received", "block_getdata_received"]
        );
        assert_eq!(rows[0]["hash"], hash(2).to_string());
        assert_eq!(rows[1]["hash"], hash(last).to_string());
    }

    #[tokio::test(start_paused = true)]
    async fn a_peer_s_announcements_are_traced_up_to_a_number_a_minute() {
        let (trace, mut written) = test_trace(&[]);
        let peer = trace.for_peer(&direct("203.0.113.7:8233"));
        let other_peer = trace.for_peer(&direct("198.51.100.9:8233"));
        let traced = usize::try_from(TRACED_ANNOUNCEMENTS_PER_WINDOW).expect("a small constant");

        for _ in 0..traced + 5 {
            peer.block_inv_received(hash(1));
        }
        assert_eq!(written.take().len(), traced);

        // Another connection has its own allowance.
        other_peer.block_inv_received(hash(1));
        assert_eq!(events(&written.take()), ["block_inv_received"]);

        // Still inside the minute: nothing more from the first peer.
        tokio::time::advance(ANNOUNCEMENT_WINDOW - Duration::from_secs(1)).await;
        peer.block_inv_received(hash(2));
        assert_eq!(written.take(), Vec::<Value>::new());

        tokio::time::advance(Duration::from_secs(1)).await;
        peer.block_inv_received(hash(2));
        assert_eq!(events(&written.take()), ["block_inv_received"]);
    }

    #[tokio::test]
    async fn peer_names_come_from_configuration() {
        let directory = tempfile::tempdir().expect("temporary trace directory");
        let mut config = Config::default();
        config
            .zakura
            .trace_peer_names
            .insert("192.0.2.1".parse().unwrap(), "us-0".to_owned());
        config
            .zakura
            .trace_peer_names
            .insert("2001:db8::9".parse().unwrap(), "asia-0".to_owned());
        assert!(
            LegacyGossipTrace::new(&config).peer_names.is_empty(),
            "tracing is disabled without a directory"
        );
        config.zakura.trace_dir = Some(directory.path().to_owned());
        let trace = LegacyGossipTrace::new(&config);
        assert_eq!(
            trace
                .for_peer(&direct("[::ffff:192.0.2.1]:8233"))
                .peer_name
                .as_deref(),
            Some("us-0")
        );
        assert_eq!(
            trace
                .for_peer(&direct("[2001:db8::9]:8233"))
                .peer_name
                .as_deref(),
            Some("asia-0")
        );
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
