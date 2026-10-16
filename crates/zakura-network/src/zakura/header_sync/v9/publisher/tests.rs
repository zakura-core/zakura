//! Tests for the header sync version 9 publisher.
//!
//! A fake port serves a header tree from memory, with scripted failures. The
//! subscriber uses the real [`Subscriptions`] tool and checks each page's
//! linkage, the target marker, and each outcome's window against the
//! specification's table. A mirror [`Publications`] ledger replays every page
//! and outcome from the wire, so each update must get the same result from
//! the publisher and from the mirror.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};

use proptest::prelude::*;
use tokio::sync::Semaphore;
use zakura_chain::{
    block::{genesis::regtest_genesis_block, merkle::AuthDataRoot},
    fmt::HexDebug,
    ironwood, orchard, sapling,
    work::difficulty::U256,
};
use zakura_header_chain::{
    AlarmSet, BodyWorkOwner, ChainScore, EngineMode, FrontierSet, HeaderLocator, StateVersion,
    SuffixWork, TreeAuxRecordV1, VerifiedGeneration,
};
use zakura_node_services::header_chain::{
    AdapterKey, ApplyHeaderTargetReply, HeaderChainFuture, PortError, PrepareHeaderTarget,
    PrepareHeaderTargetReply, PreparedHeaderTarget, VctRepairContextReply,
};

use super::super::{smallest_page, RegtestMessage};
use super::*;
use crate::zakura::{
    framed_channel,
    header_sync::config::REGTEST_HEADER_BYTES,
    regulation::{ServeCapacity, ServeLimits, Subscriptions},
    wire_codec::decode_frame,
    Credit, FramedRecv, ZakuraPeerId,
};

/// Regtest header bytes, so pages stay small.
const HB: usize = REGTEST_HEADER_BYTES;

/// How long the paused clock runs before a receive counts as quiet.
const QUIET: Duration = Duration::from_secs(1);

/// `P`, the smallest legal page.
fn p() -> u32 {
    u32::try_from(smallest_page(HB, AuxSchema::None)).expect("a page fits u32")
}

/// Three headers per page, and bytes for two schema-1 entries, so pages
/// split by count without records and by bytes with them.
fn limits() -> PageLimits {
    PageLimits {
        max_headers: 3,
        max_bytes: u32::try_from(HEADERS_RESPONSE_FIXED_BYTES + 2 * entry_bytes(HB, AuxSchema::V1))
            .expect("a small page fits u32"),
        aux_schema_mask: AuxSchema::V1.mask_bit(),
    }
}

/// Ample serving budgets.
fn serve_limits() -> ServeLimits {
    ServeLimits {
        node_execution: 4,
        peer_execution: 2,
        peer_output_responses: 8,
        node_output_responses: 16,
        peer_output_bytes: 1 << 20,
        node_output_bytes: 1 << 22,
    }
}

/// A schema-1 record with every root at its empty default.
fn record(height: block::Height) -> TreeAuxRecordV1 {
    TreeAuxRecordV1 {
        height,
        sapling_root: sapling::tree::Root::default(),
        orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
        ironwood_root: ironwood::tree::NoteCommitmentTree::default().root(),
        sapling_tx_count: 0,
        orchard_tx_count: 0,
        ironwood_tx_count: 0,
        auth_data_root: AuthDataRoot::from([0; 32]),
    }
}

/// A selected chain ending at `tip`.
fn snapshot(tip: Frontier, generation: u64, genesis: Frontier) -> EngineSnapshot {
    EngineSnapshot {
        mode: EngineMode::Integrated,
        state_version: StateVersion::new(generation),
        header_generation: HeaderGeneration::new(generation),
        verified_generation: VerifiedGeneration::new(1),
        frontiers: FrontierSet {
            finalized: genesis,
            header_best: tip,
            verified_best: genesis,
        },
        header_best_score: ChainScore::new(SuffixWork::new(U256::from(tip.height.0)), tip.hash),
        oldest_retained_height: block::Height(0),
        alarms: AlarmSet::default(),
    }
}

/// A header tree, the leases issued on it, and scripted replies.
#[derive(Default)]
struct Store {
    nodes: HashMap<block::Hash, (Frontier, Arc<block::Header>)>,
    next_lease: u64,
    leases: HashSet<u64>,
    acquire_script: VecDeque<Result<AcquirePathReply, PortError>>,
    read_script: VecDeque<Result<ReadPathReply, PortError>>,
    /// Headers without a schema-1 record.
    no_record: HashSet<block::Hash>,
    reads_running: usize,
    most_reads_running: usize,
    /// Distinguishes sibling headers.
    next_nonce: u32,
}

impl Store {
    /// Add `count` headers above `parent`, and return them.
    fn extend(&mut self, parent: Frontier, count: u32) -> Vec<Frontier> {
        let mut added = Vec::new();
        let mut parent = parent;
        for _ in 0..count {
            let mut header = *self.nodes[&parent.hash].1;
            header.previous_block_hash = parent.hash;
            self.next_nonce += 1;
            let mut nonce = [0; 32];
            nonce[..4].copy_from_slice(&self.next_nonce.to_le_bytes());
            header.nonce = HexDebug(nonce);
            let child = Frontier::new(
                parent.height.next().expect("test heights are small"),
                header.hash(),
            );
            self.nodes.insert(child.hash, (child, Arc::new(header)));
            added.push(child);
            parent = child;
        }
        added
    }

    /// The parent of `frontier`, if retained.
    fn parent(&self, frontier: Frontier) -> Option<Frontier> {
        let (_, header) = self.nodes.get(&frontier.hash)?;
        self.nodes
            .get(&header.previous_block_hash)
            .map(|(parent, _)| *parent)
    }

    /// The retained ancestor of `of` at `height`.
    fn ancestor(&self, of: Frontier, height: block::Height) -> Option<Frontier> {
        let mut current = of;
        while current.height > height {
            current = self.parent(current)?;
        }
        Some(current)
    }

    /// Lease the path from the nearest locator to the target, as state does.
    fn acquire(
        &mut self,
        key: &AdapterKey,
        request: AcquirePath,
    ) -> Result<AcquirePathReply, PortError> {
        if let Some(reply) = self.acquire_script.pop_front() {
            return reply;
        }
        let Some(&(target, _)) = self.nodes.get(&request.target_tip_hash) else {
            return Ok(AcquirePathReply::TargetNotRetained);
        };
        let mut current = target;
        while !request.locator_hashes.contains(&current.hash) {
            current = match self.parent(current) {
                Some(parent) => parent,
                None if current.height == block::Height(0) => {
                    return Ok(AcquirePathReply::NoLocatorIntersection)
                }
                None => return Ok(AcquirePathReply::HistoryPruned),
            };
        }
        self.next_lease += 1;
        self.leases.insert(self.next_lease);
        Ok(AcquirePathReply::Acquired(Box::new(
            RetainedHeaderPath::from_adapter(
                key,
                self.next_lease,
                request.source,
                request.session_id,
                current,
                target,
                request.scope,
            ),
        )))
    }

    /// Read one page and consume the lease, as state does.
    fn read(
        &mut self,
        path: &RetainedHeaderPath,
        request: &ReadPath,
    ) -> Result<ReadPathReply, PortError> {
        if let Some(reply) = self.read_script.pop_front() {
            if matches!(reply, Ok(ReadPathReply::Unavailable)) {
                self.leases.remove(&path.handle_id());
            }
            return reply;
        }
        if !self.leases.remove(&path.handle_id()) || request.after_hash != path.common_ancestor.hash
        {
            return Ok(ReadPathReply::Unavailable);
        }
        let mut headers = Vec::new();
        let mut current = path.target;
        while current != path.common_ancestor {
            let (Some((_, header)), Some(parent)) =
                (self.nodes.get(&current.hash), self.parent(current))
            else {
                return Ok(ReadPathReply::Unavailable);
            };
            headers.push(header.clone());
            current = parent;
        }
        headers.reverse();
        let count = usize::try_from(request.max_header_count).unwrap_or(usize::MAX);
        let complete = headers.len() <= count;
        headers.truncate(count);
        let finalized_tree_aux = headers
            .iter()
            .zip(1..)
            .map(|(header, offset)| {
                (!self.no_record.contains(&header.hash()))
                    .then(|| record(block::Height(path.common_ancestor.height.0 + offset)))
            })
            .collect();
        Ok(ReadPathReply::Page(Box::new(RetainedHeaderPathPage {
            common_ancestor: path.common_ancestor,
            target: path.target,
            scope: path.scope,
            aux_deliveries: vec![Vec::new(); headers.len()],
            finalized_tree_aux,
            finalized_body_sizes: vec![None; headers.len()],
            headers,
            complete,
        })))
    }
}

/// A port over a [`Store`], with a selected chain the test moves.
struct FakePort {
    key: AdapterKey,
    store: Mutex<Store>,
    genesis: Frontier,
    selected: watch::Sender<Option<EngineSnapshot>>,
    /// Every read waits for a permit here.
    gate: Semaphore,
    /// Reads panic, standing in for a local bug.
    panic_reads: std::sync::atomic::AtomicBool,
}

impl FakePort {
    /// A store holding the Regtest genesis header, selected, with `gate`
    /// read permits.
    fn new(gate: usize) -> Arc<Self> {
        let header = regtest_genesis_block().header.clone();
        let genesis = Frontier::new(block::Height(0), header.hash());
        let mut store = Store::default();
        store.nodes.insert(genesis.hash, (genesis, header));
        Arc::new(Self {
            key: AdapterKey::new(),
            store: Mutex::new(store),
            genesis,
            selected: watch::channel(Some(snapshot(genesis, 1, genesis))).0,
            gate: Semaphore::new(gate),
            panic_reads: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// The fake's store.
    fn store(&self) -> std::sync::MutexGuard<'_, Store> {
        self.store.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The selected tip.
    fn tip(&self) -> Frontier {
        self.selected
            .borrow()
            .as_ref()
            .expect("the fake always has a selected chain")
            .frontiers
            .header_best
    }

    /// Select the chain ending at `tip`.
    fn select(&self, tip: Frontier) {
        self.selected.send_modify(|selected| {
            let generation = selected
                .as_ref()
                .map_or(1, |selected| selected.header_generation.get() + 1);
            *selected = Some(snapshot(tip, generation, self.genesis));
        });
    }

    /// Extend the selected chain by `count` headers and select the new tip.
    fn grow(&self, count: u32) -> Vec<Frontier> {
        let added = self.store().extend(self.tip(), count);
        if let Some(&tip) = added.last() {
            self.select(tip);
        }
        added
    }
}

impl Port for FakePort {
    fn continuation_locator(
        &self,
    ) -> HeaderChainFuture<'_, Result<Option<HeaderLocator>, PortError>> {
        Box::pin(std::future::pending())
    }

    fn vct_repair_context(
        &self,
        _owner: BodyWorkOwner,
        _height: block::Height,
    ) -> HeaderChainFuture<'_, Result<VctRepairContextReply, PortError>> {
        Box::pin(std::future::pending())
    }

    fn acquire_header_path(
        &self,
        request: AcquirePath,
    ) -> HeaderChainFuture<'_, Result<AcquirePathReply, PortError>> {
        Box::pin(async move { self.store().acquire(&self.key, request) })
    }

    fn read_header_path(
        &self,
        path: RetainedHeaderPath,
        request: ReadPath,
    ) -> HeaderChainFuture<'_, Result<ReadPathReply, PortError>> {
        Box::pin(async move {
            {
                let mut store = self.store();
                store.reads_running += 1;
                store.most_reads_running = store.most_reads_running.max(store.reads_running);
            }
            let _permit = self.gate.acquire().await.expect("the gate never closes");
            assert!(
                !self.panic_reads.load(std::sync::atomic::Ordering::Relaxed),
                "the test asked this read to panic"
            );
            let mut store = self.store();
            store.reads_running -= 1;
            store.read(&path, &request)
        })
    }

    fn release_header_path(
        &self,
        path: RetainedHeaderPath,
    ) -> HeaderChainFuture<'_, Result<(), PortError>> {
        Box::pin(async move {
            self.store().leases.remove(&path.handle_id());
            Ok(())
        })
    }

    fn prepare_header_target(
        &self,
        _request: PrepareHeaderTarget,
    ) -> HeaderChainFuture<'_, PrepareHeaderTargetReply> {
        Box::pin(std::future::pending())
    }

    fn apply_header_target(
        &self,
        _target: PreparedHeaderTarget,
    ) -> HeaderChainFuture<'_, ApplyHeaderTargetReply> {
        Box::pin(std::future::pending())
    }

    fn selected_chain(&self) -> watch::Receiver<Option<EngineSnapshot>> {
        self.selected.subscribe()
    }
}

/// What the subscriber received.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Received {
    Page {
        parent: Frontier,
        last: Frontier,
        marker: bool,
    },
    Ended(HeadersOutcomeCode),
}

/// The subscriber's view of its one live subscription.
#[derive(Debug)]
struct Opened {
    id: u64,
    target: block::Hash,
    locators: Vec<block::Hash>,
    schema: AuxSchema,
    last: Option<Frontier>,
    /// Whether a locator was the target or a page carried the marker.
    reached: bool,
}

/// A conformant subscriber over the real subscription tool.
struct Subscriber {
    tools: Subscriptions<u64, Cursor>,
    next_id: u64,
    live: Option<Opened>,
}

impl Subscriber {
    /// A subscriber with no subscription.
    fn new() -> Self {
        let limits = SubscriptionLimits::from_rule(&SUBSCRIBE_HEADERS)
            .expect("SUBSCRIBE_HEADERS is a subscription row");
        Self {
            tools: Subscriptions::new(limits, &RegtestMessage::ROWS),
            next_id: 1,
            live: None,
        }
    }

    /// Record an `Open` and return it.
    fn open(
        &mut self,
        target: block::Hash,
        locators: Vec<Frontier>,
        credit: Credit,
        schema: AuxSchema,
    ) -> SubscribeHeaders {
        let id = self.next_id;
        self.next_id += 1;
        let start = Cursor {
            height: locators[0].height,
            hash: locators[0].hash,
            headers: 0,
            bytes: 0,
        };
        let update = self
            .tools
            .open(id, credit, start)
            .expect("a conformant subscriber opens one subscription at a time");
        let locator_hashes: Vec<block::Hash> =
            locators.iter().map(|locator| locator.hash).collect();
        self.live = Some(Opened {
            id,
            target,
            reached: locator_hashes.contains(&target),
            locators: locator_hashes.clone(),
            schema,
            last: None,
        });
        SubscribeHeaders {
            operation: Operation::Open,
            subscription_id: id,
            update_sequence: update.sequence,
            initial_target_tip_hash: target,
            acknowledged: update.acknowledged,
            locator_hashes,
            added_credit: update.added,
            tree_aux_schema: schema,
        }
    }

    /// Record a `Grant` of at most `headers` and `bytes`, if the window and
    /// the minimum allow one.
    fn grant(&mut self, headers: u32, bytes: u32) -> Option<SubscribeHeaders> {
        let opened = self.live.as_ref()?;
        let room = self.tools.grantable(&opened.id)?;
        let added = Credit {
            objects: headers.min(room.objects),
            bytes: bytes.min(room.bytes),
        };
        if added.objects == 0 && added.bytes < p() {
            return None;
        }
        let update = self.tools.grant(&opened.id, added).ok()?;
        Some(self.update(
            Operation::Grant,
            update.sequence,
            update.acknowledged,
            added,
        ))
    }

    /// Record a `Close`, if the subscription is live and not closing.
    fn close(&mut self) -> Option<SubscribeHeaders> {
        let id = self.live.as_ref()?.id;
        let update = self.tools.close(&id).ok()?;
        Some(self.update(
            Operation::Close,
            update.sequence,
            update.acknowledged,
            update.added,
        ))
    }

    /// An update on the live subscription.
    fn update(
        &self,
        operation: Operation,
        sequence: u32,
        acknowledged: Cursor,
        added: Credit,
    ) -> SubscribeHeaders {
        let opened = self.live.as_ref().expect("updates follow an Open");
        SubscribeHeaders {
            operation,
            subscription_id: opened.id,
            update_sequence: sequence,
            initial_target_tip_hash: opened.target,
            acknowledged,
            locator_hashes: Vec::new(),
            added_credit: added,
            tree_aux_schema: opened.schema,
        }
    }

    /// Admit one frame, checking every rule a subscriber checks, and accept
    /// each page.
    fn receive(&mut self, frame: &Frame) -> Received {
        match decode_frame::<RegtestMessage>(frame).expect("the publisher writes valid frames") {
            Message::Headers(page) => {
                let opened = self.live.as_mut().expect("a page for a live subscription");
                assert_eq!(page.subscription_id, opened.id);
                assert_eq!(page.initial_target_tip_hash, opened.target);
                assert_eq!(page.tree_aux_schema, opened.schema, "the schema matches");
                let parent = Frontier::new(page.common_ancestor_height, page.common_ancestor_hash);
                match opened.last {
                    Some(last) => assert_eq!(parent, last, "a page extends the one before"),
                    None if opened.reached => {
                        assert_eq!(
                            parent.hash, opened.target,
                            "the first push extends the target"
                        )
                    }
                    None => assert!(
                        opened.locators.contains(&parent.hash),
                        "the first page extends a sent locator"
                    ),
                }
                let mut previous = parent.hash;
                for (index, entry) in page.entries.iter().enumerate() {
                    assert_eq!(entry.header.previous_block_hash, previous, "headers link");
                    previous = entry.header.hash();
                    assert!(
                        opened.reached
                            || index + 1 == page.entries.len()
                            || previous != opened.target,
                        "a path page ends at the target"
                    );
                }
                let count = u32::try_from(page.entries.len()).expect("a page fits u32");
                let last = Frontier::new(block::Height(parent.height.0 + count), previous);
                let marker = !opened.reached && last.hash == opened.target;
                assert_eq!(
                    page.reaches_initial_target, marker,
                    "the marker is on exactly the page that ends at the target"
                );
                opened.reached |= marker;
                opened.last = Some(last);
                let consumed = self
                    .tools
                    .credit(&opened.id)
                    .expect("the subscription is live")
                    .consumed();
                // Widening usize to u64 is lossless on supported targets.
                let cursor = Cursor {
                    height: last.height,
                    hash: last.hash,
                    headers: consumed.objects + u64::from(count),
                    bytes: consumed.bytes + frame.payload.len() as u64,
                };
                self.tools
                    .claim_page(&opened.id, count, frame.payload.len(), cursor)
                    .expect("a page fits its credit");
                self.tools
                    .accept(&opened.id, &cursor)
                    .expect("the page was just claimed");
                Received::Page {
                    parent,
                    last,
                    marker,
                }
            }
            Message::HeadersOutcome(outcome) => {
                let opened = self
                    .live
                    .take()
                    .expect("an outcome for a live subscription");
                assert_eq!(outcome.subscription_id, opened.id);
                assert_eq!(outcome.initial_target_tip_hash, opened.target);
                let ended = self
                    .tools
                    .claim_end(&opened.id)
                    .expect("the subscription is live");
                use HeadersOutcomeCode::*;
                let legal = match outcome.outcome {
                    NoLocatorIntersection | TargetNotRetained | TargetNotSelected => {
                        ended.pages == 0
                    }
                    HistoryPruned => !opened.reached,
                    SubscriptionSuperseded => true,
                    SubscriptionClosed => ended.close_sent,
                };
                assert!(legal, "{:?} arrived outside its window", outcome.outcome);
                Received::Ended(outcome.outcome)
            }
            other => panic!("the publisher wrote {other:?}"),
        }
    }
}

/// Apply `update` to a bare ledger.
fn mirror_apply(
    mirror: &mut Publications<u64, Cursor>,
    update: &SubscribeHeaders,
) -> Result<Applied, SubscriptionFault> {
    let id = update.subscription_id;
    match update.operation {
        Operation::Open => mirror
            .open(
                id,
                update.update_sequence,
                update.added_credit,
                update.acknowledged,
            )
            .map(|()| Applied::Live),
        Operation::Grant => mirror.grant(
            &id,
            update.update_sequence,
            &update.acknowledged,
            update.added_credit,
        ),
        Operation::Close => mirror.close(&id, update.update_sequence, &update.acknowledged),
    }
}

/// A publisher on one session, its subscriber, and the mirror ledger.
struct Harness {
    port: Arc<FakePort>,
    capacity: ServeCapacity,
    publisher: Publisher,
    recv: FramedRecv,
    cancel: CancellationToken,
    /// Frames written and not yet delivered to the subscriber.
    inbox: VecDeque<Frame>,
    mirror: Publications<u64, Cursor>,
    subscriber: Subscriber,
}

/// The test peer.
fn peer(byte: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![byte; 32]).expect("a 32-byte peer ID is valid")
}

/// Start a publisher for `peer` on `capacity`.
fn session(
    port: &Arc<FakePort>,
    capacity: &ServeCapacity,
    peer: &ZakuraPeerId,
    session_id: u64,
) -> (Publisher, FramedRecv, CancellationToken) {
    let (publisher, recv, cancel, _task) = session_with_task(port, capacity, peer, session_id);
    (publisher, recv, cancel)
}

/// Start a publisher for `peer` on `capacity`, and keep its task handle.
fn session_with_task(
    port: &Arc<FakePort>,
    capacity: &ServeCapacity,
    peer: &ZakuraPeerId,
    session_id: u64,
) -> (
    Publisher,
    FramedRecv,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let (send, recv) = framed_channel(64);
    let cancel = CancellationToken::new();
    let (publisher, task) = Publisher::spawn::<HB>(
        port.clone(),
        capacity.push(peer),
        send,
        LeaseOwner {
            peer: SourceId::from_digest(peer.digest()),
            session_id,
        },
        limits(),
        cancel.clone(),
    );
    (publisher, recv, cancel, task)
}

impl Harness {
    /// A publisher over `port` with `serve` budgets.
    fn new(port: Arc<FakePort>, serve: ServeLimits) -> Self {
        let capacity = ServeCapacity::new("header_sync_v9", &SUBSCRIBE_HEADERS, serve)
            .expect("the test budgets are valid");
        let (publisher, recv, cancel) = session(&port, &capacity, &peer(7), 1);
        let limits = SubscriptionLimits::from_rule(&SUBSCRIBE_HEADERS)
            .expect("SUBSCRIBE_HEADERS is a subscription row");
        Self {
            port,
            capacity,
            publisher,
            recv,
            cancel,
            inbox: VecDeque::new(),
            mirror: Publications::new(limits),
            subscriber: Subscriber::new(),
        }
    }

    /// Take every frame written until the publisher goes quiet, and replay
    /// each in the mirror.
    async fn drain(&mut self) {
        while let Ok(Some(frame)) = tokio::time::timeout(QUIET, self.recv.recv()).await {
            match decode_frame::<RegtestMessage>(&frame).expect("the publisher writes valid frames")
            {
                Message::Headers(page) => {
                    let id = page.subscription_id;
                    let count = u32::try_from(page.entries.len()).expect("a page fits u32");
                    let last = page.entries.last().expect("a page has a header");
                    let consumed = self
                        .mirror
                        .credit(&id)
                        .expect("a page for a live subscription")
                        .consumed();
                    // Widening usize to u64 is lossless on supported targets.
                    let cursor = Cursor {
                        height: block::Height(page.common_ancestor_height.0 + count),
                        hash: last.header.hash(),
                        headers: consumed.objects + u64::from(count),
                        bytes: consumed.bytes + frame.payload.len() as u64,
                    };
                    self.mirror
                        .reserve_page(&id, count, frame.payload.len(), cursor)
                        .expect("the publisher spends only credit it holds");
                }
                Message::HeadersOutcome(outcome) => {
                    drop(
                        self.mirror
                            .end(&outcome.subscription_id)
                            .expect("an outcome ends a live subscription"),
                    );
                }
                other => panic!("the publisher wrote {other:?}"),
            }
            self.inbox.push_back(frame);
        }
    }

    /// Deliver up to `count` written frames to the subscriber.
    async fn deliver(&mut self, count: usize) -> Vec<Received> {
        self.drain().await;
        let count = count.min(self.inbox.len());
        self.inbox
            .drain(..count)
            .map(|frame| self.subscriber.receive(&frame))
            .collect()
    }

    /// Deliver every frame the publisher writes.
    async fn deliver_all(&mut self) -> Vec<Received> {
        self.deliver(usize::MAX).await
    }

    /// Apply `update` to the publisher once it is quiet, and check that the
    /// mirror agrees.
    async fn apply(&mut self, update: SubscribeHeaders) -> Result<Applied, PublisherFault> {
        encode_frame(&RegtestMessage::SubscribeHeaders(update.clone()))
            .expect("the subscriber sends only legal updates");
        self.drain().await;
        let expected = mirror_apply(&mut self.mirror, &update).map_err(PublisherFault::from);
        let applied = self.publisher.apply(&update).await;
        assert_eq!(applied, expected, "the publisher and the mirror agree");
        applied
    }

    /// Open a subscription to `target` with `credit`.
    async fn open(
        &mut self,
        target: block::Hash,
        locators: Vec<Frontier>,
        credit: Credit,
        schema: AuxSchema,
    ) -> u64 {
        let update = self.subscriber.open(target, locators, credit, schema);
        let id = update.subscription_id;
        assert_eq!(self.apply(update).await, Ok(Applied::Live));
        id
    }

    /// Check what holds whenever the publisher is quiet: no lease, permit,
    /// or output stays held, and the tools stay within their bounds.
    async fn check_quiet(&mut self) {
        self.drain().await;
        assert!(
            self.port.store().leases.is_empty(),
            "every lease is released"
        );
        assert_eq!(self.capacity.node_execution_held(), 0);
        assert_eq!(self.capacity.node_output_held(), 0);
        let (live, tombstones, _) = self.publisher.tally();
        assert!(live <= 2 && tombstones <= 2);
    }
}

/// A storage failure.
fn failed<T>() -> Result<T, PortError> {
    Err(PortError::Unavailable {
        source: Some(Arc::new(std::io::Error::other("disk"))),
    })
}

/// A window of 100 headers and 100 smallest pages.
fn credit() -> Credit {
    Credit {
        objects: 100,
        bytes: 100 * u32::try_from(smallest_page(HB, AuxSchema::V1)).expect("fits u32"),
    }
}

/// The parents and markers of `received` pages.
fn pages(received: &[Received]) -> Vec<(block::Height, block::Height, bool)> {
    received
        .iter()
        .filter_map(|received| match received {
            Received::Page {
                parent,
                last,
                marker,
            } => Some((parent.height, last.height, *marker)),
            Received::Ended(_) => None,
        })
        .collect()
}

/// Path pages start at the highest locator on the path, split by the page
/// limits, mark only the page ending at the target, then push descendants.
#[tokio::test(start_paused = true)]
async fn path_pages_carry_one_marker_then_follow_the_tip() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(6);
    let mut harness = Harness::new(port.clone(), serve_limits());
    let elsewhere = Frontier::new(block::Height(3), block::Hash([9; 32]));
    harness
        .open(
            chain[5].hash,
            vec![elsewhere, chain[1], port.genesis],
            credit(),
            AuxSchema::None,
        )
        .await;
    let received = harness.deliver_all().await;
    let h = block::Height;
    // The path starts at the highest locator on it and splits at three
    // headers. Only the page that ends at the target is marked.
    assert_eq!(
        pages(&received),
        [(h(2), h(5), false), (h(5), h(6), true)],
        "{received:?}"
    );

    port.grow(4);
    let received = harness.deliver_all().await;
    assert_eq!(
        pages(&received),
        [(h(6), h(9), false), (h(9), h(10), false)]
    );
    harness.check_quiet().await;
}

/// An `Open` whose locator is the target sends no path page and no marker.
/// Its first push extends the target.
#[tokio::test(start_paused = true)]
async fn a_locator_at_the_target_sends_no_path_page() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(6);
    let mut harness = Harness::new(port.clone(), serve_limits());
    harness
        .open(
            chain[5].hash,
            vec![chain[2], chain[5]],
            credit(),
            AuxSchema::None,
        )
        .await;
    assert!(harness.deliver_all().await.is_empty());
    port.grow(2);
    let h = block::Height;
    assert_eq!(pages(&harness.deliver_all().await), [(h(6), h(8), false)]);
    harness.check_quiet().await;
}

/// A schema-1 page stops before a header without a record, and the
/// subscription waits for it instead of downgrading the page.
#[tokio::test(start_paused = true)]
async fn schema_one_pages_stop_before_a_header_without_a_record() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(4);
    port.store().no_record.insert(chain[1].hash);
    let mut harness = Harness::new(port.clone(), serve_limits());
    harness
        .open(chain[3].hash, vec![port.genesis], credit(), AuxSchema::V1)
        .await;
    let h = block::Height;
    assert_eq!(pages(&harness.deliver_all().await), [(h(0), h(1), false)]);
    // The record arrives with a selected-chain change, and the path resumes.
    port.store().no_record.clear();
    port.select(port.tip());
    assert_eq!(
        pages(&harness.deliver_all().await),
        [(h(1), h(3), false), (h(3), h(4), true)]
    );
    harness.check_quiet().await;
}

/// Before the first page, an unknown target, a locator off the path, and a
/// target that left the selected chain each end with their own outcome.
#[tokio::test(start_paused = true)]
async fn outcomes_before_the_first_page_name_their_cause() {
    use HeadersOutcomeCode::*;
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(3);
    let side = port.store().extend(chain[0], 3);
    let mut harness = Harness::new(port.clone(), serve_limits());
    let unknown = block::Hash([5; 32]);
    let elsewhere = Frontier::new(block::Height(1), block::Hash([6; 32]));
    for (target, locator, outcome) in [
        (unknown, port.genesis, TargetNotRetained),
        (chain[2].hash, elsewhere, NoLocatorIntersection),
        // The side branch left the selected chain after its Status.
        (side[2].hash, port.genesis, TargetNotSelected),
    ] {
        harness
            .open(target, vec![locator], credit(), AuxSchema::None)
            .await;
        assert_eq!(harness.deliver_all().await, [Received::Ended(outcome)]);
    }
    harness.check_quiet().await;
}

/// After the first page, lease failures end in the window that is still open:
/// `HistoryPruned` before the target, `SubscriptionSuperseded` after it.
#[tokio::test(start_paused = true)]
async fn lease_failures_after_the_first_page_end_in_their_window() {
    use HeadersOutcomeCode::*;
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(8);
    let mut harness = Harness::new(port.clone(), serve_limits());
    let one_page = Credit {
        objects: 3,
        bytes: credit().bytes,
    };
    // The target leaves retention after the first page: `HistoryPruned`,
    // not the `TargetNotRetained` the lease reports.
    harness
        .open(chain[7].hash, vec![port.genesis], one_page, AuxSchema::None)
        .await;
    assert_eq!(harness.deliver_all().await.len(), 1);
    port.store().nodes.remove(&chain[7].hash);
    let grant = harness
        .subscriber
        .grant(10, 0)
        .expect("the window has room");
    harness
        .apply(grant)
        .await
        .expect("a conformant grant applies");
    assert_eq!(
        harness.deliver_all().await,
        [Received::Ended(HistoryPruned)]
    );

    // After the target, the selected chain moves below the last header:
    // `SubscriptionSuperseded`.
    port.select(chain[5]);
    harness
        .open(chain[5].hash, vec![chain[4]], credit(), AuxSchema::None)
        .await;
    assert_eq!(pages(&harness.deliver_all().await).len(), 1);
    port.select(chain[3]);
    assert_eq!(
        harness.deliver_all().await,
        [Received::Ended(SubscriptionSuperseded)]
    );

    // A higher tip on another branch: the lease finds no intersection.
    harness
        .open(chain[3].hash, vec![chain[3]], credit(), AuxSchema::None)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    let side = port.store().extend(chain[1], 4);
    port.select(side[3]);
    assert_eq!(
        harness.deliver_all().await,
        [Received::Ended(SubscriptionSuperseded)]
    );

    // History between the selected tip and the last header is gone: the
    // lease reports `HistoryPruned`, past its window.
    port.select(chain[4]);
    harness
        .open(chain[4].hash, vec![chain[4]], credit(), AuxSchema::None)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    let above = port.grow(2);
    port.store().nodes.remove(&above[0].hash);
    assert_eq!(
        harness.deliver_all().await,
        [Received::Ended(SubscriptionSuperseded)]
    );
    harness.check_quiet().await;
}

/// `Close` with pages queued ends after them, and every update after `Close`
/// faults, before and after the end.
#[tokio::test(start_paused = true)]
async fn close_with_pages_queued_ends_after_them() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(9);
    let mut harness = Harness::new(port.clone(), serve_limits());
    harness
        .open(chain[8].hash, vec![port.genesis], credit(), AuxSchema::None)
        .await;
    // Pages sit queued, unread by the subscriber, when Close arrives.
    harness.drain().await;
    assert_eq!(harness.inbox.len(), 3);
    let close = harness
        .subscriber
        .close()
        .expect("the subscription is live");
    let sequence = close.update_sequence;
    assert_eq!(harness.apply(close.clone()).await, Ok(Applied::Live));
    // Before the end, every further update faults.
    let mut grant = close.clone();
    grant.operation = Operation::Grant;
    grant.update_sequence = sequence + 1;
    grant.added_credit = Credit {
        objects: 1,
        bytes: 0,
    };
    // Apply them without yielding, so the task cannot end the subscription
    // first.
    let after_close = Err(PublisherFault::Subscription(SubscriptionFault::AfterClose));
    {
        let mut shared = lock(&harness.publisher.shared);
        assert_eq!(shared.apply(&grant, limits()), after_close);
        let mut second = close.clone();
        second.update_sequence = sequence + 1;
        assert_eq!(shared.apply(&second, limits()), after_close);
    }

    let received = harness.deliver_all().await;
    assert_eq!(pages(&received).len(), 3);
    assert_eq!(
        received.last(),
        Some(&Received::Ended(HeadersOutcomeCode::SubscriptionClosed))
    );
    // After the end, the tombstone remembers the Close, so the update still
    // faults.
    assert_eq!(harness.apply(grant).await, after_close);
    assert!(harness.deliver_all().await.is_empty());
    harness.check_quiet().await;
}

/// Updates that cross `SubscriptionSuperseded` or `HistoryPruned` are dropped
/// up to the window past the consumed credit and fault beyond it, a crossing
/// `Close` consumes the tombstone, and the next `Open` clears it.
#[tokio::test(start_paused = true)]
async fn crossing_updates_drop_on_each_outcome_tombstone() {
    use HeadersOutcomeCode::*;
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(6);
    let mut harness = Harness::new(port.clone(), serve_limits());

    // A Superseded tombstone: repeated crossing grants, then a crossing
    // Close that consumes it.
    port.select(chain[2]);
    harness
        .open(chain[2].hash, vec![chain[2]], credit(), AuxSchema::None)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    port.select(chain[1]);
    harness.drain().await;
    let mut last = None;
    while let Some(grant) = harness.subscriber.grant(1, 0) {
        assert_eq!(harness.apply(grant.clone()).await, Ok(Applied::Crossed));
        last = Some(grant);
    }
    // The crossing grants used the whole window past the consumed credit, so
    // one more header faults and changes nothing.
    let mut beyond = last.expect("the window had room");
    beyond.update_sequence += 1;
    assert_eq!(
        harness.apply(beyond.clone()).await,
        Err(PublisherFault::Subscription(
            SubscriptionFault::CrossedAboveWindow
        ))
    );
    let mut close = beyond;
    close.operation = Operation::Close;
    close.added_credit = Credit {
        objects: 0,
        bytes: 0,
    };
    assert_eq!(harness.apply(close.clone()).await, Ok(Applied::Crossed));
    let mut late = close;
    late.update_sequence += 1;
    assert_eq!(
        harness.publisher.apply(&late).await,
        Err(PublisherFault::Subscription(SubscriptionFault::Unknown))
    );
    assert_eq!(
        harness.deliver_all().await,
        [Received::Ended(SubscriptionSuperseded)]
    );

    // A HistoryPruned tombstone: a crossing grant keeps it until the next
    // Open clears it.
    port.select(chain[5]);
    let one_page = Credit {
        objects: 3,
        bytes: credit().bytes,
    };
    harness
        .open(chain[5].hash, vec![port.genesis], one_page, AuxSchema::None)
        .await;
    harness.deliver_all().await;
    port.store().nodes.remove(&chain[3].hash);
    let grant = harness.subscriber.grant(3, 0).expect("the window has room");
    harness
        .apply(grant)
        .await
        .expect("a conformant grant applies");
    let crossing = harness.subscriber.grant(3, 0).expect("the window has room");
    assert_eq!(harness.apply(crossing.clone()).await, Ok(Applied::Crossed));
    // A tombstone matches by ID and sequence, not by the fixed fields.
    let mut moved = harness.subscriber.grant(3, 0).expect("the window has room");
    moved.initial_target_tip_hash = block::Hash([4; 32]);
    assert_eq!(harness.apply(moved.clone()).await, Ok(Applied::Crossed));
    let crossing = moved;
    assert_eq!(
        harness.deliver_all().await,
        [Received::Ended(HistoryPruned)]
    );
    harness
        .open(
            block::Hash([1; 32]),
            vec![chain[0]],
            credit(),
            AuxSchema::None,
        )
        .await;
    let mut late = crossing;
    late.update_sequence += 1;
    assert_eq!(
        harness.apply(late).await,
        Err(PublisherFault::Subscription(SubscriptionFault::Unknown))
    );
    harness.check_quiet().await;
}

/// An outcome this node queued before `Close` arrived is the one it sends.
#[tokio::test(start_paused = true)]
async fn a_queued_outcome_wins_over_a_later_close() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(2);
    let mut harness = Harness::new(port.clone(), serve_limits());
    let id = harness
        .open(chain[1].hash, vec![chain[1]], credit(), AuxSchema::None)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    let close = harness
        .subscriber
        .close()
        .expect("the subscription is live");
    // The task has not run between these, so both reach `end_due` together.
    let ended = {
        let mut shared = lock(&harness.publisher.shared);
        shared.queue_end(id, HeadersOutcomeCode::SubscriptionSuperseded);
        assert_eq!(shared.apply(&close, limits()), Ok(Applied::Live));
        shared.end_due::<HB>()
    };
    let [(ended_id, frame, _terminal)] = <[_; 1]>::try_from(ended).expect("one subscription ends");
    assert_eq!(ended_id, id);
    assert!(matches!(
        decode_frame::<RegtestMessage>(&frame),
        Ok(Message::HeadersOutcome(HeadersOutcome {
            outcome: HeadersOutcomeCode::SubscriptionSuperseded,
            ..
        }))
    ));
}

/// A page job's panic reaches the publisher task's handle instead of
/// stalling the subscription.
#[tokio::test(start_paused = true)]
async fn a_page_job_panic_reaches_the_session_owner() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(2);
    port.panic_reads
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let capacity = ServeCapacity::new("header_sync_v9", &SUBSCRIBE_HEADERS, serve_limits())
        .expect("the test budgets are valid");
    let (publisher, _recv, _cancel, task) = session_with_task(&port, &capacity, &peer(7), 1);
    let mut subscriber = Subscriber::new();
    let open = subscriber.open(chain[1].hash, vec![port.genesis], credit(), AuxSchema::None);
    publisher.apply(&open).await.expect("the Open applies");
    let ended = tokio::time::timeout(QUIET, task)
        .await
        .expect("the publisher task ends");
    assert!(ended.is_err_and(|error| error.is_panic()));
}

/// A following page whose headers leave the selected chain while it is in
/// progress is dropped and read again toward the new tip.
#[tokio::test(start_paused = true)]
async fn a_following_page_that_leaves_the_selected_chain_is_read_again() {
    let port = FakePort::new(0);
    let chain = port.grow(3);
    let mut harness = Harness::new(port.clone(), serve_limits());
    harness
        .open(chain[2].hash, vec![chain[2]], credit(), AuxSchema::None)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    // The page for heights 4 and 5 waits in its read while the chain reorgs
    // onto a branch that forks at the last header sent.
    port.grow(2);
    harness.drain().await;
    assert_eq!(port.store().reads_running, 1);
    let side = port.store().extend(chain[2], 3);
    port.select(side[2]);
    port.gate.add_permits(1);
    let received = harness.deliver_all().await;
    let h = block::Height;
    assert_eq!(pages(&received), [(h(3), h(6), false)], "{received:?}");
    assert!(matches!(received[0], Received::Page { last, .. } if last == side[2]));
    harness.check_quiet().await;
}

/// Cancelling the session stops the publisher even while a terminal outcome
/// waits for a full transport queue.
#[tokio::test(start_paused = true)]
async fn cancel_stops_a_terminal_that_waits_for_a_full_queue() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(6);
    let capacity = ServeCapacity::new("header_sync_v9", &SUBSCRIBE_HEADERS, serve_limits())
        .expect("the test budgets are valid");
    // One slot, which the first page fills because nobody reads it.
    let (send, _recv) = framed_channel(1);
    let cancel = CancellationToken::new();
    let (publisher, task) = Publisher::spawn::<HB>(
        port.clone(),
        capacity.push(&peer(7)),
        send,
        LeaseOwner {
            peer: SourceId::from_digest(peer(7).digest()),
            session_id: 1,
        },
        limits(),
        cancel.clone(),
    );
    let mut subscriber = Subscriber::new();
    let open = subscriber.open(chain[5].hash, vec![port.genesis], credit(), AuxSchema::None);
    publisher.apply(&open).await.expect("the Open applies");
    tokio::time::sleep(QUIET).await;
    let close = subscriber.close().expect("the subscription is live");
    publisher.apply(&close).await.expect("the Close applies");
    tokio::time::sleep(QUIET).await;
    assert!(
        !task.is_finished(),
        "the terminal outcome waits for the queue"
    );
    cancel.cancel();
    tokio::time::timeout(QUIET, task)
        .await
        .expect("cancel stops the publisher")
        .expect("the publisher task does not panic");
}

/// The margin admits a second `Open`. A third, a reused ID, a changed target,
/// and an unadvertised schema fault.
#[tokio::test(start_paused = true)]
async fn a_second_open_is_admitted_and_a_third_or_a_reused_id_faults() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(2);
    let harness = Harness::new(port.clone(), serve_limits());
    let open = |id| SubscribeHeaders {
        operation: Operation::Open,
        subscription_id: id,
        update_sequence: 0,
        initial_target_tip_hash: chain[1].hash,
        acknowledged: Cursor {
            height: chain[1].height,
            hash: chain[1].hash,
            headers: 0,
            bytes: 0,
        },
        locator_hashes: vec![chain[1].hash],
        added_credit: credit(),
        tree_aux_schema: AuxSchema::None,
    };
    let publisher = &harness.publisher;
    assert_eq!(publisher.apply(&open(1)).await, Ok(Applied::Live));
    assert_eq!(publisher.apply(&open(2)).await, Ok(Applied::Live));
    assert_eq!(publisher.tally(), (2, 0, 1), "the margin admits the second");
    assert_eq!(
        publisher.apply(&open(3)).await,
        Err(PublisherFault::Subscription(SubscriptionFault::NoSlot {
            held: 2,
            limit: 1
        }))
    );
    assert_eq!(
        publisher.apply(&open(1)).await,
        Err(PublisherFault::Subscription(SubscriptionFault::ReusedKey))
    );
    let mut moved = open(1);
    moved.operation = Operation::Grant;
    moved.update_sequence = 1;
    moved.locator_hashes.clear();
    moved.initial_target_tip_hash = chain[0].hash;
    assert_eq!(
        publisher.apply(&moved).await,
        Err(PublisherFault::ChangedFields)
    );
    let mut records = open(4);
    records.tree_aux_schema = AuxSchema::V1;
    let (narrow, _recv, _cancel) = {
        let capacity = ServeCapacity::new("header_sync_v9", &SUBSCRIBE_HEADERS, serve_limits())
            .expect("the test budgets are valid");
        let (send, recv) = framed_channel(4);
        let cancel = CancellationToken::new();
        let without_records = PageLimits {
            aux_schema_mask: 0,
            ..limits()
        };
        let (publisher, _task) = Publisher::spawn::<HB>(
            port.clone(),
            capacity.push(&peer(8)),
            send,
            LeaseOwner {
                peer: SourceId::from_digest(peer(8).digest()),
                session_id: 1,
            },
            without_records,
            cancel.clone(),
        );
        (publisher, recv, cancel)
    };
    assert_eq!(
        narrow.apply(&records).await,
        Err(PublisherFault::UnadvertisedSchema)
    );
}

/// A capacity refusal waits for its release signal, and a lost lease is
/// acquired again, without a message on the wire.
#[tokio::test(start_paused = true)]
async fn capacity_refusals_and_lost_leases_wait_without_a_message() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(3);
    let mut harness = Harness::new(port.clone(), serve_limits());
    let signal = ServingCapacitySignal::default();
    port.store()
        .acquire_script
        .push_back(Ok(AcquirePathReply::CapacityBusy(signal.clone())));
    harness
        .open(chain[2].hash, vec![port.genesis], credit(), AuxSchema::None)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    harness.check_quiet().await;
    // One lost lease is acquired again at once.
    port.store()
        .read_script
        .push_back(Ok(ReadPathReply::Unavailable));
    signal.release();
    assert_eq!(pages(&harness.deliver_all().await).len(), 1);

    // Two lost leases wait for the next selected-chain change.
    port.store().read_script.extend([
        Ok(ReadPathReply::Unavailable),
        Ok(ReadPathReply::Unavailable),
    ]);
    port.grow(1);
    assert!(harness.deliver_all().await.is_empty());
    port.grow(1);
    assert_eq!(pages(&harness.deliver_all().await).len(), 1);
    harness.check_quiet().await;
}

/// Storage failures release every lease and permit, send nothing, and retry
/// on the next selected-chain change.
#[tokio::test(start_paused = true)]
async fn storage_failures_release_resources_once_and_never_fault() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(3);
    let mut harness = Harness::new(port.clone(), serve_limits());
    port.store().acquire_script.push_back(failed());
    harness
        .open(chain[2].hash, vec![port.genesis], credit(), AuxSchema::None)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    harness.check_quiet().await;
    // A failed read leaves its lease, and the publisher releases it.
    port.store().read_script.push_back(failed());
    port.select(port.tip());
    assert!(harness.deliver_all().await.is_empty());
    harness.check_quiet().await;
    port.select(port.tip());
    assert_eq!(pages(&harness.deliver_all().await).len(), 1);
    harness.check_quiet().await;
}

/// With every node slot and output byte held, grants still apply and `Close`
/// still ends the subscription and frees its slot.
#[tokio::test(start_paused = true)]
async fn close_needs_no_capacity_and_grants_still_apply() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(3);
    let mut harness = Harness::new(port.clone(), serve_limits());
    let hold = harness.capacity.hold_node_for_test();
    let id = harness
        .open(chain[2].hash, vec![port.genesis], credit(), AuxSchema::None)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    let grant = harness.subscriber.grant(1, 0).expect("the window has room");
    assert_eq!(harness.apply(grant).await, Ok(Applied::Live));
    let close = harness
        .subscriber
        .close()
        .expect("the subscription is live");
    assert_eq!(harness.apply(close).await, Ok(Applied::Live));
    assert_eq!(
        harness.deliver_all().await,
        [Received::Ended(HeadersOutcomeCode::SubscriptionClosed)]
    );
    assert_eq!(harness.publisher.unacknowledged(id), None);
    assert_eq!(harness.publisher.tally().0, 0, "the slot is free");
    drop(hold);
    harness.check_quiet().await;
}

/// A blocked read keeps its execution permit through disconnect, and
/// reconnects never run more reads than the node-wide bound.
#[tokio::test(start_paused = true)]
async fn a_blocked_read_keeps_its_permit_through_disconnect_and_churn() {
    let port = FakePort::new(0);
    let chain = port.grow(3);
    let serve = ServeLimits {
        node_execution: 1,
        ..serve_limits()
    };
    let mut harness = Harness::new(port.clone(), serve);
    harness
        .open(chain[2].hash, vec![port.genesis], credit(), AuxSchema::None)
        .await;
    harness.drain().await;
    assert_eq!(port.store().reads_running, 1);
    assert_eq!(harness.capacity.node_execution_held(), 1);

    // Disconnect: the read keeps its permit until state answers.
    harness.cancel.cancel();
    drop(harness.publisher);
    tokio::time::sleep(QUIET).await;
    assert_eq!(harness.capacity.node_execution_held(), 1);

    // Reconnect repeatedly, from this peer and others, while it stays
    // blocked. Each new read waits for the node-wide execution slot.
    let mut sessions = Vec::new();
    for round in 0..4_u8 {
        let peer = peer(10 + round % 2);
        let (publisher, recv, cancel) =
            session(&port, &harness.capacity, &peer, u64::from(round) + 2);
        let mut subscriber = Subscriber::new();
        let open = subscriber.open(chain[2].hash, vec![port.genesis], credit(), AuxSchema::None);
        publisher.apply(&open).await.expect("the Open applies");
        tokio::time::sleep(QUIET).await;
        assert_eq!(port.store().most_reads_running, 1);
        assert_eq!(harness.capacity.node_execution_held(), 1);
        sessions.push((publisher, recv, cancel, subscriber));
    }

    // State answers: the slot passes from read to read, one at a time.
    port.gate.add_permits(1);
    for (_publisher, recv, _cancel, subscriber) in &mut sessions {
        let frame = tokio::time::timeout(QUIET * 10, recv.recv())
            .await
            .expect("each session's page arrives")
            .expect("the session is open");
        assert!(matches!(subscriber.receive(&frame), Received::Page { .. }));
    }
    assert_eq!(port.store().most_reads_running, 1);
    tokio::time::sleep(QUIET).await;
    assert_eq!(harness.capacity.node_execution_held(), 0);
    assert!(port.store().leases.is_empty());
}

/// A burst of conformant grants yields to another peer's task.
#[test]
fn update_bursts_yield_to_other_peers() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("the test runtime starts");
    runtime.block_on(async {
        let port = FakePort::new(Semaphore::MAX_PERMITS);
        let chain = port.grow(1);
        let harness = Harness::new(port.clone(), serve_limits());
        let other = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let ticks = other.clone();
        let other_peer = tokio::spawn(async move {
            loop {
                ticks.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
        });
        let mut subscriber = Subscriber::new();
        let first = Credit {
            objects: 1,
            bytes: p(),
        };
        let open = subscriber.open(chain[0].hash, vec![chain[0]], first, AuxSchema::None);
        harness
            .publisher
            .apply(&open)
            .await
            .expect("the Open applies");
        let before = other.load(std::sync::atomic::Ordering::Relaxed);
        // The window takes 3,999 more one-header grants.
        let mut grants = 0;
        while let Some(grant) = subscriber.grant(1, 0) {
            harness
                .publisher
                .apply(&grant)
                .await
                .expect("a conformant grant applies");
            grants += 1;
        }
        assert_eq!(grants, 3_999);
        let during = other.load(std::sync::atomic::Ordering::Relaxed) - before;
        assert!(during > 0, "the burst yielded to another peer");
        other_peer.abort();
    });
}

/// Every outcome this node queues is adjusted into a legal window, and a
/// legal one is kept.
#[test]
fn every_own_outcome_lands_in_its_window() {
    use HeadersOutcomeCode::*;
    for outcome in [
        TargetNotRetained,
        NoLocatorIntersection,
        HistoryPruned,
        TargetNotSelected,
        SubscriptionSuperseded,
    ] {
        for pages_sent in [false, true] {
            for reached in [false, true] {
                let sent = in_window(outcome, pages_sent, reached);
                assert!(
                    is_legal(sent, pages_sent, reached, false),
                    "{outcome:?} became {sent:?} with pages {pages_sent} and reached {reached}"
                );
                if is_legal(outcome, pages_sent, reached, false) {
                    assert_eq!(sent, outcome, "a legal outcome is kept");
                }
            }
        }
    }
}

/// One step of a generated session.
#[derive(Clone, Debug)]
enum Op {
    /// Extend the selected chain.
    Grow(u8),
    /// Select a branch forking `depth` below the tip, `len` headers long.
    Reorg {
        depth: u8,
        len: u8,
    },
    /// Open to the selected tip `stale` selections ago, with a locator
    /// `depth` below it. `aim` 0 names an unknown target, and 1 sends only
    /// a locator off every path.
    Open {
        stale: u8,
        depth: u8,
        headers: u16,
        pages: u8,
        records: bool,
        aim: u8,
    },
    /// Deliver queued frames.
    Deliver(u8),
    /// Grant headers and smallest pages of bytes.
    Grant {
        headers: u16,
        pages: u8,
    },
    Close,
    /// Refuse the next lease for capacity.
    CapacityBusy,
    /// Release every capacity refusal.
    ReleaseCapacity,
    /// Fail the next lease or read in storage.
    StorageFailure {
        read: bool,
    },
    /// Lose the next read's lease.
    LoseLease,
    /// Forget the header `depth` below the tip.
    Prune(u8),
}

/// A generated operation.
fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (1..4_u8).prop_map(Op::Grow),
        1 => (1..4_u8, 0..5_u8).prop_map(|(depth, len)| Op::Reorg { depth, len }),
        3 => (0..4_u8, 0..8_u8, 1..12_u16, 1..8_u8, any::<bool>(), 0..10_u8).prop_map(
            |(stale, depth, headers, pages, records, aim)| Op::Open {
                stale, depth, headers, pages, records, aim
            }
        ),
        4 => (1..6_u8).prop_map(Op::Deliver),
        3 => (0..8_u16, 0..4_u8).prop_map(|(headers, pages)| Op::Grant { headers, pages }),
        1 => Just(Op::Close),
        1 => Just(Op::CapacityBusy),
        1 => Just(Op::ReleaseCapacity),
        1 => any::<bool>().prop_map(|read| Op::StorageFailure { read }),
        1 => Just(Op::LoseLease),
        1 => (1..6_u8).prop_map(Op::Prune),
    ]
}

/// Run `ops` against one session, checking every rule after each step.
async fn run(ops: Vec<Op>) {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    port.grow(4);
    let mut harness = Harness::new(port.clone(), serve_limits());
    let mut tips = vec![port.tip()];
    let mut signals = Vec::new();
    for op in ops {
        match op {
            Op::Grow(count) => {
                port.grow(u32::from(count));
                tips.push(port.tip());
            }
            Op::Reorg { depth, len } => {
                let tip = port.tip();
                let height = block::Height(tip.height.0.saturating_sub(u32::from(depth)));
                let base = port.store().ancestor(tip, height);
                if let Some(base) = base {
                    let added = port.store().extend(base, u32::from(len));
                    port.select(added.last().copied().unwrap_or(base));
                    tips.push(port.tip());
                }
            }
            Op::Open {
                stale,
                depth,
                headers,
                pages,
                records,
                aim,
            } if harness.subscriber.live.is_none() => {
                let mut target = tips[tips.len() - 1 - usize::from(stale).min(tips.len() - 1)];
                let height = block::Height(target.height.0.saturating_sub(u32::from(depth)));
                let base = port
                    .store()
                    .ancestor(target, height)
                    .unwrap_or(port.genesis);
                let mut locators = vec![base];
                if base != port.genesis {
                    locators.push(port.genesis);
                }
                match aim {
                    0 => target.hash = block::Hash([0xee; 32]),
                    1 => locators = vec![Frontier::new(height, block::Hash([0xef; 32]))],
                    _ => {}
                }
                let schema = if records {
                    AuxSchema::V1
                } else {
                    AuxSchema::None
                };
                let page = u32::try_from(smallest_page(HB, schema)).expect("fits u32");
                let credit = Credit {
                    objects: u32::from(headers),
                    bytes: u32::from(pages) * page,
                };
                harness.open(target.hash, locators, credit, schema).await;
            }
            Op::Open { .. } => {}
            Op::Deliver(count) => {
                harness.deliver(usize::from(count)).await;
            }
            Op::Grant { headers, pages } => {
                if let Some(grant) = harness
                    .subscriber
                    .grant(u32::from(headers), u32::from(pages) * p())
                {
                    harness
                        .apply(grant)
                        .await
                        .expect("a conformant grant never faults");
                }
            }
            Op::Close => {
                if let Some(close) = harness.subscriber.close() {
                    harness
                        .apply(close)
                        .await
                        .expect("a conformant close never faults");
                }
            }
            Op::CapacityBusy => {
                let signal = ServingCapacitySignal::default();
                port.store()
                    .acquire_script
                    .push_back(Ok(AcquirePathReply::CapacityBusy(signal.clone())));
                signals.push(signal);
            }
            Op::ReleaseCapacity => signals.drain(..).for_each(|signal| signal.release()),
            Op::StorageFailure { read } => {
                let mut store = port.store();
                if read {
                    store.read_script.push_back(failed());
                } else {
                    store.acquire_script.push_back(failed());
                }
            }
            Op::LoseLease => port
                .store()
                .read_script
                .push_back(Ok(ReadPathReply::Unavailable)),
            Op::Prune(depth) => {
                let tip = port.tip();
                let height = block::Height(tip.height.0.saturating_sub(u32::from(depth)));
                let mut store = port.store();
                if let Some(pruned) = store.ancestor(tip, height) {
                    if pruned != tip && pruned != port.genesis {
                        store.nodes.remove(&pruned.hash);
                    }
                }
            }
        }
        harness.check_quiet().await;
        if let Some(opened) = &harness.subscriber.live {
            let unacknowledged = harness.publisher.unacknowledged(opened.id).unwrap_or(0);
            assert!(unacknowledged <= 4_000, "the sent cursor ring never fills");
        }
    }

    // Close ends any live subscription with exactly one outcome.
    signals.drain(..).for_each(|signal| signal.release());
    if let Some(close) = harness.subscriber.close() {
        harness
            .apply(close)
            .await
            .expect("a conformant close never faults");
    }
    harness.deliver_all().await;
    assert!(
        harness.subscriber.live.is_none(),
        "every subscription ended"
    );
    assert_eq!(harness.publisher.tally().0, 0);
    harness.check_quiet().await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Generated sessions follow every page, marker, window, credit, and
    /// ownership rule, and the publisher's ledger matches the mirror.
    #[test]
    fn publisher_sessions_keep_every_rule(ops in prop::collection::vec(op(), 1..40)) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("the test runtime starts")
            .block_on(run(ops));
    }
}
