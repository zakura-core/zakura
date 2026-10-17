//! Tests for the header sync version 9 publisher.
//!
//! A fake port serves a header tree from memory, with scripted failures. The
//! subscriber uses the real [`Subscriptions`] tool and checks each page's
//! linkage, the target marker, and each outcome's window.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};

use proptest::prelude::*;
use tokio::sync::Semaphore;
use zakura_chain::work::difficulty::U256;
use zakura_header_chain::{
    AlarmSet, BodyWorkOwner, ChainScore, EngineMode, FrontierSet, HeaderLocator, StateVersion,
    SuffixWork, VerifiedGeneration,
};
use zakura_node_services::header_chain::{
    AdapterKey, ApplyHeaderTargetReply, HeaderChainFuture, PortError, PrepareHeaderTarget,
    PrepareHeaderTargetReply, PreparedHeaderTarget, VctRepairContextReply,
};

use super::super::{
    smallest_page,
    test_support::{p, record, HeaderTree, OUTCOME_CODES},
    RegtestMessage,
};
use super::*;
use crate::zakura::{
    framed_channel,
    header_sync::config::REGTEST_HEADER_BYTES,
    regulation::{ServeCapacity, ServeLimits, Subscriptions},
    wire_codec::decode_frame,
    Credit, FramedRecv, ZakuraPeerId,
};

mod load;

/// Regtest header bytes, so pages stay small.
const HB: usize = REGTEST_HEADER_BYTES;

/// How long the paused clock runs before a receive counts as quiet.
const QUIET: Duration = Duration::from_secs(1);

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
///
/// Like state, it holds one lease per peer, refuses a second for capacity
/// until the first ends, and keeps a peer's continuation after each partial
/// read, which a later lease toward the same target from the last header
/// read reuses without walking the ancestry.
#[derive(Default)]
struct Store {
    tree: HeaderTree,
    next_lease: u64,
    /// Each live lease's peer and the signal its end releases.
    leases: HashMap<u64, (SourceId, ServingCapacitySignal)>,
    /// Each peer's target and last header read, after a partial read.
    continuations: HashMap<SourceId, (block::Hash, Frontier)>,
    /// Leases that walked the ancestry instead of reusing a continuation.
    walks: usize,
    /// Parent steps those walks took.
    walk_steps: usize,
    /// The next release fails and leaves its lease held.
    leak_release: bool,
    /// The selected tip grows by one header after each of this many leases.
    move_tip_on_acquire: usize,
    acquire_script: VecDeque<Result<AcquirePathReply, PortError>>,
    read_script: VecDeque<Result<ReadPathReply, PortError>>,
    /// Headers without a schema-1 record.
    no_record: HashSet<block::Hash>,
    reads_running: usize,
    most_reads_running: usize,
}

impl Store {
    /// Lease the path from the nearest locator to the target, as state does.
    fn acquire(
        &mut self,
        key: &AdapterKey,
        request: AcquirePath,
    ) -> Result<AcquirePathReply, PortError> {
        if let Some(reply) = self.acquire_script.pop_front() {
            return reply;
        }
        if let Some((_, signal)) = self
            .leases
            .values()
            .find(|(peer, _)| *peer == request.source)
        {
            return Ok(AcquirePathReply::CapacityBusy(signal.clone()));
        }
        let Some(&(target, _)) = self.tree.nodes.get(&request.target_tip_hash) else {
            return Ok(AcquirePathReply::TargetNotRetained);
        };
        let current = match self.continuations.remove(&request.source) {
            Some((continued, last))
                if continued == target.hash && request.locator_hashes == [last.hash] =>
            {
                last
            }
            _ => {
                self.walks += 1;
                let mut current = target;
                while !request.locator_hashes.contains(&current.hash) {
                    self.walk_steps += 1;
                    current = match self.tree.parent(current) {
                        Some(parent) => parent,
                        None if current.height == block::Height(0) => {
                            return Ok(AcquirePathReply::NoLocatorIntersection)
                        }
                        None => return Ok(AcquirePathReply::HistoryPruned),
                    };
                }
                current
            }
        };
        self.next_lease += 1;
        self.leases.insert(
            self.next_lease,
            (request.source, ServingCapacitySignal::default()),
        );
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

    /// End lease `id`, release its signal, and return its peer, if it was
    /// live.
    fn end_lease(&mut self, id: u64) -> Option<SourceId> {
        let (peer, signal) = self.leases.remove(&id)?;
        signal.release();
        Some(peer)
    }

    /// Read one page and consume the lease, as state does.
    fn read(
        &mut self,
        path: &RetainedHeaderPath,
        request: &ReadPath,
    ) -> Result<ReadPathReply, PortError> {
        if let Some(reply) = self.read_script.pop_front() {
            if matches!(reply, Ok(ReadPathReply::Unavailable)) {
                self.end_lease(path.handle_id());
            }
            return reply;
        }
        let Some(peer) = self.end_lease(path.handle_id()) else {
            return Ok(ReadPathReply::Unavailable);
        };
        if request.after_hash != path.common_ancestor.hash {
            return Ok(ReadPathReply::Unavailable);
        }
        let mut headers = Vec::new();
        let mut current = path.target;
        while current != path.common_ancestor {
            let (Some((_, header)), Some(parent)) = (
                self.tree.nodes.get(&current.hash),
                self.tree.parent(current),
            ) else {
                return Ok(ReadPathReply::Unavailable);
            };
            headers.push(header.clone());
            current = parent;
        }
        headers.reverse();
        let count = usize::try_from(request.max_header_count).unwrap_or(usize::MAX);
        let complete = headers.len() <= count;
        headers.truncate(count);
        if !complete {
            let height = path.common_ancestor.height.0 + u32::try_from(count).unwrap_or(0);
            let last = headers.last().expect("a partial read has a header").hash();
            self.continuations.insert(
                peer,
                (path.target.hash, Frontier::new(block::Height(height), last)),
            );
        }
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
pub(crate) struct FakePort {
    key: AdapterKey,
    store: Mutex<Store>,
    genesis: Frontier,
    selected: watch::Sender<Option<EngineSnapshot>>,
    /// Every read waits for a permit here.
    gate: Semaphore,
    /// Every lease acquisition waits for a permit here.
    acquire_gate: Semaphore,
    /// Reads panic, standing in for a local bug.
    panic_reads: std::sync::atomic::AtomicBool,
}

impl FakePort {
    /// A store holding the Regtest genesis header, selected, with `gate`
    /// read permits.
    pub(crate) fn new(gate: usize) -> Arc<Self> {
        let store = Store::default();
        let genesis = store.tree.genesis;
        Arc::new(Self {
            key: AdapterKey::new(),
            store: Mutex::new(store),
            genesis,
            selected: watch::channel(Some(snapshot(genesis, 1, genesis))).0,
            gate: Semaphore::new(gate),
            acquire_gate: Semaphore::new(Semaphore::MAX_PERMITS),
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
    pub(crate) fn grow(&self, count: u32) -> Vec<Frontier> {
        let added = self.store().tree.extend(self.tip(), count);
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
        Box::pin(async move {
            let _permit = self
                .acquire_gate
                .acquire()
                .await
                .expect("the gate never closes");
            let (reply, moves) = {
                let mut store = self.store();
                let reply = store.acquire(&self.key, request);
                let moves = store.move_tip_on_acquire.checked_sub(1);
                if let Some(left) = moves {
                    store.move_tip_on_acquire = left;
                }
                (reply, moves.is_some())
            };
            if moves {
                self.grow(1);
            }
            reply
        })
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
            let mut store = self.store();
            if std::mem::take(&mut store.leak_release) {
                return Err(PortError::Unavailable { source: None });
            }
            store.end_lease(path.handle_id());
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
        if added.objects == 0 && added.bytes < p::<HB>() {
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
                // The specification's window table, kept apart from the
                // publisher's own `is_legal`.
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

/// A publisher on one session and its subscriber.
struct Harness {
    port: Arc<FakePort>,
    capacity: ServeCapacity,
    publisher: Publisher,
    recv: FramedRecv,
    cancel: CancellationToken,
    /// Frames written and not yet delivered to the subscriber.
    inbox: VecDeque<Frame>,
    subscriber: Subscriber,
}

/// The test peer.
fn peer(byte: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![byte; 32]).expect("a 32-byte peer ID is valid")
}

/// Start a publisher for `peer` on `capacity`, writing to a transport queue
/// of `queue` frames.
fn session(
    port: &Arc<FakePort>,
    capacity: &ServeCapacity,
    peer: &ZakuraPeerId,
    session_id: u64,
    queue: usize,
) -> (
    Publisher,
    FramedRecv,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    session_advertising(port, capacity, peer, session_id, queue, limits())
}

/// [`session`], advertising `pages` instead of the default page limits.
fn session_advertising(
    port: &Arc<FakePort>,
    capacity: &ServeCapacity,
    peer: &ZakuraPeerId,
    session_id: u64,
    queue: usize,
    pages: PageLimits,
) -> (
    Publisher,
    FramedRecv,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let (send, recv) = framed_channel(queue);
    let cancel = CancellationToken::new();
    let (publisher, task) = Publisher::spawn::<HB>(
        port.clone(),
        capacity.push(peer),
        send,
        LeaseOwner {
            peer: SourceId::from_digest(peer.digest()),
            session_id,
        },
        pages,
        cancel.clone(),
    );
    (publisher, recv, cancel, task)
}

impl Publisher {
    /// Live or closing subscriptions and tombstones, as the subscription
    /// tools count them.
    fn tally(&self) -> (usize, usize) {
        let shared = lock(&self.shared);
        (
            shared.publications.keys().count(),
            shared.publications.tombstones(),
        )
    }

    /// Unacknowledged pages of `id`, if it is live.
    fn unacknowledged(&self, id: u64) -> Option<usize> {
        lock(&self.shared).publications.unacknowledged(&id)
    }
}

impl Harness {
    /// A publisher over `port` with `serve` budgets.
    fn new(port: Arc<FakePort>, serve: ServeLimits) -> Self {
        Self::advertising(port, serve, limits())
    }

    /// A publisher over `port` with `serve` budgets that advertises `pages`.
    fn advertising(port: Arc<FakePort>, serve: ServeLimits, pages: PageLimits) -> Self {
        let capacity = ServeCapacity::new("header_sync_v9", &SUBSCRIBE_HEADERS, serve)
            .expect("the test budgets are valid");
        let (publisher, recv, cancel, _task) =
            session_advertising(&port, &capacity, &peer(7), 1, 64, pages);
        Self {
            port,
            capacity,
            publisher,
            recv,
            cancel,
            inbox: VecDeque::new(),
            subscriber: Subscriber::new(),
        }
    }

    /// Take every frame written until the publisher goes quiet.
    async fn drain(&mut self) {
        while let Ok(Some(frame)) = tokio::time::timeout(QUIET, self.recv.recv()).await {
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

    /// Apply `update` to the publisher once it is quiet.
    async fn apply(&mut self, update: SubscribeHeaders) -> Result<Applied, PublisherFault> {
        encode_frame(&RegtestMessage::SubscribeHeaders(update.clone()))
            .expect("the subscriber sends only legal updates");
        self.drain().await;
        self.publisher.apply(&update).await
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
        let (live, tombstones) = self.publisher.tally();
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
    // Each run toward one target walks the ancestry once, and later pages
    // reuse state's continuation.
    assert_eq!(port.store().walks, 2);
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

/// On the path to its target, a schema-1 subscription sends the pages that
/// have records and ends with `HistoryPruned` at the first header without
/// one, never skipping it to reach records above.
#[tokio::test(start_paused = true)]
async fn schema_one_path_pages_end_as_history_pruned_at_a_missing_record() {
    use HeadersOutcomeCode::HistoryPruned;
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(6);
    let mut harness = Harness::new(port.clone(), serve_limits());
    let h = block::Height;

    // The first record is missing: no page, and the subscription ends.
    port.store().no_record.insert(chain[0].hash);
    harness
        .open(chain[5].hash, vec![port.genesis], credit(), AuxSchema::V1)
        .await;
    assert_eq!(
        harness.deliver_all().await,
        [Received::Ended(HistoryPruned)]
    );

    // Records are missing below a height and present above it, as on a
    // pruned node. The page cannot skip the hole to reach them.
    port.store().no_record.insert(chain[1].hash);
    harness
        .open(chain[5].hash, vec![port.genesis], credit(), AuxSchema::V1)
        .await;
    assert_eq!(
        harness.deliver_all().await,
        [Received::Ended(HistoryPruned)]
    );

    // A prefix with records goes out, then the hole ends the subscription.
    port.store().no_record = HashSet::from([chain[2].hash]);
    harness
        .open(chain[5].hash, vec![port.genesis], credit(), AuxSchema::V1)
        .await;
    let received = harness.deliver_all().await;
    assert_eq!(pages(&received), [(h(0), h(2), false)]);
    assert_eq!(received.last(), Some(&Received::Ended(HistoryPruned)));
    harness.check_quiet().await;
}

/// A complete schema-1 path still reaches its target, and past the target,
/// whether a page or the `Open` reached it, a missing record waits.
#[tokio::test(start_paused = true)]
async fn schema_one_past_the_target_waits_for_a_missing_record() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(4);
    let mut harness = Harness::new(port.clone(), serve_limits());
    let h = block::Height;
    harness
        .open(chain[3].hash, vec![port.genesis], credit(), AuxSchema::V1)
        .await;
    assert_eq!(
        pages(&harness.deliver_all().await),
        [(h(0), h(2), false), (h(2), h(4), true)]
    );
    let next = port.grow(1);
    port.store().no_record.insert(next[0].hash);
    assert!(harness.deliver_all().await.is_empty());
    let close = harness
        .subscriber
        .close()
        .expect("the subscription is live");
    harness.apply(close).await.expect("the Close applies");
    assert_eq!(
        harness.deliver_all().await,
        [Received::Ended(HeadersOutcomeCode::SubscriptionClosed)]
    );

    // A target reached at Open waits the same way, and resumes once the
    // record arrives with a selected-chain change.
    let tip = port.tip();
    harness
        .open(tip.hash, vec![tip], credit(), AuxSchema::V1)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    let next = port.grow(1);
    port.store().no_record.insert(next[0].hash);
    assert!(harness.deliver_all().await.is_empty());
    port.store().no_record.clear();
    port.select(port.tip());
    assert_eq!(pages(&harness.deliver_all().await), [(h(5), h(6), false)]);
    harness.check_quiet().await;
}

/// Missing records change nothing without schema 1, and a capacity refusal
/// still waits before the publisher looks at any record.
#[tokio::test(start_paused = true)]
async fn missing_records_leave_schema_zero_and_capacity_waits_alone() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(3);
    port.store()
        .no_record
        .extend(chain.iter().map(|frontier| frontier.hash));
    let mut harness = Harness::new(port.clone(), serve_limits());
    let h = block::Height;
    harness
        .open(chain[2].hash, vec![port.genesis], credit(), AuxSchema::None)
        .await;
    assert_eq!(pages(&harness.deliver_all().await), [(h(0), h(3), true)]);
    let close = harness
        .subscriber
        .close()
        .expect("the subscription is live");
    harness.apply(close).await.expect("the Close applies");
    harness.deliver_all().await;

    let signal = ServingCapacitySignal::default();
    port.store()
        .acquire_script
        .push_back(Ok(AcquirePathReply::CapacityBusy(signal.clone())));
    let target = *port.grow(2).last().expect("two headers were added");
    harness
        .open(target.hash, vec![port.genesis], credit(), AuxSchema::V1)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    signal.release();
    assert_eq!(
        harness.deliver_all().await,
        [Received::Ended(HeadersOutcomeCode::HistoryPruned)]
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
    let side = port.store().tree.extend(chain[0], 3);
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

/// A selection probe that runs into pruned history ends the subscription as
/// `HistoryPruned` instead of waiting for history that will not return.
#[tokio::test(start_paused = true)]
async fn a_selection_probe_through_pruned_history_ends_as_history_pruned() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(5);
    let mut harness = Harness::new(port.clone(), serve_limits());
    // The target, height 2, is from an older Status, and a header between it
    // and the selected tip is gone.
    port.store().tree.nodes.remove(&chain[3].hash);
    harness
        .open(chain[1].hash, vec![port.genesis], credit(), AuxSchema::None)
        .await;
    assert_eq!(
        harness.deliver_all().await,
        [Received::Ended(HeadersOutcomeCode::HistoryPruned)]
    );
    // With a locator at the target, the Open reached it, so `HistoryPruned`
    // is out of its window and the subscription ends as superseded.
    harness
        .open(chain[1].hash, vec![chain[1]], credit(), AuxSchema::None)
        .await;
    assert_eq!(
        harness.deliver_all().await,
        [Received::Ended(HeadersOutcomeCode::SubscriptionSuperseded)]
    );
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
    port.store().tree.nodes.remove(&chain[7].hash);
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

    port.select(chain[3]);

    // A higher tip on another branch: the lease finds no intersection.
    harness
        .open(chain[3].hash, vec![chain[3]], credit(), AuxSchema::None)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    let side = port.store().tree.extend(chain[1], 4);
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
    port.store().tree.nodes.remove(&above[0].hash);
    assert_eq!(
        harness.deliver_all().await,
        [Received::Ended(SubscriptionSuperseded)]
    );
    harness.check_quiet().await;
}

/// `Close` with pages queued ends after them.
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
    assert_eq!(harness.apply(close).await, Ok(Applied::Live));
    let received = harness.deliver_all().await;
    assert_eq!(pages(&received).len(), 3);
    assert_eq!(
        received.last(),
        Some(&Received::Ended(HeadersOutcomeCode::SubscriptionClosed))
    );
    harness.check_quiet().await;
}

/// Updates that cross this node's own outcome are dropped, and its tombstone
/// matches them by ID and sequence, not by their target.
#[tokio::test(start_paused = true)]
async fn crossing_updates_drop_on_an_outcome_tombstone() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(3);
    let mut harness = Harness::new(port.clone(), serve_limits());
    harness
        .open(chain[2].hash, vec![chain[2]], credit(), AuxSchema::None)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    port.select(chain[1]);
    harness.drain().await;
    let crossing = harness.subscriber.grant(1, 0).expect("the window has room");
    assert_eq!(harness.apply(crossing).await, Ok(Applied::Crossed));
    let mut moved = harness.subscriber.grant(1, 0).expect("the window has room");
    moved.initial_target_tip_hash = block::Hash([4; 32]);
    assert_eq!(harness.apply(moved).await, Ok(Applied::Crossed));
    assert_eq!(
        harness.deliver_all().await,
        [Received::Ended(HeadersOutcomeCode::SubscriptionSuperseded)]
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
    let (publisher, _recv, _cancel, task) = session(&port, &capacity, &peer(7), 1, 64);
    let mut subscriber = Subscriber::new();
    let open = subscriber.open(chain[1].hash, vec![port.genesis], credit(), AuxSchema::None);
    publisher.apply(&open).await.expect("the Open applies");
    let ended = tokio::time::timeout(QUIET, task)
        .await
        .expect("the publisher task ends");
    assert!(ended.is_err_and(|error| error.is_panic()));
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
    let (publisher, _recv, cancel, task) = session(&port, &capacity, &peer(7), 1, 1);
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

/// A page job that panics after `Close` abandoned its page still panics the
/// publisher task.
#[tokio::test(start_paused = true)]
async fn an_abandoned_page_job_panic_reaches_the_session_owner() {
    let port = FakePort::new(0);
    let chain = port.grow(2);
    port.panic_reads
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let capacity = ServeCapacity::new("header_sync_v9", &SUBSCRIBE_HEADERS, serve_limits())
        .expect("the test budgets are valid");
    let (publisher, mut recv, _cancel, task) = session(&port, &capacity, &peer(7), 1, 64);
    let mut subscriber = Subscriber::new();
    let open = subscriber.open(chain[1].hash, vec![port.genesis], credit(), AuxSchema::None);
    publisher.apply(&open).await.expect("the Open applies");
    // The page's read waits at the gate when Close abandons the page.
    tokio::time::sleep(QUIET).await;
    let close = subscriber.close().expect("the subscription is live");
    publisher.apply(&close).await.expect("the Close applies");
    let closed = tokio::time::timeout(QUIET, recv.recv())
        .await
        .expect("the outcome arrives")
        .expect("the session is open");
    assert_eq!(
        subscriber.receive(&closed),
        Received::Ended(HeadersOutcomeCode::SubscriptionClosed)
    );
    assert!(!task.is_finished());
    port.gate.add_permits(1);
    let ended = tokio::time::timeout(QUIET, task)
        .await
        .expect("the publisher task ends");
    assert!(ended.is_err_and(|error| error.is_panic()));
}

/// A page job's panic reaches the session owner even while a terminal
/// outcome waits for a full transport queue.
#[tokio::test(start_paused = true)]
async fn a_page_job_panic_ends_a_terminal_wait_on_a_full_queue() {
    let port = FakePort::new(0);
    let chain = port.grow(2);
    let capacity = ServeCapacity::new("header_sync_v9", &SUBSCRIBE_HEADERS, serve_limits())
        .expect("the test budgets are valid");
    // A frame nobody reads fills the one-slot queue.
    let (send, _recv) = framed_channel(1);
    send.try_send(Frame {
        message_type: 1,
        flags: 0,
        payload: Vec::new(),
    })
    .expect("the queue has room");
    let (publisher, task) = Publisher::spawn::<HB>(
        port.clone(),
        capacity.push(&peer(7)),
        send,
        LeaseOwner {
            peer: SourceId::from_digest(peer(7).digest()),
            session_id: 1,
        },
        limits(),
        CancellationToken::new(),
    );
    let mut subscriber = Subscriber::new();
    let open = subscriber.open(chain[1].hash, vec![port.genesis], credit(), AuxSchema::None);
    publisher.apply(&open).await.expect("the Open applies");
    // The page's read waits at the gate, then Close abandons it and the
    // terminal outcome waits for the queue.
    tokio::time::sleep(QUIET).await;
    let close = subscriber.close().expect("the subscription is live");
    publisher.apply(&close).await.expect("the Close applies");
    tokio::time::sleep(QUIET).await;
    assert!(!task.is_finished());
    port.panic_reads
        .store(true, std::sync::atomic::Ordering::Relaxed);
    port.gate.add_permits(1);
    let ended = tokio::time::timeout(QUIET, task)
        .await
        .expect("the publisher task ends");
    assert!(ended.is_err_and(|error| error.is_panic()));
}

/// While one subscription's terminal outcome waits for a full queue, the
/// other subscription's page keeps moving, so its finished job gives its
/// execution slot back.
#[tokio::test(start_paused = true)]
async fn a_terminal_wait_keeps_the_other_page_moving() {
    let port = FakePort::new(0);
    let chain = port.grow(2);
    let serve = ServeLimits {
        node_execution: 1,
        ..serve_limits()
    };
    let capacity = ServeCapacity::new("header_sync_v9", &SUBSCRIBE_HEADERS, serve)
        .expect("the test budgets are valid");
    // A frame nobody reads fills the one-slot queue.
    let (send, _recv) = framed_channel(1);
    send.try_send(Frame {
        message_type: 1,
        flags: 0,
        payload: Vec::new(),
    })
    .expect("the queue has room");
    let (publisher, _task) = Publisher::spawn::<HB>(
        port.clone(),
        capacity.push(&peer(7)),
        send,
        LeaseOwner {
            peer: SourceId::from_digest(peer(7).digest()),
            session_id: 1,
        },
        limits(),
        CancellationToken::new(),
    );
    // The first subscription's page waits in its read, holding the slot.
    let mut first = Subscriber::new();
    let open = first.open(chain[1].hash, vec![port.genesis], credit(), AuxSchema::None);
    publisher.apply(&open).await.expect("the Open applies");
    tokio::time::sleep(QUIET).await;
    assert_eq!(capacity.node_execution_held(), 1);
    // The margin admits a second subscription, whose Close leaves a
    // terminal outcome waiting for the full queue.
    let mut second = Subscriber::new();
    second.next_id = 2;
    let open = second.open(chain[1].hash, vec![chain[1]], credit(), AuxSchema::None);
    publisher.apply(&open).await.expect("the margin admits it");
    let close = second.close().expect("the subscription is live");
    publisher.apply(&close).await.expect("the Close applies");
    tokio::time::sleep(QUIET).await;
    // The first page's read finishes, and its job gives the slot back.
    port.gate.add_permits(1);
    tokio::time::sleep(QUIET).await;
    assert_eq!(capacity.node_execution_held(), 0);
}

/// A page that holds the queue's only slot when it finishes during another
/// subscription's terminal wait is queued at once, so neither frame waits on
/// the other.
#[tokio::test(start_paused = true)]
async fn a_terminal_wait_queues_a_page_that_holds_the_queue_slot() {
    let port = FakePort::new(0);
    let chain = port.grow(3);
    let capacity = ServeCapacity::new("header_sync_v9", &SUBSCRIBE_HEADERS, serve_limits())
        .expect("the test budgets are valid");
    let (publisher, mut recv, _cancel, _task) = session(&port, &capacity, &peer(7), 1, 1);
    // Two subscriptions reach their target at Open; the margin admits the
    // second.
    let mut first = Subscriber::new();
    let open = first.open(chain[2].hash, vec![chain[2]], credit(), AuxSchema::None);
    publisher.apply(&open).await.expect("the Open applies");
    let mut second = Subscriber::new();
    second.next_id = 2;
    let open = second.open(chain[2].hash, vec![chain[2]], credit(), AuxSchema::None);
    publisher.apply(&open).await.expect("the margin admits it");
    tokio::time::sleep(QUIET).await;
    // The first subscription's page waits in its read. The chain grows, so
    // after the read the page reserves the only queue slot and then waits in
    // its recheck lease.
    port.grow(2);
    tokio::time::sleep(QUIET).await;
    port.grow(1);
    port.acquire_gate.forget_permits(Semaphore::MAX_PERMITS);
    port.gate.add_permits(1);
    tokio::time::sleep(QUIET).await;
    // The second subscription's terminal outcome waits for that slot, and
    // then the recheck confirms the page.
    let close = second.close().expect("the subscription is live");
    publisher.apply(&close).await.expect("the Close applies");
    tokio::time::sleep(QUIET).await;
    port.acquire_gate.add_permits(Semaphore::MAX_PERMITS);
    let page = tokio::time::timeout(QUIET, recv.recv())
        .await
        .expect("the page is queued")
        .expect("the session is open");
    assert!(matches!(first.receive(&page), Received::Page { .. }));
    let outcome = tokio::time::timeout(QUIET, recv.recv())
        .await
        .expect("the outcome follows")
        .expect("the session is open");
    assert_eq!(
        second.receive(&outcome),
        Received::Ended(HeadersOutcomeCode::SubscriptionClosed)
    );
}

/// A following page that leaves the selected chain while in progress is read
/// again toward the new tip, and the lease that rechecks it waits for and
/// holds an execution slot, as the page's read did.
#[tokio::test(start_paused = true)]
async fn a_following_page_recheck_holds_an_execution_slot() {
    let port = FakePort::new(0);
    let chain = port.grow(3);
    let serve = ServeLimits {
        node_execution: 1,
        ..serve_limits()
    };
    let mut harness = Harness::new(port.clone(), serve);
    harness
        .open(chain[2].hash, vec![chain[2]], credit(), AuxSchema::None)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    port.grow(2);
    harness.drain().await;
    let side = port.store().tree.extend(chain[2], 3);
    port.select(side[2]);
    // The read finishes, and the recheck's lease blocks in state.
    port.acquire_gate.forget_permits(Semaphore::MAX_PERMITS);
    port.gate.add_permits(1);
    tokio::time::sleep(QUIET).await;
    assert_eq!(port.store().reads_running, 0);
    assert_eq!(harness.capacity.node_execution_held(), 1);
    port.acquire_gate.add_permits(Semaphore::MAX_PERMITS);
    // The page for heights 4 and 5 left the selected chain, so it is read
    // again toward the new tip.
    let received = harness.deliver_all().await;
    let h = block::Height;
    assert_eq!(pages(&received), [(h(3), h(6), false)]);
    assert!(matches!(received[0], Received::Page { last, .. } if last == side[2]));
    harness.check_quiet().await;
}

/// A recheck whose tip keeps moving stops after a bounded number of leases
/// and sends the page it last confirmed.
#[tokio::test(start_paused = true)]
async fn a_following_page_recheck_stops_while_the_tip_keeps_moving() {
    let port = FakePort::new(0);
    let chain = port.grow(3);
    let mut harness = Harness::new(port.clone(), serve_limits());
    // Credit for the one page for heights 4 and 5.
    let one_page = Credit {
        objects: 2,
        bytes: credit().bytes,
    };
    harness
        .open(chain[2].hash, vec![chain[2]], one_page, AuxSchema::None)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    port.grow(2);
    harness.drain().await;
    // The tip moves past the page, then again during each of the recheck's
    // leases.
    port.grow(1);
    port.store().move_tip_on_acquire = RECHECK_LEASES;
    let walks = port.store().walks;
    port.gate.add_permits(1);
    let h = block::Height;
    assert_eq!(pages(&harness.deliver_all().await), [(h(3), h(5), false)]);
    // The recheck's leases, then one check of the newest tip, since no
    // credit is left for a page.
    assert_eq!(port.store().walks - walks, RECHECK_LEASES + 1);
    harness.check_quiet().await;
}

/// A recheck that waits for execution slots judges the page against the
/// selected tip after that wait, not the one before it.
#[tokio::test(start_paused = true)]
async fn a_following_page_recheck_reads_the_tip_after_admission() {
    let port = FakePort::new(0);
    let chain = port.grow(3);
    let serve = ServeLimits {
        node_execution: 1,
        ..serve_limits()
    };
    let mut harness = Harness::new(port.clone(), serve);
    harness
        .open(chain[2].hash, vec![chain[2]], credit(), AuxSchema::None)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    // The page for heights 4 and 5 waits in its read, holding the node slot.
    let main = port.grow(2);
    harness.drain().await;

    // Another peer's page queues for that slot, then blocks in its own read.
    let other_port = FakePort::new(0);
    let other_chain = other_port.grow(1);
    let (other, mut other_recv, _other_cancel, _other_task) =
        session(&other_port, &harness.capacity, &peer(9), 1, 64);
    let mut other_subscriber = Subscriber::new();
    let open = other_subscriber.open(
        other_chain[0].hash,
        vec![other_port.genesis],
        credit(),
        AuxSchema::None,
    );
    other.apply(&open).await.expect("the Open applies");
    tokio::time::sleep(QUIET).await;

    // The chain grows, so the page is still selected, and the read ends.
    // The recheck then waits behind the other peer's read.
    let grown = port.store().tree.extend(main[1], 1);
    port.select(grown[0]);
    port.gate.add_permits(1);
    tokio::time::sleep(QUIET).await;
    assert_eq!(harness.capacity.node_execution_held(), 1);

    // While it waits, the chain reorgs away from the page.
    let side = port.store().tree.extend(chain[2], 4);
    port.select(side[3]);
    other_port.gate.add_permits(1);
    let h = block::Height;
    assert_eq!(
        pages(&harness.deliver_all().await),
        [(h(3), h(6), false), (h(6), h(7), false)]
    );
    let other_page = other_recv
        .try_recv()
        .expect("the other peer's page arrived");
    assert!(matches!(
        other_subscriber.receive(&other_page),
        Received::Page { .. }
    ));
    harness.check_quiet().await;
}

/// A recheck whose lease outlasts another tip change judges the page again
/// against the newer tip.
#[tokio::test(start_paused = true)]
async fn a_following_page_recheck_repeats_when_the_tip_moves_during_its_lease() {
    let port = FakePort::new(0);
    let chain = port.grow(3);
    let mut harness = Harness::new(port.clone(), serve_limits());
    harness
        .open(chain[2].hash, vec![chain[2]], credit(), AuxSchema::None)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    // The page for heights 4 and 5 waits in its read.
    let main = port.grow(2);
    harness.drain().await;
    // The chain grows past the page, and the recheck's lease toward that tip
    // blocks in state.
    let grown = port.store().tree.extend(main[1], 1);
    port.select(grown[0]);
    port.acquire_gate.forget_permits(Semaphore::MAX_PERMITS);
    port.gate.add_permits(1);
    tokio::time::sleep(QUIET).await;
    // While that lease waits, the chain reorgs away from the page.
    let side = port.store().tree.extend(chain[2], 4);
    port.select(side[3]);
    port.acquire_gate.add_permits(Semaphore::MAX_PERMITS);
    let h = block::Height;
    assert_eq!(
        pages(&harness.deliver_all().await),
        [(h(3), h(6), false), (h(6), h(7), false)]
    );
    harness.check_quiet().await;
}

/// A recheck that cannot confirm the page drops it, and the next pass reads
/// again toward the new tip.
#[tokio::test(start_paused = true)]
async fn a_following_page_recheck_that_cannot_run_drops_the_page() {
    let port = FakePort::new(0);
    let chain = port.grow(3);
    let mut harness = Harness::new(port.clone(), serve_limits());
    harness
        .open(chain[2].hash, vec![chain[2]], credit(), AuxSchema::None)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    // The page for heights 4 and 5 waits in its read while the chain grows.
    let main = port.grow(2);
    harness.drain().await;
    let grown = port.store().tree.extend(main[1], 1);
    port.select(grown[0]);
    // State refuses the recheck's lease.
    port.store()
        .acquire_script
        .push_back(Ok(AcquirePathReply::Busy));
    port.gate.add_permits(1);
    let h = block::Height;
    assert_eq!(pages(&harness.deliver_all().await), [(h(3), h(6), false)]);
    harness.check_quiet().await;
}

/// A stall from a job that read an older selected chain is not recorded,
/// local or for capacity, because the change that would clear it already
/// passed.
#[tokio::test(start_paused = true)]
async fn a_stall_from_an_older_selected_chain_retries_at_once() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(3);
    let mut harness = Harness::new(port.clone(), serve_limits());
    harness
        .open(chain[2].hash, vec![chain[2]], credit(), AuxSchema::None)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    // A following page's lease waits in state toward height 4.
    port.acquire_gate.forget_permits(Semaphore::MAX_PERMITS);
    port.grow(1);
    harness.drain().await;
    // The chain moves on, and the wake for it passes while the lease waits.
    // Then state calls the old tip stale.
    port.grow(1);
    harness.drain().await;
    port.store()
        .acquire_script
        .push_back(Ok(AcquirePathReply::TargetNotRetained));
    port.acquire_gate.add_permits(Semaphore::MAX_PERMITS);
    let h = block::Height;
    assert_eq!(pages(&harness.deliver_all().await), [(h(3), h(5), false)]);

    // The same race with a capacity refusal whose signal never fires.
    port.acquire_gate.forget_permits(Semaphore::MAX_PERMITS);
    port.grow(1);
    harness.drain().await;
    port.grow(1);
    harness.drain().await;
    port.store()
        .acquire_script
        .push_back(Ok(AcquirePathReply::CapacityBusy(
            ServingCapacitySignal::default(),
        )));
    port.acquire_gate.add_permits(Semaphore::MAX_PERMITS);
    assert_eq!(pages(&harness.deliver_all().await), [(h(5), h(7), false)]);
    harness.check_quiet().await;
}

/// A capacity stall from a lease state never releases retries on the next
/// selected-chain change.
#[tokio::test(start_paused = true)]
async fn a_leaked_lease_capacity_stall_retries_on_a_chain_change() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(3);
    let mut harness = Harness::new(port.clone(), serve_limits());
    // The Open's lease reaches the target, and its release fails, so state
    // keeps this peer's one lease.
    port.store().leak_release = true;
    harness
        .open(chain[2].hash, vec![chain[2]], credit(), AuxSchema::None)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    port.grow(1);
    assert!(harness.deliver_all().await.is_empty());
    // State expires the lease without a signal, as it does lazily.
    port.store().leases.clear();
    port.grow(1);
    let h = block::Height;
    assert_eq!(pages(&harness.deliver_all().await), [(h(3), h(5), false)]);
    harness.check_quiet().await;
}

/// A follower with no room for a page still learns that the selected chain
/// stopped extending its last header. Growth costs one check per tip.
#[tokio::test(start_paused = true)]
async fn a_follower_without_page_credit_is_superseded_by_a_higher_reorg() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(3);
    let mut harness = Harness::new(port.clone(), serve_limits());
    // Credit for the one page for heights 4 and 5.
    let one_page = Credit {
        objects: 2,
        bytes: credit().bytes,
    };
    harness
        .open(chain[2].hash, vec![chain[2]], one_page, AuxSchema::None)
        .await;
    let main = port.grow(2);
    let h = block::Height;
    assert_eq!(pages(&harness.deliver_all().await), [(h(3), h(5), false)]);

    // The chain grows with no credit left: one check, and nothing sent.
    let walks = port.store().walks;
    port.grow(3);
    assert!(harness.deliver_all().await.is_empty());
    assert_eq!(port.store().walks - walks, 1);
    harness.drain().await;
    assert_eq!(
        port.store().walks - walks,
        1,
        "a checked tip is not checked again"
    );
    // The next check stops at the tip it already checked instead of walking
    // back to the last header.
    let steps = port.store().walk_steps;
    port.grow(1);
    assert!(harness.deliver_all().await.is_empty());
    assert_eq!(port.store().walk_steps - steps, 1);

    // A higher branch that forks below the last header supersedes it.
    let side = port.store().tree.extend(main[0], 4);
    port.select(side[3]);
    assert_eq!(
        harness.deliver_all().await,
        [Received::Ended(HeadersOutcomeCode::SubscriptionSuperseded)]
    );
    harness.check_quiet().await;
}

/// A check without a page needs no output, so it runs and ends the
/// subscription even while an unread page holds the peer's one response slot.
#[tokio::test(start_paused = true)]
async fn a_check_without_a_page_needs_no_output() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(3);
    let serve = ServeLimits {
        peer_output_responses: 1,
        ..serve_limits()
    };
    let mut harness = Harness::new(port.clone(), serve);
    let one_page = Credit {
        objects: 2,
        bytes: credit().bytes,
    };
    harness
        .open(chain[2].hash, vec![chain[2]], one_page, AuxSchema::None)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    // The page for heights 4 and 5 goes out, and nobody reads it.
    let main = port.grow(2);
    tokio::time::sleep(QUIET).await;
    // A higher branch that forks below the last header needs a check.
    let side = port.store().tree.extend(main[0], 3);
    port.select(side[2]);
    tokio::time::sleep(QUIET).await;
    let page = harness.recv.try_recv().expect("the page waits unread");
    assert!(matches!(
        harness.subscriber.receive(&page),
        Received::Page { .. }
    ));
    let outcome = harness
        .recv
        .try_recv()
        .expect("the outcome is already queued behind the page");
    assert_eq!(
        harness.subscriber.receive(&outcome),
        Received::Ended(HeadersOutcomeCode::SubscriptionSuperseded)
    );
    harness.check_quiet().await;
}

/// A follower stalled on a capacity refusal still ends as superseded when
/// the selected tip drops below its last header.
#[tokio::test(start_paused = true)]
async fn a_capacity_stalled_follower_is_superseded_by_a_lower_tip() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(4);
    let mut harness = Harness::new(port.clone(), serve_limits());
    harness
        .open(chain[3].hash, vec![chain[3]], credit(), AuxSchema::None)
        .await;
    assert!(harness.deliver_all().await.is_empty());
    let signal = ServingCapacitySignal::default();
    port.store()
        .acquire_script
        .push_back(Ok(AcquirePathReply::CapacityBusy(signal.clone())));
    port.grow(1);
    assert!(harness.deliver_all().await.is_empty());
    // The refusal is never released.
    port.select(chain[1]);
    assert_eq!(
        harness.deliver_all().await,
        [Received::Ended(HeadersOutcomeCode::SubscriptionSuperseded)]
    );
    assert!(!signal.is_released());
    harness.check_quiet().await;
}

/// An outcome a job finds after `Close` arrived gives way to
/// `SubscriptionClosed`.
#[tokio::test(start_paused = true)]
async fn close_wins_over_an_outcome_found_after_it() {
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
        assert_eq!(shared.apply(&close, limits()), Ok(Applied::Live));
        let found = Output::End(HeadersOutcomeCode::SubscriptionSuperseded);
        shared.finish(id, found, false);
        shared.end_due::<HB>()
    };
    let [(_, frame, _terminal)] = <[_; 1]>::try_from(ended).expect("one subscription ends");
    assert!(matches!(
        decode_frame::<RegtestMessage>(&frame),
        Ok(Message::HeadersOutcome(HeadersOutcome {
            outcome: HeadersOutcomeCode::SubscriptionClosed,
            ..
        }))
    ));
}

/// The margin admits a second `Open`. A third, a changed target, and an
/// unadvertised schema fault.
#[tokio::test(start_paused = true)]
async fn a_second_open_is_admitted_and_a_third_faults() {
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
    assert_eq!(
        publisher.apply(&open(3)).await,
        Err(PublisherFault::Subscription(SubscriptionFault::NoSlot {
            held: 2,
            limit: 1
        }))
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
    let without_records = PageLimits {
        aux_schema_mask: 0,
        ..limits()
    };
    assert_eq!(
        lock(&publisher.shared).apply(&records, without_records),
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
        let (publisher, recv, cancel, _task) =
            session(&port, &harness.capacity, &peer, u64::from(round) + 2, 64);
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
#[tokio::test(start_paused = true)]
async fn update_bursts_yield_to_other_peers() {
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
        bytes: p::<HB>(),
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
}

/// Every outcome this node queues is adjusted into a legal window, and a
/// legal one is kept.
#[test]
fn every_own_outcome_lands_in_its_window() {
    // `SubscriptionClosed` answers the peer's `Close`, never this node.
    let own = OUTCOME_CODES
        .into_iter()
        .filter(|outcome| *outcome != HeadersOutcomeCode::SubscriptionClosed);
    for outcome in own {
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
                let base = port.store().tree.ancestor(tip, height);
                if let Some(base) = base {
                    let added = port.store().tree.extend(base, u32::from(len));
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
                    .tree
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
                    .grant(u32::from(headers), u32::from(pages) * p::<HB>())
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
                if let Some(pruned) = store.tree.ancestor(tip, height) {
                    if pruned != tip && pruned != port.genesis {
                        store.tree.nodes.remove(&pruned.hash);
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
    #![proptest_config(ProptestConfig::with_cases(std::env::var("HEADER_SYNC_PROPTEST_CASES").ok().and_then(|cases| cases.parse().ok()).unwrap_or(256)))]

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

/// The real subscriber, on an in-process session.
fn real_subscriber() -> super::super::subscriber::Subscriber {
    use crate::zakura::{regulation::WriterFence, CloseCause};
    super::super::subscriber::Subscriber::new(
        HB,
        WriterFence::new(CancellationToken::new(), CloseCause::default()),
        &framed_channel(1).1,
    )
}

/// A `Status` for `tip` with the publisher's own page caps.
fn advertised(tip: Frontier) -> super::super::Status {
    super::super::Status {
        max_headers_per_response: limits().max_headers,
        max_message_bytes: limits().max_bytes,
        ..super::super::test_support::status::<HB>(tip)
    }
}

/// Feed every written frame to the real subscriber, accepting each page as
/// validated and applying each due `Grant`.
async fn follow(
    harness: &mut Harness,
    subscriber: &mut super::super::subscriber::Subscriber,
    staging: u32,
) -> Vec<Received> {
    let mut received = Vec::new();
    harness.drain().await;
    while let Some(frame) = harness.inbox.pop_front() {
        subscriber.admit(&frame).expect("the frame header passes");
        match decode_frame::<RegtestMessage>(&frame).expect("the publisher writes valid frames") {
            Message::Headers(page) => {
                let admitted = subscriber
                    .receive_page(&page, frame.payload.len())
                    .expect("the publisher's pages pass the subscriber");
                received.push(Received::Page {
                    parent: admitted.parent,
                    last: Frontier::new(admitted.cursor.height, admitted.cursor.hash),
                    marker: admitted.reaches_target,
                });
                let grant = subscriber
                    .accept_through(admitted.id, &admitted.cursor, staging)
                    .expect("a received page is accepted");
                if let Some(grant) = grant {
                    harness
                        .apply(grant)
                        .await
                        .expect("the subscriber's grants apply");
                }
            }
            Message::HeadersOutcome(outcome) => {
                let ended = subscriber
                    .receive_outcome(&outcome)
                    .expect("the publisher's outcomes land in their windows");
                received.push(Received::Ended(ended.outcome));
            }
            other => panic!("the publisher wrote {other:?}"),
        }
    }
    received
}

/// The real subscriber follows the real publisher. A small staging budget
/// splits the path into half-window grants, the marker lands on the page that
/// ends at the target, pushes follow, and `Close` ends the subscription. A
/// locator at the target sends no path page, and a reorganization below the
/// last header ends with `SubscriptionSuperseded`.
#[tokio::test(start_paused = true)]
async fn the_subscriber_follows_the_publisher() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(10);
    let mut harness = Harness::new(port.clone(), serve_limits());
    let h = block::Height;

    // Six headers of staging: three-header pages, granted three at a time.
    let mut subscriber = real_subscriber();
    let open = subscriber
        .open(
            &advertised(chain[9]),
            &[chain[2], port.genesis],
            AuxSchema::None,
            6,
        )
        .expect("the first Open is recorded")
        .0;
    assert_eq!(harness.apply(open).await, Ok(Applied::Live));
    assert_eq!(
        pages(&follow(&mut harness, &mut subscriber, 6).await),
        [
            (h(3), h(6), false),
            (h(6), h(9), false),
            (h(9), h(10), true)
        ]
    );
    port.grow(4);
    assert_eq!(
        pages(&follow(&mut harness, &mut subscriber, 6).await),
        [(h(10), h(13), false), (h(13), h(14), false)]
    );
    let close = subscriber.close().expect("Close is recorded");
    assert_eq!(harness.apply(close).await, Ok(Applied::Live));
    assert_eq!(
        follow(&mut harness, &mut subscriber, 6).await,
        [Received::Ended(HeadersOutcomeCode::SubscriptionClosed)]
    );

    // A locator at the target: no path page, then a push that extends it.
    let tip = port.tip();
    let open = subscriber
        .open(
            &advertised(tip),
            &[tip, chain[3], port.genesis],
            AuxSchema::None,
            6,
        )
        .expect("the terminal outcome freed the slot")
        .0;
    assert_eq!(harness.apply(open).await, Ok(Applied::Live));
    assert!(follow(&mut harness, &mut subscriber, 6).await.is_empty());
    port.grow(1);
    assert_eq!(
        pages(&follow(&mut harness, &mut subscriber, 6).await),
        [(h(14), h(15), false)]
    );
    let side = port.store().tree.extend(chain[8], 7);
    port.select(side[6]);
    assert_eq!(
        follow(&mut harness, &mut subscriber, 6).await,
        [Received::Ended(HeadersOutcomeCode::SubscriptionSuperseded)]
    );
    harness.check_quiet().await;
}

/// A pruned publisher sends the schema-1 pages it has records for, then ends
/// with `HistoryPruned` at the first header without one. The real subscriber
/// reports only the page it accepted, refuses the late result for the page
/// still in validation, and its reopen without records reaches the target
/// from the accepted cursor.
#[tokio::test(start_paused = true)]
async fn the_subscriber_reopens_a_pruned_peer_without_records() {
    let port = FakePort::new(Semaphore::MAX_PERMITS);
    let chain = port.grow(6);
    port.store().no_record.insert(chain[3].hash);
    let mut harness = Harness::new(port.clone(), serve_limits());
    let h = block::Height;
    let mut subscriber = real_subscriber();
    let (open, _) = subscriber
        .open(&advertised(chain[5]), &[port.genesis], AuxSchema::V1, 6)
        .expect("the first Open is recorded");
    assert_eq!(harness.apply(open).await, Ok(Applied::Live));

    harness.drain().await;
    let mut admitted = Vec::new();
    let mut ended = None;
    while let Some(frame) = harness.inbox.pop_front() {
        subscriber.admit(&frame).expect("the frame header passes");
        match decode_frame::<RegtestMessage>(&frame).expect("the publisher writes valid frames") {
            Message::Headers(page) => admitted.push(
                subscriber
                    .receive_page(&page, frame.payload.len())
                    .expect("the record prefix passes"),
            ),
            Message::HeadersOutcome(outcome) => {
                ended = Some(
                    subscriber
                        .receive_outcome(&outcome)
                        .expect("HistoryPruned is legal before the target"),
                );
            }
            other => panic!("the publisher wrote {other:?}"),
        }
        // Accept the first page before the rest arrive. The second waits for
        // validation when the outcome lands.
        if admitted.len() == 1 && ended.is_none() {
            assert_eq!(
                subscriber.accept_through(admitted[0].id, &admitted[0].cursor, 6),
                Ok(None)
            );
        }
    }
    let heights: Vec<_> = admitted
        .iter()
        .map(|page| (page.parent.height, page.cursor.height))
        .collect();
    assert_eq!(
        heights,
        [(h(0), h(2)), (h(2), h(3))],
        "pages stop before height 4"
    );
    let ended = ended.expect("the path ends with an outcome");
    assert_eq!(ended.outcome, HeadersOutcomeCode::HistoryPruned);
    assert_eq!(ended.accepted, admitted[0].cursor);
    assert!(subscriber
        .accept_through(admitted[1].id, &admitted[1].cursor, 6)
        .is_err());

    let start = Frontier::new(ended.accepted.height, ended.accepted.hash);
    let (open, _) = subscriber
        .open(
            &advertised(chain[5]),
            &[start, port.genesis],
            AuxSchema::None,
            6,
        )
        .expect("the terminal outcome freed the slot");
    assert_eq!(harness.apply(open).await, Ok(Applied::Live));
    assert_eq!(
        pages(&follow(&mut harness, &mut subscriber, 6).await),
        [(h(2), h(5), false), (h(5), h(6), true)],
        "the plain reopen reaches the target"
    );
    harness.check_quiet().await;
}
