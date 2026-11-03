//! The production regulation tools behind narrow handles, for the
//! simulator's model agreement tests (properties P3.2).
//!
//! Each handle drives one tool with plain values and returns the tool's own
//! decision, named by the tool's stable label, such as `over_frames`. The
//! tools stay crate-private. These handles are the only way past the crate
//! boundary, and they exist only in the simulator build.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use zakura_rt::{
    sync::{oneshot, CancellationToken},
    time::Instant,
};

use crate::zakura::{
    block_sync::block_sync_streams,
    discovery::discovery_streams,
    framed_channel,
    header_sync::v9::CommonMessage,
    regulation::{
        test_family::{self, Probe},
        CadenceBuckets, CadenceCharge, Produce, Publications, ReservationPool, Reservations,
        ReserveRefused, Responded, ResponseCap, ResponseSink, Serve, ServeCapacity, ServeEnd,
        ServeLimits, SinkProgress, SubscribeRefused, SubscriptionLimits, Subscriptions,
        TerminalPermit, WorkLease,
    },
    transport::{FrameFilter, InboundReader},
    Clock, CloseCause, Credit, FrameRejection, FramedRecv, MessageRule, Stream, ZakuraPeerId,
    FRAME_HEADER_BYTES,
};

/// A tabled stream and the family that declares it.
#[derive(Copy, Clone, Debug)]
pub struct FamilyTable {
    /// The family, such as `header-sync`.
    pub family: &'static str,
    /// The stream, with its kind, version, and table.
    pub stream: Stream,
}

/// The production tables: header sync version 9 on Mainnet and Testnet,
/// block sync version 3, and discovery version 2.
pub fn production_tables() -> [FamilyTable; 3] {
    [
        FamilyTable {
            family: "header-sync",
            stream: CommonMessage::LAYOUT[0],
        },
        FamilyTable {
            family: "block-sync",
            stream: block_sync_streams()[0],
        },
        FamilyTable {
            family: "discovery",
            stream: discovery_streams()[0],
        },
    ]
}

/// The regulation tests' family: `get`, answered by `part`s and then `done`
/// or `failed`; the `status` announcement; and `ping`, answered by `pong`.
pub const TEST_FAMILY: &[MessageRule] = test_family::RULES;

/// The end of a stream that reads a frame.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Reader {
    /// A persistent stream's reader, which accepts every row.
    Persistent,
    /// The responder's end of a request/response stream: requests only.
    Responder,
    /// The requester's end of a request/response stream: responses only.
    Requester,
}

/// The reader's header check for `rules` with no response precheck. On
/// success, the largest payload the reader then accepts; on refusal, the
/// rejection's label, such as `reserved_flags`.
pub fn check_header(
    rules: &'static [MessageRule],
    reader: Reader,
    message_type: u16,
    flags: u16,
    frame_cap: usize,
    payload_len: usize,
) -> Result<usize, &'static str> {
    let reader = match reader {
        Reader::Persistent => InboundReader::Persistent,
        Reader::Responder => InboundReader::Responder,
        Reader::Requester => InboundReader::Requester,
    };
    FrameFilter::new(Some(rules), reader)
        .check_header(message_type, flags, payload_len, frame_cap)
        .map(|cap| cap.saturating_sub(FRAME_HEADER_BYTES))
        .map_err(FrameRejection::label)
}

/// The node-wide limits behind [`Requests`], wide enough that only the
/// request margin refuses.
const SERVE_LIMITS: ServeLimits = ServeLimits {
    node_execution: 1024,
    peer_execution: 1024,
    peer_output_responses: 1024,
    node_output_responses: 1024,
    peer_output_bytes: 1 << 20,
    node_output_bytes: 1 << 20,
};

/// Ends each request when the test releases it.
#[derive(Debug)]
struct Held;

impl Produce for Held {
    type Request = StdMutex<Option<oneshot::Receiver<()>>>;
    type Message = Probe;

    fn response_cap(&self, _: &Self::Request) -> ResponseCap {
        ResponseCap {
            frames: 0,
            bytes: 4,
        }
    }

    async fn produce(
        &self,
        request: &Self::Request,
        _: WorkLease,
        sink: ResponseSink<Probe>,
    ) -> Result<Responded, ServeEnd> {
        let release = request
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(release) = release {
            // A dropped sender also releases the request.
            let _ = release.await;
        }
        sink.finish(&Probe::Done(0))
            .map_err(|error| ServeEnd::LocalFault(error.to_string()))
    }

    fn local_failure(&self, _: &Self::Request, sent: SinkProgress) -> Probe {
        Probe::Failed(sent.frames)
    }
}

/// A request the serving side refused: more open requests than any
/// conformant peer could have.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct OverCommitted {
    /// Open requests, the refused one included.
    pub open: u32,
    /// The highest limit advertised, capped by the row.
    pub limit: u32,
}

/// `Serve` for the test family's `get` row on one peer session. Each
/// admitted request stays open until [`Requests::end_oldest`] ends it.
///
/// Serving spawns tasks, so build and use it inside a tokio runtime.
pub struct Requests {
    serve: Serve<Held>,
    held: VecDeque<oneshot::Sender<()>>,
    _output: FramedRecv,
    _cancel: CancellationToken,
}

impl Requests {
    /// A session that advertised `limit` at its start.
    pub fn new(limit: u32) -> Self {
        let capacity = ServeCapacity::new("model-agreement", &test_family::GET, SERVE_LIMITS)
            .expect("the agreement limits are valid");
        let (send, output) = framed_channel(4096);
        let cancel = CancellationToken::new();
        let peer = ZakuraPeerId::new(vec![1; 32]).expect("32 bytes is a valid peer id");
        let serve = capacity.session(
            Arc::new(Held),
            &peer,
            limit,
            send,
            cancel.clone(),
            cancel.clone(),
            CloseCause::default(),
        );
        Requests {
            serve,
            held: VecDeque::new(),
            _output: output,
            _cancel: cancel,
        }
    }

    /// Records a limit advertised to the peer.
    pub fn advertise(&self, limit: u32) {
        self.serve.advertise(limit);
    }

    /// Admits one request, or refuses it as a violation.
    pub fn admit(&mut self) -> Result<(), OverCommitted> {
        let (release, wait) = oneshot::channel();
        match self.serve.admit(StdMutex::new(Some(wait))) {
            Ok(()) => {
                self.held.push_back(release);
                Ok(())
            }
            Err(crate::zakura::regulation::ServeViolation::OverCommitted { open, limit }) => {
                Err(OverCommitted { open, limit })
            }
        }
    }

    /// Ends the oldest open request and waits until serving releases it.
    /// Does nothing if none is open.
    pub async fn end_oldest(&mut self) {
        let Some(release) = self.held.pop_front() else {
            return;
        };
        let before = self.serve.open();
        let _ = release.send(());
        while self.serve.open() >= before {
            zakura_rt::task::yield_now().await;
        }
    }

    /// Open requests on the session.
    pub fn open(&self) -> u32 {
        self.serve.open()
    }
}

/// `Reservations` for one session over a family's rows, keyed by `u64`.
pub struct Responses {
    pool: ReservationPool,
    reservations: Reservations<u64>,
}

impl Responses {
    /// An empty session over `rules` that holds at most `cap` live
    /// reservations.
    pub fn new(rules: &'static [MessageRule], cap: usize) -> Self {
        Responses {
            pool: ReservationPool::new(cap.max(1)).expect("a nonzero pool fits a semaphore"),
            reservations: Reservations::new(rules, cap),
        }
    }

    /// Reserves `key` for a response to a `request_type` request of at most
    /// `frames` frames before its ending and `bytes` payload bytes.
    pub fn reserve(
        &mut self,
        key: u64,
        request_type: u16,
        frames: u32,
        bytes: u64,
    ) -> Result<(), &'static str> {
        let entry = self.pool.try_entry().ok_or("pool_full")?;
        self.reservations
            .reserve(key, request_type, ResponseCap { frames, bytes }, entry)
            .map_err(|refused| match refused {
                ReserveRefused::AtCapacity => "at_capacity",
                ReserveRefused::KeyLive => "key_live",
                ReserveRefused::NotARequest { .. } => "not_a_request",
            })
    }

    /// Records that this node no longer wants `key`'s work.
    pub fn abandon(&mut self, key: u64) -> bool {
        self.reservations.abandon(&key)
    }

    /// Claims one response frame for `key` before decode: a frame that
    /// doesn't end the exchange, or with `ends`, its ending.
    pub fn claim(
        &mut self,
        key: u64,
        message_type: u16,
        payload_len: usize,
        ends: bool,
    ) -> Result<(), &'static str> {
        let claimed = if ends {
            self.reservations
                .claim_end(&key, message_type, payload_len)
                .map(|_| ())
        } else {
            self.reservations
                .claim_frame(&key, message_type, payload_len)
                .map(|_| ())
        };
        claimed.map_err(|refused| refused.label())
    }

    /// Live reservations.
    pub fn len(&self) -> usize {
        self.reservations.len()
    }

    /// Whether no reservation is live.
    pub fn is_empty(&self) -> bool {
        self.reservations.is_empty()
    }
}

/// The limits of `rules`' subscription row `subscription_type`.
fn limits(rules: &[MessageRule], subscription_type: u16) -> SubscriptionLimits {
    MessageRule::find(rules, subscription_type)
        .and_then(SubscriptionLimits::from_rule)
        .expect("the caller names a subscription row")
}

/// `Publications`, the publisher's side of one session, with `u64` keys
/// and cursors: the updates a peer sends to this node.
pub struct Updates {
    publications: Publications<u64, u64>,
    /// Ended subscriptions' terminal outcomes still being written, oldest
    /// first. Each permit holds its subscription's slot.
    writing: Vec<(u64, TerminalPermit)>,
}

impl Updates {
    /// An empty session for `rules`' subscription row `subscription_type`.
    pub fn new(rules: &'static [MessageRule], subscription_type: u16) -> Self {
        Updates {
            publications: Publications::new(limits(rules, subscription_type)),
            writing: Vec::new(),
        }
    }

    /// Applies `Open`.
    pub fn open(
        &mut self,
        key: u64,
        sequence: u32,
        credit: Credit,
        start: u64,
    ) -> Result<(), &'static str> {
        self.publications
            .open(key, sequence, credit, start)
            .map_err(|fault| fault.label())
    }

    /// Applies `Grant`. `Ok(true)` if it crossed its subscription's end.
    pub fn grant(
        &mut self,
        key: u64,
        sequence: u32,
        acknowledged: u64,
        added: Credit,
    ) -> Result<bool, &'static str> {
        self.publications
            .grant(&key, sequence, &acknowledged, added)
            .map(|applied| applied == crate::zakura::regulation::Applied::Crossed)
            .map_err(|fault| fault.label())
    }

    /// Applies `Close`. `Ok(true)` if it crossed its subscription's end.
    pub fn close(
        &mut self,
        key: u64,
        sequence: u32,
        acknowledged: u64,
    ) -> Result<bool, &'static str> {
        self.publications
            .close(&key, sequence, &acknowledged)
            .map(|applied| applied == crate::zakura::regulation::Applied::Crossed)
            .map_err(|fault| fault.label())
    }

    /// Reserves credit and `cursor` for a page of `objects` and `bytes`
    /// that this node sends. Refusals are local stalls, never faults.
    pub fn send_page(
        &mut self,
        key: u64,
        objects: u32,
        bytes: usize,
        cursor: u64,
    ) -> Result<(), &'static str> {
        use crate::zakura::regulation::PageStall;
        self.publications
            .reserve_page(&key, objects, bytes, cursor)
            .map_err(|stall| match stall {
                PageStall::Ended => "ended",
                PageStall::Closing => "closing",
                PageStall::NoCredit => "no_credit",
                PageStall::HistoryFull => "history_full",
                PageStall::EmptyPage => "empty_page",
            })
    }

    /// Ends `key`'s subscription and starts writing its terminal outcome,
    /// which holds the subscription's slot until [`Self::terminal_written`].
    /// False if it isn't live.
    pub fn end(&mut self, key: u64) -> bool {
        let Some(permit) = self.publications.end(&key) else {
            return false;
        };
        self.writing.push((key, permit));
        true
    }

    /// Finishes writing `key`'s terminal outcome, which frees its slot.
    /// False if no such write is pending.
    pub fn terminal_written(&mut self, key: u64) -> bool {
        let Some(at) = self.writing.iter().position(|(pending, _)| *pending == key) else {
            return false;
        };
        drop(self.writing.remove(at));
        true
    }

    /// Tombstones held.
    pub fn tombstones(&self) -> usize {
        self.publications.tombstones()
    }
}

/// An update [`Pages`] records before this node writes it: the fields that
/// go on the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Update {
    /// Zero for `Open`, then one more for each update.
    pub sequence: u32,
    /// The start for `Open`, then the last page the handler accepted.
    pub acknowledged: u64,
    /// The credit the update adds. `Close` adds none.
    pub added: Credit,
}

impl From<crate::zakura::regulation::Update<u64>> for Update {
    fn from(update: crate::zakura::regulation::Update<u64>) -> Self {
        Update {
            sequence: update.sequence,
            acknowledged: update.acknowledged,
            added: update.added,
        }
    }
}

/// `Subscriptions`, the subscriber's side of one session, with `u64` keys
/// and cursors: the pages a peer sends to this node.
pub struct Pages {
    subscriptions: Subscriptions<u64, u64>,
}

/// The label of a local refusal to send an update.
fn refused(refused: SubscribeRefused) -> &'static str {
    match refused {
        SubscribeRefused::AtCapacity => "at_capacity",
        SubscribeRefused::KeyInUse => "key_in_use",
        SubscribeRefused::Unknown => "unknown",
        SubscribeRefused::Closing => "closing",
        SubscribeRefused::EmptyCredit => "empty_credit",
        SubscribeRefused::AboveWindow => "above_window",
        SubscribeRefused::UnknownCursor => "unknown_cursor",
        SubscribeRefused::SequenceExhausted => "sequence_exhausted",
    }
}

impl Pages {
    /// An empty session for `rules`' subscription row `subscription_type`.
    pub fn new(rules: &'static [MessageRule], subscription_type: u16) -> Self {
        Pages {
            subscriptions: Subscriptions::new(limits(rules, subscription_type), rules),
        }
    }

    /// Records `Open` before this node writes it, and returns it.
    pub fn open(&mut self, key: u64, credit: Credit, start: u64) -> Result<Update, &'static str> {
        self.subscriptions
            .open(key, credit, start)
            .map(Update::from)
            .map_err(refused)
    }

    /// Records `Grant` before this node writes it, and returns it.
    pub fn grant(&mut self, key: u64, added: Credit) -> Result<Update, &'static str> {
        self.subscriptions
            .grant(&key, added)
            .map(Update::from)
            .map_err(refused)
    }

    /// Records `Close` before this node writes it, and returns it.
    pub fn close(&mut self, key: u64) -> Result<Update, &'static str> {
        self.subscriptions
            .close(&key)
            .map(Update::from)
            .map_err(refused)
    }

    /// Records that the handler accepted every page through `cursor`.
    pub fn accept(&mut self, key: u64, cursor: u64) -> Result<(), &'static str> {
        self.subscriptions.accept(&key, &cursor).map_err(refused)
    }

    /// Admits a page of `objects` and `payload_len` bytes for `key`.
    pub fn page(
        &mut self,
        key: u64,
        objects: u32,
        payload_len: usize,
        cursor: u64,
    ) -> Result<(), &'static str> {
        self.subscriptions
            .claim_page(&key, objects, payload_len, cursor)
            .map_err(|fault| fault.label())
    }

    /// Consumes `key`'s subscription with its terminal outcome.
    pub fn end(&mut self, key: u64) -> Result<(), &'static str> {
        self.subscriptions
            .claim_end(&key)
            .map(|_| ())
            .map_err(|fault| fault.label())
    }
}

/// A clock the test moves by hand.
#[derive(Clone, Debug)]
struct HandClock(Arc<StdMutex<Instant>>);

impl Clock for HandClock {
    fn now(&self) -> Instant {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// `CadenceBuckets` for one connection, on a clock the test moves.
pub struct Rates {
    clock: HandClock,
    buckets: CadenceBuckets<HandClock>,
}

impl Default for Rates {
    fn default() -> Self {
        Self::new()
    }
}

impl Rates {
    /// Fresh buckets at an arbitrary start.
    pub fn new() -> Self {
        let clock = HandClock(Arc::new(StdMutex::new(Instant::now())));
        Rates {
            buckets: CadenceBuckets::new(clock.clone()),
            clock,
        }
    }

    /// Moves the clock forward by `by`.
    pub fn advance(&self, by: Duration) {
        let mut now = self
            .clock
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *now += by;
    }

    /// Charges one frame of `rule` on `layout`: `exempt`, `admit`, or
    /// `exhausted`.
    pub fn charge(&mut self, layout: u16, rule: &MessageRule) -> &'static str {
        match self.buckets.charge(layout, rule) {
            CadenceCharge::Exempt => "exempt",
            CadenceCharge::Admit => "admit",
            CadenceCharge::Exhausted => "exhausted",
        }
    }

    /// Credits a local read pause of `paused` on a stream of `layout` that
    /// carries `rules`.
    pub fn credit_pause(&mut self, layout: u16, rules: &[MessageRule], paused: Duration) {
        self.buckets.credit_pause(layout, rules, paused);
    }
}
