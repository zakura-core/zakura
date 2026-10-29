//! Synchronous transport events for simulator tests.
//!
//! The handler reports each connection's handshake, registration, stream
//! admissions and refusals, frame reads and writes, regulation transitions,
//! close decision and teardown to an installed [`TransportObserver`]. This
//! module exists only in test and `zakura_sim` builds. Production builds
//! compile none of it, and the `transport_observe!` macro drops its tokens.
//!
//! No production panic catch sits between an emission and the task that
//! makes it, so a panic in [`TransportObserver::observe`] unwinds that task
//! with the observer's own payload.
//!
//! The taps' own state takes std's mutex and plain atomics, which have no
//! preemption points in either build, so reporting an event adds no
//! scheduling point to a simulated run.

use std::{
    collections::VecDeque,
    fmt,
    net::SocketAddr,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError},
};

use zakura_rt::sync::counter::{AtomicU64, Ordering};

use crate::zakura::{FrameRejection, ZakuraConnId, ZakuraConnectionLimits};

/// Receives transport events as they happen.
///
/// Called synchronously on the task that makes the transition, sometimes
/// while it holds a regulation lock, as [`TransportEvent::Regulated`]
/// describes. An
/// implementation must not block, yield, take a `zakura_rt` lock, or call
/// into the transport.
pub trait TransportObserver: Send + Sync + fmt::Debug {
    /// Records one event.
    fn observe(&self, event: TransportEvent);
}

/// One transport event.
///
/// A connection's events before [`TransportEvent::Registered`] are keyed by
/// the remote address, since registration assigns its `conn_id`. Frame
/// sequence numbers count from 0 per QUIC stream: reads in the order their
/// headers arrived, writes in the order they started.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TransportEvent {
    /// The control handshake succeeded.
    Negotiated {
        /// The remote address the connection was admitted from.
        remote: SocketAddr,
        /// The capability mask this node sent: its hello's, or, as the
        /// responder, the mask it intersected with the peer's hello.
        local: u64,
        /// The capability mask the peer sent: its hello's, or its ack's.
        offered: u64,
        /// The capability mask the connection uses.
        negotiated: u64,
    },
    /// The connection was refused before registration: its control
    /// handshake failed, or the handler or supervisor refused to register
    /// it, as a duplicate of a live connection for one.
    HandshakeRefused {
        /// The remote address the connection was admitted from.
        remote: SocketAddr,
        /// The handshake error, or the refusal's trace label.
        reason: String,
    },
    /// The supervisor registered the connection.
    Registered {
        /// The connection's registration ID.
        conn_id: ZakuraConnId,
        /// The remote address the connection was admitted from.
        remote: SocketAddr,
    },
    /// A service session's stream started, with the limits and queue depths
    /// its worker installed.
    Admitted {
        /// The connection.
        conn_id: ZakuraConnId,
        /// The stream's QUIC stream ID.
        quic_stream: u64,
        /// The stream kind.
        kind: u16,
        /// The stream version.
        version: u16,
        /// The service session's ID.
        session_id: u64,
        /// The connection's limits after negotiation and clamping.
        negotiated_limits: ZakuraConnectionLimits,
        /// Frames the reader may queue for the service.
        inbound_depth: usize,
        /// Frames the service may queue for the writer.
        outbound_depth: usize,
    },
    /// The reader has a frame's 8 header bytes, before any check of them.
    HeaderRead {
        /// The connection.
        conn_id: ZakuraConnId,
        /// The QUIC stream.
        quic_stream: u64,
        /// The frame's position among this stream's reads.
        seq: u64,
        /// The header's message type.
        message_type: u16,
        /// The header's flags.
        flags: u16,
        /// The header's declared payload length.
        payload_len: u32,
    },
    /// The reader has the frame's whole payload.
    FrameRead {
        /// The connection.
        conn_id: ZakuraConnId,
        /// The QUIC stream.
        quic_stream: u64,
        /// The frame's position among this stream's reads.
        seq: u64,
    },
    /// A regulation table changed state. It's reported inside the lock that
    /// applies the change, before the lock is released.
    Regulated {
        /// The connection.
        conn_id: ZakuraConnId,
        /// The QUIC stream of the service session.
        quic_stream: u64,
        /// What changed.
        transition: Transition,
    },
    /// An unknown or unnegotiated stream was reset without closing the
    /// connection.
    StreamRefused {
        /// The connection.
        conn_id: ZakuraConnId,
        /// The QUIC stream.
        quic_stream: u64,
        /// The prelude's stream kind.
        kind: u16,
        /// The prelude's stream version.
        version: u16,
        /// Why, as the stream trace names it.
        reason: &'static str,
    },
    /// The writer is about to hand a frame's first bytes to the stream.
    FrameWriteStarted {
        /// The connection.
        conn_id: ZakuraConnId,
        /// The QUIC stream.
        quic_stream: u64,
        /// The frame's position among this stream's writes.
        seq: u64,
        /// The frame's message type.
        message_type: u16,
    },
    /// The writer finished writing a frame.
    FrameWritten {
        /// The connection.
        conn_id: ZakuraConnId,
        /// The QUIC stream.
        quic_stream: u64,
        /// The frame's position among this stream's writes.
        seq: u64,
        /// The frame's message type.
        message_type: u16,
    },
    /// The node decided to close the connection: the first recorded close
    /// cause, or `None` when its token was cancelled with no cause recorded
    /// first. A cancellation before registration is keyed by `remote` alone.
    /// The `None` decision comes from the simulator token's cancellation hook,
    /// so a test build without `zakura_sim` reports only recorded causes.
    CloseDecided {
        /// The remote address the connection was admitted from.
        remote: SocketAddr,
        /// The connection, once registered.
        conn_id: Option<ZakuraConnId>,
        /// The first recorded cause.
        cause: Option<&'static str>,
    },
    /// The connection's workers drained and its peer was unregistered. A
    /// drain that times out with a worker still running reports no
    /// `TornDown`, and neither does a serving future that is dropped. Each
    /// close is still decided.
    TornDown {
        /// The connection.
        conn_id: ZakuraConnId,
    },
}

/// A regulation table's state change.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Transition {
    /// The subscriber's precheck judged a response header.
    HeaderChecked {
        /// The frame's position among the stream's reads.
        seq: u64,
        /// The verdict.
        outcome: HeaderCheck,
    },
    /// The service claimed a page the reader had reserved.
    PageClaimed {
        /// The page's position among the stream's reads, or `None` if it
        /// didn't come through a tagged reader.
        seq: Option<u64>,
        /// Objects the page spent.
        objects: u32,
        /// Payload bytes the page spent.
        bytes: u64,
    },
    /// A subscription this node opened ended with its terminal outcome.
    EndClaimed {
        /// The subscription.
        subscription: u64,
    },
    /// A subscription the peer opened on this node ended.
    PublicationEnded {
        /// The subscription.
        subscription: u64,
    },
    /// The publisher applied the peer's `Open` and its initial credit.
    OpenApplied {
        /// The subscription.
        subscription: u64,
        /// Objects the `Open` granted.
        objects: u32,
        /// Bytes the `Open` granted.
        bytes: u32,
        /// The `Open` frame's position among the stream's reads, or `None`
        /// if it didn't come through a tagged reader.
        seq: Option<u64>,
    },
    /// The publisher applied the peer's `Close` to a live subscription.
    CloseApplied {
        /// The subscription.
        subscription: u64,
    },
    /// The publisher applied the peer's `Grant` to a live subscription.
    GrantApplied {
        /// The subscription.
        subscription: u64,
        /// Objects the grant added.
        objects: u32,
        /// Bytes the grant added.
        bytes: u32,
    },
    /// This node recorded its own `Open` before writing it.
    OpenIssued {
        /// The subscription.
        subscription: u64,
        /// Objects the `Open` grants.
        objects: u32,
        /// Bytes the `Open` grants.
        bytes: u32,
    },
    /// This node removed its own `Open` because the frame was dropped before
    /// its first byte was written.
    OpenRetracted {
        /// The subscription.
        subscription: u64,
    },
    /// This node recorded its own `Grant` before writing it.
    GrantIssued {
        /// The subscription.
        subscription: u64,
        /// Objects the grant adds.
        objects: u32,
        /// Bytes the grant adds.
        bytes: u32,
    },
}

/// The subscriber precheck's verdict on a response header.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeaderCheck {
    /// A page, charged to the queue until the service claims it.
    Reserved {
        /// Objects charged: one per page.
        objects: u64,
        /// Payload bytes charged.
        bytes: u64,
    },
    /// A terminal outcome for a live subscription. It takes no charge.
    AdmittedTerminal,
    /// Refused, closing the connection.
    Refused {
        /// The rejection the reader reports.
        reason: FrameRejection,
    },
}

/// One connection's observer, from just before registration.
#[derive(Debug)]
pub(crate) struct ConnTap {
    observer: Arc<dyn TransportObserver>,
    remote: SocketAddr,
    /// Set when the supervisor registers the connection.
    conn_id: OnceLock<ZakuraConnId>,
    /// 1 once `CloseDecided` was reported.
    decided: AtomicU64,
    /// 1 once the observer panicked. It gets no more of this connection's
    /// events, so a teardown hook running during that panic's unwind can't
    /// panic in it a second time and abort the process.
    panicked: AtomicU64,
}

impl ConnTap {
    /// A tap that reports the events of the connection from `remote` to
    /// `observer`.
    pub(crate) fn new(observer: Arc<dyn TransportObserver>, remote: SocketAddr) -> Self {
        Self {
            observer,
            remote,
            conn_id: OnceLock::new(),
            decided: AtomicU64::new(0),
            panicked: AtomicU64::new(0),
        }
    }

    /// Record the connection's registration ID.
    pub(crate) fn registered(&self, conn_id: ZakuraConnId) {
        let _ = self.conn_id.set(conn_id);
    }

    /// The connection's registration ID.
    pub(crate) fn conn_id(&self) -> ZakuraConnId {
        *self
            .conn_id
            .get()
            .expect("stream and teardown events come after registration, which sets the ID")
    }

    /// Reports `event`, unless the observer already panicked on this
    /// connection. An observer panic resumes with its own payload, except
    /// while the thread is already unwinding, where a second panic would
    /// abort.
    pub(crate) fn emit(&self, event: TransportEvent) {
        if self.panicked.load(Ordering::Acquire) == 1 {
            return;
        }
        let observed = std::panic::catch_unwind(AssertUnwindSafe(|| self.observer.observe(event)));
        if let Err(payload) = observed {
            self.panicked.store(1, Ordering::Release);
            if !std::thread::panicking() {
                std::panic::resume_unwind(payload);
            }
        }
    }

    /// Marks the close as decided without reporting it, for a connection the
    /// supervisor refused. Returns whether this was the decision, so the
    /// caller reports `HandshakeRefused` only if no cancellation reported
    /// `CloseDecided` first.
    pub(crate) fn refused(&self) -> bool {
        self.decided.swap(1, Ordering::AcqRel) == 0
    }

    /// Reports `CloseDecided` with `cause`, unless a decision was already
    /// reported.
    pub(crate) fn decide(&self, cause: Option<&'static str>) {
        if self.decided.swap(1, Ordering::AcqRel) == 0 {
            self.emit(TransportEvent::CloseDecided {
                remote: self.remote,
                conn_id: self.conn_id.get().copied(),
                cause,
            });
        }
    }
}

/// One service stream's observer and frame counters, shared by its reader,
/// its writer, its service and its regulation tables.
#[derive(Debug)]
pub(crate) struct StreamTap {
    conn: Arc<ConnTap>,
    quic_stream: u64,
    /// The next read's sequence number.
    next_read: AtomicU64,
    /// The sequence number of the header the reader has now.
    reading: AtomicU64,
    /// The next write's sequence number.
    next_write: AtomicU64,
    /// 1 once the service takes each frame's tag. Until then the reader
    /// queues none, so a stream nobody tags never grows its queue.
    tagged: AtomicU64,
    /// Tags of frames the reader queued for the service, oldest first.
    queued: Mutex<VecDeque<u64>>,
    /// The tag of the frame the service took last.
    serving: Mutex<Option<u64>>,
}

impl StreamTap {
    /// A tap for QUIC stream `quic_stream` of `conn`'s connection.
    pub(crate) fn new(conn: Arc<ConnTap>, quic_stream: u64) -> Self {
        Self {
            conn,
            quic_stream,
            next_read: AtomicU64::new(0),
            reading: AtomicU64::new(0),
            next_write: AtomicU64::new(0),
            tagged: AtomicU64::new(0),
            queued: Mutex::new(VecDeque::new()),
            serving: Mutex::new(None),
        }
    }

    /// Reports `HeaderRead` for the next read, and makes it the header the
    /// reader has now.
    pub(crate) fn header_read(&self, message_type: u16, flags: u16, payload_len: usize) {
        let seq = self.next_read.fetch_add(1, Ordering::AcqRel);
        self.reading.store(seq, Ordering::Release);
        self.conn.emit(TransportEvent::HeaderRead {
            conn_id: self.conn.conn_id(),
            quic_stream: self.quic_stream,
            seq,
            message_type,
            flags,
            // The reader parsed it from a u32.
            payload_len: u32::try_from(payload_len).unwrap_or(u32::MAX),
        });
    }

    /// Reports `FrameRead` for the header the reader has now.
    pub(crate) fn frame_read(&self) {
        self.conn.emit(TransportEvent::FrameRead {
            conn_id: self.conn.conn_id(),
            quic_stream: self.quic_stream,
            seq: self.reading(),
        });
    }

    /// The sequence number of the header the reader has now.
    pub(crate) fn reading(&self) -> u64 {
        self.reading.load(Ordering::Acquire)
    }

    /// Has the service take a tag for every frame the reader queues from now
    /// on. Call it before the reader can read a frame.
    pub(crate) fn tag_frames(&self) {
        self.tagged.store(1, Ordering::Release);
    }

    /// Queues the current frame's tag, just before the reader queues the
    /// frame.
    pub(crate) fn queue_tag(&self) {
        if self.tagged.load(Ordering::Acquire) == 1 {
            let seq = self.reading();
            lock(&self.queued).push_back(seq);
        }
    }

    /// Removes the tag [`Self::queue_tag`] queued last, for a frame the
    /// reader then failed to queue. Only the reader queues tags, so the last
    /// one is its own.
    pub(crate) fn unqueue_tag(&self) {
        if self.tagged.load(Ordering::Acquire) == 1 {
            lock(&self.queued).pop_back();
        }
    }

    /// Takes the oldest queued tag as the frame the service handles now.
    /// `FramedRecv` calls it for each frame it hands the service. On a stream
    /// nobody tags it does nothing.
    pub(crate) fn take_tag(&self) {
        if self.tagged.load(Ordering::Acquire) == 1 {
            let seq = lock(&self.queued).pop_front();
            *lock(&self.serving) = seq;
        }
    }

    /// The tag of the frame the service handles now.
    pub(crate) fn serving(&self) -> Option<u64> {
        *lock(&self.serving)
    }

    /// Reports `FrameWriteStarted` for the next write and returns its
    /// sequence number.
    pub(crate) fn write_started(&self, message_type: u16) -> u64 {
        let seq = self.next_write.fetch_add(1, Ordering::AcqRel);
        self.conn.emit(TransportEvent::FrameWriteStarted {
            conn_id: self.conn.conn_id(),
            quic_stream: self.quic_stream,
            seq,
            message_type,
        });
        seq
    }

    /// Reports `FrameWritten` for the write numbered `seq`.
    pub(crate) fn written(&self, seq: u64, message_type: u16) {
        self.conn.emit(TransportEvent::FrameWritten {
            conn_id: self.conn.conn_id(),
            quic_stream: self.quic_stream,
            seq,
            message_type,
        });
    }

    /// Reports `transition` on this stream.
    pub(crate) fn regulated(&self, transition: Transition) {
        self.conn.emit(TransportEvent::Regulated {
            conn_id: self.conn.conn_id(),
            quic_stream: self.quic_stream,
            transition,
        });
    }
}

/// A regulation table's tap: its stream's tap and how to read a key as the
/// subscription ID the events carry.
pub(crate) struct RegulationTap<K> {
    stream: Arc<StreamTap>,
    key: fn(&K) -> u64,
}

impl<K> RegulationTap<K> {
    /// A tap on `stream` that reads keys with `key`.
    pub(crate) fn new(stream: Arc<StreamTap>, key: fn(&K) -> u64) -> Self {
        Self { stream, key }
    }

    /// The stream's tap.
    pub(crate) fn stream(&self) -> &StreamTap {
        &self.stream
    }

    /// `key` as a subscription ID.
    pub(crate) fn id(&self, key: &K) -> u64 {
        (self.key)(key)
    }
}

impl<K> fmt::Debug for RegulationTap<K> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegulationTap")
            .field("stream", &self.stream)
            .finish_non_exhaustive()
    }
}

/// Locks `mutex`. No holder panics while holding it, so a poisoned lock
/// still holds consistent state.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
