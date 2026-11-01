//! TCP delivery progress for large uploads on a legacy peer connection.
//!
//! A framed write returns once its bytes are in the kernel send buffer, which Linux autotunes up
//! to several megabytes. This module reads the kernel's `TCP_INFO` to learn when the peer has
//! actually acknowledged those bytes, so a slow upload can be attributed to queueing,
//! congestion, retransmission, or a slow receiver.
//!
//! - [`TcpSocket`] is a handle to a connection's socket, captured before the stream is wrapped.
//! - [`UploadProgress`] starts an [`UploadWatch`] when a large message write begins. The watch
//!   samples the socket every [`SAMPLE_INTERVAL`] until the peer acknowledges the message, the
//!   connection closes, or [`ACK_TIMEOUT`] passes, and returns an [`UploadReport`].
//! - [`UploadRateEstimator`] turns acknowledged bytes into an upload bandwidth estimate.
//!
//! Only Linux exposes the counters this needs. Elsewhere [`TcpSocket::from_transport`] returns
//! `None`, so no watch is ever started.
//!
//! # Cost
//!
//! Each sample is one `getsockopt(TCP_INFO)` call, a few hundred nanoseconds with no allocation.
//! Starting a watch reads `TCP_INFO` around `SIOCOUTQ` and retries up to three times.
//! Sampling runs only while a watched upload is unacknowledged: at most
//! [`MAX_WATCHED_UPLOADS`] watches per connection, each sampling every [`SAMPLE_INTERVAL`] for
//! at most [`ACK_TIMEOUT`]. That bounds the cost to 80 system calls per second per connection.
//! A watch keeps only its latest sample. Nothing samples while no upload is watched.
//!
//! Each sampled connection holds one extra file descriptor. At most an eighth of the process's
//! open file limit goes to these descriptors, and connections beyond that are not sampled.

use std::{
    any::Any,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// How often an [`UploadWatch`] samples the socket while its upload is unacknowledged.
///
/// Fine enough to resolve the multi-second delivery tails seen on block bodies, and coarse
/// enough that a sample costs nothing measurable.
pub const SAMPLE_INTERVAL: Duration = Duration::from_millis(50);

/// How long an [`UploadWatch`] waits for its upload to be acknowledged before giving up.
pub const ACK_TIMEOUT: Duration = Duration::from_secs(30);

/// The most uploads one connection watches at once. Uploads started beyond this are not
/// watched, which bounds sampling cost when a peer requests many blocks at once.
pub const MAX_WATCHED_UPLOADS: usize = 4;

/// One reading of the kernel's TCP state for a connection.
///
/// Byte counters are in bytes; `unacked` and `snd_cwnd` are in segments of `snd_mss` bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TcpInfo {
    /// Bytes the peer has acknowledged since the connection opened (`tcpi_bytes_acked`).
    pub bytes_acked: u64,
    /// Bytes written to the socket that TCP has not sent yet (`tcpi_notsent_bytes`).
    /// `None` on kernels older than 4.6.
    pub notsent_bytes: Option<u32>,
    /// Segments sent and not yet acknowledged (`tcpi_unacked`).
    pub unacked: u32,
    /// Segments retransmitted since the connection opened (`tcpi_total_retrans`).
    pub total_retrans: u32,
    /// Congestion window, in segments (`tcpi_snd_cwnd`).
    pub snd_cwnd: u32,
    /// Smoothed round-trip time, in microseconds (`tcpi_rtt`).
    pub rtt_us: u32,
    /// Round-trip time variance, in microseconds (`tcpi_rttvar`).
    pub rttvar_us: u32,
    /// The kernel's most recent delivery rate estimate, in bytes per second
    /// (`tcpi_delivery_rate`). `None` on kernels older than 4.9.
    pub delivery_rate: Option<u64>,
    /// Sender maximum segment size, in bytes (`tcpi_snd_mss`).
    pub snd_mss: u32,
}

/// A handle to a connected TCP socket that reads its kernel state.
///
/// On Linux it owns a duplicate of the socket's file descriptor, so a sample can never read a
/// closed or reused descriptor. A duplicate keeps the socket open, so the connection must own
/// the only strong handle and drop it with its stream: watches hold a [`WeakTcpSocket`] and
/// upgrade it only for the duration of one system call. Once the connection drops its handle
/// the duplicate closes, and the kernel releases the socket as if it had never been duplicated.
#[derive(Debug)]
pub struct TcpSocket {
    #[cfg(target_os = "linux")]
    fd: Arc<BudgetedFd>,
}

/// A duplicated socket descriptor and its share of the [`DESCRIPTOR_BUDGET`].
///
/// The permit drops after the descriptor closes, so the budget never undercounts open duplicates.
#[cfg(target_os = "linux")]
#[derive(Debug)]
struct BudgetedFd {
    fd: std::os::fd::OwnedFd,
    _budget: OwnedSemaphorePermit,
}

/// The duplicated socket descriptors the process may hold.
///
/// Connections and database files share the open file limit. The handshake duplicates a descriptor
/// for each connection a peer opens, so this budget keeps sampling from using up that limit. Read
/// on the first connection, after startup has raised the limit.
#[cfg(target_os = "linux")]
static DESCRIPTOR_BUDGET: std::sync::LazyLock<Arc<Semaphore>> = std::sync::LazyLock::new(|| {
    Arc::new(Semaphore::new(descriptor_budget(sys::open_file_limit())))
});

/// Returns how many duplicates `open_file_limit` allows: one eighth of it, or one eighth of
/// [`ASSUMED_OPEN_FILE_LIMIT`] if the limit is unknown.
#[cfg(target_os = "linux")]
fn descriptor_budget(open_file_limit: Option<u64>) -> usize {
    /// A common default soft limit.
    const ASSUMED_OPEN_FILE_LIMIT: u64 = 1024;
    /// Far more connections than a node accepts, and within the semaphore's permit range.
    const MAX_DESCRIPTOR_BUDGET: u64 = 1 << 20;

    let budget = open_file_limit.unwrap_or(ASSUMED_OPEN_FILE_LIMIT) / 8;
    usize::try_from(budget.clamp(1, MAX_DESCRIPTOR_BUDGET)).expect("the budget fits in usize")
}

/// A non-owning [`TcpSocket`] handle that reads nothing once the connection is gone.
#[derive(Clone, Debug)]
pub struct WeakTcpSocket {
    #[cfg(target_os = "linux")]
    fd: std::sync::Weak<BudgetedFd>,
}

impl TcpSocket {
    /// Returns a handle to `transport`'s socket if it is a [`tokio::net::TcpStream`] and the
    /// platform exposes TCP delivery counters, and `None` otherwise (for example, for a Tor
    /// stream, an in-memory test stream, or a non-Linux platform).
    pub fn from_transport<T: Any>(transport: &T) -> Option<Self> {
        let stream = (transport as &dyn Any).downcast_ref::<tokio::net::TcpStream>()?;
        Self::from_tcp_stream(stream)
    }

    /// Returns a handle to `stream`'s socket, or `None` if the platform exposes no TCP delivery
    /// counters, the descriptor budget is spent, or the descriptor cannot be duplicated.
    pub fn from_tcp_stream(stream: &tokio::net::TcpStream) -> Option<Self> {
        #[cfg(target_os = "linux")]
        {
            let socket = Self::duplicate(stream, &DESCRIPTOR_BUDGET);
            if socket.is_none() {
                metrics::counter!("peer.tcp_progress.socket_unsampled").increment(1);
            }
            socket
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = stream;
            None
        }
    }

    /// Duplicates `stream`'s descriptor if `budget` has room.
    #[cfg(target_os = "linux")]
    fn duplicate(stream: &tokio::net::TcpStream, budget: &Arc<Semaphore>) -> Option<Self> {
        use std::os::fd::AsFd;
        let budget = budget.clone().try_acquire_owned().ok()?;
        let fd = stream.as_fd().try_clone_to_owned().ok()?;
        Some(Self {
            fd: Arc::new(BudgetedFd {
                fd,
                _budget: budget,
            }),
        })
    }

    /// Reads the socket's current TCP state.
    pub fn sample(&self) -> Option<TcpInfo> {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            sys::tcp_info(self.fd.fd.as_raw_fd())
        }
        #[cfg(not(target_os = "linux"))]
        None
    }

    /// Returns a handle that stops reading once this one is dropped.
    pub fn downgrade(&self) -> WeakTcpSocket {
        WeakTcpSocket {
            #[cfg(target_os = "linux")]
            fd: Arc::downgrade(&self.fd),
        }
    }

    /// Returns the bytes written to the socket that the peer has not acknowledged yet: sent and
    /// unacknowledged, plus not yet sent.
    fn send_queue_bytes(&self) -> Option<u64> {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            sys::send_queue_bytes(self.fd.fd.as_raw_fd())
        }
        #[cfg(not(target_os = "linux"))]
        None
    }
}

impl WeakTcpSocket {
    /// Reads the socket's current TCP state, or returns `None` once the connection is gone.
    pub fn sample(&self) -> Option<TcpInfo> {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;
            // Holding the upgraded handle keeps the descriptor open for the call.
            let fd = self.fd.upgrade()?;
            sys::tcp_info(fd.fd.as_raw_fd())
        }
        #[cfg(not(target_os = "linux"))]
        None
    }
}

/// Watches the delivery of large uploads on one connection.
///
/// The connection owns this, and with it the only strong [`TcpSocket`].
#[derive(Debug)]
pub struct UploadProgress {
    socket: TcpSocket,
    watches: Arc<Semaphore>,
}

impl UploadProgress {
    /// Watches uploads on `socket`.
    pub fn new(socket: TcpSocket) -> Self {
        Self {
            socket,
            watches: Arc::new(Semaphore::new(MAX_WATCHED_UPLOADS)),
        }
    }

    /// Reads the socket's current TCP state.
    pub fn sample(&self) -> Option<TcpInfo> {
        self.socket.sample()
    }

    /// The number of uploads being watched.
    #[allow(
        dead_code,
        reason = "kept for upload bandwidth estimates; nothing consumes it yet"
    )]
    pub fn watching(&self) -> usize {
        MAX_WATCHED_UPLOADS.saturating_sub(self.watches.available_permits())
    }

    /// Starts watching a message of `message_len` encoded bytes that is about to be written.
    ///
    /// Call this before the write starts, so the delivery target counts every byte queued ahead
    /// of the message. Returns `None` if [`MAX_WATCHED_UPLOADS`] watches are running or the
    /// socket cannot be read.
    pub fn start(&self, message_len: u64) -> Option<UploadWatch> {
        let permit = self.watches.clone().try_acquire_owned().ok()?;
        let started_at = Instant::now();
        let (start, queued_ahead) =
            upload_start_snapshot(|| self.socket.sample(), || self.socket.send_queue_bytes())?;
        Some(UploadWatch {
            socket: self.socket.downgrade(),
            started_at,
            start,
            message_len,
            target_bytes_acked: start
                .bytes_acked
                .saturating_add(queued_ahead)
                .saturating_add(message_len),
            _permit: permit,
        })
    }
}

/// Reads a consistent ACK counter and send queue before the writer publishes new bytes.
///
/// ACKs can arrive between syscalls. Retry a changed counter so bytes acknowledged between
/// reads cannot lower the delivery target. Skip sampling if the counter never stabilizes.
fn upload_start_snapshot(
    mut sample: impl FnMut() -> Option<TcpInfo>,
    mut send_queue_bytes: impl FnMut() -> Option<u64>,
) -> Option<(TcpInfo, u64)> {
    let mut before = sample()?;
    for _ in 0..3 {
        let queued = send_queue_bytes()?;
        let after = sample()?;
        if before.bytes_acked == after.bytes_acked {
            return Some((after, queued));
        }
        before = after;
    }
    None
}

/// An upload whose acknowledgement is being watched, started by [`UploadProgress::start`].
#[derive(Debug)]
pub struct UploadWatch {
    socket: WeakTcpSocket,
    started_at: Instant,
    start: TcpInfo,
    message_len: u64,
    /// The `bytes_acked` value at which the peer has acknowledged the whole message.
    target_bytes_acked: u64,
    _permit: OwnedSemaphorePermit,
}

/// How an [`UploadWatch`] ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadOutcome {
    /// The peer acknowledged every byte of the message.
    Acked,
    /// [`ACK_TIMEOUT`] passed first.
    TimedOut,
    /// The connection closed, or its socket could no longer be read, first.
    Closed,
}

/// One sample taken by an [`UploadWatch`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UploadSample {
    /// Time since the watch started.
    pub elapsed: Duration,
    /// The socket state at that time.
    pub info: TcpInfo,
    /// Bytes of the message the peer had acknowledged by then.
    pub acked_message_bytes: u64,
}

/// The result of an [`UploadWatch`].
#[derive(Clone, Debug)]
pub struct UploadReport {
    /// Why the watch ended.
    pub outcome: UploadOutcome,
    /// When the watch started, just before the write.
    #[allow(
        dead_code,
        reason = "part of the report for consumers other than the trace"
    )]
    pub started_at: Instant,
    /// Time from the start of the watch to its end; for [`UploadOutcome::Acked`], to the first
    /// sample that saw the whole message acknowledged.
    pub elapsed: Duration,
    /// The encoded length of the message.
    #[allow(
        dead_code,
        reason = "part of the report for consumers other than the trace"
    )]
    pub message_len: u64,
    /// The socket state when the watch started.
    pub start: TcpInfo,
    /// How many samples the watch took.
    pub sample_count: u64,
    /// The latest sample: the socket state when the watch ended, unless the connection closed
    /// first. A peer can hold a watch open for [`ACK_TIMEOUT`], so earlier samples are not kept.
    pub last_sample: Option<UploadSample>,
}

impl UploadReport {
    /// Bytes of the message the peer had acknowledged by the last sample.
    pub fn acked_message_bytes(&self) -> u64 {
        self.last_sample
            .map_or(0, |sample| sample.acked_message_bytes)
    }

    /// The socket state at the last sample.
    pub fn last(&self) -> Option<&TcpInfo> {
        self.last_sample.as_ref().map(|sample| &sample.info)
    }

    /// Segments retransmitted on the connection between the start of the watch and the last
    /// sample.
    pub fn retransmits(&self) -> Option<u32> {
        self.last()
            .map(|last| last.total_retrans.saturating_sub(self.start.total_retrans))
    }
}

impl UploadWatch {
    /// Samples the socket every [`SAMPLE_INTERVAL`] until the peer acknowledges the message,
    /// the connection closes, or [`ACK_TIMEOUT`] passes.
    pub async fn wait_acked(self) -> UploadReport {
        let deadline = self.started_at + ACK_TIMEOUT;
        let mut ticks = tokio::time::interval(SAMPLE_INTERVAL);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut sample_count = 0;
        let mut last_sample = None;

        let outcome = loop {
            ticks.tick().await;
            let now = Instant::now();
            let Some(info) = self.socket.sample() else {
                break UploadOutcome::Closed;
            };
            sample_count += 1;
            last_sample = Some(UploadSample {
                elapsed: now.saturating_duration_since(self.started_at),
                info,
                acked_message_bytes: self.acked_message_bytes(&info),
            });
            if info.bytes_acked >= self.target_bytes_acked {
                break UploadOutcome::Acked;
            }
            if now >= deadline {
                break UploadOutcome::TimedOut;
            }
        };

        UploadReport {
            outcome,
            started_at: self.started_at,
            elapsed: match (outcome, last_sample) {
                (UploadOutcome::Acked, Some(sample)) => sample.elapsed,
                _ => self.started_at.elapsed(),
            },
            message_len: self.message_len,
            start: self.start,
            sample_count,
            last_sample,
        }
    }

    /// Returns the bytes of the message acknowledged in `info`, counting bytes queued ahead of
    /// the message as acknowledged first.
    fn acked_message_bytes(&self, info: &TcpInfo) -> u64 {
        let message_start = self.target_bytes_acked.saturating_sub(self.message_len);
        info.bytes_acked
            .saturating_sub(message_start)
            .min(self.message_len)
    }
}

/// Estimates upload bandwidth from acknowledged upload bytes.
///
/// The rate is acknowledged bytes divided by the time at least one upload was outstanding, so
/// idle time between uploads does not dilute it. Each observation updates an exponentially
/// weighted moving average whose weight grows with the busy time it covers: an observation
/// spanning `time_constant` of busy time moves the estimate about 63% of the way to its rate.
#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "kept for upload bandwidth estimates; nothing consumes it yet"
)]
pub struct UploadRateEstimator {
    time_constant: Duration,
    outstanding: usize,
    /// When busy time was last accrued, while at least one upload is outstanding.
    busy_mark: Option<Instant>,
    /// Busy time accrued since the last observation.
    unobserved_busy: Duration,
    /// Acknowledged bytes reported since the last observation.
    unobserved_bytes: u64,
    bytes_per_second: Option<f64>,
}

#[allow(
    dead_code,
    reason = "kept for upload bandwidth estimates; nothing consumes it yet"
)]
impl UploadRateEstimator {
    /// Creates an estimator with no estimate that weights observations by `time_constant`.
    pub fn new(time_constant: Duration) -> Self {
        Self {
            time_constant,
            outstanding: 0,
            busy_mark: None,
            unobserved_busy: Duration::ZERO,
            unobserved_bytes: 0,
            bytes_per_second: None,
        }
    }

    /// Records that an upload started at `now`.
    pub fn upload_started(&mut self, now: Instant) {
        self.accrue(now);
        if self.outstanding == 0 {
            self.busy_mark = Some(now);
        }
        self.outstanding = self.outstanding.saturating_add(1);
    }

    /// Records that `bytes` more upload bytes were acknowledged by `now`, and folds every
    /// acknowledgement and busy interval since the last observation into the estimate.
    pub fn record_acked(&mut self, bytes: u64, now: Instant) {
        self.accrue(now);
        self.unobserved_bytes = self.unobserved_bytes.saturating_add(bytes);
        self.observe();
    }

    /// Records that an upload ended at `now`, acknowledged or abandoned.
    ///
    /// Call [`Self::record_acked`] first for its final acknowledged bytes.
    pub fn upload_finished(&mut self, now: Instant) {
        self.accrue(now);
        self.outstanding = self.outstanding.saturating_sub(1);
        if self.outstanding == 0 {
            self.busy_mark = None;
        }
    }

    /// The smoothed upload rate in bytes per second, or `None` before the first observation.
    pub fn bytes_per_second(&self) -> Option<f64> {
        self.bytes_per_second
    }

    /// The number of uploads started and not yet finished.
    pub fn outstanding(&self) -> usize {
        self.outstanding
    }

    fn accrue(&mut self, now: Instant) {
        if let Some(mark) = self.busy_mark.as_mut() {
            self.unobserved_busy = self
                .unobserved_busy
                .saturating_add(now.saturating_duration_since(*mark));
            *mark = now;
        }
    }

    /// Folds the unobserved bytes and busy time into the estimate, once both are nonzero.
    fn observe(&mut self) {
        if self.unobserved_bytes == 0 || self.unobserved_busy.is_zero() {
            return;
        }
        let busy = self.unobserved_busy.as_secs_f64();
        // Precision loss above 2^53 bytes is irrelevant to a rate estimate.
        #[allow(clippy::cast_precision_loss)]
        let rate = self.unobserved_bytes as f64 / busy;
        self.bytes_per_second = Some(match self.bytes_per_second {
            None => rate,
            Some(previous) => {
                let weight = if self.time_constant.is_zero() {
                    1.0
                } else {
                    1.0 - (-busy / self.time_constant.as_secs_f64()).exp()
                };
                previous + weight * (rate - previous)
            }
        });
        self.unobserved_bytes = 0;
        self.unobserved_busy = Duration::ZERO;
    }
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
mod sys {
    //! `TCP_INFO` and `SIOCOUTQ` on a raw socket descriptor.

    use std::{
        mem::{offset_of, size_of},
        os::fd::RawFd,
    };

    use super::TcpInfo;

    /// Returns the process's soft open file limit, or `None` if it cannot be read.
    pub(super) fn open_file_limit() -> Option<u64> {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: `limit` is a valid `rlimit` that outlives the call.
        let result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
        (result == 0).then_some(limit.rlim_cur)
    }

    /// The prefix of the kernel's `struct tcp_info` (`include/uapi/linux/tcp.h`) through
    /// `tcpi_delivery_rate`, as of Linux 4.9. The kernel appends fields over time and copies
    /// `min(optlen, sizeof(struct tcp_info))` bytes, so asking for this prefix works on newer
    /// kernels, and a shorter returned length marks an older kernel. The `libc` crate's glibc
    /// definition stops at `tcpi_total_retrans`, so it cannot be used here.
    #[repr(C)]
    #[derive(Default)]
    struct KernelTcpInfo {
        tcpi_state: u8,
        tcpi_ca_state: u8,
        tcpi_retransmits: u8,
        tcpi_probes: u8,
        tcpi_backoff: u8,
        tcpi_options: u8,
        tcpi_wscale: u8,
        tcpi_rate_flags: u8,
        tcpi_rto: u32,
        tcpi_ato: u32,
        tcpi_snd_mss: u32,
        tcpi_rcv_mss: u32,
        tcpi_unacked: u32,
        tcpi_sacked: u32,
        tcpi_lost: u32,
        tcpi_retrans: u32,
        tcpi_fackets: u32,
        tcpi_last_data_sent: u32,
        tcpi_last_ack_sent: u32,
        tcpi_last_data_recv: u32,
        tcpi_last_ack_recv: u32,
        tcpi_pmtu: u32,
        tcpi_rcv_ssthresh: u32,
        tcpi_rtt: u32,
        tcpi_rttvar: u32,
        tcpi_snd_ssthresh: u32,
        tcpi_snd_cwnd: u32,
        tcpi_advmss: u32,
        tcpi_reordering: u32,
        tcpi_rcv_rtt: u32,
        tcpi_rcv_space: u32,
        tcpi_total_retrans: u32,
        tcpi_pacing_rate: u64,
        tcpi_max_pacing_rate: u64,
        tcpi_bytes_acked: u64,
        tcpi_bytes_received: u64,
        tcpi_segs_out: u32,
        tcpi_segs_in: u32,
        tcpi_notsent_bytes: u32,
        tcpi_min_rtt: u32,
        tcpi_data_segs_in: u32,
        tcpi_data_segs_out: u32,
        tcpi_delivery_rate: u64,
    }

    // The layout must match the kernel's, which has no padding up to this field.
    const _: () = assert!(offset_of!(KernelTcpInfo, tcpi_delivery_rate) == 160);

    /// Returns the end offset of a field, the shortest returned length that includes it.
    macro_rules! end_of {
        ($field:ident) => {{
            let probe = KernelTcpInfo::default();
            offset_of!(KernelTcpInfo, $field) + std::mem::size_of_val(&probe.$field)
        }};
    }

    /// Reads `TCP_INFO` for `fd`. Returns `None` if the call fails or the kernel predates
    /// `tcpi_bytes_acked` (Linux 4.2).
    pub(super) fn tcp_info(fd: RawFd) -> Option<TcpInfo> {
        let mut info = KernelTcpInfo::default();
        // The struct is a few hundred bytes, far below `socklen_t::MAX`.
        let mut len = size_of::<KernelTcpInfo>() as libc::socklen_t;
        // SAFETY: `info` is a writable, plain-data buffer of `len` bytes, and `len` is a valid
        // in/out length. The kernel writes at most `len` bytes and stores the written length.
        // A closed or non-socket `fd` makes the call fail without touching the buffer.
        let result = unsafe {
            libc::getsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_INFO,
                std::ptr::from_mut(&mut info).cast(),
                &mut len,
            )
        };
        if result != 0 {
            return None;
        }
        // The kernel returns a non-negative length no larger than the one passed in.
        let len = len as usize;
        if len < end_of!(tcpi_bytes_acked) {
            return None;
        }

        Some(TcpInfo {
            bytes_acked: info.tcpi_bytes_acked,
            notsent_bytes: (len >= end_of!(tcpi_notsent_bytes)).then_some(info.tcpi_notsent_bytes),
            unacked: info.tcpi_unacked,
            total_retrans: info.tcpi_total_retrans,
            snd_cwnd: info.tcpi_snd_cwnd,
            rtt_us: info.tcpi_rtt,
            rttvar_us: info.tcpi_rttvar,
            delivery_rate: (len >= end_of!(tcpi_delivery_rate)).then_some(info.tcpi_delivery_rate),
            snd_mss: info.tcpi_snd_mss,
        })
    }

    /// Returns the bytes in `fd`'s send queue: sent and unacknowledged, plus not yet sent.
    pub(super) fn send_queue_bytes(fd: RawFd) -> Option<u64> {
        let mut queued: libc::c_int = 0;
        // SAFETY: `SIOCOUTQ` (`TIOCOUTQ` on sockets) writes one `int` through the pointer, which
        // points to a live `c_int`. A closed or non-socket `fd` makes the call fail.
        let result = unsafe { libc::ioctl(fd, libc::TIOCOUTQ, &mut queued) };
        if result != 0 {
            return None;
        }
        u64::try_from(queued).ok()
    }
}

#[cfg(test)]
mod tests;
