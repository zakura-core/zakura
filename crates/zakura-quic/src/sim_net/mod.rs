//! `SimNet`: an in-memory datagram network that the driver runs on through
//! the socket seam.
//!
//! A send puts an immutable datagram in flight on its directed link. Nothing
//! arrives on its own: the harness picks each delivery or drop, and each one
//! handles exactly one datagram. A datagram sent at `t` may be delivered from
//! `t + min_delay` and must be delivered or dropped by `t + max_delay`. Time is
//! the driver's clock (`tokio::time`), so a harness that pauses it controls
//! both the driver and the network.
//!
//! Sockets bind explicit addresses and never touch the OS. They report no GSO,
//! no MTU discovery, no buffer sizes and no inode, so the endpoint starts no
//! probe or poller for them (SOCK-3 to SOCK-7, SOCK-13).

use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    io,
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use bytes::Bytes;
use quinn_proto::Transmit;

use crate::{
    driver::connection::now,
    socket::{BoundSocket, RecvMeta, SocketFactory, UdpIo},
};

#[cfg(test)]
mod tests;

/// How one directed link treats the datagrams sent on it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LinkModel {
    /// The earliest a datagram may arrive after its send.
    pub(crate) min_delay: Duration,
    /// The latest a datagram may arrive after its send. Time can't pass it
    /// while the datagram is in flight.
    pub(crate) max_delay: Duration,
    /// Whether any eligible datagram may arrive next. Without it, only the
    /// link's oldest datagram in flight may.
    pub(crate) reorder: bool,
    /// How many datagrams the link may lose over the whole run.
    pub(crate) loss_budget: u64,
}

impl LinkModel {
    /// Panics unless the delay window opens before it closes. An inverted
    /// window would leave a datagram due but still too early to deliver,
    /// with nothing allowed next.
    fn check(&self) {
        assert!(
            self.min_delay <= self.max_delay,
            "min_delay must not exceed max_delay"
        );
    }
}

impl Default for LinkModel {
    /// The header-sync defaults: 0 to 100 ms, reordering, no loss.
    fn default() -> Self {
        Self {
            min_delay: Duration::ZERO,
            max_delay: Duration::from_millis(100),
            reorder: true,
            loss_budget: 0,
        }
    }
}

/// A directed link between two addresses.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct Link {
    pub(crate) src: SocketAddr,
    pub(crate) dst: SocketAddr,
}

/// A datagram's stable identity: its link and its send sequence on that link.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct DatagramId {
    pub(crate) link: Link,
    pub(crate) seq: u64,
}

/// Why a datagram never reached a socket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DropReason {
    /// The harness dropped it within the link's loss budget.
    Loss,
    /// Nothing was bound to its destination when it arrived, which UDP
    /// drops silently.
    Unbound,
}

/// What happened on the network, in order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NetEvent {
    /// A socket bound `addr`.
    Bound { addr: SocketAddr },
    /// The socket on `addr` closed, discarding the datagrams it hadn't read.
    Unbound { addr: SocketAddr, unread: usize },
    /// A datagram went in flight.
    Sent { id: DatagramId, len: usize },
    /// A datagram reached its destination's receive queue.
    Delivered { id: DatagramId },
    /// A datagram left the network without reaching a socket.
    Dropped { id: DatagramId, reason: DropReason },
}

/// Why the harness can't deliver or drop a datagram.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NetError {
    /// It isn't in flight: never sent, or already delivered or dropped.
    NotInFlight,
    /// Its minimum delay hasn't passed.
    TooEarly,
    /// Its link keeps order and an older datagram is still in flight.
    NotOldest,
    /// Its link has no loss budget left.
    NoLossBudget,
}

/// A datagram in flight.
#[derive(Debug)]
struct InFlight {
    bytes: Bytes,
    earliest: Instant,
    latest: Instant,
}

/// One link's model and counters.
#[derive(Debug)]
struct LinkState {
    model: LinkModel,
    next_seq: u64,
    drops: u64,
}

/// A bound socket's receive side.
#[derive(Debug)]
struct Binding {
    /// Tells this socket apart from a later one on the same address.
    id: u64,
    queue: VecDeque<(SocketAddr, Bytes)>,
    /// The last task that waited for a datagram and found none.
    waker: Option<Waker>,
    /// A receive error the scenario injected, returned before any datagram.
    fail_next: Option<io::ErrorKind>,
}

#[derive(Debug, Default)]
struct State {
    default_model: LinkModel,
    links: BTreeMap<Link, LinkState>,
    in_flight: BTreeMap<DatagramId, InFlight>,
    sockets: BTreeMap<SocketAddr, Binding>,
    next_binding: u64,
    events: Vec<NetEvent>,
}

impl State {
    /// The state of `link`, created from the default model on first use.
    fn link(&mut self, link: Link) -> &mut LinkState {
        let model = self.default_model;
        self.links.entry(link).or_insert(LinkState {
            model,
            next_seq: 0,
            drops: 0,
        })
    }

    /// Whether `link` keeps its datagrams in order.
    fn is_fifo(&self, link: Link) -> bool {
        !self
            .links
            .get(&link)
            .map_or(self.default_model, |state| state.model)
            .reorder
    }

    /// The oldest datagram in flight on `link`.
    fn oldest(&self, link: Link) -> Option<(&DatagramId, &InFlight)> {
        self.in_flight
            .range(
                DatagramId { link, seq: 0 }..=DatagramId {
                    link,
                    seq: u64::MAX,
                },
            )
            .next()
    }

    /// Whether the harness may deliver `id` at `now`.
    fn check_deliverable(&self, id: DatagramId, now: Instant) -> Result<(), NetError> {
        let datagram = self.in_flight.get(&id).ok_or(NetError::NotInFlight)?;
        if now < datagram.earliest {
            return Err(NetError::TooEarly);
        }
        if self.is_fifo(id.link) && self.oldest(id.link).map(|(oldest, _)| *oldest) != Some(id) {
            return Err(NetError::NotOldest);
        }
        Ok(())
    }

    /// Puts one datagram in flight from `src` at `now`.
    fn send(&mut self, src: SocketAddr, dst: SocketAddr, bytes: Bytes, now: Instant) {
        let link = Link { src, dst };
        let state = self.link(link);
        let seq = state.next_seq;
        state.next_seq += 1;
        let earliest = now + state.model.min_delay;
        let latest = now + state.model.max_delay;
        let id = DatagramId { link, seq };
        self.events.push(NetEvent::Sent {
            id,
            len: bytes.len(),
        });
        self.in_flight.insert(
            id,
            InFlight {
                bytes,
                earliest,
                latest,
            },
        );
    }
}

/// The simulated network. Clones share it.
#[derive(Clone, Debug, Default)]
pub(crate) struct SimNet {
    state: Arc<Mutex<State>>,
}

impl SimNet {
    /// An empty network whose links all follow `default_model` until
    /// [`SimNet::set_link`] says otherwise.
    ///
    /// # Panics
    ///
    /// If `default_model`'s `min_delay` exceeds its `max_delay`.
    pub(crate) fn new(default_model: LinkModel) -> Self {
        default_model.check();
        Self {
            state: Arc::new(Mutex::new(State {
                default_model,
                ..State::default()
            })),
        }
    }

    /// Locks the network. A panic elsewhere leaves it consistent, because
    /// every change happens under one lock.
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Sets `link`'s model for the rest of the run.
    ///
    /// Keeping the model fixed while datagrams are in flight keeps a FIFO
    /// link's windows in send order. Its oldest datagram is then always the
    /// first to come due, and is deliverable when it does.
    ///
    /// # Panics
    ///
    /// If datagrams are in flight on the link, or if `model`'s `min_delay`
    /// exceeds its `max_delay`.
    pub(crate) fn set_link(&self, link: Link, model: LinkModel) {
        model.check();
        let mut state = self.lock();
        assert!(
            state.oldest(link).is_none(),
            "a link's model can't change while datagrams are in flight on it"
        );
        state.link(link).model = model;
    }

    /// Binds a socket to `addr`, which must name an IP and a port.
    pub(crate) fn bind(&self, addr: SocketAddr) -> io::Result<SimSocket> {
        if addr.ip().is_unspecified() || addr.port() == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a simulated socket binds an explicit address",
            ));
        }
        let mut state = self.lock();
        if state.sockets.contains_key(&addr) {
            return Err(io::ErrorKind::AddrInUse.into());
        }
        let id = state.next_binding;
        state.next_binding += 1;
        state.sockets.insert(
            addr,
            Binding {
                id,
                queue: VecDeque::new(),
                waker: None,
                fail_next: None,
            },
        );
        state.events.push(NetEvent::Bound { addr });
        Ok(SimSocket {
            net: self.clone(),
            addr,
            binding: id,
        })
    }

    /// The datagrams in flight, oldest link and sequence first.
    pub(crate) fn in_flight(&self) -> Vec<DatagramId> {
        self.lock().in_flight.keys().copied().collect()
    }

    /// The datagrams the harness may deliver now: every eligible one on a
    /// reordering link, and the oldest one on any other link once eligible.
    pub(crate) fn deliverable(&self) -> Vec<DatagramId> {
        let now = now();
        let state = self.lock();
        state
            .in_flight
            .keys()
            .copied()
            .filter(|id| state.check_deliverable(*id, now).is_ok())
            .collect()
    }

    /// The datagrams the harness may drop now: any in flight on a link with
    /// loss budget left.
    pub(crate) fn droppable(&self) -> Vec<DatagramId> {
        let state = self.lock();
        state
            .in_flight
            .keys()
            .copied()
            .filter(|id| {
                state
                    .links
                    .get(&id.link)
                    .is_some_and(|link| link.drops < link.model.loss_budget)
            })
            .collect()
    }

    /// The earliest maximum-delay deadline in flight. Time must not pass it.
    pub(crate) fn time_limit(&self) -> Option<Instant> {
        self.lock()
            .in_flight
            .values()
            .map(|datagram| datagram.latest)
            .min()
    }

    /// Whether time may move forward at all. It may not while a datagram is
    /// at its deadline, so a due datagram forces a delivery or drop instead
    /// of a zero-length advance. [`SimNet::deliverable`] is never empty then.
    pub(crate) fn can_advance(&self) -> bool {
        self.time_limit().is_none_or(|limit| now() < limit)
    }

    /// The next time a datagram in flight becomes eligible, if any isn't yet.
    pub(crate) fn next_eligible(&self) -> Option<Instant> {
        let now = now();
        self.lock()
            .in_flight
            .values()
            .map(|datagram| datagram.earliest)
            .filter(|earliest| *earliest > now)
            .min()
    }

    /// Delivers one datagram to the socket bound at its destination, or
    /// drops it if none is (UDP semantics). Wakes that socket's reader.
    pub(crate) fn deliver(&self, id: DatagramId) -> Result<(), NetError> {
        let now = now();
        let waker = {
            let mut state = self.lock();
            state.check_deliverable(id, now)?;
            let datagram = state
                .in_flight
                .remove(&id)
                .expect("check_deliverable found the datagram in flight");
            match state.sockets.get_mut(&id.link.dst) {
                Some(binding) => {
                    binding.queue.push_back((id.link.src, datagram.bytes));
                    let waker = binding.waker.take();
                    state.events.push(NetEvent::Delivered { id });
                    waker
                }
                None => {
                    state.events.push(NetEvent::Dropped {
                        id,
                        reason: DropReason::Unbound,
                    });
                    None
                }
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    /// Drops one datagram, spending one unit of its link's loss budget.
    pub(crate) fn drop_datagram(&self, id: DatagramId) -> Result<(), NetError> {
        let mut state = self.lock();
        if !state.in_flight.contains_key(&id) {
            return Err(NetError::NotInFlight);
        }
        let link = state.link(id.link);
        if link.drops >= link.model.loss_budget {
            return Err(NetError::NoLossBudget);
        }
        link.drops += 1;
        state.in_flight.remove(&id);
        state.events.push(NetEvent::Dropped {
            id,
            reason: DropReason::Loss,
        });
        Ok(())
    }

    /// Makes the next receive on `addr` fail with `kind`, as a fatal socket
    /// error would (SOCK-9), and wakes its reader. Returns whether a socket
    /// was bound there.
    pub(crate) fn fail_next_recv(&self, addr: SocketAddr, kind: io::ErrorKind) -> bool {
        let waker = {
            let mut state = self.lock();
            let Some(binding) = state.sockets.get_mut(&addr) else {
                return false;
            };
            binding.fail_next = Some(kind);
            binding.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        true
    }

    /// Everything that happened on the network so far, in order.
    pub(crate) fn events(&self) -> Vec<NetEvent> {
        self.lock().events.clone()
    }
}

impl SocketFactory for SimNet {
    type Socket = SimSocket;

    /// Binds a simulated socket, which has no buffer sizes and no inode.
    fn bind(&self, addr: SocketAddr) -> io::Result<BoundSocket<SimSocket>> {
        Ok(BoundSocket {
            io: SimNet::bind(self, addr)?,
            local_addr: addr,
            buffers: None,
            inode: None,
        })
    }
}

/// A socket bound on a [`SimNet`]. Dropping it unbinds its address and
/// discards the datagrams it hadn't read.
#[derive(Debug)]
pub(crate) struct SimSocket {
    net: SimNet,
    addr: SocketAddr,
    binding: u64,
}

impl SimSocket {
    /// The address this socket is bound to.
    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Runs `f` on this socket's binding. It exists for as long as the
    /// socket does.
    fn with_binding<T>(&self, f: impl FnOnce(&mut Binding) -> T) -> T {
        let mut state = self.net.lock();
        let binding = state
            .sockets
            .get_mut(&self.addr)
            .filter(|binding| binding.id == self.binding)
            .expect("a socket's binding lives until the socket drops");
        f(binding)
    }

    /// Ready once a datagram or an injected error is queued. Otherwise it
    /// stores the caller's waker, under the same lock a delivery takes, so a
    /// delivery can't slip between the check and the registration.
    fn poll_readable(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.with_binding(|binding| {
            if !binding.queue.is_empty() || binding.fail_next.is_some() {
                return Poll::Ready(Ok(()));
            }
            match &mut binding.waker {
                Some(waker) if waker.will_wake(cx.waker()) => {}
                slot => *slot = Some(cx.waker().clone()),
            }
            Poll::Pending
        })
    }
}

impl Drop for SimSocket {
    /// Unbinds the address. The stored waker goes with it, so a later
    /// socket on the same address never wakes this socket's reader.
    fn drop(&mut self) {
        let mut state = self.net.lock();
        if state
            .sockets
            .get(&self.addr)
            .is_some_and(|binding| binding.id == self.binding)
        {
            let binding = state
                .sockets
                .remove(&self.addr)
                .expect("the binding was just found");
            state.events.push(NetEvent::Unbound {
                addr: self.addr,
                unread: binding.queue.len(),
            });
        }
    }
}

impl UdpIo for SimSocket {
    /// Pending until a delivery or an injected error reaches this socket.
    fn readable(&self) -> impl Future<Output = io::Result<()>> + Send {
        std::future::poll_fn(|cx| self.poll_readable(cx))
    }

    /// Always ready: the network takes every send.
    async fn writable(&self) -> io::Result<()> {
        Ok(())
    }

    /// Pops the oldest delivered datagram, or returns `WouldBlock` like an
    /// empty OS socket. A datagram longer than `buf` is dropped, as the
    /// production socket drops a truncated one.
    fn try_recv(&self, buf: &mut [u8]) -> io::Result<Option<RecvMeta>> {
        self.with_binding(|binding| {
            if let Some(kind) = binding.fail_next.take() {
                return Err(io::Error::new(kind, "injected socket failure"));
            }
            let Some((remote, bytes)) = binding.queue.pop_front() else {
                return Err(io::ErrorKind::WouldBlock.into());
            };
            let Some(target) = buf.get_mut(..bytes.len()) else {
                return Ok(None);
            };
            target.copy_from_slice(&bytes);
            Ok(Some(RecvMeta {
                remote,
                len: bytes.len(),
                stride: bytes.len(),
                dst_ip: None,
            }))
        })
    }

    /// Puts each datagram of the transmit in flight from this socket's
    /// address. It never fails.
    fn try_send(&self, transmit: &Transmit, contents: &[u8]) -> io::Result<()> {
        let segment = transmit.segment_size.unwrap_or(contents.len()).max(1);
        let mut state = self.net.lock();
        // Read the clock under the lock that assigns sequence numbers, so
        // concurrent sends get windows in sequence order.
        let now = now();
        // `chunks` yields nothing for an empty payload, which UDP still
        // sends as one datagram.
        let empty = std::iter::once(contents).filter(|contents| contents.is_empty());
        for datagram in empty.chain(contents.chunks(segment)) {
            state.send(
                self.addr,
                transmit.destination,
                Bytes::copy_from_slice(datagram),
                now,
            );
        }
        Ok(())
    }

    /// No segmentation offload: one datagram per transmit.
    fn gso(&self) -> bool {
        false
    }

    /// No MTU discovery: datagrams stay at the initial MTU.
    fn mtud_allowed(&self) -> bool {
        false
    }
}
