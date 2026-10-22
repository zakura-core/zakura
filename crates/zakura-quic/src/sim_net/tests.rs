//! `SimNet` properties M2.1 to M2.6, and generated sequences checked against
//! a queue model.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    io,
    net::{Ipv4Addr, SocketAddr},
    pin::pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

use quinn_proto::Transmit;

use super::*;
use crate::socket::{SocketFactory, UdpIo};

/// `10.0.0.n:4000`.
fn addr(n: u8) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::new(10, 0, 0, n), 4000))
}

/// A one-datagram transmit of `len` bytes to `dst`.
fn transmit(dst: SocketAddr, len: usize) -> Transmit {
    Transmit {
        destination: dst,
        ecn: None,
        size: len,
        segment_size: None,
        src_ip: None,
    }
}

/// Sends `bytes` from `socket` to `dst` as one datagram.
fn send(socket: &SimSocket, dst: SocketAddr, bytes: &[u8]) {
    socket.try_send(&transmit(dst, bytes.len()), bytes).unwrap();
}

/// Reads one datagram: its sender and bytes, or the error.
fn recv(socket: &SimSocket) -> io::Result<(SocketAddr, Vec<u8>)> {
    let mut buf = vec![0; 2048];
    let meta = socket.try_recv(&mut buf)?.expect("simulated datagrams fit");
    assert_eq!(meta.stride, meta.len);
    assert_eq!(meta.dst_ip, None);
    Ok((meta.remote, buf[..meta.len].to_vec()))
}

/// Whether a read found the queue empty.
fn would_block(result: io::Result<(SocketAddr, Vec<u8>)>) -> bool {
    matches!(result, Err(error) if error.kind() == io::ErrorKind::WouldBlock)
}

/// A link model with fixed delays, no reordering and no loss.
fn fifo(min_ms: u64, max_ms: u64) -> LinkModel {
    LinkModel {
        min_delay: Duration::from_millis(min_ms),
        max_delay: Duration::from_millis(max_ms),
        reorder: false,
        loss_budget: 0,
    }
}

/// A waker that counts its wakes.
#[derive(Default)]
struct CountingWaker(AtomicUsize);

impl Wake for CountingWaker {
    /// Counts one wake.
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl CountingWaker {
    /// A fresh waker and its counter.
    fn new() -> (Waker, Arc<Self>) {
        let counter = Arc::new(Self::default());
        (Waker::from(counter.clone()), counter)
    }

    /// Wakes so far.
    fn wakes(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

/// Polls `future` once with `waker`.
fn poll_with<F: Future>(future: std::pin::Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

// M2.1

#[tokio::test(start_paused = true)]
async fn a_datagram_is_unreadable_until_delivered_and_resolves_once() {
    let net = SimNet::new(fifo(0, 10));
    let a = net.bind(addr(1)).unwrap();
    let b = net.bind(addr(2)).unwrap();
    send(&a, addr(2), b"hello");
    let [id] = net.in_flight()[..] else {
        panic!("one datagram in flight")
    };
    assert_eq!(
        id,
        DatagramId {
            link: Link {
                src: addr(1),
                dst: addr(2)
            },
            seq: 0
        }
    );
    assert!(would_block(recv(&b)));

    net.deliver(id).unwrap();
    assert_eq!(recv(&b).unwrap(), (addr(1), b"hello".to_vec()));
    assert!(would_block(recv(&b)));
    assert!(would_block(recv(&a)));
    assert_eq!(net.deliver(id), Err(NetError::NotInFlight));
    assert_eq!(net.drop_datagram(id), Err(NetError::NotInFlight));
    assert_eq!(
        net.events(),
        [
            NetEvent::Bound { addr: addr(1) },
            NetEvent::Bound { addr: addr(2) },
            NetEvent::Sent { id, len: 5 },
            NetEvent::Delivered { id },
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_segmented_transmit_becomes_one_datagram_per_segment() {
    let net = SimNet::new(fifo(0, 10));
    let a = net.bind(addr(1)).unwrap();
    let b = net.bind(addr(2)).unwrap();
    let mut batch = transmit(addr(2), 7);
    batch.segment_size = Some(3);
    a.try_send(&batch, b"abcdefg").unwrap();
    let ids = net.in_flight();
    assert_eq!(ids.iter().map(|id| id.seq).collect::<Vec<_>>(), [0, 1, 2]);
    for id in ids {
        net.deliver(id).unwrap();
    }
    for expected in [&b"abc"[..], b"def", b"g"] {
        assert_eq!(recv(&b).unwrap(), (addr(1), expected.to_vec()));
    }
}

#[test]
fn an_empty_payload_is_one_datagram() {
    let net = SimNet::new(fifo(0, 10));
    let a = net.bind(addr(1)).unwrap();
    let b = net.bind(addr(2)).unwrap();
    send(&a, addr(2), b"");
    let [id] = net.in_flight()[..] else {
        panic!("one datagram in flight")
    };
    net.deliver(id).unwrap();
    assert_eq!(recv(&b).unwrap(), (addr(1), Vec::new()));
    assert!(would_block(recv(&b)));
}

// M2.2

#[tokio::test(start_paused = true)]
async fn delivery_waits_for_the_minimum_delay() {
    let net = SimNet::new(fifo(10, 20));
    let a = net.bind(addr(1)).unwrap();
    let _b = net.bind(addr(2)).unwrap();
    let sent = now();
    send(&a, addr(2), b"x");
    let [id] = net.in_flight()[..] else {
        panic!("one datagram in flight")
    };
    assert_eq!(net.deliver(id), Err(NetError::TooEarly));
    assert!(net.deliverable().is_empty());
    assert_eq!(net.next_eligible(), Some(sent + Duration::from_millis(10)));
    assert_eq!(net.time_limit(), Some(sent + Duration::from_millis(20)));

    tokio::time::advance(Duration::from_millis(9)).await;
    assert_eq!(net.deliver(id), Err(NetError::TooEarly));
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(net.deliverable(), [id]);
    assert_eq!(net.next_eligible(), None);
    net.deliver(id).unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_due_datagram_stops_time_until_it_resolves() {
    let net = SimNet::new(fifo(0, 5));
    let a = net.bind(addr(1)).unwrap();
    let _b = net.bind(addr(2)).unwrap();
    send(&a, addr(2), b"x");
    assert!(net.can_advance());
    tokio::time::advance(Duration::from_millis(5)).await;
    // At the deadline time may not move at all, not even by zero, and the
    // datagram is deliverable, so the harness must make progress.
    assert!(!net.can_advance());
    let [id] = net.deliverable()[..] else {
        panic!("the due datagram is deliverable")
    };
    net.deliver(id).unwrap();
    assert!(net.can_advance());
    assert_eq!(net.time_limit(), None);
}

#[tokio::test(start_paused = true)]
async fn zero_delay_and_equal_deadlines_need_delivery_first() {
    let net = SimNet::new(fifo(0, 0));
    let a = net.bind(addr(1)).unwrap();
    let b = net.bind(addr(2)).unwrap();
    send(&a, addr(2), b"one");
    send(&b, addr(1), b"two");
    // Both are due at once, on two links.
    assert!(!net.can_advance());
    assert_eq!(net.deliverable().len(), 2);
    for id in net.deliverable() {
        net.deliver(id).unwrap();
    }
    assert!(net.can_advance());
    assert_eq!(recv(&b).unwrap(), (addr(1), b"one".to_vec()));
    assert_eq!(recv(&a).unwrap(), (addr(2), b"two".to_vec()));
}

/// Sends from several threads on one link get windows in sequence order, so
/// a FIFO link's oldest datagram is always the first to come due.
#[test]
fn concurrent_sends_get_windows_in_sequence_order() {
    let net = SimNet::new(fifo(10, 10));
    let a = Arc::new(net.bind(addr(1)).unwrap());
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let a = a.clone();
            std::thread::spawn(move || {
                for _ in 0..2000 {
                    send(&a, addr(2), b"x");
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    let state = net.lock();
    let windows: Vec<_> = state
        .in_flight
        .values()
        .map(|datagram| (datagram.earliest, datagram.latest))
        .collect();
    assert_eq!(windows.len(), 16_000);
    assert!(
        windows.windows(2).all(|pair| pair[0] <= pair[1]),
        "a later sequence got an earlier window"
    );
}

// M2.3

#[tokio::test(start_paused = true)]
async fn a_fifo_link_delivers_in_send_order() {
    let net = SimNet::new(fifo(0, 10));
    let a = net.bind(addr(1)).unwrap();
    let b = net.bind(addr(2)).unwrap();
    for byte in [1, 2, 3] {
        send(&a, addr(2), &[byte]);
    }
    let ids = net.in_flight();
    assert_eq!(net.deliverable(), [ids[0]]);
    assert_eq!(net.deliver(ids[1]), Err(NetError::NotOldest));
    for id in &ids {
        assert_eq!(net.deliverable(), [*id]);
        net.deliver(*id).unwrap();
    }
    for byte in [1, 2, 3] {
        assert_eq!(recv(&b).unwrap().1, [byte]);
    }
}

#[tokio::test(start_paused = true)]
async fn a_reordering_link_admits_either_order() {
    let net = SimNet::new(LinkModel {
        reorder: true,
        ..fifo(0, 10)
    });
    let a = net.bind(addr(1)).unwrap();
    let b = net.bind(addr(2)).unwrap();
    send(&a, addr(2), b"first");
    send(&a, addr(2), b"second");
    let ids = net.in_flight();
    assert_eq!(net.deliverable(), ids);
    net.deliver(ids[1]).unwrap();
    net.deliver(ids[0]).unwrap();
    assert_eq!(recv(&b).unwrap().1, b"second");
    assert_eq!(recv(&b).unwrap().1, b"first");
}

#[tokio::test(start_paused = true)]
async fn loss_stays_within_each_links_budget() {
    let net = SimNet::new(fifo(0, 10));
    let lossy = Link {
        src: addr(1),
        dst: addr(2),
    };
    net.set_link(
        lossy,
        LinkModel {
            loss_budget: 1,
            ..fifo(0, 10)
        },
    );
    let a = net.bind(addr(1)).unwrap();
    let b = net.bind(addr(2)).unwrap();
    send(&a, addr(2), b"one");
    send(&a, addr(2), b"two");
    send(&b, addr(1), b"back");
    // Only the lossy link's datagrams may drop, and the reverse link has none.
    assert_eq!(net.droppable().len(), 2);
    let ids = net.in_flight();
    // Dropping a later datagram doesn't reorder a FIFO link.
    net.drop_datagram(ids[1]).unwrap();
    assert!(net.droppable().is_empty());
    assert_eq!(net.drop_datagram(ids[0]), Err(NetError::NoLossBudget));
    assert_eq!(net.drop_datagram(ids[2]), Err(NetError::NoLossBudget));
    assert!(net.events().contains(&NetEvent::Dropped {
        id: ids[1],
        reason: DropReason::Loss
    }));
}

/// A window that closes before it opens would leave a due datagram that
/// is still too early, so nothing could happen next.
#[test]
#[should_panic(expected = "min_delay must not exceed max_delay")]
fn a_default_model_with_an_inverted_window_is_refused() {
    SimNet::new(fifo(50, 10));
}

/// See `a_default_model_with_an_inverted_window_is_refused`.
#[test]
#[should_panic(expected = "min_delay must not exceed max_delay")]
fn a_link_model_with_an_inverted_window_is_refused() {
    let net = SimNet::new(fifo(0, 10));
    net.set_link(
        Link {
            src: addr(1),
            dst: addr(2),
        },
        fifo(50, 10),
    );
}

#[test]
#[should_panic(expected = "while datagrams are in flight")]
fn a_link_model_is_fixed_while_datagrams_are_in_flight() {
    let net = SimNet::new(fifo(0, 10));
    let a = net.bind(addr(1)).unwrap();
    send(&a, addr(2), b"x");
    net.set_link(
        Link {
            src: addr(1),
            dst: addr(2),
        },
        fifo(5, 10),
    );
}

// M2.4

#[test]
fn an_empty_read_blocks_and_registers_the_latest_waker() {
    let net = SimNet::new(fifo(0, 10));
    let a = net.bind(addr(1)).unwrap();
    let b = net.bind(addr(2)).unwrap();
    assert!(would_block(recv(&b)));
    let mut readable = pin!(b.readable());
    let (first, first_wakes) = CountingWaker::new();
    let (second, second_wakes) = CountingWaker::new();
    assert!(poll_with(readable.as_mut(), &first).is_pending());
    // A repeated poll from another task replaces the stored waker.
    assert!(poll_with(readable.as_mut(), &second).is_pending());
    assert!(poll_with(readable.as_mut(), &second).is_pending());

    send(&a, addr(2), b"x");
    assert_eq!(second_wakes.wakes(), 0, "a send alone delivers nothing");
    net.deliver(net.in_flight()[0]).unwrap();
    assert_eq!(first_wakes.wakes(), 0);
    assert_eq!(second_wakes.wakes(), 1);
    assert!(matches!(
        poll_with(readable.as_mut(), &second),
        Poll::Ready(Ok(()))
    ));
    assert_eq!(recv(&b).unwrap().1, b"x");
}

#[test]
fn a_delivery_before_the_wait_is_seen_without_a_wake() {
    let net = SimNet::new(fifo(0, 10));
    let a = net.bind(addr(1)).unwrap();
    let b = net.bind(addr(2)).unwrap();
    send(&a, addr(2), b"x");
    net.deliver(net.in_flight()[0]).unwrap();
    let (waker, wakes) = CountingWaker::new();
    assert!(matches!(
        poll_with(pin!(b.readable()), &waker),
        Poll::Ready(Ok(()))
    ));
    assert_eq!(wakes.wakes(), 0);
}

/// A reader on one worker thread races deliveries from this thread. A lost
/// wake would leave the reader parked and trip the timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_racing_delivery_never_loses_a_wake() {
    let net = SimNet::new(fifo(0, 0));
    let a = net.bind(addr(1)).unwrap();
    let b = Arc::new(net.bind(addr(2)).unwrap());
    for round in 0..500u32 {
        let reader = tokio::spawn({
            let b = b.clone();
            async move {
                loop {
                    b.readable().await.unwrap();
                    match recv(&b) {
                        Ok((_, bytes)) => return bytes,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                        Err(error) => panic!("{error}"),
                    }
                }
            }
        });
        if round % 2 == 0 {
            tokio::task::yield_now().await;
        }
        send(&a, addr(2), &round.to_le_bytes());
        net.deliver(net.in_flight()[0]).unwrap();
        let bytes = tokio::time::timeout(Duration::from_secs(5), reader)
            .await
            .expect("the reader is woken")
            .unwrap();
        assert_eq!(bytes, round.to_le_bytes());
    }
}

#[test]
fn a_dropped_socket_discards_its_queue_and_its_waker() {
    let net = SimNet::new(fifo(0, 10));
    let a = net.bind(addr(1)).unwrap();
    let b = net.bind(addr(2)).unwrap();
    send(&a, addr(2), b"unread");
    net.deliver(net.in_flight()[0]).unwrap();
    let (waker, wakes) = CountingWaker::new();
    {
        // Drain, then park a waiter.
        let _ = recv(&b);
        assert!(poll_with(pin!(b.readable()), &waker).is_pending());
    }
    send(&a, addr(2), b"queued");
    net.deliver(net.in_flight()[0]).unwrap();
    assert_eq!(wakes.wakes(), 1);
    drop(b);

    send(&a, addr(2), b"late");
    let [late] = net.in_flight()[..] else {
        panic!("one datagram in flight")
    };
    net.deliver(late).unwrap();
    assert_eq!(wakes.wakes(), 1, "a closed socket's waiter is never woken");
    let events = net.events();
    assert!(events.contains(&NetEvent::Unbound {
        addr: addr(2),
        unread: 1
    }));
    assert_eq!(
        events.last(),
        Some(&NetEvent::Dropped {
            id: late,
            reason: DropReason::Unbound
        })
    );
}

#[test]
fn a_rebound_address_gets_in_flight_datagrams_but_not_the_old_waker() {
    let net = SimNet::new(fifo(0, 10));
    let a = net.bind(addr(1)).unwrap();
    let old = net.bind(addr(2)).unwrap();
    assert_eq!(
        net.bind(addr(2)).unwrap_err().kind(),
        io::ErrorKind::AddrInUse
    );
    let (old_waker, old_wakes) = CountingWaker::new();
    assert!(poll_with(pin!(old.readable()), &old_waker).is_pending());
    send(&a, addr(2), b"across the rebind");
    drop(old);

    let new = net.bind(addr(2)).unwrap();
    let (new_waker, new_wakes) = CountingWaker::new();
    let mut readable = pin!(new.readable());
    assert!(poll_with(readable.as_mut(), &new_waker).is_pending());
    net.deliver(net.in_flight()[0]).unwrap();
    assert_eq!(old_wakes.wakes(), 0);
    assert_eq!(new_wakes.wakes(), 1);
    assert_eq!(
        recv(&new).unwrap(),
        (addr(1), b"across the rebind".to_vec())
    );
}

// M2.5

#[test]
fn a_datagram_to_an_unbound_address_is_dropped_on_arrival() {
    let net = SimNet::new(fifo(0, 10));
    let a = net.bind(addr(1)).unwrap();
    send(&a, addr(9), b"nobody");
    let [id] = net.in_flight()[..] else {
        panic!("one datagram in flight")
    };
    // UDP gives the sender no error.
    net.deliver(id).unwrap();
    assert!(would_block(recv(&a)));
    assert_eq!(
        net.events().last(),
        Some(&NetEvent::Dropped {
            id,
            reason: DropReason::Unbound
        })
    );
}

#[test]
fn a_simulated_socket_binds_only_explicit_addresses() {
    let net = SimNet::new(fifo(0, 10));
    for bad in [
        SocketAddr::from((Ipv4Addr::UNSPECIFIED, 4000)),
        SocketAddr::from((Ipv4Addr::new(10, 0, 0, 1), 0)),
    ] {
        assert_eq!(
            net.bind(bad).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}

#[test]
fn an_injected_receive_failure_wakes_the_reader_once() {
    let net = SimNet::new(fifo(0, 10));
    let b = net.bind(addr(2)).unwrap();
    let (waker, wakes) = CountingWaker::new();
    let mut readable = pin!(b.readable());
    assert!(poll_with(readable.as_mut(), &waker).is_pending());
    assert!(net.fail_next_recv(addr(2), io::ErrorKind::Other));
    assert!(!net.fail_next_recv(addr(9), io::ErrorKind::Other));
    assert_eq!(wakes.wakes(), 1);
    assert!(matches!(
        poll_with(readable.as_mut(), &waker),
        Poll::Ready(Ok(()))
    ));
    assert_eq!(recv(&b).unwrap_err().kind(), io::ErrorKind::Other);
    assert!(would_block(recv(&b)));
}

// M2.6

#[test]
fn simulated_sockets_skip_every_os_feature() {
    let net = SimNet::new(fifo(0, 10));
    let bound = SocketFactory::bind(&net, addr(1)).unwrap();
    assert_eq!(bound.local_addr, addr(1));
    assert_eq!(bound.io.local_addr(), addr(1));
    assert_eq!(bound.buffers, None);
    assert_eq!(bound.inode, None);
    assert!(!bound.io.gso());
    assert_eq!(bound.io.max_transmit_segments(), 1);
    assert!(!bound.io.mtud_allowed());
}

// Generated sequences against a queue model.

/// A small deterministic generator, so a failing seed replays exactly.
struct Rng(u64);

impl Rng {
    /// The next value, from xorshift64*.
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// A value below `n`.
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// One element of a non-empty slice.
    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len() as u64) as usize]
    }
}

/// One datagram in the model.
struct ModelDatagram {
    id: DatagramId,
    bytes: Vec<u8>,
    earliest: Instant,
    latest: Instant,
}

/// The reference model: sends in order, per-address receive queues, and the
/// delivery rules restated plainly.
#[derive(Default)]
struct Model {
    models: BTreeMap<Link, LinkModel>,
    next_seq: BTreeMap<Link, u64>,
    drops: BTreeMap<Link, u64>,
    pending: Vec<ModelDatagram>,
    queues: BTreeMap<SocketAddr, VecDeque<(SocketAddr, Vec<u8>)>>,
    resolved: BTreeSet<DatagramId>,
    last_delivered: BTreeMap<Link, u64>,
}

impl Model {
    /// Records a send at `now` and returns the datagram's identity.
    fn send(
        &mut self,
        src: SocketAddr,
        dst: SocketAddr,
        bytes: Vec<u8>,
        now: Instant,
    ) -> DatagramId {
        let link = Link { src, dst };
        let model = self.models[&link];
        let seq = self.next_seq.entry(link).or_default();
        let id = DatagramId { link, seq: *seq };
        *seq += 1;
        self.pending.push(ModelDatagram {
            id,
            bytes,
            earliest: now + model.min_delay,
            latest: now + model.max_delay,
        });
        id
    }

    /// What delivering `id` at `now` should return.
    fn deliver_result(&self, id: DatagramId, now: Instant) -> Result<(), NetError> {
        let Some(datagram) = self.pending.iter().find(|d| d.id == id) else {
            return Err(NetError::NotInFlight);
        };
        if now < datagram.earliest {
            return Err(NetError::TooEarly);
        }
        let older_pending = self
            .pending
            .iter()
            .any(|d| d.id.link == id.link && d.id.seq < id.seq);
        if !self.models[&id.link].reorder && older_pending {
            return Err(NetError::NotOldest);
        }
        Ok(())
    }

    /// The datagrams deliverable at `now`.
    fn deliverable(&self, now: Instant) -> Vec<DatagramId> {
        let mut ids: Vec<_> = self
            .pending
            .iter()
            .map(|d| d.id)
            .filter(|id| self.deliver_result(*id, now).is_ok())
            .collect();
        ids.sort();
        ids
    }

    /// What dropping `id` should return.
    fn drop_result(&self, id: DatagramId) -> Result<(), NetError> {
        if !self.pending.iter().any(|d| d.id == id) {
            return Err(NetError::NotInFlight);
        }
        if self.drops.get(&id.link).copied().unwrap_or(0) >= self.models[&id.link].loss_budget {
            return Err(NetError::NoLossBudget);
        }
        Ok(())
    }

    /// The datagrams droppable now.
    fn droppable(&self) -> Vec<DatagramId> {
        let mut ids: Vec<_> = self
            .pending
            .iter()
            .map(|d| d.id)
            .filter(|id| self.drop_result(*id).is_ok())
            .collect();
        ids.sort();
        ids
    }

    /// Removes `id` from flight, checking it resolves once.
    fn resolve(&mut self, id: DatagramId) -> ModelDatagram {
        assert!(self.resolved.insert(id), "{id:?} resolved twice");
        let at = self.pending.iter().position(|d| d.id == id).unwrap();
        self.pending.remove(at)
    }

    /// Records a delivery, checking FIFO order.
    fn deliver(&mut self, id: DatagramId) {
        let datagram = self.resolve(id);
        if !self.models[&id.link].reorder {
            if let Some(last) = self.last_delivered.get(&id.link) {
                assert!(*last < id.seq, "FIFO link {:?} reordered", id.link);
            }
        }
        self.last_delivered.insert(id.link, id.seq);
        if let Some(queue) = self.queues.get_mut(&id.link.dst) {
            queue.push_back((id.link.src, datagram.bytes));
        }
    }

    /// Records a loss, checking the budget.
    fn drop_datagram(&mut self, id: DatagramId) {
        self.resolve(id);
        let drops = self.drops.entry(id.link).or_default();
        *drops += 1;
        assert!(*drops <= self.models[&id.link].loss_budget);
    }

    /// Whether time may move forward at `now`.
    fn can_advance(&self, now: Instant) -> bool {
        self.pending.iter().all(|d| now < d.latest)
    }
}

/// Checks that the network and the model agree at this step.
fn assert_agree(net: &SimNet, model: &Model, context: &str) {
    let now = now();
    let pending: Vec<_> = {
        let mut ids: Vec<_> = model.pending.iter().map(|d| d.id).collect();
        ids.sort();
        ids
    };
    assert_eq!(net.in_flight(), pending, "{context}: in flight");
    assert_eq!(
        net.deliverable(),
        model.deliverable(now),
        "{context}: deliverable"
    );
    assert_eq!(net.droppable(), model.droppable(), "{context}: droppable");
    assert_eq!(
        net.can_advance(),
        model.can_advance(now),
        "{context}: can advance"
    );
    // M2.2: time never passed a pending deadline, and a blocked clock
    // always has a delivery to make.
    assert!(
        net.time_limit().is_none_or(|limit| now <= limit),
        "{context}: deadline passed"
    );
    if !net.can_advance() {
        assert!(
            !net.deliverable().is_empty(),
            "{context}: blocked with nothing deliverable"
        );
    }
}

/// One generated run of `steps` operations.
async fn run_sequence(seed: u64, steps: usize) {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let senders = [addr(1), addr(2), addr(3)];
    let targets = [addr(1), addr(2), addr(3), addr(4)];
    let net = SimNet::new(LinkModel::default());
    let mut model = Model::default();
    for src in senders {
        for dst in targets {
            let min = Duration::from_millis(rng.pick(&[0, 1, 5]));
            let link_model = LinkModel {
                min_delay: min,
                max_delay: min + Duration::from_millis(rng.pick(&[0, 0, 2, 10])),
                reorder: rng.below(2) == 0,
                loss_budget: rng.pick(&[0, 1, 2]),
            };
            let link = Link { src, dst };
            net.set_link(link, link_model);
            model.models.insert(link, link_model);
        }
    }
    let mut sockets: Vec<Option<SimSocket>> = senders
        .iter()
        .map(|a| Some(net.bind(*a).unwrap()))
        .collect();
    for a in senders {
        model.queues.insert(a, VecDeque::new());
    }
    let mut sent: Vec<DatagramId> = Vec::new();

    for step in 0..steps {
        let context = format!("seed {seed} step {step}");
        assert_agree(&net, &model, &context);
        let now = now();
        let i = rng.below(3) as usize;
        match rng.below(7) {
            0 | 1 => {
                let Some(socket) = &sockets[i] else { continue };
                let dst = rng.pick(&targets);
                let len = rng.below(17) as usize;
                let bytes: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
                send(socket, dst, &bytes);
                sent.push(model.send(senders[i], dst, bytes, now));
            }
            2 => {
                let ids = net.deliverable();
                if ids.is_empty() {
                    continue;
                }
                let id = rng.pick(&ids);
                net.deliver(id).unwrap();
                model.deliver(id);
            }
            3 => {
                let ids = net.droppable();
                if ids.is_empty() {
                    continue;
                }
                let id = rng.pick(&ids);
                net.drop_datagram(id).unwrap();
                model.drop_datagram(id);
            }
            4 => {
                if !net.can_advance() {
                    continue;
                }
                let room = net
                    .time_limit()
                    .map_or(20, |limit| (limit - now).as_millis() as u64);
                let by = 1 + rng.below(room.max(1));
                tokio::time::advance(Duration::from_millis(by)).await;
            }
            5 => {
                let Some(socket) = &sockets[i] else { continue };
                let expected = model.queues.get_mut(&senders[i]).unwrap().pop_front();
                match (recv(socket), expected) {
                    (Ok(got), Some(expected)) => assert_eq!(got, expected, "{context}"),
                    (Err(error), None) => assert_eq!(error.kind(), io::ErrorKind::WouldBlock),
                    (got, expected) => panic!("{context}: read {got:?}, model {expected:?}"),
                }
            }
            _ => {
                // Close the socket, or bind it again if closed. Unread
                // datagrams go with the old socket.
                if sockets[i].take().is_some() {
                    model.queues.remove(&senders[i]);
                } else {
                    sockets[i] = Some(net.bind(senders[i]).unwrap());
                    model.queues.insert(senders[i], VecDeque::new());
                }
            }
        }
        // Probe one past datagram with operations the model says fail.
        if !sent.is_empty() {
            let id = rng.pick(&sent);
            let expected = model.deliver_result(id, super::now());
            if expected.is_err() {
                assert_eq!(net.deliver(id), expected, "{context}: probe {id:?}");
            }
            let expected = model.drop_result(id);
            if expected.is_err() {
                assert_eq!(net.drop_datagram(id), expected, "{context}: probe {id:?}");
            }
        }
    }

    // Drain: delivery or time always makes progress, so this ends.
    let mut budget = 4 * model.pending.len() + 4;
    while !model.pending.is_empty() {
        assert_agree(&net, &model, &format!("seed {seed} drain"));
        budget = budget.checked_sub(1).expect("draining made no progress");
        if let Some(id) = net.deliverable().first().copied() {
            net.deliver(id).unwrap();
            model.deliver(id);
        } else {
            let next = net.next_eligible().expect("something becomes eligible");
            tokio::time::advance(next - now()).await;
        }
    }
    assert_agree(&net, &model, &format!("seed {seed} end"));

    // M2.1 and M2.3 from the event log: each datagram resolved exactly once,
    // and no link lost more than its budget.
    let mut outcomes: BTreeMap<DatagramId, usize> = BTreeMap::new();
    let mut losses: BTreeMap<Link, u64> = BTreeMap::new();
    for event in net.events() {
        match event {
            NetEvent::Sent { id, .. } => assert!(outcomes.insert(id, 0).is_none()),
            NetEvent::Delivered { id } | NetEvent::Dropped { id, .. } => {
                *outcomes.get_mut(&id).unwrap() += 1;
                if let NetEvent::Dropped {
                    reason: DropReason::Loss,
                    ..
                } = event
                {
                    *losses.entry(id.link).or_default() += 1;
                }
            }
            NetEvent::Bound { .. } | NetEvent::Unbound { .. } => {}
        }
    }
    assert!(outcomes.values().all(|count| *count == 1), "seed {seed}");
    for (link, lost) in losses {
        assert!(lost <= model.models[&link].loss_budget, "seed {seed}");
    }
}

#[tokio::test(start_paused = true)]
async fn generated_sequences_match_a_queue_model() {
    for seed in 0..300 {
        run_sequence(seed, 150).await;
    }
}
