//! The supervisor map's boundary read (M6a-7b): what it lists through a
//! registration, an eviction and removals, when it's held, when it's gone,
//! and how the handler registers it.

use std::sync::Mutex;

use super::*;
use crate::zakura::{
    diag::{ComponentRead, MapEntry, OwnerState, Read, ReadKey},
    TransportEvent, TransportObserver,
};

/// Registers `peer` from `ip` with transcript hash bytes `hash`, and
/// returns its `conn_id`. A smaller hash wins against an incumbent.
async fn register(
    supervisor: &ZakuraSupervisorHandle,
    peer: &ZakuraPeerId,
    hash: u8,
    ip: IpAddr,
) -> ZakuraConnId {
    let (outbound_tx, _outbound_rx) = mpsc::channel(1);
    let registration = supervisor
        .register(
            test_conn_id(),
            peer.clone(),
            Some(ip),
            [hash; TRANSCRIPT_HASH_BYTES],
            ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx),
            CancellationToken::new(),
            ZAKURA_CAP_HEADER_SYNC,
        )
        .await;
    match registration {
        ZakuraRegistration::Registered { conn_id, .. } => conn_id,
        other => panic!("the peer registers: {other:?}"),
    }
}

/// `read`'s entries as `(conn_id, peer, ip)`, or `None` unless it's
/// current.
fn entries(read: &ComponentRead) -> Option<Vec<(ZakuraConnId, ZakuraPeerId, Option<IpAddr>)>> {
    match read() {
        Read::Current(OwnerState::SupervisorMap(entries)) => Some(
            entries
                .into_iter()
                .map(
                    |MapEntry {
                         conn_id,
                         peer,
                         remote_ip,
                     }| (conn_id, peer, remote_ip),
                )
                .collect(),
        ),
        _ => None,
    }
}

/// An IP from the test range.
fn ip(last: u8) -> IpAddr {
    IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, last))
}

/// The read lists each registration by its own `conn_id`: a registration,
/// an eviction by a newer registration of the same peer, which replaces the
/// entry, a `deregister` for the evicted connection, which removes
/// nothing, and the winner's own `deregister`, which empties the map.
///
/// This fails if a read keeps an evicted entry.
#[tokio::test]
async fn the_map_follows_insert_replace_and_removal() {
    let supervisor = ZakuraSupervisorHandle::new(4);
    let read = supervisor.read();
    let (a, b) = (test_peer(1), test_peer(2));
    assert_eq!(entries(&read), Some(Vec::new()));

    let first = register(&supervisor, &a, 5, ip(1)).await;
    let other = register(&supervisor, &b, 5, ip(2)).await;
    assert_eq!(
        entries(&read),
        Some(vec![
            (first, a.clone(), Some(ip(1))),
            (other, b.clone(), Some(ip(2)))
        ])
    );

    let winner = register(&supervisor, &a, 1, ip(3)).await;
    assert_ne!(winner, first, "the eviction is a new registration");
    let evicted = vec![
        (other, b.clone(), Some(ip(2))),
        (winner, a.clone(), Some(ip(3))),
    ];
    assert_eq!(
        entries(&read),
        Some(evicted.clone()),
        "the evicted entry is gone"
    );

    supervisor.deregister(&a, first).await;
    assert_eq!(
        entries(&read),
        Some(evicted),
        "the evicted connection's deregister removes nothing"
    );

    supervisor.deregister(&a, winner).await;
    supervisor.deregister(&b, other).await;
    assert_eq!(entries(&read), Some(Vec::new()));
}

/// With the lock free the read is current, and a task can take the lock
/// right after it. A task holding the supervisor's lock reads `Held`, and
/// so does a lock its holder released to a waiter that hasn't run yet. The
/// read never waits, and never takes the lock from the waiter, which gets
/// it once it runs.
///
/// This fails if the read keeps its guard past the read.
#[tokio::test]
async fn the_map_read_is_held_while_a_task_holds_or_takes_the_lock() {
    let supervisor = ZakuraSupervisorHandle::new(4);
    let read = supervisor.read();
    assert_eq!(entries(&read), Some(Vec::new()), "the lock is free");
    let guard = tokio::time::timeout(Duration::from_secs(5), supervisor.inner.lock())
        .await
        .expect("the read released the lock");
    assert!(matches!(read(), Read::Held { .. }), "a holder");

    let waiter = {
        let supervisor = supervisor.clone();
        tokio::spawn(async move { supervisor.registered_ids().await })
    };
    // The waiter queues behind the guard.
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    drop(guard);
    assert!(
        matches!(read(), Read::Held { .. }),
        "the lock is handed to a waiter that hasn't run"
    );

    let ids = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("the waiter gets the lock")
        .expect("the waiter doesn't panic");
    assert!(ids.is_empty());
    assert_eq!(entries(&read), Some(Vec::new()), "the lock is free");
}

/// The map the handler registers lives as long as any handle to the
/// supervisor's state, such as the one a connection's cleanup guard keeps
/// after its handler is gone, and reads `Gone` once the last one drops.
///
/// This fails if the map ends with the handler.
#[tokio::test]
async fn the_map_outlives_the_handler_while_cleanup_holds_it() {
    let recorder = Arc::new(Recorder::default());
    let supervisor = ZakuraSupervisorHandle::new(4);
    let handler = ZakuraProtocolHandler::new(
        supervisor.clone(),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        ZakuraLocalLimits::from_config(&Config::default()),
    )
    .with_transport_observer(recorder.clone());
    let read = recorder
        .reads
        .lock()
        .expect("no test panics while holding it")
        .pop()
        .expect("the handler registered the map's read");
    let peer = test_peer(1);
    let conn_id = register(&supervisor, &peer, 5, ip(1)).await;
    let cleanup = RegisteredPeerCleanupGuard::new(
        supervisor.clone(),
        handler.registry.clone(),
        peer.clone(),
        conn_id,
        CancellationToken::new(),
    );
    drop(handler);
    drop(supervisor);
    assert_eq!(
        entries(&read),
        Some(vec![(conn_id, peer, Some(ip(1)))]),
        "the cleanup guard keeps the map"
    );

    cleanup.cleanup_registered_peer().await;
    assert_eq!(read(), Read::Gone);
}

/// What an observer was handed, in order.
#[derive(Debug, PartialEq, Eq)]
enum Heard {
    /// A read registered under this key.
    Read(ReadKey),
    /// An event.
    Event(TransportEvent),
}

/// Keeps every read and event it's handed, in order.
#[derive(Default)]
struct Recorder {
    heard: Mutex<Vec<Heard>>,
    reads: Mutex<Vec<ComponentRead>>,
}

impl std::fmt::Debug for Recorder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Recorder")
    }
}

impl TransportObserver for Recorder {
    fn observe(&self, event: TransportEvent) {
        self.heard
            .lock()
            .expect("no test panics while holding it")
            .push(Heard::Event(event));
    }

    fn register_read(&self, key: ReadKey, read: ComponentRead) {
        self.heard
            .lock()
            .expect("no test panics while holding it")
            .push(Heard::Read(key));
        self.reads
            .lock()
            .expect("no test panics while holding it")
            .push(read);
    }
}

/// Installing a transport observer hands it the supervisor map's read,
/// then reports its `ReadRegistered`, before any connection, and the read
/// it took reads the handler's map.
///
/// This fails if a handler takes an observer without registering the read.
#[tokio::test]
async fn the_map_registers_its_read() {
    let recorder = Arc::new(Recorder::default());
    let supervisor = ZakuraSupervisorHandle::new(4);
    let handler = ZakuraProtocolHandler::new(
        supervisor.clone(),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        ZakuraLocalLimits::from_config(&Config::default()),
    )
    .with_transport_observer(recorder.clone());
    assert_eq!(
        *recorder
            .heard
            .lock()
            .expect("no test panics while holding it"),
        [
            Heard::Read(ReadKey::SupervisorMap),
            Heard::Event(TransportEvent::ReadRegistered {
                key: ReadKey::SupervisorMap
            }),
        ]
    );

    let peer = test_peer(1);
    let conn_id = register(&handler.supervisor, &peer, 5, ip(1)).await;
    let reads = recorder
        .reads
        .lock()
        .expect("no test panics while holding it");
    assert_eq!(entries(&reads[0]), Some(vec![(conn_id, peer, Some(ip(1)))]));
}

/// Drops every `RegistryRemoved` it's handed, and records the rest.
#[derive(Debug, Default)]
struct DropsRemovals(Mutex<Vec<TransportEvent>>);

impl TransportObserver for DropsRemovals {
    fn observe(&self, event: TransportEvent) {
        if !matches!(event, TransportEvent::RegistryRemoved { .. }) {
            self.0
                .lock()
                .expect("no test panics while holding it")
                .push(event);
        }
    }
}

/// The transport counts each event before its observer sees it, through
/// the handler's slot and a connection's tap alike, so an observer that
/// drops one leaves the count ahead of what it recorded.
///
/// This fails if an event reaches the observer uncounted.
#[test]
fn the_transport_counts_every_event_before_its_observer() {
    let observer = Arc::new(DropsRemovals::default());
    let emitted = Arc::new(zakura_rt::sync::counter::AtomicU64::new(0));
    let slot = crate::zakura::TransportSlot::counting(observer.clone(), emitted.clone());
    let handler = ZakuraProtocolHandler::new(
        ZakuraSupervisorHandle::new(4),
        Network::Mainnet,
        ZakuraHandshakeConfig::for_network(&Network::Mainnet),
        ZakuraLocalLimits::from_config(&Config::default()),
    )
    .with_transport_slot(slot.clone());
    let tap = crate::zakura::ConnTap::new(slot, std::net::SocketAddr::new(ip(1), 9));
    tap.registered(7);
    tap.emit(TransportEvent::RegistryFanout {
        conn_id: 7,
        negotiated: ZAKURA_CAP_HEADER_SYNC,
        admitted_mask: ZAKURA_CAP_HEADER_SYNC,
    });
    tap.emit(TransportEvent::RegistryRemoved {
        conn_id: 7,
        admitted_mask: ZAKURA_CAP_HEADER_SYNC,
    });
    let recorded = observer
        .0
        .lock()
        .expect("no test panics while holding it")
        .len();
    assert_eq!(
        recorded, 2,
        "the observer kept the registration and the fanout"
    );
    assert_eq!(
        emitted.load(zakura_rt::sync::counter::Ordering::Relaxed),
        3,
        "the dropped removal was counted"
    );
    drop(handler);
}

/// Each `remove_peer` fanout of a connection's cleanup guard is reported
/// through the connection's tap, with the services it reached: the
/// awaited cleanup, the guard's `Drop`, and none for a guard that reached
/// no service.
///
/// This fails if a removal goes unreported or names the wrong services.
#[tokio::test]
async fn the_cleanup_guard_reports_each_removal() {
    let observer = Arc::new(Events::default());
    let supervisor = ZakuraSupervisorHandle::new(4);
    let registry = Arc::new(ServiceRegistry::default());
    let tap = Arc::new(crate::zakura::ConnTap::new(
        observer.clone(),
        std::net::SocketAddr::new(ip(1), 9),
    ));
    let guard = |conn_id, mask| {
        let mut guard = RegisteredPeerCleanupGuard::new(
            supervisor.clone(),
            registry.clone(),
            test_peer(1),
            conn_id,
            CancellationToken::new(),
        );
        guard.observe_with(Some(&tap));
        guard.add_admitted_capabilities(mask);
        guard
    };
    guard(7, ZAKURA_CAP_HEADER_SYNC | 1)
        .cleanup_registered_peer()
        .await;
    drop(guard(8, ZAKURA_CAP_HEADER_SYNC));
    drop(guard(9, 0));
    assert_eq!(
        observer.take(),
        [
            TransportEvent::RegistryRemoved {
                conn_id: 7,
                admitted_mask: ZAKURA_CAP_HEADER_SYNC | 1,
            },
            TransportEvent::RegistryRemoved {
                conn_id: 8,
                admitted_mask: ZAKURA_CAP_HEADER_SYNC,
            },
        ]
    );
}

/// Records every event, in order.
#[derive(Debug, Default)]
struct Events(Mutex<Vec<TransportEvent>>);

impl TransportObserver for Events {
    fn observe(&self, event: TransportEvent) {
        self.0
            .lock()
            .expect("no test panics while holding it")
            .push(event);
    }
}

impl Events {
    /// The events recorded since the last call.
    fn take(&self) -> Vec<TransportEvent> {
        std::mem::take(&mut *self.0.lock().expect("no test panics while holding it"))
    }
}
