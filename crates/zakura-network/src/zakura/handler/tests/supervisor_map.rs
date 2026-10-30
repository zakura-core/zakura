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
