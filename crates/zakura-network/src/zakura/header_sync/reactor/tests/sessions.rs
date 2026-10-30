//! Header sessions (M6a-7b): each `HeaderSyncService` on a reactor handle
//! registers its own session table's read, and reports every install,
//! refusal and removal through the session's connection tap as the table
//! changes.

use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Mutex as StdMutex,
};

use super::*;
use crate::zakura::{
    diag::{ComponentRead, OwnerState, Read, ReadKey, SessionEntry},
    header_sync::{
        observe::{HeaderSyncObserver, ObserverSlot},
        service::HeaderSyncService,
        ZAKURA_STREAM_HEADER_SYNC,
    },
    transport::ServiceStream,
    CloseCause, ConnTap, Peer, RemoveCause, Service, ServicePeerDirection, TransportEvent,
    TransportObserver, ZakuraConnId, ZAKURA_CAP_HEADER_SYNC,
};

/// Keeps every boundary read it's handed, by key.
#[derive(Default)]
struct KeepsReads(StdMutex<BTreeMap<ReadKey, ComponentRead>>);

impl std::fmt::Debug for KeepsReads {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("KeepsReads")
    }
}

impl HeaderSyncObserver for KeepsReads {
    fn observe(&self, _: ReactorEvent) {}

    fn register_read(&self, key: ReadKey, read: ComponentRead) {
        let previous = self.0.lock().expect("never poisoned").insert(key, read);
        assert!(previous.is_none(), "each key is registered once");
    }
}

impl KeepsReads {
    /// The read registered under `key`.
    fn read(&self, key: &ReadKey) -> Read<OwnerState> {
        (self.0.lock().expect("never poisoned")[key])()
    }
}

/// Records every transport event, in order.
#[derive(Debug, Default)]
struct Events(StdMutex<Vec<TransportEvent>>);

impl TransportObserver for Events {
    fn observe(&self, event: TransportEvent) {
        self.0.lock().expect("never poisoned").push(event);
    }
}

impl Events {
    /// The session events recorded since the last call.
    fn take(&self) -> Vec<TransportEvent> {
        std::mem::take(&mut *self.0.lock().expect("never poisoned"))
            .into_iter()
            .filter(|event| {
                matches!(
                    event,
                    TransportEvent::Installed { .. }
                        | TransportEvent::InstallRefused { .. }
                        | TransportEvent::Removed { .. }
                        | TransportEvent::RemoveIgnored { .. }
                )
            })
            .collect()
    }
}

/// A connection's tap, registered as `conn_id`, reporting to `events`.
fn tapped(events: &Arc<Events>, conn_id: ZakuraConnId) -> CloseCause {
    let tap = Arc::new(ConnTap::new(
        events.clone(),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9),
    ));
    tap.registered(conn_id);
    let close_cause = CloseCause::new();
    close_cause.observe_with(tap);
    close_cause
}

/// A header-sync peer on connection `conn_id`, with session `conn_id`, a
/// header stream at `version` if there is one, and its connection's tap
/// reporting to `events`. Returns it with the stream's cancel token.
fn header_peer(
    events: &Arc<Events>,
    conn_id: ZakuraConnId,
    version: Option<u16>,
) -> (Peer, CancellationToken) {
    let cancel = CancellationToken::new();
    let streams = version.map(|version| {
        let (_incoming, recv) = framed_channel(8);
        let (send, _outgoing) = framed_channel(8);
        (
            ZAKURA_STREAM_HEADER_SYNC,
            ServiceStream::new(conn_id, version, recv, send, cancel.clone()),
        )
    });
    let peer = Peer::new_with_service_streams(
        conn_id,
        peer(),
        None,
        ZAKURA_CAP_HEADER_SYNC,
        ServicePeerDirection::Outbound,
        streams.into_iter().collect(),
        CancellationToken::new(),
        tapped(events, conn_id),
    );
    (peer, cancel)
}

/// An observed reactor's handle, with what its observer keeps.
fn observed() -> (HeaderSyncHandle, Arc<KeepsReads>, HeaderSyncReactor) {
    let keeps = Arc::new(KeepsReads::default());
    let mut startup = startup(CancellationToken::new());
    startup.observer = ObserverSlot::new(keeps.clone());
    let (handle, _actions, reactor) =
        build_header_sync_reactor(startup).expect("the observed reactor builds");
    (handle, keeps, reactor)
}

/// A table read's sessions as `(conn_id, session_id)`, if it's current.
fn sessions(read: Read<OwnerState>) -> Option<Vec<(ZakuraConnId, u64)>> {
    match read {
        Read::Current(OwnerState::HeaderSessions(entries)) => Some(
            entries
                .iter()
                .map(|entry: &SessionEntry| (entry.conn_id, entry.session_id))
                .collect(),
        ),
        _ => None,
    }
}

/// Lets the session pipes run.
async fn settle() {
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
}

/// Each service on a reactor handle takes the next allocation ID and
/// registers its own table's read under it. Each read reads only its own
/// service's table, with what the session was installed from, and reads
/// `Gone` once its service and every session pipe holding its table are
/// gone.
///
/// This fails if a service skips its registration.
#[tokio::test]
async fn the_table_registers_its_read() {
    let (handle, keeps, _reactor) = observed();
    let first = HeaderSyncService::new(handle.clone());
    let second = HeaderSyncService::new(handle.clone());
    assert_eq!((first.diag_id(), second.diag_id()), (1, 2));
    let (one, two) = (
        ReadKey::HeaderSessions { service: 1 },
        ReadKey::HeaderSessions { service: 2 },
    );
    assert_eq!(sessions(keeps.read(&one)), Some(Vec::new()));
    assert_eq!(sessions(keeps.read(&two)), Some(Vec::new()));

    let events = Arc::new(Events::default());
    let (session, cancel) = header_peer(&events, 7, Some(9));
    second.add_peer(session);
    assert_eq!(sessions(keeps.read(&one)), Some(Vec::new()));
    let Read::Current(OwnerState::HeaderSessions(entries)) = keeps.read(&two) else {
        panic!("the second table reads current");
    };
    assert_eq!(entries.len(), 1);
    let facts = entries[0].installed.expect("installed from its peer");
    assert_eq!(
        (facts.negotiated, facts.stream_version, facts.direction),
        (ZAKURA_CAP_HEADER_SYNC, 9, ServicePeerDirection::Outbound)
    );

    drop(first);
    assert_eq!(keeps.read(&one), Read::Gone);
    cancel.cancel();
    settle().await;
    drop(second);
    assert_eq!(keeps.read(&two), Read::Gone);
}

/// Each change is reported through its session's own connection tap,
/// where it happens: an install; a newer connection's install, which
/// reports the old session's replacement and then its own install inside
/// one `replace`; the replaced session's teardown, which finds the newer
/// session and removes nothing; a stale `remove_peer`, which reports
/// nothing; the current session's `remove_peer`; and a pipe teardown.
///
/// This fails if a removal isn't reported where the table changes.
#[tokio::test]
async fn removals_are_recorded_where_they_happen() {
    let (handle, _keeps, _reactor) = observed();
    let service = HeaderSyncService::new(handle);
    let events = Arc::new(Events::default());
    let installed = |conn_id| TransportEvent::Installed {
        service: 1,
        conn_id,
        session_id: conn_id,
        quic_stream: None,
    };
    let removed = |conn_id, cause| TransportEvent::Removed {
        service: 1,
        conn_id,
        session_id: conn_id,
        cause,
    };

    service.add_peer(header_peer(&events, 7, Some(9)).0);
    assert_eq!(events.take(), [installed(7)]);
    let (session, _keep) = header_peer(&events, 8, Some(9));
    service.add_peer(session);
    assert_eq!(
        events.take(),
        [removed(7, RemoveCause::Replaced), installed(8)],
        "one replace removes the old session, then installs the new one"
    );
    settle().await;
    assert_eq!(
        events.take(),
        [TransportEvent::RemoveIgnored {
            service: 1,
            conn_id: 7,
            session_id: 7,
            cause: RemoveCause::Teardown,
        }],
        "the replaced session's teardown removes nothing"
    );

    service.remove_peer(&peer(), 7);
    assert_eq!(events.take(), []);
    service.remove_peer(&peer(), 8);
    assert_eq!(events.take(), [removed(8, RemoveCause::RemovePeer)]);
    settle().await;
    assert_eq!(
        events.take(),
        [TransportEvent::RemoveIgnored {
            service: 1,
            conn_id: 8,
            session_id: 8,
            cause: RemoveCause::Teardown,
        }],
        "the removed session's own teardown finds it gone"
    );

    let (session, cancel) = header_peer(&events, 9, Some(9));
    service.add_peer(session);
    cancel.cancel();
    settle().await;
    assert_eq!(
        events.take(),
        [installed(9), removed(9, RemoveCause::Teardown)]
    );
}

/// An `add_peer` that installs nothing says why, through the connection's
/// tap: a stream at the wrong version, and a connection older than the
/// one holding the peer.
///
/// This fails if a refusal goes unreported.
#[tokio::test]
async fn refusals_are_recorded_with_their_reason() {
    let (handle, _keeps, _reactor) = observed();
    let service = HeaderSyncService::new(handle);
    let events = Arc::new(Events::default());
    let refused = |conn_id, reason| TransportEvent::InstallRefused {
        service: 1,
        conn_id,
        reason,
    };

    service.add_peer(header_peer(&events, 7, Some(8)).0);
    let (session, _keep) = header_peer(&events, 9, Some(9));
    service.add_peer(session);
    service.add_peer(header_peer(&events, 8, Some(9)).0);
    let mut recorded = events.take();
    assert!(matches!(
        recorded.remove(1),
        TransportEvent::Installed { conn_id: 9, .. }
    ));
    assert_eq!(
        recorded,
        [
            refused(7, "regulation_contract"),
            refused(8, "newer_connection"),
        ]
    );
}
