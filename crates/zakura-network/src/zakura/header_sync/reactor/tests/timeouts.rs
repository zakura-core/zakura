use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use futures::{FutureExt, StreamExt};
use tokio::sync::Notify;
use zakura_node_services::header_chain as port;

use super::*;
use crate::zakura::header_sync::scheduler::{
    peer_work::HEADER_CHUNK_BUDGET_CAPACITY_V1, repair::MAX_SUPPLIERS_PER_CYCLE,
};

#[derive(Debug)]
struct PendingVctLocalPort {
    prepare_calls: Arc<AtomicUsize>,
    apply_calls: Arc<AtomicUsize>,
    prepare_release: Arc<Notify>,
    apply_release: Arc<Notify>,
    prepare_delay: Option<std::time::Duration>,
    apply_delay: Option<std::time::Duration>,
    apply_succeeds: bool,
}

impl PendingVctLocalPort {
    fn new(apply_succeeds: bool) -> Self {
        Self {
            prepare_calls: Arc::new(AtomicUsize::new(0)),
            apply_calls: Arc::new(AtomicUsize::new(0)),
            prepare_release: Arc::new(Notify::new()),
            apply_release: Arc::new(Notify::new()),
            prepare_delay: None,
            apply_delay: None,
            apply_succeeds,
        }
    }

    fn pending(apply_succeeds: bool) -> Arc<Self> {
        Arc::new(Self::new(apply_succeeds))
    }

    fn with_apply_delay(delay: std::time::Duration) -> Arc<Self> {
        let mut port = Self::new(true);
        port.apply_delay = Some(delay);
        Arc::new(port)
    }

    fn total_calls(&self) -> usize {
        self.prepare_calls.load(Ordering::SeqCst) + self.apply_calls.load(Ordering::SeqCst)
    }
}

impl port::Port for PendingVctLocalPort {
    fn continuation_locator(
        &self,
    ) -> port::HeaderChainFuture<
        '_,
        Result<Option<zakura_header_chain::HeaderLocator>, port::PortError>,
    > {
        Box::pin(async { Ok(None) })
    }

    fn vct_repair_context(
        &self,
        _owner: zakura_header_chain::BodyWorkOwner,
        _height: block::Height,
    ) -> port::HeaderChainFuture<'_, Result<port::VctRepairContextReply, port::PortError>> {
        Box::pin(async { Err(port::PortError::Unavailable { source: None }) })
    }

    fn acquire_header_path(
        &self,
        _request: port::AcquirePath,
    ) -> port::HeaderChainFuture<'_, Result<port::AcquirePathReply, port::PortError>> {
        Box::pin(async { Ok(port::AcquirePathReply::TargetNotRetained) })
    }

    fn read_header_path(
        &self,
        _path: port::RetainedHeaderPath,
        _request: port::ReadPath,
    ) -> port::HeaderChainFuture<'_, Result<port::ReadPathReply, port::PortError>> {
        Box::pin(async { Ok(port::ReadPathReply::Unavailable) })
    }

    fn release_header_path(
        &self,
        _path: port::RetainedHeaderPath,
    ) -> port::HeaderChainFuture<'_, Result<(), port::PortError>> {
        Box::pin(async { Ok(()) })
    }

    fn prepare_header_target(
        &self,
        request: port::PrepareHeaderTarget,
    ) -> port::HeaderChainFuture<'_, port::PrepareHeaderTargetReply> {
        let calls = self.prepare_calls.clone();
        let release = self.prepare_release.clone();
        let delay = self.prepare_delay;
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            match delay {
                Some(delay) => time::sleep(delay).await,
                None => release.notified().await,
            }
            Err(Arc::new(
                zakura_header_chain::HeaderChainError::local_resource(
                    zakura_header_chain::ErrorSubject::Branch(
                        request.owner.header_authority().branch,
                    ),
                    None,
                ),
            ))
        })
    }

    fn apply_header_target(
        &self,
        target: port::PreparedHeaderTarget,
    ) -> port::HeaderChainFuture<'_, port::ApplyHeaderTargetReply> {
        let calls = self.apply_calls.clone();
        let release = self.apply_release.clone();
        let delay = self.apply_delay;
        let succeeds = self.apply_succeeds;
        let owner = target.owner();
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            match delay {
                Some(delay) => time::sleep(delay).await,
                None => release.notified().await,
            }
            if succeeds {
                Ok(port::ApplyHeaderTargetOutcome::Applied)
            } else {
                Err(Arc::new(
                    zakura_header_chain::HeaderChainError::local_resource(
                        zakura_header_chain::ErrorSubject::Branch(owner.header_authority().branch),
                        None,
                    ),
                ))
            }
        })
    }
}

fn seed_vct_active_request(
    reactor: &mut HeaderSyncReactor,
    snapshot: &zakura_header_chain::EngineSnapshot,
    peer: ZakuraPeerId,
    session_id: u64,
    phase: HeaderTargetPhase,
) -> (
    zakura_header_chain::SourceId,
    zakura_header_chain::HeaderSyncWorkOwner,
    zakura_header_chain::VctRepairContext,
) {
    let (source, header_owner, _) =
        seed_applying_request(reactor, snapshot, peer.clone(), session_id);
    let owner = zakura_header_chain::BodyWorkAuthority::for_snapshot(snapshot)
        .bind(header_owner.session_id(), header_owner.request_id());
    let active = reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the fixture has one active request");
    active.owner = owner.into();
    let target = zakura_header_chain::Frontier::new(
        active.target.status.selected_tip_height,
        active.target.status.selected_tip_hash,
    );
    active.purpose = HeaderTargetPurpose::SelectedAuxiliaryRepair {
        selected_target: target,
        repair_generation: 11,
    };
    active.phase = phase;
    let context = zakura_header_chain::VctRepairContext {
        target,
        locator: zakura_header_chain::HeaderLocator::for_continuation(snapshot.frontiers.finalized),
    };
    let mut task = RepairRequirement::new(owner, target.height, 11);
    task.state = RepairPolicyState::Assigned {
        context: context.clone(),
    };
    reactor.vct_repair.insert(task);
    (source, owner.into(), context)
}

fn direct_vct_reactor(
    port: Arc<dyn port::Port>,
) -> (HeaderSyncReactor, zakura_header_chain::EngineSnapshot) {
    let (_handle, reactor, snapshot, _fatal_events) = direct_vct_reactor_with_fatal_events(port);
    (reactor, snapshot)
}

fn direct_vct_reactor_with_fatal_events(
    port: Arc<dyn port::Port>,
) -> (
    HeaderSyncHandle,
    HeaderSyncReactor,
    zakura_header_chain::EngineSnapshot,
    mpsc::UnboundedReceiver<HeaderSyncFatalEvent>,
) {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    startup.header_chain_port = port;
    startup.use_direct_port();
    let (fatal_tx, fatal_rx) = mpsc::unbounded_channel();
    startup.fatal_events = Some(fatal_tx);
    let (handle, _, reactor) =
        build_header_sync_reactor(startup).expect("the direct VCT fixture builds");
    (handle, reactor, snapshot, fatal_rx)
}

fn prepared_vct_target(
    reactor: &HeaderSyncReactor,
    snapshot: &zakura_header_chain::EngineSnapshot,
    peer: &ZakuraPeerId,
    source: zakura_header_chain::SourceId,
    owner: zakura_header_chain::HeaderSyncWorkOwner,
    context: &zakura_header_chain::VctRepairContext,
) -> port::PreparedHeaderTarget {
    let active = reactor
        .peer_work_queue
        .active(peer)
        .expect("the fixture has one active repair");
    let header = active
        .entries
        .first()
        .expect("the exact repair has one header")
        .header
        .clone();
    let anchor = snapshot.frontiers.finalized;
    let lease = zakura_header_chain::ValidationLease::new(
        anchor,
        vec![zakura_header_chain::HeaderContextFact {
            frontier: anchor,
            header: zakura_chain::block::genesis::regtest_genesis_block()
                .header
                .clone(),
        }],
        reactor.startup.network.clone(),
        [9; 32],
    );
    let rules = zakura_header_chain::HeaderRules::for_validation_lease(&lease)
        .expect("the fixture validation lease produces rules");
    let batch = zakura_header_chain::prepare_headers(
        zakura_header_chain::HeaderBatchInput::new(std::slice::from_ref(&header)),
        anchor,
        &rules,
        &zakura_header_chain::SystemClock,
    )
    .expect("the fixture repair header prepares");
    let delivery = zakura_header_chain::AuxDelivery::new(
        zakura_header_chain::EvidenceId::from_digest([0x44; 32]),
        header.hash(),
        source,
        owner,
        zakura_header_chain::BodySizeHint::Unknown,
        None,
    );
    let adapter_key = port::AdapterKey::new();
    port::PreparedHeaderTarget::from_insert(
        &adapter_key,
        Box::new(zakura_header_chain::InsertHeaders {
            owner,
            source,
            parent_hash: anchor.hash,
            target_tip_hash: context.target.hash,
            completion: zakura_header_chain::TargetCompletion::SelectedAuxiliaryRepair {
                common_ancestor: anchor,
                selected_target: context.target,
            },
            batch,
            aux: vec![delivery],
        }),
    )
}

struct VctLocalFixture {
    _handle: HeaderSyncHandle,
    port: Arc<PendingVctLocalPort>,
    reactor: HeaderSyncReactor,
    snapshot: zakura_header_chain::EngineSnapshot,
    fatal_events: mpsc::UnboundedReceiver<HeaderSyncFatalEvent>,
    peer: ZakuraPeerId,
    source: zakura_header_chain::SourceId,
    owner: zakura_header_chain::HeaderSyncWorkOwner,
    context: zakura_header_chain::VctRepairContext,
    phase: HeaderTargetPhase,
}

impl VctLocalFixture {
    fn new(port: Arc<PendingVctLocalPort>, phase: HeaderTargetPhase) -> Self {
        let (handle, mut reactor, snapshot, fatal_events) =
            direct_vct_reactor_with_fatal_events(port.clone());
        let peer = peer();
        let (source, owner, context) =
            seed_vct_active_request(&mut reactor, &snapshot, peer.clone(), 7, phase);
        Self {
            _handle: handle,
            port,
            reactor,
            snapshot,
            fatal_events,
            peer,
            source,
            owner,
            context,
            phase,
        }
    }

    fn operation(&self) -> HeaderPortOperation {
        let active = self
            .reactor
            .peer_work_queue
            .active(&self.peer)
            .expect("the fixture has one active repair");
        match self.phase {
            HeaderTargetPhase::Preparing => HeaderPortOperation::PrepareHeaderTarget {
                purpose: active.purpose.clone(),
                peer: self.peer.clone(),
                source: self.source,
                owner: self.owner,
                common_ancestor: self.snapshot.frontiers.finalized,
                target: self.context.target,
                completion: zakura_header_chain::TargetCompletion::SelectedAuxiliaryRepair {
                    common_ancestor: self.snapshot.frontiers.finalized,
                    selected_target: self.context.target,
                },
                entries: active.entries.clone(),
            },
            HeaderTargetPhase::Applying => HeaderPortOperation::ApplyHeaderTarget {
                purpose: active.purpose.clone(),
                peer: self.peer.clone(),
                source: self.source,
                owner: self.owner,
                target: prepared_vct_target(
                    &self.reactor,
                    &self.snapshot,
                    &self.peer,
                    self.source,
                    self.owner,
                    &self.context,
                ),
            },
            HeaderTargetPhase::Receiving => {
                unreachable!("the fixture covers local operation phases")
            }
        }
    }

    fn dispatch(&mut self) {
        let operation = self.operation();
        assert!(self.reactor.dispatch_action(operation));
    }

    fn assert_single_operation_through_timeouts(&mut self) {
        let mut deadline = Instant::now();
        self.reactor
            .request_deadlines
            .insert(self.peer.clone(), deadline);
        for _ in 0..3 {
            self.reactor.retire_timed_out_requests(deadline);
            poll_pending_operation(&mut self.reactor);
            assert_eq!(self.reactor.pending_port_operations.len(), 1);
            assert_eq!(
                self.port.prepare_calls.load(Ordering::SeqCst),
                usize::from(self.phase == HeaderTargetPhase::Preparing)
            );
            assert_eq!(
                self.port.apply_calls.load(Ordering::SeqCst),
                usize::from(self.phase == HeaderTargetPhase::Applying)
            );
            assert_eq!(
                self.reactor
                    .peer_work_queue
                    .active(&self.peer)
                    .map(|active| active.phase),
                Some(self.phase)
            );
            deadline = self.reactor.request_deadlines[&self.peer];
        }
    }

    async fn release_and_complete(&mut self) {
        match self.phase {
            HeaderTargetPhase::Preparing => self.port.prepare_release.notify_one(),
            HeaderTargetPhase::Applying => self.port.apply_release.notify_one(),
            HeaderTargetPhase::Receiving => unreachable!("receiving is not a local operation"),
        }
        let completion = self
            .reactor
            .pending_port_operations
            .next()
            .await
            .expect("the owned local operation completes");
        self.reactor.handle_port_completion(completion);
    }
}

fn phase_label(phase: HeaderTargetPhase) -> &'static str {
    match phase {
        HeaderTargetPhase::Preparing => "prepare",
        HeaderTargetPhase::Applying => "apply",
        HeaderTargetPhase::Receiving => unreachable!("receiving is not a local operation"),
    }
}

struct ReadyVctRepairFixture {
    _handle: HeaderSyncHandle,
    reactor: HeaderSyncReactor,
    snapshot: zakura_header_chain::EngineSnapshot,
    anchor: zakura_header_chain::Frontier,
    target: zakura_header_chain::Frontier,
    owner: zakura_header_chain::BodyWorkOwner,
    context: zakura_header_chain::VctRepairContext,
}

impl ReadyVctRepairFixture {
    fn new() -> Self {
        let mut startup = startup(CancellationToken::new());
        let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
        let mut snapshot = committed_snapshot(anchor);
        let target = zakura_header_chain::Frontier::new(block::Height(1), block::Hash([0x41; 32]));
        snapshot.frontiers.header_best =
            zakura_header_chain::Frontier::new(block::Height(2), block::Hash([0x42; 32]));
        let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
        startup.committed_snapshots = Some(snapshots_rx);
        let (handle, _actions, reactor) =
            build_header_sync_reactor(startup).expect("the VCT repair fixture builds");
        let owner = zakura_header_chain::BodyWorkAuthority::for_snapshot(&snapshot).bind(
            INTERNAL_VCT_REPAIR_SESSION_ID,
            std::num::NonZeroU64::new(1).expect("one is nonzero"),
        );
        let context = zakura_header_chain::VctRepairContext {
            target,
            locator: zakura_header_chain::HeaderLocator::for_continuation(anchor),
        };
        Self {
            _handle: handle,
            reactor,
            snapshot,
            anchor,
            target,
            owner,
            context,
        }
    }

    fn schedule(&mut self) {
        let mut repair = RepairRequirement::new(self.owner, self.target.height, 11);
        repair.state = RepairPolicyState::Ready {
            context: self.context.clone(),
        };
        self.reactor.vct_repair.insert(repair);
    }

    fn status(&self) -> Status {
        Status {
            work_anchor_height: self.anchor.height,
            work_anchor_hash: self.anchor.hash,
            selected_tip_height: self.snapshot.frontiers.header_best.height,
            selected_tip_hash: self.snapshot.frontiers.header_best.hash,
            suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(2_u8),
            oldest_retained_height: self.anchor.height,
            max_headers_per_response: 1,
            max_inflight_requests: 1,
            max_message_bytes: 2_000_000,
            tree_aux_schema_mask: AuxSchema::V1.mask_bit(),
        }
    }

    fn connect(
        &mut self,
        markers: &[u8],
        first_session_id: u64,
    ) -> (Vec<ZakuraPeerId>, Vec<crate::zakura::FramedRecv>) {
        let peers: Vec<_> = markers
            .iter()
            .map(|marker| {
                ZakuraPeerId::new(vec![*marker; 32]).expect("the peer ID has the required length")
            })
            .collect();
        let outbounds = peers
            .iter()
            .enumerate()
            .map(|(index, peer)| {
                let session_id =
                    first_session_id + u64::try_from(index).expect("the peer index fits in u64");
                let (send, outbound) = framed_channel(8);
                self.reactor
                    .handle_peer_connected(PeerSession::from_parts_with_session_id(
                        peer.clone(),
                        session_id,
                        send,
                        CancellationToken::new(),
                    ));
                outbound
            })
            .collect();
        (peers, outbounds)
    }

    fn advertise(&mut self, peers: &[ZakuraPeerId], first_session_id: u64) {
        let status = self.status();
        for (index, peer) in peers.iter().enumerate() {
            self.reactor.handle_wire_message(
                peer.clone(),
                first_session_id + u64::try_from(index).expect("the peer index fits in u64"),
                HeaderSyncMessage::Status(status.clone()),
            );
        }
    }
}

fn poll_pending_operation(reactor: &mut HeaderSyncReactor) {
    assert!(
        reactor
            .pending_port_operations
            .next()
            .now_or_never()
            .is_none(),
        "the local operation remains pending"
    );
}

#[tokio::test(start_paused = true)]
async fn pending_vct_prepare_and_apply_emit_one_fatal_event_at_thirty_minutes() {
    for phase in [HeaderTargetPhase::Preparing, HeaderTargetPhase::Applying] {
        let mut fixture = VctLocalFixture::new(PendingVctLocalPort::pending(true), phase);
        fixture.dispatch();
        poll_pending_operation(&mut fixture.reactor);
        fixture.reactor.request_deadlines.insert(
            fixture.peer.clone(),
            Instant::now() + fixture.reactor.startup.request_timeout,
        );

        for diagnostic in 1..60 {
            time::advance(fixture.reactor.startup.request_timeout).await;
            let now = Instant::now();
            fixture.reactor.retire_timed_out_requests(now);
            assert!(!fixture.reactor.report_fatal_vct_local_operation(now));
            assert!(matches!(
                fixture.fatal_events.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            assert_eq!(fixture.reactor.pending_port_operations.len(), 1);
            assert_eq!(
                fixture.port.total_calls(),
                1,
                "diagnostic {diagnostic} must not duplicate the state operation",
            );
        }

        time::advance(fixture.reactor.startup.request_timeout).await;
        let now = Instant::now();
        fixture.reactor.retire_timed_out_requests(now);
        assert!(fixture.reactor.report_fatal_vct_local_operation(now));
        let fatal = fixture
            .fatal_events
            .try_recv()
            .expect("the hard deadline emits one fatal event");
        assert_eq!(fatal.phase, phase_label(phase));
        assert_eq!(fatal.owner, fixture.owner);
        assert_eq!(fatal.repair_generation, 11);
        assert_eq!(fatal.target, fixture.context.target);
        assert_eq!(fatal.elapsed, VCT_LOCAL_OPERATION_FATAL_AFTER);
        assert_eq!(fixture.reactor.pending_port_operations.len(), 1);
        assert!(fixture.reactor.report_fatal_vct_local_operation(now));
        assert!(matches!(
            fixture.fatal_events.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn reactor_run_wakes_for_each_vct_local_operation_hard_deadline() {
    for phase in [HeaderTargetPhase::Preparing, HeaderTargetPhase::Applying] {
        let mut fixture = VctLocalFixture::new(PendingVctLocalPort::pending(true), phase);
        fixture.dispatch();
        let shutdown = fixture.reactor.startup.shutdown.clone();
        let owner = fixture.owner;
        let port = fixture.port.clone();
        let mut fatal_events = fixture.fatal_events;
        let task = tokio::spawn(fixture.reactor.run());
        tokio::task::yield_now().await;
        assert_eq!(port.total_calls(), 1);

        time::advance(VCT_LOCAL_OPERATION_FATAL_AFTER - std::time::Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert!(matches!(
            fatal_events.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(!task.is_finished());

        time::advance(std::time::Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        let fatal = fatal_events
            .try_recv()
            .expect("the running reactor emits the deadline event");
        assert_eq!(fatal.owner, owner);
        assert_eq!(fatal.phase, phase_label(phase));
        assert_eq!(port.total_calls(), 1);
        assert!(!task.is_finished(), "the pending operation remains owned");

        shutdown.cancel();
        task.await
            .expect("the fatal reactor stops through normal shutdown");
    }
}

#[tokio::test(start_paused = true)]
async fn vct_apply_completion_wins_at_the_hard_deadline() {
    let port = PendingVctLocalPort::with_apply_delay(VCT_LOCAL_OPERATION_FATAL_AFTER);
    let mut fixture = VctLocalFixture::new(port.clone(), HeaderTargetPhase::Applying);
    fixture.dispatch();
    let shutdown = fixture.reactor.startup.shutdown.clone();
    let mut fatal_events = fixture.fatal_events;
    let task = tokio::spawn(fixture.reactor.run());
    tokio::task::yield_now().await;
    assert_eq!(port.apply_calls.load(Ordering::SeqCst), 1);

    time::advance(VCT_LOCAL_OPERATION_FATAL_AFTER).await;
    tokio::task::yield_now().await;

    let fatal_result = fatal_events.try_recv();
    assert!(
        matches!(fatal_result, Err(mpsc::error::TryRecvError::Empty)),
        "completion at the deadline emitted an unexpected event: {fatal_result:?}",
    );
    assert!(
        !task.is_finished(),
        "completion must not terminate the reactor"
    );
    assert_eq!(port.apply_calls.load(Ordering::SeqCst), 1);
    shutdown.cancel();
    task.await
        .expect("the reactor stops through normal shutdown");
}

#[test]
fn normal_header_operations_have_no_vct_fatal_deadline() {
    let port = PendingVctLocalPort::pending(true);
    let (mut reactor, snapshot) = direct_vct_reactor(port);
    let peer = peer();
    let (source, owner, context) = seed_vct_active_request(
        &mut reactor,
        &snapshot,
        peer.clone(),
        7,
        HeaderTargetPhase::Preparing,
    );
    let active = reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the fixture has one preparing target");
    active.purpose = HeaderTargetPurpose::Normal;
    let action = HeaderPortOperation::PrepareHeaderTarget {
        purpose: HeaderTargetPurpose::Normal,
        peer,
        source,
        owner,
        common_ancestor: snapshot.frontiers.finalized,
        target: context.target,
        completion: zakura_header_chain::TargetCompletion::TargetComplete {
            common_ancestor: snapshot.frontiers.finalized,
        },
        entries: active.entries.clone(),
    };

    assert!(reactor.dispatch_action(action));
    assert!(reactor.vct_local_operation.is_none());
}

#[test]
fn request_timeout_retires_owned_work_and_wakes_maintenance() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_handle, mut actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the timeout fixture builds");
    let peer = peer();
    seed_applying_request(&mut reactor, &snapshot, peer.clone(), 7);
    let deadline = Instant::now();
    reactor.request_deadlines.insert(peer.clone(), deadline);

    assert!(reactor.next_maintenance_deadline() <= deadline);
    reactor.retire_timed_out_requests(deadline);

    assert!(reactor.peer_work_queue.active(&peer).is_none());
    assert!(!reactor.request_deadlines.contains_key(&peer));
    assert!(actions.try_recv().is_err());
}

#[test]
fn vct_request_timeout_keeps_required_work_and_rotates_the_supplier() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_handle, _actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the timeout fixture builds");
    let peer = peer();
    let (source, owner, _) = seed_applying_request(&mut reactor, &snapshot, peer.clone(), 7);
    let owner = zakura_header_chain::BodyWorkAuthority::for_snapshot(&snapshot)
        .bind(owner.session_id(), owner.request_id());
    reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the fixture has one applying request")
        .owner = owner.into();
    let repair_status = &reactor
        .peer_work_queue
        .active(&peer)
        .expect("the fixture has one applying request")
        .target
        .status;
    let target = zakura_header_chain::Frontier::new(
        repair_status.selected_tip_height,
        repair_status.selected_tip_hash,
    );
    let mut task = RepairRequirement::new(owner, target.height, 11);
    let deadline = Instant::now();
    let context = zakura_header_chain::VctRepairContext {
        target,
        locator: zakura_header_chain::HeaderLocator::for_continuation(anchor),
    };
    task.state = RepairPolicyState::Assigned {
        context: context.clone(),
    };
    reactor.vct_repair.insert(task);
    reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the fixture has one applying request")
        .purpose = HeaderTargetPurpose::SelectedAuxiliaryRepair {
        selected_target: target,
        repair_generation: 11,
    };
    reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the fixture has one repair request")
        .phase = HeaderTargetPhase::Receiving;
    reactor.request_deadlines.insert(peer, deadline);

    assert!(reactor.next_maintenance_deadline() <= deadline);
    reactor.retire_timed_out_requests(deadline);

    let task = reactor
        .vct_repair
        .current()
        .expect("a timeout cannot discard a current repair requirement");
    assert!(matches!(
        &task.state,
        RepairPolicyState::SupplierBackoff {
            context: retained,
            retry_at,
        } if retained == &context && *retry_at > deadline
    ));
    assert_eq!(task.attempts, 1);
    assert!(task.tried_sources.contains(&source));
    assert!(task.next_deadline().is_some());
}

#[test]
fn vct_local_phase_deadlines_preserve_operation_ownership() {
    for phase in [HeaderTargetPhase::Preparing, HeaderTargetPhase::Applying] {
        let mut startup = startup(CancellationToken::new());
        let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
        let snapshot = committed_snapshot(anchor);
        let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
        startup.committed_snapshots = Some(snapshots_rx);
        let (_handle, _actions, mut reactor) =
            build_header_sync_reactor(startup).expect("the local timeout fixture builds");
        let peer = peer();
        let (source, _owner, context) =
            seed_vct_active_request(&mut reactor, &snapshot, peer.clone(), 7, phase);
        let prior = zakura_header_chain::SourceId::from_digest([0x22; 32]);
        let task = reactor
            .vct_repair
            .current_mut()
            .expect("the fixture has one repair task");
        task.tried_sources.insert(prior);
        task.attempts = 1;
        let deadline = Instant::now();
        reactor.request_deadlines.insert(peer.clone(), deadline);

        reactor.retire_timed_out_requests(deadline);

        let task = reactor
            .vct_repair
            .current()
            .expect("a local operation deadline keeps the current repair");
        assert_eq!(
            task.state,
            RepairPolicyState::Assigned {
                context: context.clone()
            }
        );
        assert_eq!(task.tried_sources, [prior].into_iter().collect());
        assert!(!task.tried_sources.contains(&source));
        assert_eq!(task.attempts, 1);
        assert_eq!(
            reactor
                .peer_work_queue
                .active(&peer)
                .map(|active| active.phase),
            Some(phase)
        );
        assert!(reactor.request_deadlines[&peer] > deadline);
        assert_eq!(
            reactor
                .vct_repair_stall
                .expect("the local operation starts the generation stall clock")
                .outcome,
            VctRepairStallOutcome::LocalOperationPending
        );
    }
}

#[tokio::test]
async fn pending_vct_prepare_remains_single_until_its_failure_completes() {
    let port = PendingVctLocalPort::pending(false);
    let mut fixture = VctLocalFixture::new(port.clone(), HeaderTargetPhase::Preparing);
    fixture.dispatch();
    poll_pending_operation(&mut fixture.reactor);
    assert_eq!(port.prepare_calls.load(Ordering::SeqCst), 1);
    fixture.assert_single_operation_through_timeouts();
    fixture.release_and_complete().await;

    assert!(fixture.reactor.pending_port_operations.is_empty());
    assert!(fixture
        .reactor
        .peer_work_queue
        .active(&fixture.peer)
        .is_none());
    let task = fixture
        .reactor
        .vct_repair
        .current()
        .expect("the attributed local failure keeps the repair");
    assert!(matches!(
        &task.state,
        RepairPolicyState::LocalBackoff {
            context: retained,
            ..
        } if retained == &fixture.context
    ));
    assert_eq!(task.attempts, 1);
}

#[tokio::test]
async fn pending_vct_apply_remains_single_until_success() {
    let port = PendingVctLocalPort::pending(true);
    let mut fixture = VctLocalFixture::new(port.clone(), HeaderTargetPhase::Applying);
    fixture.dispatch();
    poll_pending_operation(&mut fixture.reactor);
    assert_eq!(port.apply_calls.load(Ordering::SeqCst), 1);
    fixture.assert_single_operation_through_timeouts();
    fixture.release_and_complete().await;

    assert!(fixture.reactor.pending_port_operations.is_empty());
    assert!(fixture
        .reactor
        .peer_work_queue
        .active(&fixture.peer)
        .is_none());
    assert!(!fixture
        .reactor
        .request_deadlines
        .contains_key(&fixture.peer));
    assert_eq!(
        fixture.reactor.vct_repair.current().map(|task| &task.state),
        Some(&RepairPolicyState::Completed)
    );
}

#[tokio::test]
async fn obsolete_vct_generation_retires_ownership_and_ignores_late_apply() {
    let port = PendingVctLocalPort::pending(true);
    let mut fixture = VctLocalFixture::new(port.clone(), HeaderTargetPhase::Applying);
    fixture.dispatch();
    poll_pending_operation(&mut fixture.reactor);
    assert_eq!(port.apply_calls.load(Ordering::SeqCst), 1);

    let mut changed = fixture.snapshot.clone();
    changed.header_generation = changed
        .header_generation
        .checked_next()
        .expect("the fixture generation advances");
    fixture.reactor.observe_latest_committed_snapshot(changed);
    assert!(fixture
        .reactor
        .peer_work_queue
        .active(&fixture.peer)
        .is_none());
    assert!(fixture.reactor.vct_repair.current().is_none());
    assert_eq!(fixture.reactor.pending_port_operations.len(), 1);

    port.apply_release.notify_one();
    let completion = fixture
        .reactor
        .pending_port_operations
        .next()
        .await
        .expect("the obsolete application eventually completes");
    fixture.reactor.handle_port_completion(completion);

    assert!(fixture.reactor.pending_port_operations.is_empty());
    assert!(fixture
        .reactor
        .peer_work_queue
        .active(&fixture.peer)
        .is_none());
    assert!(fixture.reactor.vct_repair.current().is_none());
    assert_eq!(port.apply_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn vct_admission_failures_preserve_retry_policy_state() {
    for supplier_attributed in [false, true] {
        let mut startup = startup(CancellationToken::new());
        let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
        let snapshot = committed_snapshot(anchor);
        let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
        startup.committed_snapshots = Some(snapshots_rx);
        let (_handle, _actions, mut reactor) =
            build_header_sync_reactor(startup).expect("the admission failure fixture builds");
        let peer = peer();
        let (source, owner, context) = seed_vct_active_request(
            &mut reactor,
            &snapshot,
            peer.clone(),
            7,
            HeaderTargetPhase::Applying,
        );
        let prior = zakura_header_chain::SourceId::from_digest([0x22; 32]);
        let task = reactor
            .vct_repair
            .current_mut()
            .expect("the fixture has one repair task");
        task.tried_sources.insert(prior);
        task.attempts = 1;
        let error = if supplier_attributed {
            invalid_header_failure(source, owner)
        } else {
            local_failure(owner)
        };

        reactor.handle_header_target_admission_ready(
            peer,
            source,
            owner,
            HeaderTargetAdmissionResult::Failed(error),
        );

        let task = reactor
            .vct_repair
            .current()
            .expect("an admission failure cannot recreate the repair task");
        assert_eq!(task.attempts, 2);
        assert!(task.tried_sources.contains(&prior));
        if supplier_attributed {
            assert!(task.tried_sources.contains(&source));
            assert!(matches!(
                &task.state,
                RepairPolicyState::SupplierBackoff {
                    context: retained,
                    ..
                } if retained == &context
            ));
            assert_eq!(
                reactor
                    .vct_repair_stall
                    .expect("the supplier failure keeps generation evidence")
                    .outcome,
                VctRepairStallOutcome::NoEligibleSupplier
            );
        } else {
            assert!(!task.tried_sources.contains(&source));
            assert!(matches!(
                &task.state,
                RepairPolicyState::LocalBackoff {
                    context: retained,
                    ..
                } if retained == &context
            ));
            assert_eq!(
                reactor
                    .vct_repair_stall
                    .expect("the local failure keeps generation evidence")
                    .outcome,
                VctRepairStallOutcome::LocalFailure
            );
        }
    }
}

#[test]
fn vct_send_failures_have_explicit_attribution() {
    assert_eq!(
        vct_send_retry_attribution(&OrderedSendError::Full),
        VctRepairRetryAttribution::Supplier
    );
    assert_eq!(
        vct_send_retry_attribution(&OrderedSendError::Closed),
        VctRepairRetryAttribution::Supplier
    );
    assert_eq!(
        vct_send_retry_attribution(&OrderedSendError::Encode("fixture failure".into())),
        VctRepairRetryAttribution::Local
    );
}

#[test]
fn resource_stall_has_an_exact_terminal_label() {
    assert_eq!(
        HeaderRequestTerminal::ResourceStalled.label(),
        "resource_stalled"
    );
}

#[test]
fn generation_stall_escalation_owns_a_maintenance_deadline() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let mut snapshot = committed_snapshot(anchor);
    let repair_target =
        zakura_header_chain::Frontier::new(block::Height(1), block::Hash([0x41; 32]));
    snapshot.frontiers.header_best =
        zakura_header_chain::Frontier::new(block::Height(2), block::Hash([0x42; 32]));
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_handle, _actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the escalation fixture builds");
    let owner = zakura_header_chain::BodyWorkAuthority::for_snapshot(&snapshot).bind(
        INTERNAL_VCT_REPAIR_SESSION_ID,
        std::num::NonZeroU64::new(1).expect("one is nonzero"),
    );
    let context = zakura_header_chain::VctRepairContext {
        target: repair_target,
        locator: zakura_header_chain::HeaderLocator::for_continuation(anchor),
    };
    let mut task = RepairRequirement::new(owner, repair_target.height, 11);
    task.state = RepairPolicyState::LocalBackoff {
        context,
        retry_at: Instant::now() + std::time::Duration::from_secs(120),
    };
    reactor.vct_repair.insert(task.clone());
    let now = Instant::now();

    reactor.note_vct_repair_stall(
        &task,
        anchor,
        VctSupplierRejections::default(),
        VctRepairStallOutcome::LocalFailure,
        now,
    );

    let stall = reactor
        .vct_repair_stall
        .expect("the failed generation owns escalation state");
    assert_eq!(stall.last_trace, Some(now));
    assert_eq!(
        reactor.next_maintenance_deadline(),
        now + VCT_REPAIR_STALL_TRACE_INTERVAL
    );
    let report_at = now + VCT_REPAIR_STALL_REPORT_AFTER;
    reactor.refresh_vct_repair_stall(report_at);
    let stall = reactor
        .vct_repair_stall
        .expect("reporting keeps sampled generation state");
    assert!(stall.reported);
    assert_eq!(stall.outcome, VctRepairStallOutcome::LocalFailure);
}

#[test]
fn initial_vct_wire_assignment_arms_the_request_deadline() {
    let mut fixture = ReadyVctRepairFixture::new();
    let (peers, _outbounds) = fixture.connect(&[0x71], 7);
    let peer = &peers[0];
    fixture.schedule();
    let before = Instant::now();
    fixture.advertise(&peers, 7);

    assert!(matches!(
        fixture
            .reactor
            .peer_work_queue
            .active(peer)
            .map(|active| &active.purpose),
        Some(HeaderTargetPurpose::SelectedAuxiliaryRepair { .. })
    ));
    let deadline = fixture
        .reactor
        .request_deadlines
        .get(peer)
        .copied()
        .expect("the exact repair wire request owns a deadline");
    assert!(deadline >= before + fixture.reactor.startup.request_timeout);
}

#[test]
fn local_capacity_backoff_starts_the_generation_stall_clock() {
    let mut fixture = ReadyVctRepairFixture::new();
    let (peers, _outbounds) = fixture.connect(&[0x71], 7);
    fixture.schedule();
    let capacity_owner = ZakuraPeerId::new(vec![0x55; 32]).expect("the peer ID is bounded");
    fixture.reactor.peer_work_queue.set_capacity_for_test(
        &capacity_owner,
        HEADER_CHUNK_BUDGET_CAPACITY_V1,
        0,
    );
    fixture.advertise(&peers, 7);

    let task = fixture
        .reactor
        .vct_repair
        .current()
        .expect("local capacity backoff keeps the repair");
    assert!(matches!(
        &task.state,
        RepairPolicyState::SupplierBackoff {
            context: retained,
            ..
        } if retained == &fixture.context
    ));
    assert_eq!(task.attempts, 0);
    assert!(task.tried_sources.is_empty());
    assert_eq!(
        fixture
            .reactor
            .vct_repair_stall
            .expect("local capacity starts the generation stall clock")
            .outcome,
        VctRepairStallOutcome::LocalCapacityUnavailable
    );
}

#[test]
fn bounded_supplier_cycles_rotate_to_the_fourth_supplier() {
    let mut fixture = ReadyVctRepairFixture::new();
    let (peers, _outbounds) = fixture.connect(&[1, 2, 3, 4], 7);
    fixture.schedule();
    fixture.advertise(&peers, 7);

    for peer in &peers[..3] {
        let active = fixture
            .reactor
            .peer_work_queue
            .active(peer)
            .expect("the next round-robin supplier owns the repair")
            .clone();
        assert!(matches!(
            active.purpose,
            HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
        ));
        fixture.reactor.handle_headers_outcome(
            peer.clone(),
            active.owner.session_id(),
            active.owner.header_authority(),
            HeadersOutcome {
                request_id: active.request_id.get(),
                target_tip_hash: fixture.target.hash,
                outcome: HeadersOutcomeCode::TargetNotRetained,
            },
        );
    }

    assert!(peers
        .iter()
        .all(|peer| fixture.reactor.peer_work_queue.active(peer).is_none()));
    let task = fixture
        .reactor
        .vct_repair
        .current()
        .expect("the bounded cycle keeps the repair requirement");
    let retry_at = match &task.state {
        RepairPolicyState::SupplierBackoff {
            context: retained,
            retry_at,
        } if retained == &fixture.context => *retry_at,
        other => panic!("three supplier failures must back off, got {other:?}"),
    };
    assert_eq!(task.tried_sources.len(), 3);
    assert!(task.supplier_cycle_exhausted());
    assert_eq!(
        fixture.reactor.vct_supplier_order,
        [
            peers[3].clone(),
            peers[0].clone(),
            peers[1].clone(),
            peers[2].clone(),
        ]
        .into_iter()
        .collect::<VecDeque<_>>()
    );
    let stall = fixture
        .reactor
        .vct_repair_stall
        .expect("a complete failed cycle starts the generation stall clock");

    fixture
        .reactor
        .vct_repair
        .current_mut()
        .expect("the repair remains scheduled")
        .resume_retry_cycle(retry_at);
    fixture.reactor.try_assign_vct_repair();

    let active = fixture
        .reactor
        .peer_work_queue
        .active(&peers[3])
        .expect("the next cycle starts after the persistent supplier cursor");
    assert!(matches!(
        active.purpose,
        HeaderTargetPurpose::SelectedAuxiliaryRepair { .. }
    ));
    let task = fixture
        .reactor
        .vct_repair
        .current()
        .expect("the fourth supplier owns the current repair");
    assert!(task.tried_sources.is_empty());
    assert_eq!(fixture.reactor.vct_supplier_order.front(), Some(&peers[3]));
    assert_eq!(
        fixture
            .reactor
            .vct_repair_stall
            .expect("assignment preserves the generation stall clock")
            .since,
        stall.since
    );
}

#[test]
fn established_supplier_precedes_new_identities_after_adversarial_churn() {
    let mut fixture = ReadyVctRepairFixture::new();
    let (established, mut _outbounds) = fixture.connect(&[1, 2, 3, 200], 10);
    fixture.schedule();
    fixture.advertise(&established, 10);
    for peer in &established[..3] {
        let active = fixture
            .reactor
            .peer_work_queue
            .active(peer)
            .expect("the next established supplier owns the repair")
            .clone();
        fixture.reactor.handle_headers_outcome(
            peer.clone(),
            active.owner.session_id(),
            active.owner.header_authority(),
            HeadersOutcome {
                request_id: active.request_id.get(),
                target_tip_hash: fixture.target.hash,
                outcome: HeadersOutcomeCode::TargetNotRetained,
            },
        );
    }
    let retry_at = match fixture
        .reactor
        .vct_repair
        .current()
        .expect("the failed cycle keeps the repair")
        .state
    {
        RepairPolicyState::SupplierBackoff { retry_at, .. } => retry_at,
        ref other => panic!("the bounded cycle must back off, got {other:?}"),
    };
    for (index, peer) in established[..3].iter().enumerate() {
        fixture.reactor.handle_peer_disconnected(
            peer,
            10 + u64::try_from(index).expect("the peer index fits in u64"),
            "test churn",
        );
    }

    let (churn, churn_outbounds) = fixture.connect(&[4, 5, 6], 20);
    fixture.advertise(&churn, 20);
    _outbounds.extend(churn_outbounds);

    assert_eq!(
        fixture.reactor.vct_supplier_order.front(),
        Some(&established[3])
    );
    assert_eq!(
        fixture.reactor.vct_supplier_order.len(),
        fixture.reactor.peer_state.len()
    );
    fixture
        .reactor
        .vct_repair
        .current_mut()
        .expect("the repair remains scheduled")
        .resume_retry_cycle(retry_at);
    fixture.reactor.try_assign_vct_repair();

    assert!(fixture
        .reactor
        .peer_work_queue
        .active(&established[3])
        .is_some());
    assert!(churn
        .iter()
        .all(|peer| fixture.reactor.peer_work_queue.active(peer).is_none()));
}

#[test]
fn replacement_session_keeps_the_authenticated_supplier_identity() {
    let mut fixture = ReadyVctRepairFixture::new();
    let (peers, _first_outbound) = fixture.connect(&[0x71], 7);
    let peer = &peers[0];
    let source = source_id_from_peer(peer);
    fixture.schedule();
    fixture.advertise(&peers, 7);
    assert!(fixture.reactor.peer_work_queue.active(peer).is_some());

    let (replacement_send, _replacement_outbound) = framed_channel(8);
    fixture
        .reactor
        .handle_peer_connected(PeerSession::from_parts_with_session_id(
            peer.clone(),
            8,
            replacement_send,
            CancellationToken::new(),
        ));

    assert_eq!(fixture.reactor.peer_state.len(), 1);
    assert!(fixture.reactor.peer_work_queue.active(peer).is_none());
    let task = fixture
        .reactor
        .vct_repair
        .current()
        .expect("the replacement keeps the repair scheduled");
    assert_eq!(task.tried_sources, [source].into_iter().collect());
    assert_eq!(
        fixture.reactor.vct_supplier_order,
        [peer.clone()].into_iter().collect::<VecDeque<_>>()
    );
    assert!(matches!(
        task.state,
        RepairPolicyState::SupplierBackoff { .. }
    ));
}

#[test]
fn replacement_and_disconnect_retain_owned_vct_local_operation() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_handle, _actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the local lifecycle fixture builds");
    let peer = peer();
    let (first_send, _first_outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts_with_session_id(
        peer.clone(),
        7,
        first_send,
        CancellationToken::new(),
    ));
    let (_source, owner, context) = seed_vct_active_request(
        &mut reactor,
        &snapshot,
        peer.clone(),
        7,
        HeaderTargetPhase::Applying,
    );
    let deadline = Instant::now() + reactor.startup.request_timeout;
    reactor.request_deadlines.insert(peer.clone(), deadline);

    let (replacement_send, _replacement_outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts_with_session_id(
        peer.clone(),
        8,
        replacement_send,
        CancellationToken::new(),
    ));
    reactor.handle_peer_disconnected(&peer, 7, "stale replaced session");

    assert_eq!(
        reactor
            .peer_work_queue
            .active(&peer)
            .map(|active| (active.owner, active.phase)),
        Some((owner, HeaderTargetPhase::Applying))
    );
    assert_eq!(reactor.request_deadlines.get(&peer), Some(&deadline));
    assert_eq!(
        reactor.vct_repair.current().map(|task| &task.state),
        Some(&RepairPolicyState::Assigned {
            context: context.clone()
        })
    );

    reactor.handle_peer_disconnected(&peer, 8, "replacement disconnected");

    assert!(!reactor.peer_state.contains_key(&peer));
    assert!(reactor.vct_supplier_order.is_empty());
    assert_eq!(
        reactor
            .peer_work_queue
            .active(&peer)
            .map(|active| (active.owner, active.phase)),
        Some((owner, HeaderTargetPhase::Applying))
    );
    assert_eq!(reactor.request_deadlines.get(&peer), Some(&deadline));
    assert_eq!(
        reactor.vct_repair.current().map(|task| &task.state),
        Some(&RepairPolicyState::Assigned { context })
    );
}

#[test]
fn send_failures_preserve_the_stall_clock_across_bounded_cycles() {
    let mut fixture = ReadyVctRepairFixture::new();
    let (peers, outbounds) = fixture.connect(&[1, 2, 3, 4], 7);
    fixture.advertise(&peers, 7);
    drop(outbounds);
    fixture.schedule();

    fixture.reactor.try_assign_vct_repair();

    let first_retry_at = match &fixture
        .reactor
        .vct_repair
        .current()
        .expect("send failures keep the repair")
        .state
    {
        RepairPolicyState::SupplierBackoff { retry_at, .. } => *retry_at,
        other => panic!("three send failures must back off, got {other:?}"),
    };
    let task = fixture
        .reactor
        .vct_repair
        .current()
        .expect("the repair remains");
    assert_eq!(task.tried_sources.len(), MAX_SUPPLIERS_PER_CYCLE);
    assert_eq!(
        fixture.reactor.vct_supplier_order,
        [
            peers[3].clone(),
            peers[0].clone(),
            peers[1].clone(),
            peers[2].clone(),
        ]
        .into_iter()
        .collect::<VecDeque<_>>()
    );
    let stall = fixture
        .reactor
        .vct_repair_stall
        .expect("send failures start the stall clock");

    fixture
        .reactor
        .vct_repair
        .current_mut()
        .expect("the repair remains")
        .resume_retry_cycle(first_retry_at);
    fixture.reactor.try_assign_vct_repair();

    let task = fixture
        .reactor
        .vct_repair
        .current()
        .expect("the repair remains");
    assert!(matches!(
        task.state,
        RepairPolicyState::SupplierBackoff { .. }
    ));
    assert_eq!(task.tried_sources.len(), MAX_SUPPLIERS_PER_CYCLE);
    assert_eq!(
        fixture.reactor.vct_supplier_order,
        [
            peers[2].clone(),
            peers[3].clone(),
            peers[0].clone(),
            peers[1].clone(),
        ]
        .into_iter()
        .collect::<VecDeque<_>>()
    );
    assert_eq!(
        fixture
            .reactor
            .vct_repair_stall
            .expect("the next send cycle preserves the stall clock")
            .since,
        stall.since
    );
}

#[test]
fn full_action_queue_retries_lease_release_on_maintenance() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_handle, mut actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the serving fixture builds");
    let peer = peer();
    for _ in 0..128 {
        reactor
            .actions
            .try_send(HeaderPortOperation::Misbehavior {
                peer: peer.clone(),
                reason: HeaderSyncMisbehavior::MalformedMessage,
            })
            .expect("the bounded action queue has exactly 128 slots");
    }
    let scope =
        zakura_header_chain::HeaderWorkAuthority::for_target(&snapshot, block::Hash([0x33; 32]));

    reactor.release_lease(peer.clone(), 7, 9, scope);

    assert_eq!(reactor.pending_lease_releases.len(), 1);
    assert!(reactor.lease_release_retry_at.is_some());
    let _ = actions
        .try_recv()
        .expect("draining one action creates release capacity");
    reactor.retry_pending_lease_releases(Instant::now());
    assert!(reactor.pending_lease_releases.is_empty());
    assert!(reactor.lease_release_retry_at.is_none());

    let mut found = false;
    while let Ok(action) = actions.try_recv() {
        if matches!(
            action,
            HeaderPortOperation::ReleaseHeaderPath {
                peer: actual_peer,
                session_id: 7,
                lease_id: 9,
                scope: actual_scope,
            } if actual_peer == peer && actual_scope == scope
        ) {
            found = true;
        }
    }
    assert!(
        found,
        "the retained release reaches the driver after capacity returns"
    );
}
