//! Deterministic resource scheduling with captured dependency durations.
//!
//! This calls real reservation/commit/transfer/drop operations. It deliberately
//! supplies dependencies instead of simulating the peer routine or QUIC writer.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};

use futures::{
    future::LocalBoxFuture,
    task::{waker, ArcWake},
    FutureExt,
};

use serde::Serialize;

use super::{super::*, profile::*};

const MAX_RUN_US: u64 = 24 * 60 * 60 * 1_000_000;

#[derive(Copy, Clone, Debug, Serialize)]
pub(super) enum ReleaseEdge {
    Start,
    Finish,
}

impl ReleaseEdge {
    fn choose(self, interval: [u64; 2]) -> u64 {
        match self {
            Self::Start => interval[0],
            Self::Finish => interval[1],
        }
    }
}

#[derive(Copy, Clone, Debug, Serialize)]
pub(super) enum SessionOrder {
    Forward,
    Reverse,
}

#[derive(Debug, Default, Eq, PartialEq, Serialize)]
pub(super) struct RequestResult {
    pub retained_us: u64,
    pub admitted_us: u64,
    pub last_release_us: Option<u64>,
}

#[derive(Debug, Serialize)]
pub(super) struct Report {
    pub scope: &'static str,
    pub release_edge: ReleaseEdge,
    pub session_order: SessionOrder,
    pub max_external_backlog: usize,
    pub max_observed_node_bytes: u64,
    pub max_observed_node_active: usize,
    pub max_observed_node_pending: usize,
    pub all_resource_owners_drained: bool,
    pub max_input_delay_us: u64,
    pub max_admission_delay_us: u64,
    pub requests_with_input_delay_over_8s: usize,
    pub queries_exceeding_candidate_timeout: usize,
    pub max_session_extension_us: u64,
    pub protocol_feedback_modelled: bool,
    pub requests: Vec<RequestResult>,
}

// Variant order preserves same-request causality when timestamps are tied.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Event {
    Connect(usize),
    Arrival(usize),
    Forwarded(usize),
    PendingRelease(usize),
    Commit(usize),
    QueryStart(usize),
    QueryFinish(usize),
    FrameQueue(usize, usize),
    FrameRelease(usize, usize),
    Settle(usize),
    SessionEnd(usize),
}

#[derive(Default)]
struct WakeFlag(AtomicBool);

impl ArcWake for WakeFlag {
    fn wake_by_ref(flag: &Arc<Self>) {
        flag.0.store(true, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct SessionState {
    account: Option<GetBlocksServingSession>,
    external: VecDeque<usize>,
    retained: VecDeque<usize>,
    forwarding: bool,
    retaining: Option<LocalBoxFuture<'static, PendingGetBlocksRequest>>,
    admission_wait: Option<LocalBoxFuture<'static, Option<AcquiredAdmissionSlot>>>,
    end_observed: bool,
}

#[derive(Default)]
struct Owners {
    pending: Option<PendingGetBlocksRequest>,
    attempt: Option<AdmissionAttempt>,
    permit: Option<GetBlocksServingPermit>,
    query: Option<BlockRangeQueryLease>,
    frames: Vec<Option<FrameLease>>,
}

struct Runner<'a> {
    profile: &'a Profile,
    regulator: GetBlocksServingRegulator,
    sessions: Vec<SessionState>,
    owners: Vec<Owners>,
    events: BTreeMap<u64, Vec<Event>>,
    now: u64,
    wake: Arc<WakeFlag>,
    report: Report,
}

impl<'a> Runner<'a> {
    fn new(
        profile: &'a Profile,
        config: ZakuraBlockSyncConfig,
        edge: ReleaseEdge,
        order: SessionOrder,
    ) -> Self {
        let mut runner = Self {
            profile,
            regulator: GetBlocksServingRegulator::new(config),
            sessions: (0..profile.sessions.len())
                .map(|_| SessionState::default())
                .collect(),
            owners: profile
                .requests
                .iter()
                .map(|request| Owners {
                    frames: (0..request.frames.len()).map(|_| None).collect(),
                    ..Owners::default()
                })
                .collect(),
            events: BTreeMap::new(),
            now: 0,
            wake: Arc::new(WakeFlag::default()),
            report: Report {
                scope: "conditional_resource_ownership_with_fixed_dependencies_not_native_sync",
                release_edge: edge,
                session_order: order,
                max_external_backlog: 0,
                max_observed_node_bytes: 0,
                max_observed_node_active: 0,
                max_observed_node_pending: 0,
                all_resource_owners_drained: false,
                max_input_delay_us: 0,
                max_admission_delay_us: 0,
                requests_with_input_delay_over_8s: 0,
                queries_exceeding_candidate_timeout: 0,
                max_session_extension_us: 0,
                protocol_feedback_modelled: false,
                requests: (0..profile.requests.len())
                    .map(|_| RequestResult::default())
                    .collect(),
            },
        };
        for session in &profile.sessions {
            runner.schedule(session.start_us, Event::Connect(session.session));
            runner.schedule(session.end_us, Event::SessionEnd(session.session));
        }
        for (index, request) in profile.requests.iter().enumerate() {
            runner.schedule(request.decoded_us, Event::Arrival(index));
        }
        runner
    }

    fn schedule(&mut self, at: u64, event: Event) {
        self.events.entry(at).or_default().push(event);
    }

    fn observe(&mut self) {
        let snapshot = self.regulator.snapshot();
        let policy = &self.regulator.inner.config.get_blocks_regulation;
        assert!(snapshot.node_outstanding <= policy.node_outstanding_bytes);
        assert!(snapshot.max_peer_outstanding <= policy.peer_outstanding_bytes);
        assert!(snapshot.node_active <= policy.node_active_requests);
        assert!(snapshot.node_pending <= policy.node_pending_requests);
        assert_eq!(snapshot.node_outstanding, snapshot.peer_outstanding);
        // A fair pending waiter may own its session slot before a node slot.
        assert!(snapshot.node_pending <= snapshot.session_pending);
        assert!(snapshot.session_pending - snapshot.node_pending <= self.sessions.len());
        self.report.max_observed_node_bytes = self
            .report
            .max_observed_node_bytes
            .max(snapshot.node_outstanding);
        self.report.max_observed_node_active = self
            .report
            .max_observed_node_active
            .max(snapshot.node_active);
        self.report.max_observed_node_pending = self
            .report
            .max_observed_node_pending
            .max(snapshot.node_pending);
    }

    fn event(&mut self, event: Event) {
        match event {
            Event::Connect(session) => {
                let mut identity = vec![0; 32];
                identity[..8].copy_from_slice(
                    &u64::try_from(self.profile.sessions[session].peer)
                        .unwrap()
                        .to_le_bytes(),
                );
                self.sessions[session].account = Some(self.regulator.session(
                    ZakuraPeerId::new(identity).unwrap(),
                    u64::try_from(session).unwrap(),
                ));
            }
            Event::Arrival(request) => {
                self.sessions[self.profile.requests[request].session]
                    .external
                    .push_back(request);
                let backlog = self
                    .sessions
                    .iter()
                    .map(|session| session.external.len())
                    .sum();
                self.report.max_external_backlog = self.report.max_external_backlog.max(backlog);
            }
            Event::Forwarded(request) => {
                self.sessions[self.profile.requests[request].session].forwarding = false
            }
            Event::PendingRelease(request) => {
                self.owners[request].pending.take().unwrap().into_parts();
            }
            Event::Commit(request) => {
                self.owners[request].permit =
                    Some(self.owners[request].attempt.take().unwrap().commit());
            }
            Event::QueryStart(request) => {
                let lease = self.owners[request].permit.as_ref().unwrap().query_lease();
                assert!(lease.try_start());
                self.owners[request].query = Some(lease);
            }
            Event::QueryFinish(request) => drop(self.owners[request].query.take().unwrap()),
            Event::FrameQueue(request, frame) => {
                let bytes = self.profile.requests[request].frames[frame].payload_bytes;
                let owner = &mut self.owners[request];
                owner.frames[frame] = Some(owner.permit.as_mut().unwrap().transfer_frame(bytes));
            }
            Event::FrameRelease(request, frame) => {
                drop(self.owners[request].frames[frame].take().unwrap());
                self.report.requests[request].last_release_us = Some(self.now);
            }
            Event::Settle(request) => {
                drop(self.owners[request].permit.take().unwrap());
                self.report.requests[request].last_release_us = Some(self.now);
            }
            Event::SessionEnd(session) => self.sessions[session].end_observed = true,
        }
        self.observe();
    }

    fn admitted(&mut self, index: usize, attempt: AdmissionAttempt) {
        let request = &self.profile.requests[index];
        self.owners[index].attempt = Some(attempt);
        self.report.requests[index].admitted_us = self.now;
        self.sessions[request.session].forwarding = true;
        let edge = self.report.release_edge;
        // All dependency durations move with the new admission. Original absolute
        // completion times must not free a counterfactually delayed request early.
        let shifted = |timestamp: u64| self.now + (timestamp - request.admitted_us);
        let mut events = vec![
            (shifted(request.forwarded_us()), Event::Forwarded(index)),
            (
                shifted(edge.choose(request.pending_release_us)),
                Event::PendingRelease(index),
            ),
            (shifted(request.committed_us), Event::Commit(index)),
            (shifted(request.query_us[0]), Event::QueryStart(index)),
            (shifted(request.query_us[1]), Event::QueryFinish(index)),
            (
                shifted(edge.choose(request.settlement_us)),
                Event::Settle(index),
            ),
        ];
        for (frame, timing) in request.frames.iter().enumerate() {
            events.push((shifted(timing.queued_us), Event::FrameQueue(index, frame)));
            events.push((
                shifted(edge.choose(timing.release_us)),
                Event::FrameRelease(index, frame),
            ));
        }
        for (at, event) in events {
            self.schedule(at, event);
        }
        self.observe();
    }

    fn retain_ready(&mut self, session: usize, context: &mut Context<'_>) {
        while let Some(&index) = self.sessions[session].external.front() {
            let pending = if let Some(wait) = &mut self.sessions[session].retaining {
                let Poll::Ready(pending) = wait.as_mut().poll(context) else {
                    break;
                };
                self.sessions[session].retaining = None;
                pending
            } else {
                let request = &self.profile.requests[index];
                let account = self.sessions[session].account.as_ref().unwrap();
                match account.try_retain_input(block::Height(request.start_height), request.count) {
                    Ok(pending) => pending,
                    Err(_) => {
                        // One blocked input per session uses the real fair wait.
                        // Further offered arrivals stay in the external harness queue.
                        let account = account.clone();
                        let start = block::Height(request.start_height);
                        let count = request.count;
                        self.sessions[session].retaining = Some(
                            async move { account.retain_input(start, count).await }.boxed_local(),
                        );
                        continue;
                    }
                }
            };
            self.sessions[session].external.pop_front();
            self.sessions[session].retained.push_back(index);
            self.owners[index].pending = Some(pending);
            self.report.requests[index].retained_us = self.now;
            self.observe();
        }
    }

    /// Preserve per-session input order and real fair slot waits. Reversing the
    /// polling order explores a different legal order for initially tied sessions.
    fn admit_ready(&mut self) -> Result<Option<u64>, String> {
        let wake = waker(self.wake.clone());
        let mut context = Context::from_waker(&wake);
        let mut next_rate_retry = None;
        let mut order: Vec<_> = (0..self.sessions.len()).collect();
        if matches!(self.report.session_order, SessionOrder::Reverse) {
            order.reverse();
        }
        for session in order {
            self.retain_ready(session, &mut context);
            if self.sessions[session].forwarding {
                continue;
            }
            if let Some(&index) = self.sessions[session].retained.front() {
                let acquired = if let Some(wait) = &mut self.sessions[session].admission_wait {
                    let Poll::Ready(slot) = wait.as_mut().poll(&mut context) else {
                        continue;
                    };
                    self.sessions[session].admission_wait = None;
                    slot
                } else {
                    None
                };
                let account = self.sessions[session]
                    .account
                    .as_ref()
                    .ok_or("retained input lost its session account")?;
                match account.try_admit_with_slot(self.profile.requests[index].count, acquired) {
                    Ok(attempt) => {
                        self.sessions[session].retained.pop_front();
                        self.admitted(index, attempt);
                    }
                    Err(blocked) => {
                        if let AdmissionWait::Rate { budget, bytes } = &blocked.wait {
                            // Use the production deadline, including fractional
                            // credit. The runner rounds up to one microsecond;
                            // it does not model Tokio timer or executor latency.
                            let unavailable = budget
                                .try_reserve(*bytes)
                                .err()
                                .ok_or("blocked rate budget unexpectedly became available")?;
                            let delay = unavailable
                                .retry_after()
                                .ok_or("validated request cost no longer fits the rate capacity")?;
                            let micros = delay.as_nanos().div_ceil(1_000).max(1);
                            let at = self
                                .now
                                .saturating_add(u64::try_from(micros).unwrap_or(u64::MAX));
                            next_rate_retry =
                                Some(next_rate_retry.map_or(at, |prior: u64| prior.min(at)));
                        } else {
                            let mut wait = blocked.wait().boxed_local();
                            assert!(
                                wait.as_mut().poll(&mut context).is_pending(),
                                "the failed bound is still unavailable at the same instant"
                            );
                            self.sessions[session].admission_wait = Some(wait);
                        }
                    }
                }
            }
            let state = &mut self.sessions[session];
            if state.end_observed
                && state.external.is_empty()
                && state.retained.is_empty()
                && !state.forwarding
                && state.account.as_ref().is_some_and(|account| {
                    account.resources.active.reserved() == 0
                        && account.resources.outstanding.reserved() == 0
                        && account.resources.pending.reserved() == 0
                })
            {
                state.account = None;
            }
        }
        Ok(next_rate_retry)
    }

    async fn run(mut self) -> Result<Report, String> {
        loop {
            while self
                .events
                .first_key_value()
                .is_some_and(|(&at, _)| at == self.now)
            {
                let (_, mut events) = self.events.pop_first().unwrap();
                events.sort();
                for event in events {
                    self.event(event);
                }
            }
            self.wake.0.store(false, Ordering::Relaxed);
            let retry = self.admit_ready()?;
            if self.wake.0.swap(false, Ordering::Relaxed) {
                continue;
            }
            let next_event = self.events.first_key_value().map(|(&at, _)| at);
            let next = [retry, next_event].into_iter().flatten().min();
            let Some(next) = next else { break };
            if next > MAX_RUN_US {
                return Err("conditional replay exceeded its 24-hour horizon".into());
            }
            tokio::time::advance(Duration::from_micros(next - self.now)).await;
            self.now = next;
        }
        let resources = self.regulator.snapshot();
        self.report.all_resource_owners_drained = resources.node_outstanding == 0
            && resources.node_active == 0
            && resources.node_pending == 0
            && self
                .sessions
                .iter()
                .all(|session| session.external.is_empty() && session.retained.is_empty())
            && self
                .report
                .requests
                .iter()
                .all(|request| request.last_release_us.is_some());
        if !self.report.all_resource_owners_drained {
            return Err("conditional replay stopped with outstanding work".into());
        }
        for (request, result) in self.profile.requests.iter().zip(&self.report.requests) {
            let input_delay = result.retained_us - request.decoded_us;
            self.report.max_input_delay_us = self.report.max_input_delay_us.max(input_delay);
            self.report.max_admission_delay_us = self
                .report
                .max_admission_delay_us
                .max(result.admitted_us - result.retained_us);
            self.report.requests_with_input_delay_over_8s += usize::from(input_delay > 8_000_000);
            self.report.queries_exceeding_candidate_timeout += usize::from(
                Duration::from_micros(request.query_us[1] - request.query_us[0])
                    >= self
                        .regulator
                        .inner
                        .config
                        .get_blocks_regulation
                        .query_timeout,
            );
            self.report.max_session_extension_us = self.report.max_session_extension_us.max(
                result
                    .last_release_us
                    .ok_or("completed request has no release time")?
                    .saturating_sub(self.profile.sessions[request.session].end_us),
            );
        }
        Ok(self.report)
    }
}

pub(super) fn replay(
    profile: &Profile,
    captured: &Policy,
    candidate: &Policy,
    edge: ReleaseEdge,
    order: SessionOrder,
) -> Result<Report, String> {
    profile.validate(captured, candidate)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(Runner::new(profile, candidate.config()?, edge, order).run())
}
