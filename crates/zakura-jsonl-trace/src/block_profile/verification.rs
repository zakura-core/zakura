//! Bounded verification evidence. Shared batches project once into each participating attempt.

use super::*;

const MAX_REQUESTS: usize = 8192;
const MAX_BATCHES: usize = 256;
const MAX_OWNERS: usize = 64;
static REQUESTS: AtomicUsize = AtomicUsize::new(0);
static BATCHES: AtomicUsize = AtomicUsize::new(0);
static NEXT_BATCH: AtomicU64 = AtomicU64::new(1);

/// The shielded pool whose bundle is checked.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Pool {
    /// Sapling shielded bundles.
    Sapling,
    /// Orchard shielded bundles.
    Orchard,
    /// Ironwood shielded bundles.
    Ironwood,
}
/// Counts of the work submitted, not transaction payloads.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Workload {
    /// Sapling spend descriptions.
    pub spends: u32,
    /// Sapling output descriptions.
    pub outputs: u32,
    /// Orchard or Ironwood actions.
    pub actions: u32,
}
impl Workload {
    fn add(&mut self, other: Self) {
        self.spends = self.spends.saturating_add(other.spends);
        self.outputs = self.outputs.saturating_add(other.outputs);
        self.actions = self.actions.saturating_add(other.actions);
    }
}
/// Actual cache decision. Unknown is not evidence of a hit.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Cache {
    #[default]
    /// No cache decision was recorded.
    Unknown,
    /// An existing successful verification was reused.
    Hit,
    /// The key was absent and verification was requested.
    Miss,
    /// No cache key was available.
    Bypass,
}
/// Completion of a measured operation, independent of consensus validity.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// The operation returned success.
    Success,
    /// The operation returned an error or failed verification.
    Failed,
    #[default]
    /// No result was observed before ownership ended.
    Abandoned,
}
/// Fixed-size optional evidence carried by one span.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Detail {
    /// One bundle request, including cache and shared execution links.
    Request {
        /// Pool containing this bundle.
        pool: Pool,
        /// Work represented by this request or the entire shared batch.
        workload: Workload,
        /// Actual cache lookup outcome.
        cache: Cache,
        /// Observed completion status.
        status: Status,
        /// First shared batch, if successfully admitted.
        primary_batch: Option<u64>,
        /// Individual retry batch, if one was recorded.
        fallback_batch: Option<u64>,
        #[serde(default)]
        /// Whether the fallback service was entered, even before batch admission.
        fallback: bool,
        /// Some evidence or ownership links could not be retained.
        partial: bool,
    },
    /// One shared execution projected into a participating block attempt.
    Batch {
        /// Run-unique shared batch identity.
        id: u64,
        /// Work represented by this request or the entire shared batch.
        workload: Workload,
        /// Total submitted requests; partial evidence may include rejected preparation.
        members: u32,
        /// Requests carrying a live block profile context.
        profiled: u32,
        /// Requests without a live block profile context.
        unprofiled: u32,
        /// Sapling member count.
        sapling: u32,
        /// Orchard member count.
        orchard: u32,
        /// Ironwood member count.
        ironwood: u32,
        /// Flush request offset from the run epoch.
        flush_us: Option<u64>,
        /// Worker submission offset from the run epoch.
        dispatch_us: Option<u64>,
        /// Worker execution start offset.
        worker_start_us: Option<u64>,
        /// End of worker setup and start of combined cryptography.
        setup_end_us: Option<u64>,
        /// Combined proof and signature validation completion.
        execution_end_us: Option<u64>,
        /// Result publication offset, absent when abandoned.
        published_us: Option<u64>,
        /// Observed completion status.
        status: Status,
        /// Some evidence or ownership links could not be retained.
        partial: bool,
    },
}
impl Detail {
    /// Validate external metadata without assuming all optional phases were captured.
    pub fn is_valid(&self, start_us: u64, end_us: u64) -> bool {
        if end_us < start_us {
            return false;
        }
        match self {
            Self::Request {
                primary_batch,
                fallback_batch,
                ..
            } => {
                primary_batch.is_none_or(|id| id > 0)
                    && fallback_batch.is_none_or(|id| id > 0)
                    && (fallback_batch.is_none() || primary_batch != fallback_batch)
            }
            Self::Batch {
                id,
                members,
                profiled,
                unprofiled,
                sapling,
                orchard,
                ironwood,
                flush_us,
                dispatch_us,
                worker_start_us,
                setup_end_us,
                execution_end_us,
                published_us,
                ..
            } => {
                let mut previous = start_us;
                *id > 0
                    && u64::from(*profiled) + u64::from(*unprofiled) == u64::from(*members)
                    && u64::from(*sapling) + u64::from(*orchard) + u64::from(*ironwood)
                        == u64::from(*members)
                    && [
                        flush_us,
                        dispatch_us,
                        worker_start_us,
                        setup_end_us,
                        execution_end_us,
                        published_us,
                    ]
                    .into_iter()
                    .flatten()
                    .all(|time| {
                        let valid = *time >= previous && *time <= end_us;
                        previous = *time;
                        valid
                    })
            }
        }
    }
}

fn reserve(counter: &AtomicUsize, limit: usize) -> bool {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
            (n < limit).then_some(n + 1)
        })
        .is_ok()
}
fn mark_lost(context: &Context) {
    if let Some((attempt, _, _)) = &context.0 {
        attempt.dropped.fetch_add(1, Ordering::Relaxed);
        attempt.recorder.dropped.fetch_add(1, Ordering::Relaxed);
    }
}
struct BatchTiming {
    flush: AtomicU64,
    dispatch: AtomicU64,
    published: AtomicU64,
}
impl Default for BatchTiming {
    fn default() -> Self {
        Self {
            flush: AtomicU64::new(u64::MAX),
            dispatch: AtomicU64::new(u64::MAX),
            published: AtomicU64::new(u64::MAX),
        }
    }
}
impl BatchTiming {
    fn read(value: &AtomicU64) -> Option<u64> {
        let value = value.load(Ordering::Acquire);
        (value != u64::MAX).then_some(value)
    }
}
struct RequestState {
    span: Option<Span>,
    context: Context,
    detail: Detail,
    admission: Option<Span>,
    formation: Option<Span>,
    batch: Option<Arc<BatchTiming>>,
    delivered: bool,
}
impl RequestState {
    fn formation_end(&mut self) {
        if let Some(mut span) = self.formation.take() {
            if let Some(end) = self
                .batch
                .as_ref()
                .and_then(|b| BatchTiming::read(&b.flush))
            {
                span.end_us = Some(end.max(span.start_us));
            } else {
                self.partial();
            }
        }
    }
    fn partial(&mut self) {
        if let Detail::Request { partial, .. } = &mut self.detail {
            *partial = true;
        }
    }
    fn delivery(&mut self) {
        if self.delivered {
            return;
        }
        self.delivered = true;
        if let Some(start) = self
            .batch
            .as_ref()
            .and_then(|b| BatchTiming::read(&b.published))
        {
            let mut span = self.context.span(Stage::VerificationDelivery);
            span.start_us = start;
        }
    }
    fn finish(&mut self, status: Status) {
        let Some(mut span) = self.span.take() else {
            return;
        };
        if status == Status::Abandoned {
            self.partial();
        }
        self.admission.take();
        self.formation_end();
        self.delivery();
        if let Detail::Request { status: result, .. } = &mut self.detail {
            *result = status;
        }
        span.verification = Some(self.detail);
        // Release the context even when an Item clone remains in a cancelled batch.
        self.context = Context::default();
    }
}
impl Drop for RequestState {
    fn drop(&mut self) {
        self.finish(Status::Abandoned);
        REQUESTS.fetch_sub(1, Ordering::Relaxed);
    }
}
/// Cloned with verifier Items. Allocation and lifetime are bounded independently of the queue.
#[derive(Clone, Default)]
pub struct Request(Option<Arc<Mutex<RequestState>>>);
impl std::fmt::Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("VerificationRequest")
            .field(&self.0.is_some())
            .finish()
    }
}
impl Request {
    /// Capture the currently polled block context. Disabled requests allocate nothing.
    pub fn new(pool: Pool, workload: Workload) -> Self {
        let context = Context::current();
        if context.0.is_none() {
            return Self::default();
        }
        if !reserve(&REQUESTS, MAX_REQUESTS) {
            mark_lost(&context);
            return Self::default();
        }
        let span = context.span(Stage::VerificationRequest);
        if span.context.0.is_none() {
            REQUESTS.fetch_sub(1, Ordering::Relaxed);
            return Self::default();
        }
        Self(Some(Arc::new(Mutex::new(RequestState {
            context: span.context(),
            span: Some(span),
            detail: Detail::Request {
                pool,
                workload,
                cache: Cache::Unknown,
                status: Status::Abandoned,
                primary_batch: None,
                fallback_batch: None,
                fallback: false,
                partial: false,
            },
            admission: None,
            formation: None,
            batch: None,
            delivered: false,
        }))))
    }
    fn with(&self, f: impl FnOnce(&mut RequestState)) {
        if let Some(state) = &self.0 {
            let mut state = state.lock().unwrap_or_else(|e| e.into_inner());
            if state.span.is_some() {
                f(&mut state);
            }
        }
    }
    /// Context for synchronous phases or future polling.
    pub fn context(&self) -> Context {
        self.0
            .as_ref()
            .map(|state| {
                state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .context
                    .clone()
            })
            .unwrap_or_default()
    }
    /// Measure a request-local elapsed phase.
    pub fn phase(&self, stage: Stage) -> Span {
        self.context().span(stage)
    }
    /// Record the actual cache decision.
    pub fn cache(&self, value: Cache) {
        self.with(|state| {
            if let Detail::Request { cache, .. } = &mut state.detail {
                *cache = value;
            }
        });
    }
    /// The service is retrying individually, even when primary admission failed before a batch.
    pub fn fallback(&self) {
        self.with(|state| {
            state.delivery();
            if let Detail::Request { fallback, .. } = &mut state.detail {
                *fallback = true;
            }
        });
    }
    /// Service submission begins admission waiting.
    pub fn submitted(&self) {
        self.with(|state| {
            state.admission = Some(state.context.span(Stage::VerificationAdmission));
        });
    }
    /// The batch service has begun handling the Item.
    pub fn admitted(&self) {
        self.with(|state| {
            state.admission.take();
        });
    }
    /// Preparation completed and this request waits for its assigned batch to dispatch.
    pub fn prepared(&self) {
        self.with(|state| {
            state.formation_end();
            state.formation = Some(state.context.span(Stage::VerificationFormation));
        });
    }
    /// The caller resumed after the latest batch publication.
    pub fn delivered(&self) {
        self.with(RequestState::delivery);
    }
    /// Completion is idempotent across service clones and cancellation guards.
    pub fn finish(&self, status: Status) {
        self.with(|state| state.finish(status));
    }
    /// Evidence could not be retained. Missing phases cannot be read as zero cost.
    pub fn mark_partial(&self) {
        self.with(RequestState::partial);
    }
    /// Mark an unpolled or cancelled service future abandoned when it is dropped.
    pub fn guard(&self) -> RequestGuard {
        RequestGuard(self.clone())
    }
}
/// Cancellation guard for the owner of the request future.
pub struct RequestGuard(Request);
impl Drop for RequestGuard {
    fn drop(&mut self) {
        self.0.finish(Status::Abandoned);
    }
}

struct BatchState {
    recorder: Arc<Recorder>,
    id: u64,
    start: u64,
    timing: Arc<BatchTiming>,
    owners: Vec<(u64, Span)>,
    detail: Detail,
}
impl Drop for BatchState {
    fn drop(&mut self) {
        BATCHES.fetch_sub(1, Ordering::Relaxed);
    }
}
/// One bounded shared batch. Its projections preserve block ownership through cancellation.
pub struct Batch(Option<BatchState>);
impl std::fmt::Debug for Batch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("VerificationBatch")
            .field(&self.0.as_ref().map(|b| b.id))
            .finish()
    }
}
impl Batch {
    /// Begin batch evidence only when recording is enabled and capture capacity is available.
    pub fn new() -> Self {
        if !enabled() {
            return Self(None);
        }
        Self::with_recorder(RECORDER.get().expect("enabled recorder exists"))
    }
    fn with_recorder(recorder: &Arc<Recorder>) -> Self {
        if !reserve(&BATCHES, MAX_BATCHES) {
            return Self(None);
        }
        let id = NEXT_BATCH.fetch_add(1, Ordering::Relaxed);
        Self(Some(BatchState {
            recorder: recorder.clone(),
            id,
            start: u64::MAX,
            timing: Arc::default(),
            owners: Vec::new(),
            detail: Detail::Batch {
                id,
                workload: Workload::default(),
                members: 0,
                profiled: 0,
                unprofiled: 0,
                sapling: 0,
                orchard: 0,
                ironwood: 0,
                flush_us: None,
                dispatch_us: None,
                worker_start_us: None,
                setup_end_us: None,
                execution_end_us: None,
                published_us: None,
                status: Status::Abandoned,
                partial: false,
            },
        }))
    }
    /// Register every submitted Item, including unprofiled members, exactly once.
    pub fn add(&mut self, request: &Request, pool: Pool, work: Workload) {
        let Some(batch) = &mut self.0 else {
            request.mark_partial();
            return;
        };
        let context = request.context();
        if batch.start == u64::MAX {
            batch.start = batch.recorder.now();
        }
        if let Detail::Batch {
            workload,
            members,
            profiled,
            unprofiled,
            sapling,
            orchard,
            ironwood,
            ..
        } = &mut batch.detail
        {
            workload.add(work);
            *members = members.saturating_add(1);
            let count = if context.0.is_some() {
                profiled
            } else {
                unprofiled
            };
            *count = count.saturating_add(1);
            let count = match pool {
                Pool::Sapling => sapling,
                Pool::Orchard => orchard,
                Pool::Ironwood => ironwood,
            };
            *count = count.saturating_add(1);
        }
        let Some((attempt, _, _)) = &context.0 else {
            return;
        };
        let mut retained = batch.owners.iter().any(|(id, _)| *id == attempt.id);
        if !retained && batch.owners.len() < MAX_OWNERS {
            // Project shared work at attempt level, never under an arbitrary transaction.
            let owner_context = Context(Some((attempt.clone(), 0, None)));
            let mut span = owner_context.span(Stage::VerificationBatch);
            retained = span.context.0.is_some();
            if retained {
                span.start_us = batch.start;
                batch.owners.push((attempt.id, span));
            }
        }
        if !retained {
            request.mark_partial();
            if let Detail::Batch { partial, .. } = &mut batch.detail {
                *partial = true;
            }
        }
        request.with(|state| {
            state.formation_end();
            if let Detail::Request {
                primary_batch,
                fallback_batch,
                fallback,
                partial,
                ..
            } = &mut state.detail
            {
                if *fallback && fallback_batch.is_none() {
                    *fallback_batch = Some(batch.id);
                } else if primary_batch.is_none() && !*fallback {
                    *primary_batch = Some(batch.id);
                } else if fallback_batch.is_none() && *primary_batch != Some(batch.id) {
                    *fallback_batch = Some(batch.id);
                } else if *primary_batch != Some(batch.id) && *fallback_batch != Some(batch.id) {
                    *partial = true;
                }
            }
            state.batch = Some(batch.timing.clone());
            state.delivered = false;
        });
    }
    /// Preparation or ownership was partial, so workload is submitted rather than executed work.
    pub fn mark_partial(&mut self) {
        if let Some(batch) = &mut self.0 {
            if let Detail::Batch { partial, .. } = &mut batch.detail {
                *partial = true;
            }
        }
    }
    /// Actual dispatch to the execution pool, immediately before spawning work.
    pub fn dispatch(&mut self) {
        if let Some(batch) = &mut self.0 {
            let now = batch.recorder.now();
            if batch.start == u64::MAX {
                batch.start = now;
            }
            batch.timing.dispatch.store(now, Ordering::Release);
            if let Detail::Batch { dispatch_us, .. } = &mut batch.detail {
                *dispatch_us = Some(now);
            }
        }
    }
    /// Flush was requested, before its async scheduling or worker submission.
    pub fn submitted(&mut self) {
        if let Some(batch) = &mut self.0 {
            let now = batch.recorder.now();
            if batch.start == u64::MAX {
                batch.start = now;
            }
            batch.timing.flush.store(now, Ordering::Release);
            if let Detail::Batch { flush_us, .. } = &mut batch.detail {
                *flush_us = Some(now);
            }
        }
    }
    /// Explicit spelling of the flush-request boundary.
    pub fn flush_requested(&mut self) {
        self.submitted();
    }
    /// Worker closure started executing.
    pub fn worker_start(&mut self) {
        if let Some(batch) = &mut self.0 {
            if let Detail::Batch {
                worker_start_us, ..
            } = &mut batch.detail
            {
                *worker_start_us = Some(batch.recorder.now());
            }
        }
    }
    /// Key/setup work completed; combined cryptographic execution begins.
    pub fn setup_finished(&mut self) {
        if let Some(batch) = &mut self.0 {
            if let Detail::Batch { setup_end_us, .. } = &mut batch.detail {
                *setup_end_us = Some(batch.recorder.now());
            }
        }
    }
    /// Combined proof/signature validator returned.
    pub fn execution_finished(&mut self) {
        if let Some(batch) = &mut self.0 {
            if let Detail::Batch {
                execution_end_us, ..
            } = &mut batch.detail
            {
                *execution_end_us = Some(batch.recorder.now());
            }
        }
    }
    /// Publish completion just before waking subscribers; emits each projection once.
    pub fn finish(&mut self, status: Status) {
        let Some(mut batch) = self.0.take() else {
            return;
        };
        let now = batch.recorder.now();
        if status != Status::Abandoned {
            batch.timing.published.store(now, Ordering::Release);
        }
        if let Detail::Batch {
            published_us,
            status: result,
            partial,
            ..
        } = &mut batch.detail
        {
            *published_us = (status != Status::Abandoned).then_some(now);
            *result = status;
            if status == Status::Abandoned {
                *partial = true;
            }
        }
        for (_, mut span) in batch.owners.drain(..) {
            span.verification = Some(batch.detail);
            span.end_us = Some(now);
            drop(span);
        }
    }
}
impl Drop for Batch {
    fn drop(&mut self) {
        self.finish(Status::Abandoned);
    }
}

impl Default for Batch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block() -> Block {
        Block {
            hash: [1; 32],
            parent: [0; 32],
            height: Some(1),
            transactions: 1,
            mode: Mode::Semantic,
        }
    }
    fn request(context: &Context) -> Request {
        context.in_scope(|| {
            Request::new(
                Pool::Sapling,
                Workload {
                    spends: 2,
                    outputs: 1,
                    actions: 0,
                },
            )
        })
    }

    #[test]
    fn shared_batch_projects_once_per_attempt_and_preserves_workload() {
        let (recorder, details, summaries) = recorder();
        let a = begin_with(&recorder, block()).unwrap();
        let b = begin_with(&recorder, block()).unwrap();
        let first = request(&a.context());
        let second = request(&a.context());
        let third = request(&b.context());
        let mut batch = Batch::with_recorder(&recorder);
        let workload = Workload {
            spends: 2,
            outputs: 1,
            actions: 0,
        };
        for member in [&first, &second, &third, &Request::default()] {
            batch.add(member, Pool::Sapling, workload);
            member.prepared();
        }
        a.finish(Outcome::Success);
        b.finish(Outcome::Success);
        first.finish(Status::Abandoned);
        second.finish(Status::Abandoned);
        third.finish(Status::Abandoned);
        assert!(!summaries
            .try_iter()
            .any(|event| matches!(event, Event::Seal { .. })));
        batch.flush_requested();
        batch.dispatch();
        batch.worker_start();
        batch.setup_finished();
        batch.execution_finished();
        batch.finish(Status::Success);
        assert_eq!(
            summaries
                .try_iter()
                .filter(|event| matches!(event, Event::Seal { .. }))
                .count(),
            2
        );
        let projected: Vec<_> = details
            .try_iter()
            .filter_map(|event| {
                if let Event::Span {
                    stage: Stage::VerificationBatch,
                    verification: Some(detail),
                    start_us,
                    end_us,
                    transaction_index,
                    ..
                } = event
                {
                    assert!(detail.is_valid(start_us, end_us));
                    assert_eq!(transaction_index, None);
                    Some(detail)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(projected.len(), 2);
        let mut ids = Vec::new();
        for detail in projected {
            let Detail::Batch {
                id,
                members,
                profiled,
                unprofiled,
                workload,
                ..
            } = detail
            else {
                panic!("batch detail")
            };
            assert_eq!((members, profiled, unprofiled), (4, 3, 1));
            assert_eq!(workload.spends, 8);
            ids.push(id);
        }
        assert_eq!(ids[0], ids[1]);
    }

    #[test]
    fn cache_hit_and_cancelled_future_do_not_fabricate_execution() {
        let (recorder, details, _) = recorder();
        let root = begin_with(&recorder, block()).unwrap();
        let cached = request(&root.context());
        cached.cache(Cache::Hit);
        cached.finish(Status::Success);
        let cancelled = request(&root.context());
        let guard = cancelled.guard();
        drop(guard);
        assert!(cancelled.context().0.is_none());
        let evidence: Vec<_> = details
            .try_iter()
            .filter_map(|event| match event {
                Event::Span {
                    verification: Some(detail),
                    ..
                } => Some(detail),
                _ => None,
            })
            .collect();
        assert_eq!(evidence.len(), 2);
        assert!(matches!(
            evidence[0],
            Detail::Request {
                cache: Cache::Hit,
                status: Status::Success,
                primary_batch: None,
                fallback_batch: None,
                ..
            }
        ));
        assert!(matches!(
            evidence[1],
            Detail::Request {
                status: Status::Abandoned,
                ..
            }
        ));
    }

    #[test]
    fn fallback_after_admission_failure_and_publication_delivery_are_explicit() {
        let (recorder, details, _) = recorder();
        let root = begin_with(&recorder, block()).unwrap();
        let request = request(&root.context());
        request.fallback();
        let mut batch = Batch::with_recorder(&recorder);
        batch.add(&request, Pool::Sapling, Workload::default());
        request.prepared();
        batch.flush_requested();
        batch.dispatch();
        batch.worker_start();
        batch.setup_finished();
        batch.execution_finished();
        batch.finish(Status::Success);
        request.finish(Status::Success);
        let evidence: Vec<_> = details.try_iter().collect();
        assert!(evidence.iter().any(|event| matches!(
            event,
            Event::Span {
                verification: Some(Detail::Request {
                    fallback: true,
                    primary_batch: None,
                    fallback_batch: Some(_),
                    status: Status::Success,
                    ..
                }),
                ..
            }
        )));
        assert_eq!(
            evidence
                .iter()
                .filter(|event| matches!(
                    event,
                    Event::Span {
                        stage: Stage::VerificationDelivery,
                        ..
                    }
                ))
                .count(),
            1
        );
        for event in evidence {
            if let Event::Span {
                verification: Some(detail),
                start_us,
                end_us,
                ..
            } = event
            {
                assert!(detail.is_valid(start_us, end_us));
            }
        }
    }

    #[test]
    fn abandoned_batch_does_not_claim_result_publication() {
        let (recorder, details, _) = recorder();
        let root = begin_with(&recorder, block()).unwrap();
        let request = request(&root.context());
        let mut batch = Batch::with_recorder(&recorder);
        batch.add(&request, Pool::Sapling, Workload::default());
        drop(batch);
        request.finish(Status::Abandoned);
        let events: Vec<_> = details.try_iter().collect();
        assert!(events.iter().any(|event| matches!(
            event,
            Event::Span {
                verification: Some(Detail::Batch {
                    published_us: None,
                    status: Status::Abandoned,
                    partial: true,
                    ..
                }),
                ..
            }
        )));
        assert!(!events.iter().any(|event| matches!(
            event,
            Event::Span {
                stage: Stage::VerificationDelivery,
                ..
            }
        )));
    }

    #[test]
    fn owner_cap_marks_request_evidence_partial_without_blocking() {
        let (recorder, details, _) = recorder();
        let mut batch = Batch::with_recorder(&recorder);
        for _ in 0..=MAX_OWNERS {
            let root = begin_with(&recorder, block()).unwrap();
            let request = request(&root.context());
            batch.add(&request, Pool::Sapling, Workload::default());
            request.finish(Status::Success);
        }
        assert_eq!(batch.0.as_ref().unwrap().owners.len(), MAX_OWNERS);
        batch.finish(Status::Failed);
        let events: Vec<_> = details.try_iter().collect();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    Event::Span {
                        verification: Some(Detail::Request { partial: true, .. }),
                        ..
                    }
                ))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    Event::Span {
                        verification: Some(Detail::Batch { partial: true, .. }),
                        ..
                    }
                ))
                .count(),
            MAX_OWNERS
        );
    }

    #[test]
    fn disabled_capture_and_capacity_reservation_stay_bounded() {
        let request = Request::new(Pool::Sapling, Workload::default());
        assert!(request.0.is_none());
        let used = AtomicUsize::new(0);
        assert!(reserve(&used, 1));
        assert!(!reserve(&used, 1));
        assert_eq!(used.load(Ordering::Relaxed), 1);
        assert!(
            std::mem::size_of::<Event>() * (DETAIL_CAPACITY + SUMMARY_CAPACITY) < 64 * 1024 * 1024
        );
    }

    #[test]
    fn maximal_evidence_fits_datagram_and_bounded_chunk() {
        let detail = Detail::Batch {
            id: u64::MAX,
            workload: Workload {
                spends: u32::MAX,
                outputs: u32::MAX,
                actions: u32::MAX,
            },
            members: u32::MAX,
            profiled: u32::MAX,
            unprofiled: u32::MAX,
            sapling: u32::MAX,
            orchard: u32::MAX,
            ironwood: u32::MAX,
            flush_us: Some(u64::MAX),
            dispatch_us: Some(u64::MAX),
            worker_start_us: Some(u64::MAX),
            setup_end_us: Some(u64::MAX),
            execution_end_us: Some(u64::MAX),
            published_us: Some(u64::MAX),
            status: Status::Abandoned,
            partial: true,
        };
        let event = Event::Span {
            attempt: u64::MAX,
            span: u64::MAX,
            parent: u64::MAX,
            stage: Stage::VerificationBatch,
            transaction_index: Some(u32::MAX),
            transaction_hash: Some([255; 32]),
            start_us: u64::MAX,
            end_us: u64::MAX,
            completion_thread: Some(u64::MAX),
            verification: Some(detail),
        };
        let payload = serde_json::json!({"run":"f".repeat(32),"data":event});
        let bytes = serde_json::to_vec(&payload).unwrap().len();
        assert!(bytes < 8192);
        // Explorer uses 2048 events per chunk; include commas and outer brackets.
        assert!((bytes + 1) * 2048 + 2 < 4 * 1024 * 1024);
    }
}
