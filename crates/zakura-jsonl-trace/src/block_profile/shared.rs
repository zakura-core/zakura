//! Batch membership and elapsed execution, without claiming exclusive CPU ownership.
use super::{micros, thread_id, verification::*, Context, Event, Stage};
use std::{
    collections::BTreeSet,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

const MAX_MEMBERS: usize = 1024;
static NEXT_BATCH: AtomicU64 = AtomicU64::new(1);

/// Bounded metadata carried alongside a proof batch. It never owns transaction data.
#[derive(Default)]
pub struct SharedBatch {
    contexts: Vec<(Context, Pool, Workload)>,
    members: u32,
    profiled: u32,
    pools: [u32; 3],
    workload: Workload,
    partial: bool,
    fallback: bool,
}
impl SharedBatch {
    /// Record an item submitted to the real batch, including unprofiled participants.
    pub fn add(&mut self, context: Context, pool: Pool, workload: Workload) {
        self.members = self.members.saturating_add(1);
        let index = match pool {
            Pool::Sapling => 0,
            Pool::Orchard => 1,
            Pool::Ironwood => 2,
        };
        self.pools[index] = self.pools[index].saturating_add(1);
        self.workload.spends = self.workload.spends.saturating_add(workload.spends);
        self.workload.outputs = self.workload.outputs.saturating_add(workload.outputs);
        self.workload.actions = self.workload.actions.saturating_add(workload.actions);
        if context.0.is_some() {
            self.profiled = self.profiled.saturating_add(1);
            if self.contexts.len() < MAX_MEMBERS {
                self.contexts.push((context, pool, workload));
            } else {
                self.partial = true;
            }
        }
    }
    /// Mark preparation that rejected an item after possibly mutating the batch.
    pub fn partial(&mut self) {
        self.partial = true;
    }
    /// Distinguish individual retries from the original combined verification.
    pub fn fallback(&mut self) {
        self.fallback = true;
    }
    /// Measure only execution on the worker, excluding queue wait and publication.
    pub fn measure(self, verify: impl FnOnce() -> bool) -> bool {
        let _unassigned = Context::default().enter();
        if self.contexts.is_empty() {
            return verify();
        }
        let mut running = Running {
            batch: self,
            start: Instant::now(),
            status: Status::Abandoned,
        };
        let result = verify();
        running.status = if result {
            Status::Success
        } else {
            Status::Failed
        };
        result
    }
}
struct Running {
    batch: SharedBatch,
    start: Instant,
    status: Status,
}
impl Drop for Running {
    fn drop(&mut self) {
        let finished = Instant::now();
        let id = NEXT_BATCH.fetch_add(1, Ordering::Relaxed);
        let mut attempts = BTreeSet::new();
        for (context, pool, workload) in &self.batch.contexts {
            let (attempt, _, _) = context
                .0
                .as_ref()
                .expect("retained members have an active context");
            let start = micros(
                self.start
                    .saturating_duration_since(attempt.recorder.started),
            );
            let end = micros(finished.saturating_duration_since(attempt.recorder.started));
            record(
                context,
                Stage::VerificationRequest,
                start,
                end,
                Detail::Request {
                    pool: *pool,
                    workload: *workload,
                    cache: Cache::Unknown,
                    status: self.status,
                    primary_batch: (!self.batch.fallback).then_some(id),
                    fallback_batch: self.batch.fallback.then_some(id),
                    fallback: self.batch.fallback,
                    partial: self.batch.partial,
                },
            );
            // A shared execution is displayed once per participating block, never once per tx.
            if attempts.insert(attempt.id) {
                let root = Context(Some((attempt.clone(), 0, None)));
                record(
                    &root,
                    Stage::VerificationBatch,
                    start,
                    end,
                    Detail::Batch {
                        id,
                        workload: self.batch.workload,
                        members: self.batch.members,
                        profiled: self.batch.profiled,
                        unprofiled: self.batch.members.saturating_sub(self.batch.profiled),
                        sapling: self.batch.pools[0],
                        orchard: self.batch.pools[1],
                        ironwood: self.batch.pools[2],
                        flush_us: None,
                        dispatch_us: None,
                        worker_start_us: Some(start),
                        setup_end_us: None,
                        execution_end_us: Some(end),
                        published_us: None,
                        status: self.status,
                        partial: self.batch.partial,
                    },
                );
            }
        }
    }
}
fn record(context: &Context, stage: Stage, start_us: u64, end_us: u64, detail: Detail) {
    let mut span = context.span(stage);
    span.finished = true;
    if let Some((attempt, id, transaction_index)) = &span.context.0 {
        if !attempt.recorder.emit(
            Event::Span {
                attempt: attempt.id,
                span: *id,
                parent: span.parent,
                stage,
                transaction_index: *transaction_index,
                transaction_hash: None,
                start_us,
                end_us,
                completion_thread: thread_id(),
                verification: Some(detail),
            },
            false,
        ) {
            attempt.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}
