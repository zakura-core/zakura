//! Record disjoint pieces of active context instead of overlapping scope envelopes.
//! Losing an inner record leaves a hole, never false attribution to its outer scope.
#[cfg(not(all(test, not(target_os = "linux"))))]
use super::thread_id;
use super::{Context, Event};
use std::{cell::Cell, sync::atomic::Ordering};

const MAX_EXECUTION_RECORDS: u64 = 65_536;
thread_local! { static START: Cell<Option<u64>> = const { Cell::new(None) }; }

pub(super) fn boundary(outgoing: &Context, incoming: &Context) {
    START.with(|start| {
        if let (Some(begin), Some((attempt, span, _))) = (start.take(), &outgoing.0) {
            let end = attempt.recorder.now();
            if end > begin {
                if attempt.execution_records.fetch_add(1, Ordering::Relaxed) < MAX_EXECUTION_RECORDS
                {
                    if let Some(thread) = execution_thread_id() {
                        attempt.recorder.emit(
                            Event::Execution {
                                attempt: attempt.id,
                                span: *span,
                                thread,
                                start_us: begin,
                                end_us: end,
                            },
                            false,
                        );
                    }
                } else {
                    // This counter reports producer loss without changing elapsed-span completeness.
                    attempt.recorder.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        start.set(
            incoming
                .0
                .as_ref()
                .map(|(attempt, _, _)| attempt.recorder.now()),
        );
    });
}

fn execution_thread_id() -> Option<u64> {
    #[cfg(all(test, not(target_os = "linux")))]
    {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        thread_local! { static ID: u64 = NEXT.fetch_add(1, Ordering::Relaxed); }
        Some(ID.with(|id| *id))
    }
    #[cfg(not(all(test, not(target_os = "linux"))))]
    thread_id()
}
