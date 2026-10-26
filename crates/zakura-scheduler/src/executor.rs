//! The scheduler as `zakura-rt`'s sim executor.
//!
//! [`Scheduler::new`](crate::Scheduler::new) installs [`EXECUTOR`] for the
//! length of its run. Each method forwards to the [`hooks`](crate::hooks),
//! converting between `zakura-rt`'s ids and the scheduler's.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use zakura_rt::sim as rt;

use crate::{
    hooks::{self, JoinHandle},
    scheduler::with_run,
    DrawKind, ObjectId, Op, Point, SimInstant, TaskExit, TaskId, TimerId,
};

/// The executor every run installs. It holds no state, since the run's state
/// lives in the scheduler's thread-local.
pub(crate) static EXECUTOR: SchedulerExecutor = SchedulerExecutor;

/// Forwards `zakura-rt`'s executor calls to the running scheduler.
pub(crate) struct SchedulerExecutor;

impl rt::Executor for SchedulerExecutor {
    fn preempt(&self, point: rt::Point) {
        hooks::preempt(Point {
            op: op_from_rt(point.op),
            object: point.object.map(object_from_rt),
            site: point.site,
        });
    }

    fn park(&self, wait: rt::ObjectId, held_by: Option<rt::TaskId>) -> bool {
        hooks::park(object_from_rt(wait), held_by.map(task_from_rt))
    }

    fn unpark_all(&self, wait: rt::ObjectId) {
        hooks::unpark_all(object_from_rt(wait));
    }

    fn spawn(&self, label: &'static str, task: rt::BoxTask) -> rt::TaskId {
        task_to_rt(hooks::spawn_boxed(label, task))
    }

    fn try_spawn(&self, label: &'static str, task: rt::BoxTask) -> Option<rt::TaskId> {
        hooks::try_spawn_boxed(label, task).map(task_to_rt)
    }

    /// Spawns `task` as a blocking job, which holds no simulated worker.
    fn spawn_blocking(&self, label: &'static str, task: rt::BoxTask) -> rt::TaskId {
        task_to_rt(hooks::spawn_blocking_boxed(label, task))
    }

    fn abort(&self, task: rt::TaskId) {
        hooks::request_abort(task_from_rt(task));
    }

    fn is_finished(&self, task: rt::TaskId) -> bool {
        JoinHandle::new(task_from_rt(task)).is_finished()
    }

    fn poll_join(&self, task: rt::TaskId, cx: &mut Context<'_>) -> Poll<rt::TaskExit> {
        let task = task_from_rt(task);
        Pin::new(&mut JoinHandle::new(task))
            .poll(cx)
            .map(|exit| match exit {
                TaskExit::Completed => rt::TaskExit::Completed,
                TaskExit::Cancelled => rt::TaskExit::Cancelled,
                TaskExit::Panicked => {
                    // The first joiner takes the payload. A second one gets a
                    // stand-in.
                    let payload = with_run(|s| s.panics.remove(&task)).flatten();
                    rt::TaskExit::Panicked(payload.unwrap_or_else(|| {
                        Box::new("the task panicked, and another joiner took the payload")
                    }))
                }
            })
    }

    fn now(&self) -> Duration {
        hooks::now()
            .expect("zakura-rt calls the executor only while it's installed for a run")
            .since_start()
    }

    fn register_timer(&self, deadline: Duration, waker: &Waker) -> rt::TimerId {
        let now =
            hooks::now().expect("zakura-rt calls the executor only while it's installed for a run");
        // A deadline behind the clock, as after `Sleep::reset` to a past
        // instant, fires at the next advance. Left as is, `advance_to` could
        // never reach it.
        let deadline = SimInstant::from_start(deadline).max(now);
        timer_to_rt(hooks::register_timer(deadline, waker))
    }

    fn cancel_timer(&self, timer: rt::TimerId) {
        hooks::cancel_timer(timer_from_rt(timer));
    }

    fn new_object(&self) -> rt::ObjectId {
        object_to_rt(
            hooks::new_object()
                .expect("zakura-rt calls the executor only while it's installed for a run"),
        )
    }

    fn current_task(&self) -> Option<rt::TaskId> {
        hooks::current_task().map(task_to_rt)
    }

    fn set_external_wait(&self, waiting: bool) {
        hooks::set_external_wait(waiting);
    }

    fn draw(&self, site: rt::DrawSite, bound: u64) -> u64 {
        hooks::draw(draw_kind_from_rt(site.kind), site.site, bound)
    }

    /// The run's configured origin, or `zakura-rt`'s default.
    fn utc_origin(&self) -> std::time::SystemTime {
        with_run(|s| s.utc_origin)
            .expect("zakura-rt calls the executor only while it's installed for a run")
            .unwrap_or_else(default_utc_origin)
    }
}

/// `zakura-rt`'s default UTC origin.
fn default_utc_origin() -> std::time::SystemTime {
    std::time::UNIX_EPOCH + rt::DEFAULT_UTC_ORIGIN_SINCE_EPOCH
}

/// Panics, in harness code, if UTC at virtual time `t` doesn't fit in a
/// `SystemTime`, which `Executor::utc_origin` requires the executor to
/// prevent.
pub(crate) fn assert_utc_in_range(origin: Option<std::time::SystemTime>, t: SimInstant) {
    let origin = origin.unwrap_or_else(default_utc_origin);
    assert!(
        origin.checked_add(t.since_start()).is_some(),
        "virtual time {t:?} is past the latest SystemTime above \
         SchedulerConfig::utc_origin {origin:?}"
    );
}

/// The scheduler's draw kind for a `zakura-rt` one.
fn draw_kind_from_rt(kind: rt::DrawKind) -> DrawKind {
    match kind {
        rt::DrawKind::Rng => DrawKind::Rng,
        rt::DrawKind::Select => DrawKind::Select,
        rt::DrawKind::HashSeed => DrawKind::HashSeed,
        rt::DrawKind::StreamMap => DrawKind::StreamMap,
        other => panic!("zakura-scheduler has no counterpart for zakura-rt's {other:?} yet"),
    }
}

/// The scheduler's operation for a `zakura-rt` one.
fn op_from_rt(op: rt::Op) -> Op {
    match op {
        rt::Op::Lock => Op::Lock,
        rt::Op::Release => Op::Release,
        rt::Op::Send => Op::Send,
        rt::Op::Recv => Op::Recv,
        rt::Op::Notify => Op::Notify,
        rt::Op::Notified => Op::Notified,
        rt::Op::Acquire => Op::Acquire,
        rt::Op::Cancel => Op::Cancel,
        rt::Op::Cancelled => Op::Cancelled,
        rt::Op::AtomicLoad => Op::AtomicLoad,
        rt::Op::AtomicStore => Op::AtomicStore,
        rt::Op::AtomicRmw => Op::AtomicRmw,
        rt::Op::Spawn => Op::Spawn,
        rt::Op::Join => Op::Join,
        rt::Op::Abort => Op::Abort,
        rt::Op::Sleep => Op::Sleep,
        rt::Op::Select => Op::Select,
        rt::Op::Watch => Op::Watch,
        other => panic!("zakura-scheduler has no counterpart for zakura-rt's {other:?} yet"),
    }
}

/// A scheduler task id as `zakura-rt` sees it.
fn task_to_rt(task: TaskId) -> rt::TaskId {
    rt::TaskId::new(u64::from(task.0))
}

/// The scheduler task id behind a `zakura-rt` one.
fn task_from_rt(task: rt::TaskId) -> TaskId {
    TaskId(u32::try_from(task.get()).expect("zakura-rt only holds ids this scheduler handed out"))
}

/// A scheduler object id as `zakura-rt` sees it.
fn object_to_rt(object: ObjectId) -> rt::ObjectId {
    rt::ObjectId {
        creator: object.creator.map(task_to_rt),
        ordinal: object.ordinal,
    }
}

/// The scheduler object id behind a `zakura-rt` one.
fn object_from_rt(object: rt::ObjectId) -> ObjectId {
    ObjectId {
        creator: object.creator.map(task_from_rt),
        ordinal: object.ordinal,
    }
}

/// A scheduler timer id as `zakura-rt` sees it.
fn timer_to_rt(timer: TimerId) -> rt::TimerId {
    rt::TimerId::new(timer.deadline.since_start(), timer.seq)
}

/// The scheduler timer id behind a `zakura-rt` one.
fn timer_from_rt(timer: rt::TimerId) -> TimerId {
    TimerId {
        deadline: SimInstant::from_start(timer.deadline()),
        seq: timer.seq(),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, time::Duration};

    use super::*;

    #[test]
    fn every_rt_op_maps_by_name() {
        let mapped: BTreeSet<Op> = rt::Op::ALL
            .into_iter()
            .map(|op| {
                let ours = op_from_rt(op);
                assert_eq!(ours.name(), op.name(), "{op:?} maps to {ours:?}");
                ours
            })
            .collect();
        assert_eq!(mapped.len(), rt::Op::ALL.len(), "no two rt ops alias");
    }

    #[test]
    fn every_rt_draw_kind_maps() {
        let mapped: BTreeSet<DrawKind> = rt::DrawKind::ALL
            .into_iter()
            .map(|kind| {
                let ours = draw_kind_from_rt(kind);
                // The two enums name their variants alike.
                assert_eq!(format!("{ours:?}"), format!("{kind:?}"));
                ours
            })
            .collect();
        assert_eq!(mapped.len(), rt::DrawKind::ALL.len(), "no two kinds alias");
    }

    #[test]
    fn ids_round_trip() {
        for raw in [0, 1, u32::MAX] {
            let task = TaskId(raw);
            assert_eq!(task_from_rt(task_to_rt(task)), task);
            assert_eq!(task_to_rt(task).get(), u64::from(raw));

            let object = ObjectId {
                creator: Some(task),
                ordinal: raw,
            };
            assert_eq!(object_from_rt(object_to_rt(object)), object);
        }
        let setup = ObjectId {
            creator: None,
            ordinal: u32::MAX,
        };
        assert_eq!(object_from_rt(object_to_rt(setup)), setup);

        for (deadline, seq) in [(Duration::ZERO, 0), (Duration::MAX, u64::MAX)] {
            let timer = TimerId {
                deadline: SimInstant::from_start(deadline),
                seq,
            };
            assert_eq!(timer_from_rt(timer_to_rt(timer)), timer);
        }
    }

    #[test]
    #[should_panic(expected = "zakura-rt only holds ids this scheduler handed out")]
    fn oversized_rt_task_id_panics() {
        task_from_rt(rt::TaskId::new(u64::from(u32::MAX) + 1));
    }
}
