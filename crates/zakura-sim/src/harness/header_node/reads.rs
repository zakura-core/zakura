//! The evidence hub's boundary reads (M6a-7a, rule 1 of the observation
//! contract).
//!
//! A shared component's owner registers a read as it builds the component,
//! through its observer, which keeps it in a [`ReadSink`]. At each step
//! boundary, after the step's other records are written and before `Sim`
//! logs its task outcome, [`BoundaryReads`] runs every live read inside
//! [`zakura_rt::sim::boundary_read`], where any use of the executor or a
//! task wake panics. A read that panics fails the run as a harness failure. Each read folds into the view's component,
//! which moves its `fresh_through` to this boundary, and a read that
//! differs from the last one logged is logged as
//! [`Observation::ComponentRead`] with the next `seq`. A read of an owner
//! that dropped is `Gone`, which ends its component, and it isn't run
//! again.
//!
//! The budget's existence evidence is its reactor's first `Settled`, since
//! the reactor builds the budget before it.

use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    rc::Rc,
    sync::{Arc, Mutex, PoisonError},
};

use zakura_network::zakura::diag::{self, ComponentRead, OwnerState, ReadKey};

use super::view::{HeaderSyncView, Reading};
use crate::{
    observe::{ComponentReading, ComponentValue, ReactorFact},
    property::{ComponentKind, Read},
    Observation, ObservationLog,
};

/// The reads an observer was handed, in registration order, waiting for
/// the hub.
pub(crate) type ReadSink = Arc<Mutex<Vec<(ReadKey, ComponentRead)>>>;

thread_local! {
    /// Set while [`without_reads`] runs its closure.
    static READS_OFF: Cell<bool> = const { Cell::new(false) };
}

/// Runs `run` with the boundary reads of every hub built inside it turned
/// off: registrations are dropped, and no component they'd feed is
/// registered, so a run can be compared, or timed, with and without reads.
pub fn without_reads<T>(run: impl FnOnce() -> T) -> T {
    /// Turns reads back on when `run` returns or unwinds.
    struct On;

    impl Drop for On {
        fn drop(&mut self) {
            READS_OFF.with(|off| off.set(false));
        }
    }

    READS_OFF.with(|off| off.set(true));
    let _on = On;
    run()
}

/// One registered read the hub still runs.
struct Live {
    kind: ComponentKind,
    generation: u64,
    read: ComponentRead,
    /// The last read logged, if any.
    last: Option<Read<Reading>>,
    /// The changes logged so far.
    seq: u64,
}

/// The node reactor's registered reads, folded into the node's view.
pub(crate) struct BoundaryReads {
    sink: ReadSink,
    view: Rc<RefCell<HeaderSyncView>>,
    /// The node reactor's tap, whose first `Settled` shows the budget
    /// exists.
    reactor: u64,
    live: Vec<Live>,
    generations: BTreeMap<ReadKey, u64>,
    budget_exists: bool,
    /// Built under [`without_reads`].
    off: bool,
}

impl BoundaryReads {
    /// The reads registered into `sink` by the reactor on tap `reactor`,
    /// folded into `view`.
    pub(crate) fn new(sink: ReadSink, view: Rc<RefCell<HeaderSyncView>>, reactor: u64) -> Self {
        BoundaryReads {
            sink,
            view,
            reactor,
            live: Vec::new(),
            generations: BTreeMap::new(),
            budget_exists: false,
            off: READS_OFF.with(Cell::get),
        }
    }

    /// Takes this step's registrations and existence evidence, then runs
    /// every live read at the boundary, logging each that changed. Call it
    /// once the step's other records are in `log`.
    pub(crate) fn boundary(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        let registered =
            std::mem::take(&mut *self.sink.lock().unwrap_or_else(PoisonError::into_inner));
        if self.off {
            return Ok(());
        }
        let step = log.step();
        let mut view = self.view.borrow_mut();
        for (key, read) in registered {
            let kind = kind(&key)?;
            let generation = self.generations.entry(key).or_default();
            *generation += 1;
            view.register(kind);
            self.live.push(Live {
                kind,
                generation: *generation,
                read,
                last: None,
                seq: 0,
            });
        }
        if !self.budget_exists && self.reactor_settled(log) {
            self.budget_exists = true;
            view.exists(ComponentKind::Budget, step);
        }
        // Each read runs on its own, so one that reaches the runtime fails
        // the run as a harness failure, after the mark is cleared, instead
        // of unwinding through `Sim`.
        let reads: Vec<Result<diag::Read<OwnerState>, String>> = self
            .live
            .iter()
            .map(|live| {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    zakura_rt::sim::boundary_read(|| (live.read)())
                }))
                .map_err(|panic| {
                    format!(
                        "FailedRead: {:?}'s read (generation {}) panicked at step {step}: {}",
                        live.kind,
                        live.generation,
                        panic_message(&*panic)
                    )
                })
            })
            .collect();
        for (live, read) in self.live.iter_mut().zip(reads) {
            let (read, logged) = reading(read?)?;
            if live.last.as_ref() != Some(&read) {
                live.seq += 1;
                log.push(Observation::ComponentRead {
                    component: format!("{:?}", live.kind),
                    generation: live.generation,
                    seq: live.seq,
                    read: logged,
                });
                live.last = Some(read.clone());
            }
            view.read(live.kind, step, read);
        }
        self.live.retain(|live| live.last != Some(Read::Gone));
        Ok(())
    }

    /// Whether the node's reactor reported a `Settled` this step.
    fn reactor_settled(&self, log: &ObservationLog) -> bool {
        log.of_step(log.step()).any(|obs| {
            matches!(obs, Observation::Reactor {
                instance,
                event: ReactorFact::Settled { .. },
                ..
            } if *instance == self.reactor)
        })
    }
}

/// A panic's message, if it has one.
fn panic_message(panic: &(dyn std::any::Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("a non-string panic")
}

/// The component `key` reads.
fn kind(key: &ReadKey) -> Result<ComponentKind, String> {
    match key {
        ReadKey::Budget => Ok(ComponentKind::Budget),
        unknown => Err(format!("UnknownRead: no component reads {unknown:?}")),
    }
}

/// `read` as the view folds it, and as the log holds it.
fn reading(read: diag::Read<OwnerState>) -> Result<(Read<Reading>, ComponentReading), String> {
    Ok(match read {
        diag::Read::Current(OwnerState::Budget(counters)) => (
            Read::Current(Reading::Budget(counters)),
            // Counts of at most 4,000 headers fit u64.
            ComponentReading::Current(ComponentValue::Budget {
                capacity: counters.capacity as u64,
                reserved: counters.reserved as u64,
                owned: counters.owned as u64,
            }),
        ),
        diag::Read::Current(unknown) => {
            return Err(format!("UnknownRead: no component holds {unknown:?}"))
        }
        diag::Read::Held { holder } => (
            Read::Held {
                holder: holder.clone(),
            },
            ComponentReading::Held { holder },
        ),
        diag::Read::Gone => (Read::Gone, ComponentReading::Gone),
    })
}
