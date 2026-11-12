//! The evidence stores a harness keeps beside the run's log.
//!
//! A store is view-side evidence: facts a tap, a storage source or the
//! harness records during the run that the log leaves out, which checks,
//! views and tests read. Each is a [`Registered`] cell, which only an
//! [`EvidenceStores`] handle builds, so no store exists outside a handle.
//!
//! A handle freezes every store it built in one hook, when the run's log
//! freezes and before the scheduler's shutdown ([`Sim::on_frozen`]), and
//! takes a snapshot of them all at that point. Shutdown still resumes the
//! run's suspended tasks. A frozen store drops their writes, so what
//! cleanup does can't change what the run ended with, and a second
//! snapshot after [`Sim::run`] returns must equal the first
//! ([`crate::harness::cleanup`]).
//!
//! Each header node has one handle, which its tap attaches to the run
//! ([`EvidenceStores::attach`]). A handle no run attached, such as a unit
//! test's, freezes only when told to.

use std::{
    fmt::Debug,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use serde_json::{json, Value};

use crate::Sim;

/// A store's state, as a value two equal stores give equally. It's what a
/// store's [`Registered::snapshot`] returns, and it reaches the run's full
/// report ([`crate::record::FullReport`]).
pub trait Snapshot {
    /// Everything the store holds, with no live handle, in an order that
    /// doesn't depend on hashing, so two processes give equal values.
    fn snapshot(&self) -> Value;
}

/// `value`'s `Debug` form, as a snapshot of a store that holds no hashed
/// collection and no live handle.
pub(crate) fn debug_snapshot(value: &impl Debug) -> Value {
    Value::String(format!("{value:?}"))
}

/// Each of `items`' `Debug` forms, sorted, as a snapshot of a hashed
/// collection, whose iteration order differs between processes.
pub(crate) fn sorted_snapshot<T: Debug>(items: impl IntoIterator<Item = T>) -> Value {
    let mut items: Vec<String> = items.into_iter().map(|item| format!("{item:?}")).collect();
    items.sort();
    Value::from(items)
}

/// One store's cell: its value, and whether its handle froze it.
struct Slot<T> {
    frozen: bool,
    value: T,
}

/// A store's name and its slot, which [`EvidenceStores`] reaches through
/// [`Store`].
struct Cell<T> {
    name: &'static str,
    slot: Mutex<Slot<T>>,
}

impl<T> Cell<T> {
    /// Locks the slot. Every change completes under the lock, and the
    /// stores call nothing else while it's held.
    fn lock(&self) -> MutexGuard<'_, Slot<T>> {
        self.slot.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What a handle does to each store it built.
trait Store: Send + Sync {
    /// The store's name in the handle's snapshots.
    fn name(&self) -> &'static str;

    /// Drops every later write.
    fn freeze(&self);

    /// [`Snapshot::snapshot`] of the store's value.
    fn snapshot(&self) -> Value;
}

impl<T: Snapshot + Send> Store for Cell<T> {
    /// The name it was registered under.
    fn name(&self) -> &'static str {
        self.name
    }

    /// Sets the slot's frozen flag.
    fn freeze(&self) {
        self.lock().frozen = true;
    }

    /// The value's snapshot.
    fn snapshot(&self) -> Value {
        self.lock().value.snapshot()
    }
}

/// One evidence store, built by an [`EvidenceStores`] handle. Writes made
/// after its handle froze it are dropped. Cloning shares the store.
pub struct Registered<T> {
    cell: Arc<Cell<T>>,
    stores: EvidenceStores,
}

impl<T> Clone for Registered<T> {
    /// Another handle to the same store.
    fn clone(&self) -> Self {
        Registered {
            cell: Arc::clone(&self.cell),
            stores: self.stores.clone(),
        }
    }
}

impl<T> Debug for Registered<T> {
    /// The store's name, not its value, which may be large.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Registered({})", self.cell.name)
    }
}

/// A store's value, locked: read through `Deref`, changed only through
/// [`Guard::live`], which a frozen store refuses.
pub(crate) struct Guard<'a, T>(MutexGuard<'a, Slot<T>>);

impl<T> std::ops::Deref for Guard<'_, T> {
    type Target = T;

    /// The value, for reading.
    fn deref(&self) -> &T {
        &self.0.value
    }
}

impl<T> Guard<'_, T> {
    /// The value, for changing, or `None` once the store is frozen.
    pub(crate) fn live(&mut self) -> Option<&mut T> {
        let slot = &mut *self.0;
        (!slot.frozen).then_some(&mut slot.value)
    }
}

impl<T> Registered<T> {
    /// Locks the store. Every change completes under the lock, and nothing
    /// else is called while it's held.
    pub(crate) fn lock(&self) -> Guard<'_, T> {
        Guard(self.cell.lock())
    }

    /// Runs `read` on the value.
    pub(crate) fn read<R>(&self, read: impl FnOnce(&T) -> R) -> R {
        read(&self.lock())
    }

    /// Runs `write` on the value, unless the store is frozen, which drops
    /// the write and returns `None`.
    pub(crate) fn write<R>(&self, write: impl FnOnce(&mut T) -> R) -> Option<R> {
        self.lock().live().map(write)
    }

    /// Whether the store's handle froze it.
    pub fn is_frozen(&self) -> bool {
        self.cell.lock().frozen
    }

    /// The handle that built the store.
    pub fn stores(&self) -> &EvidenceStores {
        &self.stores
    }
}

impl<T: Snapshot> Registered<T> {
    /// The store's snapshot now.
    pub fn snapshot(&self) -> Value {
        self.cell.lock().value.snapshot()
    }
}

/// What a handle keeps.
#[derive(Default)]
struct Registry {
    /// Each store it built, in registration order.
    stores: Vec<Arc<dyn Store>>,
    /// The run it's attached to, once a tap or storage source attached it.
    attached: Option<u64>,
    /// Every store's snapshot when the handle froze them, once it has.
    frozen: Option<Value>,
}

/// One header node's evidence stores: the only builder of a
/// [`Registered`] store, and what freezes them all with the run's log and
/// takes their snapshot then. Cloning shares the handle.
#[derive(Clone, Default)]
pub struct EvidenceStores(Arc<Mutex<Registry>>);

impl Debug for EvidenceStores {
    /// How many stores it holds.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "EvidenceStores({})",
            self.registry().stores.len()
        )
    }
}

impl EvidenceStores {
    /// Locks the registry. Nothing else is called while it's held, except
    /// a store's own lock, which never takes this one.
    fn registry(&self) -> MutexGuard<'_, Registry> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A new store named `name` holding `value`. One registered after the
    /// handle froze starts frozen, so cleanup can't fill it.
    pub fn register<T: Snapshot + Send + 'static>(
        &self,
        name: &'static str,
        value: T,
    ) -> Registered<T> {
        let mut registry = self.registry();
        let cell = Arc::new(Cell {
            name,
            slot: Mutex::new(Slot {
                frozen: registry.frozen.is_some(),
                value,
            }),
        });
        registry.stores.push(cell.clone());
        Registered {
            cell,
            stores: self.clone(),
        }
    }

    /// Whether `other` is this handle.
    pub fn same(&self, other: &EvidenceStores) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Freezes every store in `sim`'s run once its log freezes
    /// ([`Sim::on_frozen`]). Attaching it to the same run again does
    /// nothing, so a node's tap and its storage source may both attach it.
    ///
    /// # Panics
    ///
    /// If another run attached it, since one node's stores serve one run.
    pub(crate) fn attach(&self, sim: &mut Sim) {
        let run = sim.run_id();
        let first = {
            let mut registry = self.registry();
            match registry.attached {
                Some(attached) => {
                    assert_eq!(attached, run, "one node's evidence stores serve one run");
                    false
                }
                None => {
                    registry.attached = Some(run);
                    true
                }
            }
        };
        if first {
            let stores = self.clone();
            sim.on_frozen(move || stores.freeze());
        }
    }

    /// Freezes every store and keeps their snapshot. Freezing again does
    /// nothing.
    pub(crate) fn freeze(&self) {
        let mut registry = self.registry();
        if registry.frozen.is_none() {
            for store in &registry.stores {
                store.freeze();
            }
            registry.frozen = Some(snapshot_of(&registry.stores));
        }
    }

    /// Every store's snapshot now, in registration order, each named.
    pub fn snapshot(&self) -> Value {
        snapshot_of(&self.registry().stores)
    }

    /// Every store's snapshot when the handle froze them, or `None` before.
    pub fn frozen_snapshot(&self) -> Option<Value> {
        self.registry().frozen.clone()
    }

    /// [`EvidenceStores::frozen_snapshot`], or the snapshot now if the
    /// handle hasn't frozen, for [`crate::property::HasView::snapshot`]:
    /// the two are equal once it has.
    pub fn report_snapshot(&self) -> Value {
        self.frozen_snapshot().unwrap_or_else(|| self.snapshot())
    }

    /// The first store whose snapshot now differs from its snapshot at the
    /// freeze, by name, or why there's nothing to compare: the handle never
    /// froze, or it holds a store the freeze didn't. `None` if every store
    /// is as it froze.
    pub fn drift(&self) -> Option<String> {
        let (Some(frozen), now) = (self.frozen_snapshot(), self.snapshot()) else {
            return Some("the stores never froze".to_owned());
        };
        let (Value::Array(frozen), Value::Array(now)) = (frozen, now) else {
            unreachable!("`snapshot_of` makes an array");
        };
        if frozen.len() != now.len() {
            return Some(format!(
                "{} stores at the freeze, {} after cleanup",
                frozen.len(),
                now.len()
            ));
        }
        frozen
            .iter()
            .zip(&now)
            .find(|(frozen, now)| frozen != now)
            .map(|(_, now)| now["store"].as_str().unwrap_or("?").to_owned())
    }

    /// Adds `store` as [`EvidenceStores::register`] does, for a test's
    /// store whose freeze can be broken.
    #[cfg(test)]
    fn enroll(&self, store: Arc<dyn Store>) {
        self.registry().stores.push(store);
    }
}

/// Each of `stores`' snapshots, named, in order.
fn snapshot_of(stores: &[Arc<dyn Store>]) -> Value {
    Value::from(
        stores
            .iter()
            .map(|store| json!({ "store": store.name(), "snapshot": store.snapshot() }))
            .collect::<Vec<_>>(),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::Value;

    use super::{debug_snapshot, sorted_snapshot, EvidenceStores, Snapshot, Store};
    use crate::{
        harness::cleanup::{compare, Drift},
        MemorySink, NoChecks, ObservationLog, Plan, RunConfig, Sim,
    };

    impl Snapshot for Vec<u64> {
        /// The values, in order.
        fn snapshot(&self) -> Value {
            debug_snapshot(self)
        }
    }

    /// A store whose freeze does nothing, the negative control's.
    struct Unfrozen(Mutex<Vec<u64>>);

    impl Store for Unfrozen {
        /// Its name.
        fn name(&self) -> &'static str {
            "unfrozen"
        }

        /// Nothing: this store's freeze is skipped.
        fn freeze(&self) {}

        /// Its values.
        fn snapshot(&self) -> Value {
            debug_snapshot(&*self.0.lock().expect("no test panics while holding it"))
        }
    }

    /// Runs its write when dropped, as a task stopped partway does when the
    /// scheduler's shutdown drops it.
    struct OnDrop<F: FnMut()>(F);

    impl<F: FnMut()> Drop for OnDrop<F> {
        /// Runs the write.
        fn drop(&mut self) {
            (self.0)();
        }
    }

    /// Runs a run whose one task waits forever holding `cleanup`, which
    /// the scheduler's shutdown drops, with `stores` attached. Returns the
    /// log at the freeze and the run's report.
    fn run_with_cleanup(
        stores: &EvidenceStores,
        cleanup: impl FnMut() + 'static,
    ) -> (ObservationLog, crate::RunReport) {
        let mut sim = Sim::new(RunConfig::default(), Plan::from_fuzz(&[]));
        stores.attach(&mut sim);
        let frozen = std::rc::Rc::new(std::cell::RefCell::new(None));
        sim.on_end({
            let frozen = frozen.clone();
            move |_, _, log| *frozen.borrow_mut() = Some(log.clone())
        });
        let guard = OnDrop(cleanup);
        sim.scheduler().spawn_root("cleanup", async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        let report = sim.run(&mut NoChecks, &mut MemorySink::default());
        let log = frozen
            .borrow_mut()
            .take()
            .expect("`Sim::run` runs its end hooks");
        (log, report)
    }

    /// A registered store drops the write a cleanup task makes after the
    /// freeze, so the stores and the log compare equal after the run.
    #[test]
    fn a_registered_store_keeps_its_freeze_through_cleanup() {
        let stores = EvidenceStores::default();
        let values = stores.register("values", vec![1_u64]);
        let cleaned = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (log, report) = run_with_cleanup(&stores, {
            let (values, cleaned) = (values.clone(), cleaned.clone());
            move || {
                cleaned.store(true, std::sync::atomic::Ordering::Relaxed);
                values.write(|values| values.push(2));
            }
        });
        assert!(
            cleaned.load(std::sync::atomic::Ordering::Relaxed),
            "the shutdown dropped the task, which wrote"
        );
        assert!(values.is_frozen());
        assert_eq!(values.read(Vec::clone), [1]);
        assert_eq!(compare(&stores, &log, &report.log), None);
    }

    /// Negative control: a store registered with its freeze skipped keeps
    /// the cleanup task's write, and the comparison names it.
    #[test]
    fn a_store_whose_freeze_is_skipped_fails_the_comparison() {
        let stores = EvidenceStores::default();
        let _kept = stores.register("values", vec![1_u64]);
        let unfrozen = Arc::new(Unfrozen(Mutex::new(vec![1])));
        stores.enroll(unfrozen.clone());
        let (log, report) = run_with_cleanup(&stores, {
            let unfrozen = unfrozen.clone();
            move || {
                unfrozen
                    .0
                    .lock()
                    .expect("no test panics while holding it")
                    .push(2);
            }
        });
        assert_eq!(
            compare(&stores, &log, &report.log),
            Some(Drift::Store("unfrozen".to_owned()))
        );
    }

    /// Negative control: an append to the log after its freeze fails the
    /// comparison, even with every store unchanged. Nothing in cleanup can
    /// reach the run's log today, since `Sim::run` owns it, so the control
    /// thaws the returned log and appends as such a path would.
    #[test]
    fn an_append_to_the_log_during_cleanup_fails_the_comparison() {
        let stores = EvidenceStores::default();
        let _kept = stores.register("values", vec![1_u64]);
        let (log, mut report) = run_with_cleanup(&stores, || {});
        assert_eq!(compare(&stores, &log, &report.log), None);
        report.log.thaw();
        report.log.push(crate::Observation::TaskWaiting {
            task: "cleanup".to_owned(),
        });
        assert_eq!(
            compare(&stores, &log, &report.log),
            Some(Drift::Log {
                frozen: log.entries().len(),
                returned: log.entries().len() + 1,
            })
        );
    }

    /// A store registered after the freeze starts frozen, and the
    /// comparison counts it as a change.
    #[test]
    fn a_store_registered_after_the_freeze_starts_frozen() {
        let stores = EvidenceStores::default();
        stores.freeze();
        let late = stores.register("late", vec![1_u64]);
        assert_eq!(late.write(|values| values.push(2)), None);
        assert_eq!(late.read(Vec::clone), [1]);
        assert_eq!(
            stores.drift(),
            Some("0 stores at the freeze, 1 after cleanup".to_owned())
        );
    }

    /// A hashed collection's snapshot doesn't depend on its order.
    #[test]
    fn sorted_snapshots_ignore_order() {
        assert_eq!(sorted_snapshot([3, 1, 2]), sorted_snapshot([2, 3, 1]));
    }
}
