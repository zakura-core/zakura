//! Boundary reads for the simulator (M6a, the observation contract, rules
//! 1 and 2). Only test and `zakura_sim` builds have this module.
//!
//! A shared component's owner registers a [`ComponentRead`] when it builds
//! the component. The harness runs every registered read between steps,
//! inside [`zakura_rt::sim::boundary_read`], while every task is stopped.
//! A read holds only a std `Weak` to its owner, made with [`weak`], and
//! looks at the owner's value through the peeks here, which take no
//! scheduling point and never wait for a lock. A lock a stopped task holds
//! reads [`Read::Held`].
//!
//! All std sync code the reads use lives in this file, which has its own
//! `check-rt-imports` allowances. Owners call only these helpers.

use std::{
    cell::Cell,
    collections::BTreeMap,
    net::IpAddr,
    sync::{Arc, Mutex, Weak},
};

use zakura_rt::sync::PoisonError;

use crate::zakura::{ServicePeerDirection, ZakuraConnId, ZakuraPeerId};

/// One boundary read's result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Read<S> {
    /// The owner's value, read with nobody holding it.
    Current(S),
    /// A stopped task holds the owner's lock, so its value may be
    /// mid-change.
    Held {
        /// The holder, for diagnostics.
        holder: String,
    },
    /// The owner's allocation dropped.
    Gone,
}

impl<S> Read<S> {
    /// The read with its current value mapped by `f`.
    pub fn map<T>(self, f: impl FnOnce(S) -> T) -> Read<T> {
        match self {
            Read::Current(value) => Read::Current(f(value)),
            Read::Held { holder } => Read::Held { holder },
            Read::Gone => Read::Gone,
        }
    }
}

/// The state a registered read reports, one variant per component.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OwnerState {
    /// The header chunk budget's counters.
    Budget(BudgetCounters),
    /// The supervisor's map of registered connections, by `conn_id`.
    SupervisorMap(Vec<MapEntry>),
    /// A header-sync service's session table, by `conn_id`.
    HeaderSessions(Vec<SessionEntry>),
}

/// One registered connection in the supervisor's map (M6a-7b).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MapEntry {
    /// The connection's registration ID.
    pub conn_id: ZakuraConnId,
    /// The peer it's registered for.
    pub peer: ZakuraPeerId,
    /// The remote IP it was admitted from, if the transport knew it.
    pub remote_ip: Option<IpAddr>,
}

/// One peer's current session in a header-sync service's session table
/// (M6a-7b).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionEntry {
    /// The peer.
    pub peer: ZakuraPeerId,
    /// The session's connection.
    pub conn_id: ZakuraConnId,
    /// The service session's ID.
    pub session_id: u64,
    /// The connection's direction, as the table holds it.
    pub direction: ServicePeerDirection,
    /// What the service installed the session from, or `None` for an entry
    /// put in the table some other way.
    pub installed: Option<InstallFacts>,
}

/// What a header-sync service installed a session from: its `Peer` and its
/// stream's tap (M6a-7b).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstallFacts {
    /// The capability mask the connection negotiated.
    pub negotiated: u64,
    /// The header stream's version.
    pub stream_version: u16,
    /// The header stream's QUIC stream ID, if a transport tap names it.
    pub quic_stream: Option<u64>,
    /// The connection's direction, from the `Peer`.
    pub direction: ServicePeerDirection,
}

/// The header chunk budget's counters, read under its lock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BudgetCounters {
    /// The headers the budget can hold at once.
    pub capacity: usize,
    /// Headers reserved for requests and not yet owned.
    pub reserved: usize,
    /// Headers owned by staged pages and the calls that hold them.
    pub owned: usize,
}

/// Which component a read is for, with its owner's identities.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum ReadKey {
    /// A reactor's header chunk budget.
    Budget,
    /// A handler's supervisor map.
    SupervisorMap,
    /// The session table of the header-sync service with allocation ID
    /// `service`, counted from 1 per reactor handle.
    HeaderSessions {
        /// The service's allocation ID.
        service: u64,
    },
}

/// A registered boundary read. It must take no scheduling point, never wait
/// for a lock, and drop any strong reference it takes before it returns.
pub type ComponentRead = Box<dyn Fn() -> Read<OwnerState> + Send + Sync>;

/// A std `Weak` to `owner`, the only reference a read may keep.
pub fn weak<T: ?Sized>(owner: &Arc<T>) -> Weak<T> {
    Arc::downgrade(owner)
}

/// A std `Weak` to an observer, which a holder that mustn't keep the
/// observer alive keeps instead, such as a reactor handle whose services
/// register reads after the reactor may be gone.
pub struct WeakObserver<T: ?Sized>(Weak<T>);

impl<T: ?Sized> WeakObserver<T> {
    /// A weak reference to `observer`.
    pub fn new(observer: &Arc<T>) -> Self {
        WeakObserver(weak(observer))
    }

    /// The observer, if it's still alive.
    pub fn upgrade(&self) -> Option<Arc<T>> {
        self.0.upgrade()
    }
}

impl<T: ?Sized> Clone for WeakObserver<T> {
    fn clone(&self) -> Self {
        WeakObserver(self.0.clone())
    }
}

impl<T: ?Sized> std::fmt::Debug for WeakObserver<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("WeakObserver")
    }
}

/// Runs `read` on the owner behind `owner`, or reads [`Read::Gone`] once it
/// dropped. The strong reference is dropped before this returns. No task
/// runs during a boundary read, so it's never the last one.
pub fn upgrade<T: ?Sized, S>(owner: &Weak<T>, read: impl FnOnce(&T) -> Read<S>) -> Read<S> {
    let Some(owner) = owner.upgrade() else {
        return Read::Gone;
    };
    let value = read(&owner);
    drop(owner);
    value
}

/// Runs `f` on `lock`'s value if nobody holds it, with no scheduling point.
///
/// A held lock reads [`Read::Held`]. A poisoned one reads current, as every
/// owner here reads its own lock, since a holder's panic leaves no change
/// half made.
pub fn peek<T, R>(lock: &zakura_rt::sync::Mutex<T>, f: impl FnOnce(&T) -> R) -> Read<R> {
    #[cfg(zakura_sim)]
    {
        use zakura_rt::sim::Peek;

        match lock.peek(f) {
            Peek::Current(value) | Peek::Poisoned(value) => Read::Current(value),
            Peek::Held { holder } => Read::Held {
                holder: holder.map_or_else(
                    || "setup code".to_owned(),
                    |task| format!("task {}", task.get()),
                ),
            },
        }
    }
    #[cfg(not(zakura_sim))]
    {
        use std::sync::TryLockError;

        match lock.try_lock() {
            Ok(guard) => Read::Current(f(&guard)),
            Err(TryLockError::Poisoned(poisoned)) => Read::Current(f(&poisoned.into_inner())),
            Err(TryLockError::WouldBlock) => Read::Held {
                holder: "another thread".to_owned(),
            },
        }
    }
}

/// Runs `f` on an async mutex's value if nobody holds it. Tokio's
/// `try_lock` has no simulator point. It fails while a task holds the
/// guard, or while a released permit waits for its waiter to run. When it
/// succeeds, no waiter is queued, so dropping its guard wakes nobody.
pub fn peek_async<T, R>(lock: &zakura_rt::sync::AsyncMutex<T>, f: impl FnOnce(&T) -> R) -> Read<R> {
    match lock.try_lock() {
        Ok(guard) => Read::Current(f(&guard)),
        Err(_) => Read::Held {
            holder: "a task".to_owned(),
        },
    }
}

/// A coordination atomic's value, read with no scheduling point.
pub fn peek_usize(atomic: &zakura_rt::sync::atomic::AtomicUsize) -> usize {
    #[cfg(zakura_sim)]
    {
        atomic.peek()
    }
    #[cfg(not(zakura_sim))]
    {
        atomic.load(zakura_rt::sync::atomic::Ordering::SeqCst)
    }
}

/// A semaphore's free permits, read with no scheduling point.
pub fn peek_permits(semaphore: &zakura_rt::sync::Semaphore) -> usize {
    #[cfg(zakura_sim)]
    {
        semaphore.peek_available()
    }
    #[cfg(not(zakura_sim))]
    {
        semaphore.available_permits()
    }
}

thread_local! {
    /// The owner callback this thread is running, if any.
    static CALLBACK: Cell<Option<&'static str>> = const { Cell::new(None) };
}

/// Runs `report`, an observer callback an owner makes while it holds its
/// own lock, marked so that [`refuse_reentry`] panics instead of
/// deadlocking if the callback reaches an observed owner.
pub fn owner_callback<R>(owner: &'static str, report: impl FnOnce() -> R) -> R {
    /// Clears the mark when `report` returns or unwinds.
    struct Clear(Option<&'static str>);

    impl Drop for Clear {
        fn drop(&mut self) {
            CALLBACK.with(|callback| callback.set(self.0));
        }
    }

    let _clear = Clear(CALLBACK.with(|callback| callback.replace(Some(owner))));
    report()
}

/// Panics if this thread is inside an [`owner_callback`]. An observed
/// owner calls it before it takes its lock, since a callback that reached
/// it would deadlock or read it mid-change.
///
/// # Panics
///
/// Inside an owner callback.
pub fn refuse_reentry(owner: &'static str) {
    if let Some(callback) = CALLBACK.with(Cell::get) {
        panic!("ReentrantObserver: {callback}'s observer callback reached {owner}");
    }
}

/// A tagged object's phase, derived from memory alone (rule 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Registered, with no owner attached yet.
    Creating,
    /// Its owner is alive.
    Live,
    /// Its owner's last strong reference dropped, and its tag hasn't yet,
    /// since its `Drop` is still running.
    Releasing,
}

/// The live tags of one kind of object, under a std mutex that's never
/// held across anything else.
pub struct Registry<T> {
    entries: Arc<Mutex<Entries<T>>>,
}

/// A registry's entries, by registration order.
struct Entries<T> {
    last: u64,
    by_id: BTreeMap<u64, Entry<T>>,
}

/// One tag's entry: what it describes, and its owner once attached.
struct Entry<T> {
    info: T,
    alive: Option<Box<dyn Fn() -> bool + Send + Sync>>,
}

impl<T> Default for Registry<T> {
    fn default() -> Self {
        Registry {
            entries: Arc::default(),
        }
    }
}

impl<T> Default for Entries<T> {
    fn default() -> Self {
        Entries {
            last: 0,
            by_id: BTreeMap::new(),
        }
    }
}

impl<T> Registry<T> {
    /// Registers `info` and returns its tag, which removes it when dropped.
    /// It's `Creating` until the tag is attached to its owner.
    pub fn register(&self, info: T) -> Tag<T> {
        let mut entries = lock(&self.entries);
        entries.last = entries
            .last
            .checked_add(1)
            .expect("a run tags fewer than 2^64 objects");
        let id = entries.last;
        entries.by_id.insert(id, Entry { info, alive: None });
        Tag {
            entries: weak(&self.entries),
            id,
        }
    }

    /// Each live tag's info and phase, in registration order.
    pub fn list(&self) -> Vec<(T, Phase)>
    where
        T: Clone,
    {
        lock(&self.entries)
            .by_id
            .values()
            .map(|entry| {
                let phase = match &entry.alive {
                    None => Phase::Creating,
                    Some(alive) if alive() => Phase::Live,
                    Some(_) => Phase::Releasing,
                };
                (entry.info.clone(), phase)
            })
            .collect()
    }
}

/// One object's registration. It's the first field of its holder, so it
/// leaves the registry before the thing it describes is released.
pub struct Tag<T> {
    entries: Weak<Mutex<Entries<T>>>,
    id: u64,
}

impl<T> Tag<T> {
    /// Attaches the tag to `owner`, so it's `Live` while `owner` is and
    /// `Releasing` after.
    pub fn attach<O: Send + Sync + 'static>(&self, owner: &Arc<O>) {
        let owner = weak(owner);
        if let Some(entries) = self.entries.upgrade() {
            if let Some(entry) = lock(&entries).by_id.get_mut(&self.id) {
                entry.alive = Some(Box::new(move || owner.strong_count() > 0));
            }
        }
    }
}

impl<T> Drop for Tag<T> {
    fn drop(&mut self) {
        if let Some(entries) = self.entries.upgrade() {
            lock(&entries).by_id.remove(&self.id);
        }
    }
}

impl<T> std::fmt::Debug for Tag<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_tuple("Tag").field(&self.id).finish()
    }
}

/// Locks a diagnostic mutex. Every change completes under it, so a panic
/// can't leave it half changed.
fn lock<T>(mutex: &Mutex<T>) -> impl std::ops::DerefMut<Target = T> + '_ {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::{mpsc, Arc};

    use super::*;

    /// A tag is `Creating` until attached, `Live` while its owner is, and
    /// `Releasing` once the owner's last strong reference drops, until the
    /// tag drops too.
    #[test]
    fn tags_follow_their_owners_phases() {
        let registry = Registry::default();
        let tag = registry.register("lease 1");
        assert_eq!(registry.list(), [("lease 1", Phase::Creating)]);
        let owner = Arc::new(());
        tag.attach(&owner);
        assert_eq!(registry.list(), [("lease 1", Phase::Live)]);
        let other = registry.register("lease 2");
        drop(owner);
        assert_eq!(
            registry.list(),
            [("lease 1", Phase::Releasing), ("lease 2", Phase::Creating)]
        );
        drop(tag);
        assert_eq!(registry.list(), [("lease 2", Phase::Creating)]);
        drop(registry);
        drop(other);
    }

    /// A read on a dropped owner is `Gone`, and a live one hands its value
    /// out without keeping the owner alive.
    #[test]
    fn a_read_holds_only_a_weak() {
        let owner = Arc::new(7);
        let weak = weak(&owner);
        assert_eq!(
            upgrade(&weak, |value| Read::Current(*value)),
            Read::Current(7)
        );
        assert_eq!(Arc::strong_count(&owner), 1);
        drop(owner);
        assert_eq!(
            upgrade(&weak, |value| Read::Current(*value)),
            Read::<i32>::Gone
        );
    }

    /// The value a writer left under the lock, with no other signal, is
    /// what a peek reads, and a lock held on another thread reads `Held`
    /// without blocking.
    #[test]
    fn peek_reads_the_value_a_writer_left() {
        let lock = Arc::new(zakura_rt::sync::Mutex::new(1));
        *lock.lock().expect("never poisoned") = 2;
        assert_eq!(peek(&lock, |value| *value), Read::Current(2));
        let (held, release) = (mpsc::channel(), mpsc::channel::<()>());
        let holder = {
            let lock = lock.clone();
            std::thread::spawn(move || {
                let mut guard = lock.lock().expect("never poisoned");
                *guard = 3;
                held.0.send(()).expect("the test waits");
                release.1.recv().expect("the test releases");
                *guard = 2;
            })
        };
        held.1.recv().expect("the holder locks");
        assert!(matches!(peek(&lock, |value| *value), Read::Held { .. }));
        release.0.send(()).expect("the holder waits");
        holder.join().expect("the holder ends");
        assert_eq!(peek(&lock, |value| *value), Read::Current(2));
    }

    /// The async mutex, atomic and semaphore peeks read their values, and a
    /// held async mutex reads `Held`.
    #[test]
    fn the_other_peeks_read_their_values() {
        let lock = zakura_rt::sync::AsyncMutex::new(4);
        assert_eq!(peek_async(&lock, |value| *value), Read::Current(4));
        let guard = lock.try_lock().expect("free");
        assert!(matches!(
            peek_async(&lock, |value| *value),
            Read::Held { .. }
        ));
        drop(guard);
        let atomic = zakura_rt::sync::atomic::AtomicUsize::new(5);
        assert_eq!(peek_usize(&atomic), 5);
        let semaphore = zakura_rt::sync::Semaphore::new(6);
        assert_eq!(peek_permits(&semaphore), 6);
    }
}
