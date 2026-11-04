//! Instrumentation shared by the two pinned Mainnet comparison builds.
//!
//! Context is entered only around synchronous work. Async callers re-enter on every poll,
//! and parallel callers explicitly enter a captured context on the worker. No transaction
//! bytes or additional transaction hashes are needed to correlate observations.

use std::{
    cell::RefCell,
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, LazyLock, Mutex, Weak,
    },
    time::{Duration, Instant},
};

use crate::block::{BlockRef, Hash};

const STAGES: usize = 10;
const OPERATIONS: usize = 8;
static BLOCKS: LazyLock<Mutex<HashMap<Hash, Weak<Data>>>> = LazyLock::new(Default::default);
static REQUESTS: LazyLock<Mutex<HashMap<usize, Weak<Data>>>> = LazyLock::new(Default::default);

thread_local! {
    static CURRENT: RefCell<Option<(Context, Stage)>> = const { RefCell::new(None) };
}

/// The owner of the measured work; parallel task times can overlap.
#[derive(Clone, Copy, Debug)]
pub enum Stage {
    /// Initial block digest computation.
    Initial,
    /// Header transaction-root comparison.
    Merkle,
    /// Transaction verification, including its awaited proof batches.
    Transactions,
    /// Parent and UTXO lookups.
    Parent,
    /// State readiness and submission through the commit response.
    Handoff,
    /// Contextual validation and commit.
    Commit,
    /// Header authorizing-data commitment comparison.
    Commitment,
    /// Finalized storage writes triggered by this block.
    Finalized,
    /// Error-path binding checks.
    Error,
    /// Writer work including finalization after the original response.
    Writer,
}

impl Stage {
    fn name(self) -> &'static str {
        [
            "initial",
            "merkle",
            "transactions",
            "parent",
            "handoff",
            "commit",
            "commitment",
            "finalized",
            "error",
            "writer",
        ][self as usize]
    }
}

/// An executed hashing operation, rather than a count inferred from transaction volume.
#[derive(Clone, Copy, Debug)]
pub enum Operation {
    /// Compute a mined transaction identifier.
    Id,
    /// Compute an authorizing-data digest separately.
    Auth,
    /// Compute both identifiers using the shared decomposition.
    Combined,
    /// Build an authorizing-data Merkle root.
    Root,
    /// Reuse the root supplied by semantic verification.
    RootReuse,
    /// Recompute a missing authorizing-data root.
    RootRecompute,
    /// Signature-hash precomputation, which is not removed by this PR.
    Sighash,
    /// Wall time for the owning stage, not a hashing operation.
    Wall,
}

impl Operation {
    fn name(self) -> &'static str {
        [
            "id",
            "auth",
            "combined",
            "root",
            "root_reuse",
            "root_recompute",
            "sighash",
            "wall",
        ][self as usize]
    }
}

#[derive(Debug, Default)]
struct Cell {
    calls: AtomicU64,
    nanos: AtomicU64,
}

#[derive(Debug)]
struct Data {
    hash: Hash,
    cells: [Cell; STAGES * OPERATIONS],
    contextual: Mutex<HashMap<&'static str, (u64, u64)>>,
    active: AtomicU64,
    ambiguous: AtomicU64,
}

/// Cheap shared handle to one verification attempt's counters.
#[derive(Clone, Debug)]
pub struct Context(Arc<Data>);

impl Context {
    /// Capture the current synchronous context for another worker.
    pub fn current() -> Option<Self> {
        CURRENT.with(|current| {
            current
                .borrow()
                .as_ref()
                .map(|(context, _)| context.clone())
        })
    }

    /// Stage to preserve when forwarding work onto another thread.
    pub fn current_stage() -> Option<Stage> {
        CURRENT.with(|current| current.borrow().as_ref().map(|(_, stage)| *stage))
    }

    /// Number of completed operations in this attempt, useful for checking actual call paths.
    pub fn count(&self, stage: Stage, operation: Operation) -> u64 {
        self.0.cells[stage as usize * OPERATIONS + operation as usize]
            .calls
            .load(Ordering::Acquire)
    }

    /// Find the active semantic verification of a block at a state-service boundary.
    pub fn for_hash(hash: Hash) -> Option<Self> {
        BLOCKS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&hash)
            .and_then(Weak::upgrade)
            .map(Self)
    }

    /// Find a transaction request's block using its shared output-set allocation.
    pub fn for_request<T>(key: &Arc<T>) -> Option<Self> {
        // The address is only an identity token; the allocation is held by the registration.
        let key = Arc::as_ptr(key) as usize;
        REQUESTS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
            .and_then(Weak::upgrade)
            .map(Self)
    }

    /// Bind buffered transaction requests without adding fields to their public request type.
    pub fn bind<T>(&self, key: &Arc<T>) -> Registration<T> {
        // The address is only an identity token; Registration keeps the allocation alive.
        let id = Arc::as_ptr(key) as usize;
        REQUESTS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, Arc::downgrade(&self.0));
        Registration {
            key: key.clone(),
            id,
        }
    }

    /// Enter a synchronous region. Never retain this guard across an await.
    pub fn enter(context: Option<Self>, stage: Stage) -> Scope {
        let previous =
            CURRENT.with(|current| current.replace(context.map(|context| (context, stage))));
        Scope {
            previous,
            thread_bound: std::marker::PhantomData,
        }
    }

    /// Time an entire stage, including asynchronous waiting if the timer spans awaits.
    pub fn stage(&self, stage: Stage) -> Timer {
        Timer::new(Some((self.clone(), stage)), Operation::Wall)
    }

    /// Copy a measured contextual stage into the per-block summary.
    pub fn contextual(name: &'static str, duration: Duration) {
        if let Some(context) = Self::current() {
            let mut rows = context
                .0
                .contextual
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let row = rows.entry(name).or_default();
            row.0 += 1;
            row.1 = row.1.saturating_add(nanos(duration));
        }
    }
}

/// Holds a transaction request's identity allocation until its alias is removed.
#[derive(Debug)]
pub struct Registration<T> {
    key: Arc<T>,
    id: usize,
}

impl<T> Drop for Registration<T> {
    fn drop(&mut self) {
        REQUESTS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
        let _ = &self.key;
    }
}

/// Restores the previous context on exit, including panic unwinding.
#[derive(Debug)]
pub struct Scope {
    previous: Option<(Context, Stage)>,
    thread_bound: std::marker::PhantomData<*mut ()>,
}

impl Drop for Scope {
    fn drop(&mut self) {
        CURRENT.with(|current| {
            current.replace(self.previous.take());
        });
    }
}

/// Counts an actual operation and its elapsed time in the captured context.
#[derive(Debug)]
pub struct Timer {
    context: Option<(Context, Stage)>,
    operation: Operation,
    start: Instant,
}

impl Timer {
    /// Begin an operation in the current synchronous region; unbound work stays unbound.
    pub fn operation(operation: Operation) -> Self {
        let context = CURRENT.with(|current| current.borrow().clone());
        Self::new(context, operation)
    }

    fn new(context: Option<(Context, Stage)>, operation: Operation) -> Self {
        if let Some((context, stage)) = &context {
            if !matches!(
                (stage, operation),
                (Stage::Finalized | Stage::Writer, Operation::Wall)
            ) {
                context.0.active.fetch_add(1, Ordering::Relaxed);
            }
        }
        Self {
            context,
            operation,
            start: Instant::now(),
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        if let Some((context, stage)) = &self.context {
            let cell = &context.0.cells[*stage as usize * OPERATIONS + self.operation as usize];
            cell.calls.fetch_add(1, Ordering::Relaxed);
            cell.nanos
                .fetch_add(nanos(self.start.elapsed()), Ordering::Relaxed);
            if !matches!(
                (stage, self.operation),
                (Stage::Finalized | Stage::Writer, Operation::Wall)
            ) {
                context.0.active.fetch_sub(1, Ordering::AcqRel);
            } else {
                let payload = format!(
                    "{{\"hash\":\"{}\",\"phase\":\"{}\",\"duration_ns\":{}}}",
                    context.0.hash,
                    stage.name(),
                    nanos(self.start.elapsed())
                );
                tracing::info!(target: "zakura_block_perf", %payload, "block_perf_phase");
            }
        }
    }
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// One attempt's lifetime and immutable workload metadata; Drop marks abandoned attempts.
#[derive(Debug)]
pub struct Session {
    context: Context,
    start: Instant,
    height: Option<u32>,
    mix: [usize; 8],
    route: &'static str,
    outcome: &'static str,
    semantic_ns: Option<u64>,
}

impl Session {
    /// Register an already identified body without computing any transaction identifiers.
    pub fn new(hash: Hash, block: &BlockRef, route: &'static str) -> Self {
        let context = Context(Arc::new(Data {
            hash,
            cells: std::array::from_fn(|_| Cell::default()),
            contextual: Mutex::new(HashMap::new()),
            active: AtomicU64::new(0),
            ambiguous: AtomicU64::new(0),
        }));
        if let Some(previous) = BLOCKS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(hash, Arc::downgrade(&context.0))
            .and_then(|previous| previous.upgrade())
        {
            // Concurrent deliveries of the same header cannot be uniquely attributed by state.
            previous.ambiguous.store(1, Ordering::Release);
            context.0.ambiguous.store(1, Ordering::Release);
        }
        let mut mix = [0; 8];
        for tx in block.transactions() {
            mix[0] += 1;
            mix[1] += usize::from(tx.version() < 5);
            mix[2] += usize::from(tx.version() == 5);
            mix[3] += usize::from(tx.version() == 6);
            mix[4] += usize::from(tx.has_sapling_shielded_data());
            mix[5] += usize::from(tx.orchard_shielded_data().is_some());
            mix[6] += usize::from(tx.has_sprout_joinsplit_data());
            mix[7] += usize::from(tx.has_ironwood_shielded_data());
        }
        let session = Self {
            context,
            start: Instant::now(),
            height: block.coinbase_height().map(|h| h.0),
            mix,
            route,
            outcome: "abandoned",
            semantic_ns: None,
        };
        tracing::info!(target: "zakura_block_perf", hash = %hash, route, "block_perf_start");
        session
    }

    /// Shared counters for work dispatched by this attempt.
    pub fn context(&self) -> Context {
        self.context.clone()
    }
    /// Mark completion of semantic validation before state submission.
    pub fn semantic_done(&mut self) {
        self.semantic_ns = Some(nanos(self.start.elapsed()));
    }
    /// Distinguish ordinary, proposal, duplicate, and reused routes.
    pub fn route(&mut self, route: &'static str) {
        self.route = route;
    }
    /// Record the attempt's result before the lifetime ends.
    pub fn finish(&mut self, accepted: bool) {
        self.outcome = if accepted { "accepted" } else { "error" };
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let mut fields = vec![format!("\"hash\":\"{}\",\"height\":{},\"route\":\"{}\",\"outcome\":\"{}\",\"total_ns\":{},\"semantic_ns\":{},\"active\":{},\"tx\":{},\"pre_v5\":{},\"v5\":{},\"v6\":{},\"sapling\":{},\"orchard\":{}", self.context.0.hash, self.height.map(|height| height.to_string()).unwrap_or_else(|| "null".into()), self.route, self.outcome, nanos(self.start.elapsed()), self.semantic_ns.map(|n| n.to_string()).unwrap_or_else(|| "null".into()), self.context.0.active.load(Ordering::Acquire), self.mix[0], self.mix[1], self.mix[2], self.mix[3], self.mix[4], self.mix[5])];
        fields.push(format!(
            "\"schema\":1,\"sprout\":{},\"ironwood\":{}",
            self.mix[6], self.mix[7]
        ));
        fields.push(format!(
            "\"ambiguous\":{}",
            self.context.0.ambiguous.load(Ordering::Acquire)
        ));
        for stage in [
            Stage::Initial,
            Stage::Merkle,
            Stage::Transactions,
            Stage::Parent,
            Stage::Handoff,
            Stage::Commit,
            Stage::Commitment,
            Stage::Finalized,
            Stage::Error,
            Stage::Writer,
        ] {
            for operation in [
                Operation::Id,
                Operation::Auth,
                Operation::Combined,
                Operation::Root,
                Operation::RootReuse,
                Operation::RootRecompute,
                Operation::Sighash,
                Operation::Wall,
            ] {
                let cell = &self.context.0.cells[stage as usize * OPERATIONS + operation as usize];
                fields.push(format!(
                    "\"{}_{}_calls\":{},\"{}_{}_ns\":{}",
                    stage.name(),
                    operation.name(),
                    cell.calls.load(Ordering::Relaxed),
                    stage.name(),
                    operation.name(),
                    cell.nanos.load(Ordering::Relaxed)
                ));
            }
        }
        let contextual = self
            .context
            .0
            .contextual
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for (name, (calls, ns)) in contextual.iter() {
            fields.push(format!("\"{name}_calls\":{calls},\"{name}_ns\":{ns}"));
        }
        tracing::info!(target: "zakura_block_perf", payload = %format!("{{{}}}", fields.join(",")), "block_perf_finish");
        let mut blocks = BLOCKS.lock().unwrap_or_else(|e| e.into_inner());
        if blocks
            .get(&self.context.0.hash)
            .is_some_and(|weak| weak.ptr_eq(&Arc::downgrade(&self.context.0)))
        {
            blocks.remove(&self.context.0.hash);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{block::Block, serialization::ZcashDeserializeInto};

    fn block() -> Block {
        zakura_test::vectors::BLOCK_MAINNET_1687107_BYTES
            .as_slice()
            .zcash_deserialize_into()
            .expect("the Mainnet fixture decodes")
    }

    #[test]
    fn digest_observations_count_executed_operations_and_restore_scope() {
        let block = block();
        let session = Session::new(Hash([11; 32]), &block, "test");
        let context = session.context();
        let tx = block
            .transactions()
            .find(|tx| tx.version() == 5)
            .expect("fixture has a v5 transaction");
        let expected = tx.txid_and_auth_digest();
        {
            let _scope = Context::enter(Some(context.clone()), Stage::Initial);
            assert_eq!(tx.txid_and_auth_digest(), expected);
            assert_eq!(tx.hash(), expected.0);
            assert_eq!(tx.auth_digest(), expected.1);
        }
        assert!(Context::current().is_none());
        assert_eq!(context.count(Stage::Initial, Operation::Combined), 1);
        assert_eq!(context.count(Stage::Initial, Operation::Id), 1);
        assert_eq!(context.count(Stage::Initial, Operation::Auth), 1);
        assert_eq!(context.0.active.load(Ordering::Acquire), 0);
    }

    #[test]
    fn authorizing_digest_observations_follow_parallel_workers() {
        let block = block();
        let session = Session::new(Hash([12; 32]), &block, "test");
        let context = session.context();
        let expected = block.auth_data_root();
        let v5_count = block.transactions().filter(|tx| tx.version() >= 5).count();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        pool.install(|| {
            let _scope = Context::enter(Some(context.clone()), Stage::Commitment);
            assert_eq!(block.auth_data_root(), expected);
        });
        assert_eq!(
            context.count(Stage::Commitment, Operation::Auth),
            u64::try_from(v5_count).unwrap()
        );
        assert_eq!(context.count(Stage::Commitment, Operation::Root), 1);
        assert_eq!(context.count(Stage::Initial, Operation::Auth), 0);
    }

    #[test]
    fn buffered_request_alias_keeps_its_allocation_alive_and_expires() {
        let block = block();
        let session = Session::new(Hash([13; 32]), &block, "test");
        let key = Arc::new(7);
        let registration = session.context().bind(&key);
        assert!(Arc::ptr_eq(
            &Context::for_request(&key).unwrap().0,
            &session.context.0
        ));
        drop(registration);
        assert!(Context::for_request(&key).is_none());
    }

    #[test]
    fn same_header_concurrent_attempts_are_marked_ambiguous() {
        let block = block();
        let first = Session::new(Hash([14; 32]), &block, "ordinary");
        let second = Session::new(Hash([14; 32]), &block, "duplicate");
        assert_eq!(first.context.0.ambiguous.load(Ordering::Acquire), 1);
        assert_eq!(second.context.0.ambiguous.load(Ordering::Acquire), 1);
        drop(first);
        assert!(Context::for_hash(Hash([14; 32])).is_some());
        drop(second);
        assert!(Context::for_hash(Hash([14; 32])).is_none());
    }
}
