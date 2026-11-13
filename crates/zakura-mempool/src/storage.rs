//! Mempool transaction storage.
//!
//! The main struct [`Storage`] holds verified and rejected transactions.
//! [`Storage`] is effectively the data structure of the mempool. Convenient methods to
//! manage it are included.
//!
//! [`Storage`] does not expose a service so it can only be used by other code directly.
//!
//! Storage is generic over `E`, the verification error a
//! [`ExactTipRejectionError::FailedVerification`] rejection caches, so this crate
//! does not depend on the verifier. Time is an argument: every method that reads
//! or writes a chain-scoped rejection takes `now`, and [`Storage::insert`] takes
//! the wall-clock time it stamps on the transaction.

use std::{mem::size_of, time::Duration};

use chrono::{DateTime, Utc};
use zakura_rt::collections::{HashMap, HashSet};

use thiserror::Error;

use zakura_chain::{
    block::Height,
    transaction::{self, zip317, Hash, Transaction, UnminedTx, UnminedTxId, VerifiedUnminedTx},
    transparent,
};
use zakura_node_services::mempool::TransactionDependencies;
use zcash_script::solver;

use self::{eviction_list::EvictionList, verified_set::VerifiedSet};
use crate::{config, error::MempoolError, Instant};

#[cfg(any(test, feature = "proptest-impl"))]
use proptest_derive::Arbitrary;

#[cfg(test)]
pub mod tests;

mod eviction_cost;
mod eviction_list;
#[cfg(any(test, feature = "mempool-bench"))]
mod fixtures;
mod policy;
mod verified_set;

#[cfg(feature = "mempool-bench")]
pub use verified_set::benchmarks::mempool_eviction_benchmarks;
pub use verified_set::{MAX_MEMPOOL_ANCESTORS, MAX_MEMPOOL_PACKAGE_TRANSACTIONS};

/// The size limit for mempool transaction rejection lists per [ZIP-401].
///
/// > The size of RecentlyEvicted SHOULD never exceed `eviction_memory_entries`
/// > entries, which is the constant 40000.
///
/// We use the specified value for all lists for consistency.
///
/// [ZIP-401]: https://zips.z.cash/zip-0401#specification
pub const MAX_EVICTION_MEMORY_ENTRIES: usize = 40_000;

/// Transactions rejected based on transaction authorizing data (scripts, proofs, signatures),
/// lock times, or the fee per unit of cost. These rejections are only valid for the current tip.
///
/// Each committed block clears these rejections, because new blocks can supply missing inputs,
/// and free mempool space.
#[derive(Error, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "proptest-impl"), derive(Arbitrary))]
#[cfg_attr(any(test, feature = "proptest-impl"), proptest(no_bound))]
#[allow(dead_code)]
pub enum ExactTipRejectionError<E> {
    /// The verifier rejected the transaction with error `E`.
    ///
    /// Skip this variant in proptest because the verifier's error is a large enum
    /// that causes stack overflow during arbitrary value generation.
    #[error("transaction did not pass consensus validation: {0}")]
    #[cfg_attr(any(test, feature = "proptest-impl"), proptest(skip))]
    FailedVerification(#[source] E),
    /// The transaction failed a standardness check.
    #[error("transaction did not pass standard validation: {0}")]
    FailedStandard(#[from] NonStandardTransactionError),

    /// A transaction that paid more per unit of cost evicted this transaction from the full
    /// mempool.
    ///
    /// The cost depends on the transaction size, which includes the authorizing data, so the
    /// rejection covers only the exact transaction. A smaller transaction with the same effects
    /// pays more per unit of cost.
    #[error("transaction evicted from the full mempool by a transaction that pays a higher fee")]
    Evicted,

    /// The mempool is full, and the transaction does not pay enough to evict other
    /// transactions.
    ///
    /// Like [`ExactTipRejectionError::Evicted`], this rejection covers only the exact
    /// transaction.
    #[error("transaction rejected because the mempool is full and it pays too low a fee")]
    BelowEvictionCost,
}

/// Transactions rejected based only on their effects (spends, outputs, transaction header).
/// These rejections are only valid for the current tip.
///
/// Each committed block clears these rejections, because new blocks can remove other
/// transactions from the mempool, and free mempool space.
#[derive(Error, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "proptest-impl"), derive(Arbitrary))]
#[allow(dead_code)]
pub enum SameEffectsTipRejectionError {
    /// Another mempool transaction already spends one of its inputs.
    #[error(
        "transaction rejected because another transaction in the mempool has already spent some of \
        its inputs"
    )]
    SpendConflict,

    /// It spends an output that no mempool transaction creates.
    #[error(
        "transaction rejected because it spends missing outputs from \
        another transaction in the mempool"
    )]
    MissingOutput,

    /// The transaction spends outputs of more than [`MAX_MEMPOOL_ANCESTORS`] mempool
    /// transactions, directly or indirectly.
    #[error(
        "transaction rejected because it depends on more than {MAX_MEMPOOL_ANCESTORS} \
        unconfirmed transactions in the mempool"
    )]
    TooManyAncestors,

    /// Adding the transaction would exceed the limit for a connected group of
    /// unconfirmed parents and children.
    #[error(
        "transaction rejected because its connected unconfirmed dependency group \
        would exceed {MAX_MEMPOOL_PACKAGE_TRANSACTIONS} transactions"
    )]
    TooManyPackageTransactions,
}

/// Transactions rejected based only on their effects (spends, outputs, transaction header).
/// These rejections are valid while the current chain continues to grow.
///
/// Rollbacks and network upgrades clear these rejections, because they can lower the tip height,
/// or change the consensus rules.
#[derive(Error, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(any(test, feature = "proptest-impl"), derive(Arbitrary))]
#[allow(dead_code)]
pub enum SameEffectsChainRejectionError {
    /// The best chain reached its expiry height.
    #[error("best chain tip has reached transaction expiry height")]
    Expired,

    /// The best chain spent one of its inputs or revealed one of its nullifiers.
    #[error("transaction inputs were spent, or nullifiers were revealed, in the best chain")]
    DuplicateSpend,

    /// The best chain mined it.
    #[error("transaction was committed to the best chain")]
    Mined,
}

/// Storage error that combines all other specific error types.
#[derive(Error, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "proptest-impl"), derive(Arbitrary))]
#[cfg_attr(any(test, feature = "proptest-impl"), proptest(no_bound))]
#[allow(dead_code)]
pub enum RejectionError<E> {
    /// A rejection for these exact bytes, until the tip changes.
    #[error(transparent)]
    ExactTip(#[from] ExactTipRejectionError<E>),
    /// A rejection for these effects, until the tip changes.
    #[error(transparent)]
    SameEffectsTip(#[from] SameEffectsTipRejectionError),
    /// A rejection for these effects, while the chain keeps growing.
    #[error(transparent)]
    SameEffectsChain(#[from] SameEffectsChainRejectionError),
    /// The transaction is non-standard.
    #[error(transparent)]
    NonStandardTransaction(#[from] NonStandardTransactionError),
}

/// Non-standard transaction error.
#[derive(Error, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(any(test, feature = "proptest-impl"), derive(Arbitrary))]
pub enum NonStandardTransactionError {
    #[error(
        "transaction is {actual_bytes} bytes, exceeding the configured mempool maximum of {max_bytes} bytes"
    )]
    /// It is larger than the configured limit.
    TransactionTooLarge {
        /// Its serialized size.
        actual_bytes: usize,
        /// The configured limit.
        max_bytes: u64,
    },
    /// An output is dust.
    #[error("transaction is dust")]
    IsDust,
    /// An input script is larger than the standard limit.
    #[error("transaction scriptSig is too large")]
    ScriptSigTooLarge,
    /// An input script is not push-only.
    #[error("transaction scriptSig is not push-only")]
    ScriptSigNotPushOnly,
    /// An output script is not a standard kind.
    #[error("transaction scriptPubKey is non-standard")]
    ScriptPubKeyNonStandard,
    /// An output is bare multisig.
    #[error("transaction has a bare multisig output")]
    BareMultiSig,
    /// It has more than one `OP_RETURN` output.
    #[error("transaction has multiple OP_RETURN outputs")]
    MultiOpReturn,
    /// An `OP_RETURN` output is larger than the configured limit.
    #[error("transaction has an OP_RETURN output that exceeds the size limit")]
    DataCarrierTooLarge,
    /// It has more signature operations than the standard limit.
    #[error("transaction has too many signature operations")]
    TooManySigops,
    /// An input is not standard for the output it spends.
    #[error("transaction has non-standard inputs")]
    NonStandardInputs,
}

/// Represents a set of transactions that have been removed from the mempool, either because
/// they were mined, or because they were invalidated by another transaction that was mined.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemovedTransactionIds {
    /// A list of ids for transactions that were removed mined onto the best chain.
    pub mined: HashSet<UnminedTxId>,
    /// A list of ids for transactions that were invalidated by other transactions
    /// that were mined onto the best chain.
    pub invalidated: HashSet<UnminedTxId>,
}

impl RemovedTransactionIds {
    /// Returns the total number of transactions that were removed from the mempool.
    #[cfg(test)]
    pub fn total_len(&self) -> usize {
        self.mined.len() + self.invalidated.len()
    }
}

/// Hold mempool verified and rejected mempool transactions.
///
/// `E` is the verification error a rejection caches. See the module docs.
pub struct Storage<E> {
    /// The set of verified transactions in the mempool.
    verified: VerifiedSet,

    /// The set of transactions rejected due to bad authorizations, or for other
    /// reasons, and their rejection reasons. These rejections only apply to the
    /// current tip.
    ///
    /// Only transactions with the exact [`UnminedTxId`] are invalid.
    tip_rejected_exact: HashMap<UnminedTxId, ExactTipRejectionError<E>>,

    /// A set of transactions rejected for their effects, and their rejection
    /// reasons. These rejections only apply to the current tip.
    ///
    /// Any transaction with the same [`transaction::Hash`] is invalid.
    tip_rejected_same_effects: HashMap<transaction::Hash, SameEffectsTipRejectionError>,

    /// Sets of transactions rejected for their effects, keyed by rejection reason.
    /// These rejections apply until a rollback or network upgrade.
    ///
    /// Any transaction with the same [`transaction::Hash`] is invalid.
    ///
    /// Each [`EvictionList`] bounds its list by size and age.
    chain_rejected_same_effects: HashMap<SameEffectsChainRejectionError, EvictionList>,

    /// The mempool transaction eviction age limit.
    /// Same as [`config::Config::eviction_memory_time`].
    eviction_memory_time: Duration,

    /// Maximum entries retained in each rejection list.
    rejection_list_capacity: usize,

    /// Max total cost of the verified mempool set. A full mempool evicts its cheapest
    /// transactions to make room for a transaction that pays more.
    tx_cost_limit: u64,

    /// Maximum allowed size of OP_RETURN scripts, in bytes.
    max_datacarrier_bytes: u32,

    /// Cost the pool holds outside this storage, which counts against
    /// `tx_cost_limit` too. The pool core keeps transactions hidden while it
    /// verifies them again, and their bodies still take memory.
    outside_cost: u64,
}

impl<E> Drop for Storage<E> {
    fn drop(&mut self) {
        self.verified.clear();
        self.tip_rejected_exact.clear();
        self.tip_rejected_same_effects.clear();
        self.chain_rejected_same_effects.clear();
        update_rejected_metrics(0);
    }
}

impl<E: Clone + std::fmt::Debug> Storage<E> {
    /// Empty storage for `config`.
    #[allow(clippy::field_reassign_with_default)]
    pub fn new(config: &config::Config) -> Self {
        Self::new_with_rejection_list_capacity(config, MAX_EVICTION_MEMORY_ENTRIES)
    }

    /// Constructs storage with a configurable rejection-list capacity.
    ///
    /// Production always uses [`MAX_EVICTION_MEMORY_ENTRIES`]. Unit tests use
    /// smaller capacities to exercise boundary behavior without tens of
    /// thousands of insertions per property-test case.
    pub fn new_with_rejection_list_capacity(
        config: &config::Config,
        rejection_list_capacity: usize,
    ) -> Self {
        assert!(
            rejection_list_capacity > 0,
            "rejection lists must retain at least one entry"
        );

        Self {
            tx_cost_limit: config.tx_cost_limit,
            eviction_memory_time: config.eviction_memory_time,
            rejection_list_capacity,
            max_datacarrier_bytes: config
                .max_datacarrier_bytes
                .unwrap_or(config::DEFAULT_MAX_DATACARRIER_BYTES),
            verified: Default::default(),
            outside_cost: 0,
            tip_rejected_exact: Default::default(),
            tip_rejected_same_effects: Default::default(),
            chain_rejected_same_effects: Default::default(),
        }
    }

    /// Check and reject non-standard transaction.
    ///
    /// Zcashd defines non-consensus standard transaction checks in
    /// <https://github.com/zcash/zcash/blob/v6.11.0/src/policy/policy.cpp#L58-L135>
    ///
    /// This checks are applied before inserting a transaction in `AcceptToMemoryPool`:
    /// <https://github.com/zcash/zcash/blob/v6.11.0/src/main.cpp#L1819>
    ///
    /// Currently, we implement: per-transaction sigops limit, standard input script checks,
    /// input scriptSig size/push-only checks, standard output script checks (including OP_RETURN
    /// limits), and dust checks.
    fn reject_if_non_standard_tx(
        &mut self,
        tx: &VerifiedUnminedTx,
        now: Instant,
    ) -> Result<(), MempoolError<E>> {
        use zcash_script::script::{self, Evaluable as _};

        let transaction = tx.transaction.transaction().as_ref();
        let spent_outputs = &tx.spent_outputs;

        for input in transaction.inputs() {
            let unlock_script = match input {
                transparent::Input::PrevOut { unlock_script, .. } => unlock_script,
                transparent::Input::Coinbase { .. } => continue,
            };

            // Rule: scriptSig size must be within the standard limit.
            if unlock_script.as_raw_bytes().len() > policy::MAX_STANDARD_SCRIPTSIG_SIZE {
                return self.reject_non_standard(
                    tx,
                    now,
                    NonStandardTransactionError::ScriptSigTooLarge,
                );
            }

            let code = script::Code(unlock_script.as_raw_bytes().to_vec());
            // Rule: scriptSig must be push-only.
            if !code.is_push_only() {
                return self.reject_non_standard(
                    tx,
                    now,
                    NonStandardTransactionError::ScriptSigNotPushOnly,
                );
            }
        }

        if !spent_outputs.is_empty() {
            // Validate that spent_outputs aligns with transparent inputs.
            if transaction.inputs().len() != spent_outputs.len() {
                tracing::warn!(
                    inputs = transaction.inputs().len(),
                    spent_outputs = spent_outputs.len(),
                    "spent_outputs length mismatch, rejecting as non-standard"
                );
                return self.reject_non_standard(
                    tx,
                    now,
                    NonStandardTransactionError::NonStandardInputs,
                );
            }

            // Rule: all transparent inputs must pass `AreInputsStandard()` checks:
            // https://github.com/zcash/zcash/blob/v6.11.0/src/policy/policy.cpp#L137
            if !policy::are_inputs_standard(transaction, spent_outputs) {
                return self.reject_non_standard(
                    tx,
                    now,
                    NonStandardTransactionError::NonStandardInputs,
                );
            }

            // Rule: per-transaction sigops (legacy + P2SH) must not exceed the limit.
            // zcashd sums GetLegacySigOpCount + GetP2SHSigOpCount for AcceptToMemoryPool:
            // https://github.com/zcash/zcash/blob/v6.11.0/src/main.cpp#L1819
            let total_sigops = tx.block_sigop_count();
            if total_sigops > policy::MAX_STANDARD_TX_SIGOPS {
                return self.reject_non_standard(
                    tx,
                    now,
                    NonStandardTransactionError::TooManySigops,
                );
            }
        } else {
            // No spent outputs available (e.g. shielded-only transaction).
            // Only check legacy sigops.
            if tx.legacy_sigop_count > policy::MAX_STANDARD_TX_SIGOPS {
                return self.reject_non_standard(
                    tx,
                    now,
                    NonStandardTransactionError::TooManySigops,
                );
            }
        }

        // Rule: outputs must be standard script kinds, with special handling for OP_RETURN.
        let mut data_out_count = 0u32;

        for output in transaction.outputs() {
            let lock_script = &output.lock_script;
            let script_len = lock_script.as_raw_bytes().len();
            let script_kind = policy::standard_script_kind(lock_script);

            match script_kind {
                None => {
                    // Rule: output script must be standard (P2PKH/P2SH/P2PK/multisig/OP_RETURN).
                    return self.reject_non_standard(
                        tx,
                        now,
                        NonStandardTransactionError::ScriptPubKeyNonStandard,
                    );
                }
                Some(solver::ScriptKind::NullData { .. }) => {
                    // Rule: OP_RETURN script size is limited.
                    // Cast is safe: u32 always fits in usize on 32-bit and 64-bit platforms.
                    if script_len > self.max_datacarrier_bytes as usize {
                        return self.reject_non_standard(
                            tx,
                            now,
                            NonStandardTransactionError::DataCarrierTooLarge,
                        );
                    }
                    // Rule: count OP_RETURN outputs to enforce the one-output limit.
                    data_out_count += 1;
                }
                Some(solver::ScriptKind::MultiSig { pubkeys, .. }) => {
                    // Rule: multisig must be at most 3-of-3 for standardness.
                    // Note: This check is technically subsumed by the unconditional BareMultiSig
                    // rejection below, but we keep it to distinguish the two error reasons
                    // (ScriptPubKeyNonStandard for >3 keys vs BareMultiSig for valid multisig).
                    if pubkeys.len() > policy::MAX_STANDARD_MULTISIG_PUBKEYS {
                        return self.reject_non_standard(
                            tx,
                            now,
                            NonStandardTransactionError::ScriptPubKeyNonStandard,
                        );
                    }
                    // Rule: bare multisig outputs are non-standard (fIsBareMultisigStd = false).
                    return self.reject_non_standard(
                        tx,
                        now,
                        NonStandardTransactionError::BareMultiSig,
                    );
                }
                Some(_) => {
                    // Rule: non-OP_RETURN outputs must not be dust.
                    if output.is_dust() {
                        return self.reject_non_standard(
                            tx,
                            now,
                            NonStandardTransactionError::IsDust,
                        );
                    }
                }
            }
        }

        // Rule: only one OP_RETURN output is permitted.
        if data_out_count > 1 {
            return self.reject_non_standard(tx, now, NonStandardTransactionError::MultiOpReturn);
        }

        Ok(())
    }

    /// Rejects a transaction as non-standard, caches the rejection, and returns the mempool error.
    ///
    /// Note: The returned error is `MempoolError::NonStandardTransaction`, while the cached
    /// rejection (via `reject()`) is stored as
    /// `ExactTipRejectionError::FailedStandard`. Callers that later check
    /// `rejection_error()` will get `MempoolError::StorageExactTip(FailedStandard(...))`.
    fn reject_non_standard(
        &mut self,
        tx: &VerifiedUnminedTx,
        now: Instant,
        rejection_error: NonStandardTransactionError,
    ) -> Result<(), MempoolError<E>> {
        self.reject(tx.transaction.id(), rejection_error.clone().into(), now);
        Err(MempoolError::NonStandardTransaction(rejection_error))
    }

    /// Insert a [`VerifiedUnminedTx`] into the mempool, caching any rejections.
    ///
    /// Accepts the [`VerifiedUnminedTx`] being inserted and `spent_mempool_outpoints`,
    /// a list of transparent inputs of the provided [`VerifiedUnminedTx`] that were found
    /// as newly created transparent outputs in the mempool during transaction verification.
    ///
    /// `outputs` is called with each transparent output the transaction creates, once it
    /// is inserted, so callers waiting for an output can be answered. `time` is stamped on
    /// the transaction as its insertion time.
    ///
    /// Returns an error if the mempool's verified transactions or rejection caches
    /// prevent this transaction from being inserted.
    /// These errors should not be propagated to peers, because the transactions are valid.
    ///
    /// If the mempool is full, this transaction can only evict transactions that pay less. See
    /// [`Storage::insert_with_evicted_ids`].
    #[allow(clippy::unwrap_in_result)]
    pub fn insert(
        &mut self,
        tx: VerifiedUnminedTx,
        spent_mempool_outpoints: Vec<transparent::OutPoint>,
        height: Option<Height>,
        outputs: &mut dyn FnMut(&transparent::OutPoint, &transparent::Output),
        now: Instant,
        time: DateTime<Utc>,
    ) -> Result<UnminedTxId, MempoolError<E>> {
        self.insert_with_evicted_ids(tx, spent_mempool_outpoints, height, outputs, now, time)
            .0
    }

    /// Insert a transaction and return the IDs of the transactions it evicted.
    ///
    /// If the transaction does not fit under the mempool cost limit, the mempool evicts its
    /// cheapest packages first. See [`VerifiedSet::select_eviction_victims`]. The mempool admits
    /// the transaction only if its own eviction cost is at least the highest victim's plus an
    /// increment. See [`exceeds_by_increment`](eviction_cost::EvictionCost::exceeds_by_increment).
    /// Otherwise it rejects the transaction as
    /// [`ExactTipRejectionError::BelowEvictionCost`], and the mempool does not change.
    ///
    /// The mempool rejects each evicted transaction as [`ExactTipRejectionError::Evicted`].
    /// The returned IDs include the victims and their descendants.
    ///
    /// # Fee floor
    ///
    /// While the mempool is full, both the incoming transaction's own fee rate
    /// and its rate combined with all unconfirmed ancestors must exceed every
    /// selected victim's score by the increment. The node does not store a floor.
    /// Events that free space, such as a committed block, expiry, or reorg, can
    /// lower the admission threshold immediately.
    ///
    /// This departs from [ZIP-401], which evicts at random. ZIP-401 chose random eviction when
    /// every transaction paid the same fee. [ZIP-317] fees vary with transaction size, so
    /// ranking by fee protects transactions that pay more. Eviction is relay policy, not
    /// consensus.
    ///
    /// [ZIP-401]: https://zips.z.cash/zip-0401
    /// [ZIP-317]: https://zips.z.cash/zip-0317
    #[allow(clippy::unwrap_in_result)]
    pub fn insert_with_evicted_ids(
        &mut self,
        tx: VerifiedUnminedTx,
        spent_mempool_outpoints: Vec<transparent::OutPoint>,
        height: Option<Height>,
        outputs: &mut dyn FnMut(&transparent::OutPoint, &transparent::Output),
        now: Instant,
        time: DateTime<Utc>,
    ) -> (Result<UnminedTxId, MempoolError<E>>, HashSet<UnminedTxId>) {
        self.insert_checked(
            tx,
            spent_mempool_outpoints,
            height,
            outputs,
            now,
            time,
            true,
        )
    }

    /// [`Self::insert_with_evicted_ids`] for a transaction that was pooled
    /// already and has passed again. The rejection lists are not checked:
    /// another authorization of the same effects may have been refused while
    /// this one was pooled, and that says nothing about this one.
    pub fn reinsert_with_evicted_ids(
        &mut self,
        tx: VerifiedUnminedTx,
        spent_mempool_outpoints: Vec<transparent::OutPoint>,
        height: Option<Height>,
        now: Instant,
        time: DateTime<Utc>,
    ) -> (Result<UnminedTxId, MempoolError<E>>, HashSet<UnminedTxId>) {
        self.insert_checked(
            tx,
            spent_mempool_outpoints,
            height,
            &mut |_, _| {},
            now,
            time,
            false,
        )
    }

    /// Sets the cost the pool holds outside this storage, which counts
    /// against the cost limit.
    pub fn set_outside_cost(&mut self, cost: u64) {
        self.outside_cost = cost;
    }

    /// Inserts `tx`, checking the rejection lists first if
    /// `check_rejections`. See [`Self::insert_with_evicted_ids`].
    #[allow(clippy::unwrap_in_result, clippy::too_many_arguments)]
    fn insert_checked(
        &mut self,
        tx: VerifiedUnminedTx,
        spent_mempool_outpoints: Vec<transparent::OutPoint>,
        height: Option<Height>,
        outputs: &mut dyn FnMut(&transparent::OutPoint, &transparent::Output),
        now: Instant,
        time: DateTime<Utc>,
        check_rejections: bool,
    ) -> (Result<UnminedTxId, MempoolError<E>>, HashSet<UnminedTxId>) {
        // # Security
        //
        // This method must call `reject`, rather than modifying the rejection lists directly.
        let unmined_tx_id = tx.transaction.id();
        let tx_id = unmined_tx_id.mined_id();

        // First, check if we have a cached rejection for this transaction.
        if let Some(error) = check_rejections
            .then(|| self.rejection_error(&unmined_tx_id, now))
            .flatten()
        {
            tracing::trace!(
                ?tx_id,
                ?error,
                stored_transaction_count = ?self.verified.transaction_count(),
                "returning cached error for transaction",
            );

            return (Err(error), HashSet::new());
        }

        // If `tx` is already in the mempool, we don't change anything.
        //
        // Security: transactions must not get refreshed by new queries,
        // because that allows malicious peers to keep transactions live forever.
        if self.verified.contains(&tx_id) {
            tracing::trace!(
                ?tx_id,
                stored_transaction_count = ?self.verified.transaction_count(),
                "returning InMempool error for transaction that is already in the mempool",
            );

            return (Err(MempoolError::InMempool), HashSet::new());
        }

        // Check that the transaction is standard.
        if let Err(error) = self.reject_if_non_standard_tx(&tx, now) {
            return (Err(error), HashSet::new());
        }

        // Check for conflicts before evicting anything, so a rejected transaction leaves the
        // mempool unchanged.
        let ancestors = match self
            .verified
            .check_insert(&tx.transaction, &spent_mempool_outpoints)
        {
            Ok(ancestors) => ancestors,
            Err(rejection_error) => {
                return (
                    Err(self.reject_insert(unmined_tx_id, rejection_error, now)),
                    HashSet::new(),
                );
            }
        };

        let evicted_ids = match self.make_room(&tx, &ancestors, now) {
            Ok(evicted_ids) => evicted_ids,
            Err(rejection_error) => {
                return (
                    Err(self.reject_insert(unmined_tx_id, rejection_error, now)),
                    HashSet::new(),
                );
            }
        };

        // Eviction only removes transactions, and it never removes the transaction's mempool
        // ancestors, so the checks above still hold.
        let result = match self
            .verified
            .insert(tx, spent_mempool_outpoints, outputs, height, time)
        {
            Ok(()) => Ok(unmined_tx_id),
            Err(rejection_error) => Err(self.reject_insert(unmined_tx_id, rejection_error, now)),
        };

        (result, evicted_ids)
    }

    /// Evicts the packages that `tx` must replace to fit under the mempool cost limit.
    ///
    /// `ancestors` are the mempool ancestors of `tx`, which the mempool never evicts for it.
    ///
    /// Returns the IDs of the evicted transactions. Returns an error, and evicts nothing, if
    /// `tx` does not pay enough to evict them.
    fn make_room(
        &mut self,
        tx: &VerifiedUnminedTx,
        ancestors: &HashSet<transaction::Hash>,
        now: Instant,
    ) -> Result<HashSet<UnminedTxId>, ExactTipRejectionError<E>> {
        // What the pool holds outside storage can't be evicted, so it leaves
        // that much less room for what storage holds.
        let limit = self.tx_cost_limit.saturating_sub(self.outside_cost);
        if self.verified.total_cost().saturating_add(tx.cost()) <= limit {
            metrics::gauge!("zcash.mempool.eviction_cost.floor").set(0.0);
            return Ok(HashSet::new());
        }

        let victims = self.verified.select_eviction_victims(tx, ancestors, limit);

        if let Some(cheapest) = victims.cheapest {
            // In zatoshis per `MEMPOOL_TRANSACTION_COST_THRESHOLD` of cost, the increment is
            // one `MARGINAL_FEE`. Metrics tolerate the precision loss of converting to `f64`.
            metrics::gauge!("zcash.mempool.eviction_cost.floor")
                .set(cheapest.zat_per_threshold_cost() + zip317::MARGINAL_FEE as f64);
        }

        let roots = victims.roots.ok_or_else(|| {
            metrics::counter!("zcash.mempool.rejected.below_eviction_cost").increment(1);
            ExactTipRejectionError::BelowEvictionCost
        })?;

        let mut evicted_ids = HashSet::new();
        for root in roots {
            for evicted_tx in self.verified.remove(&root) {
                let evicted_id = evicted_tx.transaction.id();
                self.reject(evicted_id, ExactTipRejectionError::Evicted.into(), now);
                evicted_ids.insert(evicted_id);
            }
        }
        // A `usize` always fits in a `u64` on supported platforms.
        metrics::counter!("zcash.mempool.evicted").increment(evicted_ids.len() as u64);

        Ok(evicted_ids)
    }

    /// Rejects a transaction that failed to insert, and returns the error for the caller.
    fn reject_insert<R>(
        &mut self,
        unmined_tx_id: UnminedTxId,
        rejection_error: R,
        now: Instant,
    ) -> MempoolError<E>
    where
        R: Clone + std::fmt::Debug + Into<RejectionError<E>> + Into<MempoolError<E>>,
    {
        tracing::debug!(
            tx_id = ?unmined_tx_id.mined_id(),
            ?rejection_error,
            stored_transaction_count = ?self.verified.transaction_count(),
            "insertion error for transaction",
        );

        self.reject(unmined_tx_id, rejection_error.clone().into(), now);
        rejection_error.into()
    }

    /// Remove transactions from the mempool via exact [`UnminedTxId`].
    ///
    /// For v5 transactions, transactions are matched by WTXID, using both the:
    /// - non-malleable transaction ID, and
    /// - authorizing data hash.
    ///
    /// This matches the exact transaction, with identical blockchain effects, signatures, and proofs.
    ///
    /// Returns the number of transactions which were removed.
    ///
    /// Removes from the 'verified' set, if present.
    /// Maintains the order in which the other unmined transactions have been inserted into the mempool.
    ///
    /// Does not add or remove from the 'rejected' tracking set.
    #[cfg(test)]
    pub fn remove_exact(&mut self, exact_wtxids: &HashSet<UnminedTxId>) -> usize {
        let tx_ids: Vec<_> = exact_wtxids
            .iter()
            .filter(|id| {
                self.transactions()
                    .get(&id.mined_id())
                    .is_some_and(|tx| tx.transaction.id() == **id)
            })
            .map(UnminedTxId::mined_id)
            .collect();
        self.verified.remove_all(tx_ids).len()
    }

    /// Removes the transaction with mined id `tx_id` and nothing else. What
    /// depends on it stays, no longer counted as its dependent.
    ///
    /// Caches no rejection. Returns the transaction, if it was stored.
    pub fn remove_one(&mut self, tx_id: &transaction::Hash) -> Option<VerifiedUnminedTx> {
        if !self.verified.contains(tx_id) {
            return None;
        }
        self.verified
            .clear_mined_dependencies(&[*tx_id].into_iter().collect());
        self.verified.remove(tx_id).pop()
    }

    /// Removes and returns every stored transaction. Rejections stay.
    pub fn take_all(&mut self) -> Vec<VerifiedUnminedTx> {
        let transactions = self.verified.transactions().values().cloned().collect();
        self.verified.clear();
        transactions
    }

    /// Clears every rejection list, the chain-scoped ones too.
    pub fn clear_rejections(&mut self, now: Instant) {
        self.tip_rejected_exact.clear();
        self.tip_rejected_same_effects.clear();
        self.chain_rejected_same_effects.clear();
        self.update_rejected_metrics(now);
    }

    /// Whether `tx` spends an output or reveals a nullifier that a stored
    /// transaction does.
    pub fn has_spend_conflicts(&self, tx: &UnminedTx) -> bool {
        self.verified.has_spend_conflicts(tx)
    }

    /// Clears a list of mined transaction ids from the verified set's tracked transaction dependencies.
    pub fn clear_mined_dependencies(&mut self, mined_ids: &HashSet<transaction::Hash>) {
        self.verified.clear_mined_dependencies(mined_ids);
    }

    /// Reject and remove transactions from the mempool via non-malleable [`transaction::Hash`].
    /// - For v5 transactions, transactions are matched by TXID,
    ///   using only the non-malleable transaction ID.
    ///   This matches any transaction with the same effect on the blockchain state,
    ///   even if its signatures and proofs are different.
    /// - Returns the number of transactions which were removed.
    /// - Removes from the 'verified' set, if present.
    ///   Maintains the order in which the other unmined transactions have been inserted into the mempool.
    ///
    /// Reject and remove transactions from the mempool that contain any spent outpoints or revealed
    /// nullifiers from the passed in `transactions`.
    ///
    /// Returns the number of transactions that were removed.
    pub fn reject_and_remove_same_effects(
        &mut self,
        mined_ids: &HashSet<transaction::Hash>,
        transactions: Vec<impl AsRef<Transaction>>,
        now: Instant,
    ) -> RemovedTransactionIds {
        let removed_mined = self.verified.remove_all(mined_ids.iter().copied());

        let spent_outpoints: HashSet<_> = transactions
            .iter()
            .flat_map(|tx| tx.as_ref().spent_outpoints())
            .collect();
        let sprout_nullifiers: HashSet<_> = transactions
            .iter()
            .flat_map(|transaction| transaction.as_ref().sprout_nullifiers())
            .collect();
        let sapling_nullifiers: HashSet<_> = transactions
            .iter()
            .flat_map(|transaction| transaction.as_ref().sapling_nullifiers())
            .collect();
        let orchard_nullifiers: HashSet<_> = transactions
            .iter()
            .flat_map(|transaction| transaction.as_ref().orchard_nullifiers())
            .collect();
        let ironwood_nullifiers: HashSet<_> = transactions
            .iter()
            .flat_map(|transaction| transaction.as_ref().ironwood_nullifiers())
            .collect();

        let duplicate_spend_ids: HashSet<_> = self
            .verified
            .transactions()
            .values()
            .map(|tx| (tx.transaction.id(), tx.transaction.transaction()))
            .filter_map(|(tx_id, tx)| {
                (tx.spent_outpoints()
                    .any(|outpoint| spent_outpoints.contains(&outpoint))
                    || tx
                        .sprout_nullifiers()
                        .any(|nullifier| sprout_nullifiers.contains(nullifier))
                    || tx
                        .sapling_nullifiers()
                        .any(|nullifier| sapling_nullifiers.contains(nullifier))
                    || tx
                        .orchard_nullifiers()
                        .any(|nullifier| orchard_nullifiers.contains(nullifier))
                    || tx
                        .ironwood_nullifiers()
                        .any(|nullifier| ironwood_nullifiers.contains(nullifier)))
                .then_some(tx_id)
            })
            .collect();

        let removed_duplicate_spend = self
            .verified
            .remove_all(duplicate_spend_ids.iter().map(UnminedTxId::mined_id));

        for &mined_id in mined_ids {
            self.reject(
                // the reject and rejection_error fns that store and check `SameEffectsChainRejectionError`s
                // only use the mined id, so using `Legacy` ids will apply to v5 transactions as well.
                UnminedTxId::Legacy(mined_id),
                SameEffectsChainRejectionError::Mined.into(),
                now,
            );
        }

        for duplicate_spend_id in duplicate_spend_ids {
            self.reject(
                duplicate_spend_id,
                SameEffectsChainRejectionError::DuplicateSpend.into(),
                now,
            );
        }

        RemovedTransactionIds {
            mined: removed_mined,
            invalidated: removed_duplicate_spend,
        }
    }

    /// Clears the whole mempool storage.
    pub fn clear(&mut self, now: Instant) {
        self.verified.clear();
        self.tip_rejected_exact.clear();
        self.tip_rejected_same_effects.clear();
        self.chain_rejected_same_effects.clear();
        self.update_rejected_metrics(now);
    }

    /// Clears rejections that only apply to the current tip.
    pub fn clear_tip_rejections(&mut self, now: Instant) {
        self.tip_rejected_exact.clear();
        self.tip_rejected_same_effects.clear();
        self.update_rejected_metrics(now);
    }

    /// Clears rejections that only apply to the current tip.
    ///
    /// # Security
    ///
    /// This method must be called at the end of every method that adds rejections.
    /// Otherwise, peers could make our reject lists use a lot of RAM.
    fn limit_rejection_list_memory(&mut self, now: Instant) {
        // These lists are an optimisation - it's ok to totally clear them as needed.
        if self.tip_rejected_exact.len() > self.rejection_list_capacity {
            self.tip_rejected_exact.clear();
        }
        if self.tip_rejected_same_effects.len() > self.rejection_list_capacity {
            self.tip_rejected_same_effects.clear();
        }
        // `chain_rejected_same_effects` limits its size by itself
        self.update_rejected_metrics(now);
    }

    /// Returns the set of [`UnminedTxId`]s in the mempool.
    pub fn tx_ids(&self) -> impl Iterator<Item = UnminedTxId> + '_ {
        self.transactions().values().map(|tx| tx.transaction.id())
    }

    /// Returns a reference to the [`HashMap`] of [`VerifiedUnminedTx`]s in the verified set.
    ///
    /// Each [`VerifiedUnminedTx`] contains an [`UnminedTx`],
    /// and adds extra fields from the transaction verifier result.
    pub fn transactions(&self) -> &HashMap<transaction::Hash, VerifiedUnminedTx> {
        self.verified.transactions()
    }

    /// Returns transactions by descending ancestor-package fee per cost, with
    /// older insertion times and then TXIDs breaking ties.
    ///
    /// Scores include all unconfirmed ancestors. Template selection must account
    /// for parents it has already selected and emit them before their children.
    pub fn mining_transactions(&self) -> impl Iterator<Item = &VerifiedUnminedTx> {
        self.verified.mining_transactions()
    }

    /// Returns a reference to the [`TransactionDependencies`] in the verified set.
    pub fn transaction_dependencies(&self) -> &TransactionDependencies {
        self.verified.transaction_dependencies()
    }

    /// Returns a [`transparent::Output`] created by a mempool transaction for the provided
    /// [`transparent::OutPoint`] if one exists, or None otherwise.
    pub fn created_output(&self, outpoint: &transparent::OutPoint) -> Option<transparent::Output> {
        self.verified.created_output(outpoint)
    }

    /// Returns true if a tx in the set has spent the output at the provided outpoint.
    pub fn has_spent_outpoint(&self, outpoint: &transparent::OutPoint) -> bool {
        self.verified.has_spent_outpoint(outpoint)
    }

    /// Returns the number of transactions in the mempool.
    pub fn transaction_count(&self) -> usize {
        self.verified.transaction_count()
    }

    /// Returns the cost of the transactions in the mempool, according to ZIP-401.
    pub fn total_cost(&self) -> u64 {
        self.verified.total_cost()
    }

    /// The most cost the pool may hold, inside storage and outside it.
    pub fn cost_limit(&self) -> u64 {
        self.tx_cost_limit
    }

    /// Returns the total serialized size of the verified transactions in the set.
    ///
    /// See [`VerifiedSet::total_serialized_size()`] for details.
    pub fn total_serialized_size(&self) -> usize {
        self.verified.total_serialized_size()
    }

    /// Returns the set of [`UnminedTx`]es with exactly matching `tx_ids` in the
    /// mempool.
    ///
    /// This matches the exact transaction, with identical blockchain effects,
    /// signatures, and proofs.
    pub fn transactions_exact(
        &self,
        tx_ids: HashSet<UnminedTxId>,
    ) -> impl Iterator<Item = &UnminedTx> {
        tx_ids.into_iter().filter_map(|tx_id| {
            self.transactions()
                .get(&tx_id.mined_id())
                .map(|tx| &tx.transaction)
                // The verified set is keyed by mined ID, so check the auth digest too.
                .filter(|tx| tx.id() == tx_id)
        })
    }

    /// Returns the set of [`UnminedTx`]es with matching [`transaction::Hash`]es
    /// in the mempool.
    ///
    /// This matches transactions with the same effects, regardless of
    /// [`transaction::AuthDigest`].
    pub fn transactions_same_effects(
        &self,
        tx_ids: HashSet<Hash>,
    ) -> impl Iterator<Item = &UnminedTx> {
        tx_ids
            .into_iter()
            .filter_map(|tx_id| self.transactions().get(&tx_id).map(|tx| &tx.transaction))
    }

    /// Returns a transaction and the transaction ids of its dependencies, if it is in the verified set.
    pub fn transaction_with_deps(
        &self,
        tx_id: transaction::Hash,
    ) -> Option<(VerifiedUnminedTx, HashSet<transaction::Hash>)> {
        let tx = self.verified.transactions().get(&tx_id).cloned()?;
        let deps = self
            .verified
            .transaction_dependencies()
            .dependencies()
            .get(&tx_id)
            .cloned()
            .unwrap_or_default();

        Some((tx, deps))
    }

    /// Returns `true` if a transaction exactly matching an [`UnminedTxId`] is in
    /// the mempool.
    ///
    /// This matches the exact transaction, with identical blockchain effects,
    /// signatures, and proofs.
    pub fn contains_transaction_exact(&self, tx_id: &transaction::Hash) -> bool {
        self.verified.contains(tx_id)
    }

    /// Returns the number of rejected [`UnminedTxId`]s or [`transaction::Hash`]es.
    ///
    /// Transactions on multiple rejected lists are counted multiple times.
    pub fn rejected_transaction_count(&mut self, now: Instant) -> usize {
        self.tip_rejected_exact.len()
            + self.tip_rejected_same_effects.len()
            + self
                .chain_rejected_same_effects
                .iter_mut()
                .map(|(_, map)| map.len(now))
                .sum::<usize>()
    }

    /// Add a transaction to the rejected list for the given reason.
    pub fn reject(&mut self, tx_id: UnminedTxId, reason: RejectionError<E>, now: Instant) {
        match reason {
            RejectionError::ExactTip(e) => {
                self.tip_rejected_exact.insert(tx_id, e);
            }
            RejectionError::SameEffectsTip(e) => {
                self.tip_rejected_same_effects.insert(tx_id.mined_id(), e);
            }
            RejectionError::SameEffectsChain(e) => {
                let eviction_memory_time = self.eviction_memory_time;
                let rejection_list_capacity = self.rejection_list_capacity;
                self.chain_rejected_same_effects
                    .entry(e)
                    .or_insert_with(|| {
                        EvictionList::new(rejection_list_capacity, eviction_memory_time)
                    })
                    .insert(tx_id.mined_id(), now);
            }
            RejectionError::NonStandardTransaction(e) => {
                // Non-standard transactions are rejected based on their exact
                // transaction data.
                self.tip_rejected_exact
                    .insert(tx_id, ExactTipRejectionError::from(e));
            }
        }
        self.limit_rejection_list_memory(now);
    }

    /// Returns the rejection error if a transaction matching an [`UnminedTxId`]
    /// is in any mempool rejected list.
    ///
    /// This matches transactions based on each rejection list's matching rule.
    ///
    /// A transaction in several lists gets the first of: exact tip, effects
    /// tip, expired, duplicate spend, mined.
    pub fn rejection_error(&self, txid: &UnminedTxId, now: Instant) -> Option<MempoolError<E>> {
        if let Some(error) = self.tip_rejected_exact.get(txid) {
            return Some(error.clone().into());
        }

        if let Some(error) = self.tip_rejected_same_effects.get(&txid.mined_id()) {
            return Some(error.clone().into());
        }

        // A fixed order, so a transaction in several lists always gets the
        // same answer.
        for error in [
            SameEffectsChainRejectionError::Expired,
            SameEffectsChainRejectionError::DuplicateSpend,
            SameEffectsChainRejectionError::Mined,
        ] {
            if self
                .chain_rejected_same_effects
                .get(&error)
                .is_some_and(|set| set.contains_key(&txid.mined_id(), now))
            {
                return Some(error.into());
            }
        }

        None
    }

    /// Returns the set of [`UnminedTxId`]s matching `tx_ids` in the rejected list.
    ///
    /// This matches transactions based on each rejection list's matching rule.
    pub fn rejected_transactions(
        &self,
        tx_ids: HashSet<UnminedTxId>,
        now: Instant,
    ) -> impl Iterator<Item = UnminedTxId> + '_ {
        tx_ids
            .into_iter()
            .filter(move |txid| self.contains_rejected(txid, now))
    }

    /// Returns `true` if a transaction matching the supplied [`UnminedTxId`] is in
    /// the mempool rejected list.
    ///
    /// This matches transactions based on each rejection list's matching rule.
    pub fn contains_rejected(&self, txid: &UnminedTxId, now: Instant) -> bool {
        self.rejection_error(txid, now).is_some()
    }

    /// Remove transactions from the mempool if they have not been mined after a
    /// specified height, per [ZIP-203].
    ///
    /// > Transactions will have a new field, nExpiryHeight, which will set the
    /// > block height after which transactions will be removed from the mempool
    /// > if they have not been mined.
    ///
    ///
    /// [ZIP-203]: https://zips.z.cash/zip-0203#specification
    pub fn remove_expired_transactions(
        &mut self,
        tip_height: zakura_chain::block::Height,
        now: Instant,
    ) -> HashSet<UnminedTxId> {
        let mut tx_ids = HashSet::new();

        for (&tx_id, tx) in self.transactions() {
            if let Some(expiry_height) = tx.transaction.transaction().expiry_height() {
                if tip_height >= expiry_height {
                    tx_ids.insert(tx_id);
                }
            }
        }

        // Expiry height is effecting data, so match by non-malleable TXID.
        let removed_tx_ids = self.verified.remove_all(tx_ids.iter().copied());

        // also reject it
        for id in tx_ids {
            self.reject(
                // It's okay to omit the auth digest here as we know that `reject()` will always
                // use mined ids for `SameEffectsChainRejectionError`s.
                UnminedTxId::Legacy(id),
                SameEffectsChainRejectionError::Expired.into(),
                now,
            );
        }

        removed_tx_ids
    }

    /// Check if transaction should be downloaded and/or verified.
    ///
    /// If it is already in the mempool (or in its rejected list)
    /// then it shouldn't be downloaded/verified.
    pub fn should_download_or_verify(
        &mut self,
        txid: UnminedTxId,
        now: Instant,
    ) -> Result<(), MempoolError<E>> {
        // Check if the transaction is already in the mempool.
        if self.contains_transaction_exact(&txid.mined_id()) {
            return Err(MempoolError::InMempool);
        }
        if let Some(error) = self.rejection_error(&txid, now) {
            return Err(error);
        }
        Ok(())
    }

    /// Update metrics related to the rejected lists.
    ///
    /// Must be called every time the rejected lists change.
    fn update_rejected_metrics(&mut self, now: Instant) {
        update_rejected_metrics(self.rejected_transaction_count(now));
    }
}

/// Report the number of rejected ids, and an estimate of their size.
fn update_rejected_metrics(rejected_count: usize) {
    metrics::gauge!("mempool.rejected.transaction.ids",).set(rejected_count as f64);
    // This is just an approximation.
    // TODO: make it more accurate #2869
    let item_size = size_of::<(transaction::Hash, SameEffectsTipRejectionError)>();
    metrics::gauge!("mempool.rejected.transaction.ids.bytes",)
        .set((rejected_count * item_size) as f64);
}
