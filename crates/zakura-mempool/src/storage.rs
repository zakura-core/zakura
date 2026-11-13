//! The pool core's storage: the visible pool and the rejection lists.
//!
//! [`Storage`] is plain data owned by the machine. It emits no metrics and no
//! logs. What a metric needs is in [`Storage::stats`], which the core's task
//! reads after each step. Reads take `&self`. Time is an argument: a method
//! that writes or reads a chain-scoped rejection takes `now`, old ones are
//! dropped only by [`Storage::prune_rejections`], and an insertion takes the
//! wall-clock time it stamps on the transaction.
//!
//! A rejection covers either one exact transaction, named by its
//! [`UnminedTxId`], or every transaction with the same effects, named by its
//! mined [`transaction::Hash`]. Each list is written and read by its own id
//! kind, so a lookup cannot use the wrong one:
//!
//! - [`Storage::reject_exact`] and [`Storage::rejected_exact`]: these exact
//!   bytes, until the tip changes.
//! - [`Storage::reject_effects_tip`]: these effects, until the tip changes.
//! - [`Storage::reject_effects_chain`]: these effects, while the chain keeps
//!   growing, one list per reason, each forgetting an id after the eviction
//!   memory time.
//! - [`Storage::rejected_effects`] reads both effects lists.
//!
//! Storage is generic over `E`, the verification error a
//! [`ExactTipRejectionError::FailedVerification`] rejection caches, so this
//! crate does not depend on the verifier.

use std::time::Duration;

use chrono::{DateTime, Utc};
use zakura_rt::collections::{HashMap, HashSet};

use thiserror::Error;

use zakura_chain::{
    block::Height,
    transaction::{self, zip317, UnminedTx, UnminedTxId, VerifiedUnminedTx},
    transparent,
};
use zakura_node_services::mempool::TransactionDependencies;
use zcash_script::solver;

use self::{eviction_list::EvictionList, verified_set::VerifiedSet};
use crate::{config, error::MempoolError, Instant};

#[cfg(test)]
use proptest_derive::Arbitrary;

#[cfg(test)]
pub mod tests;

mod eviction_cost;
mod eviction_list;
#[cfg(test)]
mod fixtures;
mod policy;
mod verified_set;

pub use verified_set::{WeightTotals, MAX_MEMPOOL_ANCESTORS, MAX_MEMPOOL_PACKAGE_TRANSACTIONS};

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
#[cfg_attr(test, derive(Arbitrary))]
#[cfg_attr(test, proptest(no_bound))]
pub enum ExactTipRejectionError<E> {
    /// The verifier rejected the transaction with error `E`.
    ///
    /// Skip this variant in proptest because the verifier's error is a large enum
    /// that causes stack overflow during arbitrary value generation.
    #[error("transaction did not pass consensus validation: {0}")]
    #[cfg_attr(test, proptest(skip))]
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
#[cfg_attr(test, derive(Arbitrary))]
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
#[cfg_attr(test, derive(Arbitrary))]
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

/// A rejection of any kind. Its kind says which id it is cached under: see
/// [`Storage::reject`].
#[derive(Error, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(Arbitrary))]
#[cfg_attr(test, proptest(no_bound))]
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
}

/// A rejection that covers every transaction with the same effects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EffectsRejection {
    /// Until the tip changes.
    Tip(SameEffectsTipRejectionError),
    /// While the chain keeps growing.
    Chain(SameEffectsChainRejectionError),
}

impl<E> From<EffectsRejection> for MempoolError<E> {
    fn from(rejection: EffectsRejection) -> Self {
        match rejection {
            EffectsRejection::Tip(error) => Self::StorageEffectsTip(error),
            EffectsRejection::Chain(error) => Self::StorageEffectsChain(error),
        }
    }
}

/// Non-standard transaction error.
#[derive(Error, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(test, derive(Arbitrary))]
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

/// What storage holds, for the pool's metrics. The core's task emits it under
/// the names today's pool uses.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StorageStats {
    /// Transactions in the visible pool.
    pub transactions: usize,
    /// Their total serialized size, in bytes.
    pub serialized_size: usize,
    /// Their total ZIP-401 cost.
    pub cost: u64,
    /// Their sizes and unpaid actions, by fee weight.
    pub weights: WeightTotals,
    /// Entries in the rejection lists. An id on several lists counts once per
    /// list. Chain-scoped entries count until [`Storage::prune_rejections`]
    /// drops them.
    pub rejected: usize,
    /// The fee rate a transaction had to beat at the last insertion into a
    /// full pool, in zatoshis per [`zakura_chain::transaction::MEMPOOL_TRANSACTION_COST_THRESHOLD`]
    /// of cost, or zero if that insertion fit.
    pub eviction_cost_floor: f64,
    /// Transactions evicted since storage was made.
    pub evicted: u64,
    /// Transactions refused since storage was made because they paid too
    /// little to evict others.
    pub below_eviction_cost: u64,
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

    /// See [`StorageStats::eviction_cost_floor`].
    eviction_cost_floor: f64,

    /// See [`StorageStats::evicted`].
    evicted: u64,

    /// See [`StorageStats::below_eviction_cost`].
    below_eviction_cost: u64,
}

impl<E: Clone> Storage<E> {
    /// Empty storage for `config`.
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
            max_datacarrier_bytes: config.max_datacarrier_bytes,
            verified: Default::default(),
            outside_cost: 0,
            tip_rejected_exact: Default::default(),
            tip_rejected_same_effects: Default::default(),
            chain_rejected_same_effects: Default::default(),
            eviction_cost_floor: 0.0,
            evicted: 0,
            below_eviction_cost: 0,
        }
    }

    // Insertion.

    /// Insert a [`VerifiedUnminedTx`] into the mempool, caching any rejections.
    ///
    /// Accepts the [`VerifiedUnminedTx`] being inserted and `spent_mempool_outpoints`,
    /// a list of transparent inputs of the provided [`VerifiedUnminedTx`] that were found
    /// as newly created transparent outputs in the mempool during transaction verification.
    /// `time` is stamped on the transaction as its insertion time.
    ///
    /// Returns an error if the mempool's verified transactions or rejection caches
    /// prevent this transaction from being inserted.
    /// These errors should not be propagated to peers, because the transactions are valid.
    ///
    /// If the mempool is full, this transaction can only evict transactions that pay less. See
    /// [`Storage::insert_with_evicted_ids`].
    pub fn insert(
        &mut self,
        tx: VerifiedUnminedTx,
        spent_mempool_outpoints: Vec<transparent::OutPoint>,
        height: Option<Height>,
        now: Instant,
        time: DateTime<Utc>,
    ) -> Result<UnminedTxId, MempoolError<E>> {
        self.insert_with_evicted_ids(tx, spent_mempool_outpoints, height, now, time)
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
    pub fn insert_with_evicted_ids(
        &mut self,
        tx: VerifiedUnminedTx,
        spent_mempool_outpoints: Vec<transparent::OutPoint>,
        height: Option<Height>,
        now: Instant,
        time: DateTime<Utc>,
    ) -> (Result<UnminedTxId, MempoolError<E>>, HashSet<UnminedTxId>) {
        self.insert_checked(tx, spent_mempool_outpoints, height, now, time, true)
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
        self.insert_checked(tx, spent_mempool_outpoints, height, now, time, false)
    }

    /// Sets the cost the pool holds outside this storage, which counts
    /// against the cost limit.
    pub fn set_outside_cost(&mut self, cost: u64) {
        self.outside_cost = cost;
    }

    /// Inserts `tx`, checking the rejection lists first if
    /// `check_rejections`. See [`Self::insert_with_evicted_ids`].
    fn insert_checked(
        &mut self,
        tx: VerifiedUnminedTx,
        spent_mempool_outpoints: Vec<transparent::OutPoint>,
        height: Option<Height>,
        now: Instant,
        time: DateTime<Utc>,
        check_rejections: bool,
    ) -> (Result<UnminedTxId, MempoolError<E>>, HashSet<UnminedTxId>) {
        // # Security
        //
        // Every refusal below is cached through `reject_exact` or
        // `reject_effects_tip`, which bound the lists.
        let unmined_tx_id = tx.transaction.id();
        let tx_id = unmined_tx_id.mined_id();

        // First, check if we have a cached rejection for this transaction.
        if let Some(error) = check_rejections
            .then(|| self.rejection(&unmined_tx_id, now))
            .flatten()
        {
            return (Err(error), HashSet::new());
        }

        // If `tx` is already in the mempool, we don't change anything.
        //
        // Security: transactions must not get refreshed by new queries,
        // because that allows malicious peers to keep transactions live forever.
        if self.verified.contains(&tx_id) {
            return (Err(MempoolError::InMempool), HashSet::new());
        }

        // Check that the transaction is standard. The rejection is cached for
        // these exact bytes, and the caller sees which rule it broke.
        if let Err(error) = self.standardness(&tx) {
            self.reject_exact(unmined_tx_id, error.clone().into());
            return (
                Err(MempoolError::NonStandardTransaction(error)),
                HashSet::new(),
            );
        }

        // Check for conflicts before evicting anything, so a rejected transaction leaves the
        // mempool unchanged.
        let ancestors = match self
            .verified
            .check_insert(&tx.transaction, &spent_mempool_outpoints)
        {
            Ok(ancestors) => ancestors,
            Err(error) => {
                self.reject_effects_tip(tx_id, error.clone());
                return (Err(error.into()), HashSet::new());
            }
        };

        let evicted_ids = match self.make_room(&tx, &ancestors) {
            Ok(evicted_ids) => evicted_ids,
            Err(error) => {
                self.reject_exact(unmined_tx_id, error.clone());
                return (Err(error.into()), HashSet::new());
            }
        };

        // Eviction only removes transactions, and it never removes the transaction's mempool
        // ancestors, so the checks above still hold.
        let result = match self
            .verified
            .insert(tx, spent_mempool_outpoints, height, time)
        {
            Ok(()) => Ok(unmined_tx_id),
            Err(error) => {
                self.reject_effects_tip(tx_id, error.clone());
                Err(error.into())
            }
        };

        (result, evicted_ids)
    }

    /// Checks `tx` against zcashd's non-consensus standardness rules, in
    /// zcashd's order, and returns the first it breaks.
    ///
    /// Zcashd defines these checks in
    /// <https://github.com/zcash/zcash/blob/v6.11.0/src/policy/policy.cpp#L58-L135>
    /// and applies them before inserting a transaction in `AcceptToMemoryPool`:
    /// <https://github.com/zcash/zcash/blob/v6.11.0/src/main.cpp#L1819>
    ///
    /// Currently, we implement: per-transaction sigops limit, standard input script checks,
    /// input scriptSig size/push-only checks, standard output script checks (including OP_RETURN
    /// limits), and dust checks.
    pub fn standardness(&self, tx: &VerifiedUnminedTx) -> Result<(), NonStandardTransactionError> {
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
                return Err(NonStandardTransactionError::ScriptSigTooLarge);
            }

            let code = script::Code(unlock_script.as_raw_bytes().to_vec());
            // Rule: scriptSig must be push-only.
            if !code.is_push_only() {
                return Err(NonStandardTransactionError::ScriptSigNotPushOnly);
            }
        }

        if !spent_outputs.is_empty() {
            // Validate that spent_outputs aligns with transparent inputs.
            if transaction.inputs().len() != spent_outputs.len() {
                return Err(NonStandardTransactionError::NonStandardInputs);
            }

            // Rule: all transparent inputs must pass `AreInputsStandard()` checks:
            // https://github.com/zcash/zcash/blob/v6.11.0/src/policy/policy.cpp#L137
            if !policy::are_inputs_standard(transaction, spent_outputs) {
                return Err(NonStandardTransactionError::NonStandardInputs);
            }

            // Rule: per-transaction sigops (legacy + P2SH) must not exceed the limit.
            // zcashd sums GetLegacySigOpCount + GetP2SHSigOpCount for AcceptToMemoryPool:
            // https://github.com/zcash/zcash/blob/v6.11.0/src/main.cpp#L1819
            if tx.block_sigop_count() > policy::MAX_STANDARD_TX_SIGOPS {
                return Err(NonStandardTransactionError::TooManySigops);
            }
        } else {
            // No spent outputs available (e.g. shielded-only transaction).
            // Only check legacy sigops.
            if tx.legacy_sigop_count > policy::MAX_STANDARD_TX_SIGOPS {
                return Err(NonStandardTransactionError::TooManySigops);
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
                    return Err(NonStandardTransactionError::ScriptPubKeyNonStandard);
                }
                Some(solver::ScriptKind::NullData { .. }) => {
                    // Rule: OP_RETURN script size is limited.
                    // Cast is safe: u32 always fits in usize on 32-bit and 64-bit platforms.
                    if script_len > self.max_datacarrier_bytes as usize {
                        return Err(NonStandardTransactionError::DataCarrierTooLarge);
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
                        return Err(NonStandardTransactionError::ScriptPubKeyNonStandard);
                    }
                    // Rule: bare multisig outputs are non-standard (fIsBareMultisigStd = false).
                    return Err(NonStandardTransactionError::BareMultiSig);
                }
                Some(_) => {
                    // Rule: non-OP_RETURN outputs must not be dust.
                    if output.is_dust() {
                        return Err(NonStandardTransactionError::IsDust);
                    }
                }
            }
        }

        // Rule: only one OP_RETURN output is permitted.
        if data_out_count > 1 {
            return Err(NonStandardTransactionError::MultiOpReturn);
        }

        Ok(())
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
    ) -> Result<HashSet<UnminedTxId>, ExactTipRejectionError<E>> {
        // What the pool holds outside storage can't be evicted, so it leaves
        // that much less room for what storage holds.
        let limit = self.tx_cost_limit.saturating_sub(self.outside_cost);
        if self.verified.total_cost().saturating_add(tx.cost()) <= limit {
            self.eviction_cost_floor = 0.0;
            return Ok(HashSet::new());
        }

        let victims = self.verified.select_eviction_victims(tx, ancestors, limit);

        if let Some(cheapest) = victims.cheapest {
            // In zatoshis per `MEMPOOL_TRANSACTION_COST_THRESHOLD` of cost, the increment is
            // one `MARGINAL_FEE`. Metrics tolerate the precision loss of converting to `f64`.
            self.eviction_cost_floor =
                cheapest.zat_per_threshold_cost() + zip317::MARGINAL_FEE as f64;
        }

        let Some(roots) = victims.roots else {
            self.below_eviction_cost = self.below_eviction_cost.saturating_add(1);
            return Err(ExactTipRejectionError::BelowEvictionCost);
        };

        let mut evicted_ids = HashSet::new();
        for root in roots {
            for evicted_tx in self.verified.remove(&root) {
                let evicted_id = evicted_tx.transaction.id();
                self.reject_exact(evicted_id, ExactTipRejectionError::Evicted);
                evicted_ids.insert(evicted_id);
            }
        }
        // A `usize` always fits in a `u64` on supported platforms.
        self.evicted = self.evicted.saturating_add(evicted_ids.len() as u64);

        Ok(evicted_ids)
    }

    // Removal.

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

    // Rejections.

    /// Caches `error` for `tx_id`, under the id its kind is scoped to: the
    /// exact id for an exact-tip rejection, the mined id for an effects one.
    pub fn reject(&mut self, tx_id: UnminedTxId, error: RejectionError<E>, now: Instant) {
        match error {
            RejectionError::ExactTip(error) => self.reject_exact(tx_id, error),
            RejectionError::SameEffectsTip(error) => {
                self.reject_effects_tip(tx_id.mined_id(), error)
            }
            RejectionError::SameEffectsChain(error) => {
                self.reject_effects_chain(tx_id.mined_id(), error, now)
            }
        }
    }

    /// Caches `error` for the exact transaction `tx_id` until the tip changes.
    pub fn reject_exact(&mut self, tx_id: UnminedTxId, error: ExactTipRejectionError<E>) {
        self.tip_rejected_exact.insert(tx_id, error);
        self.limit_rejection_list_memory();
    }

    /// Caches `error` for every transaction with the effects `tx_id` until the
    /// tip changes.
    pub fn reject_effects_tip(
        &mut self,
        tx_id: transaction::Hash,
        error: SameEffectsTipRejectionError,
    ) {
        self.tip_rejected_same_effects.insert(tx_id, error);
        self.limit_rejection_list_memory();
    }

    /// Caches `error` at `now` for every transaction with the effects `tx_id`,
    /// while the chain keeps growing. The list for `error` forgets it after
    /// the eviction memory time, or sooner once it is full.
    pub fn reject_effects_chain(
        &mut self,
        tx_id: transaction::Hash,
        error: SameEffectsChainRejectionError,
        now: Instant,
    ) {
        let eviction_memory_time = self.eviction_memory_time;
        let rejection_list_capacity = self.rejection_list_capacity;
        self.chain_rejected_same_effects
            .entry(error)
            .or_insert_with(|| EvictionList::new(rejection_list_capacity, eviction_memory_time))
            .insert(tx_id, now);
    }

    /// Clears every rejection list, the chain-scoped ones too.
    pub fn clear_rejections(&mut self) {
        self.tip_rejected_exact.clear();
        self.tip_rejected_same_effects.clear();
        self.chain_rejected_same_effects.clear();
    }

    /// Clears rejections that only apply to the current tip.
    pub fn clear_tip_rejections(&mut self) {
        self.tip_rejected_exact.clear();
        self.tip_rejected_same_effects.clear();
    }

    /// Drops the chain-scoped rejections older than the eviction memory time
    /// at `now`. Lookups already ignore them, so this frees their memory and
    /// keeps [`StorageStats::rejected`] exact.
    pub fn prune_rejections(&mut self, now: Instant) {
        for list in self.chain_rejected_same_effects.values_mut() {
            list.prune_old(now);
        }
    }

    /// Clears a tip-scoped list once it holds more than the capacity.
    ///
    /// # Security
    ///
    /// This method must be called at the end of every method that adds
    /// tip-scoped rejections. Otherwise, peers could make our reject lists use
    /// a lot of RAM. Each chain-scoped list bounds itself.
    fn limit_rejection_list_memory(&mut self) {
        // These lists are an optimisation - it's ok to totally clear them as needed.
        if self.tip_rejected_exact.len() > self.rejection_list_capacity {
            self.tip_rejected_exact.clear();
        }
        if self.tip_rejected_same_effects.len() > self.rejection_list_capacity {
            self.tip_rejected_same_effects.clear();
        }
    }

    /// The rejection cached for the exact transaction `tx_id`, if any.
    pub fn rejected_exact(&self, tx_id: &UnminedTxId) -> Option<&ExactTipRejectionError<E>> {
        self.tip_rejected_exact.get(tx_id)
    }

    /// The rejection cached at `now` for every transaction with the effects
    /// `tx_id`, if any. The tip-scoped one comes first, then the chain-scoped
    /// ones in a fixed order: expired, duplicate spend, mined.
    pub fn rejected_effects(
        &self,
        tx_id: &transaction::Hash,
        now: Instant,
    ) -> Option<EffectsRejection> {
        if let Some(error) = self.tip_rejected_same_effects.get(tx_id) {
            return Some(EffectsRejection::Tip(error.clone()));
        }

        // A fixed order, so a transaction in several lists always gets the
        // same answer.
        [
            SameEffectsChainRejectionError::Expired,
            SameEffectsChainRejectionError::DuplicateSpend,
            SameEffectsChainRejectionError::Mined,
        ]
        .into_iter()
        .find(|error| {
            self.chain_rejected_same_effects
                .get(error)
                .is_some_and(|list| list.contains_key(tx_id, now))
        })
        .map(EffectsRejection::Chain)
    }

    /// The rejection that covers the transaction `tx_id` at `now`, if any:
    /// one for its exact bytes first, then one for its effects. See
    /// [`Self::rejected_effects`] for the order between effects lists.
    pub fn rejection(&self, tx_id: &UnminedTxId, now: Instant) -> Option<MempoolError<E>> {
        if let Some(error) = self.rejected_exact(tx_id) {
            return Some(error.clone().into());
        }
        self.rejected_effects(&tx_id.mined_id(), now)
            .map(MempoolError::from)
    }

    /// The members of `tx_ids` a rejection covers at `now`.
    pub fn rejected_transactions(
        &self,
        tx_ids: HashSet<UnminedTxId>,
        now: Instant,
    ) -> impl Iterator<Item = UnminedTxId> + '_ {
        tx_ids
            .into_iter()
            .filter(move |tx_id| self.rejection(tx_id, now).is_some())
    }

    /// Entries in the rejection lists. See [`StorageStats::rejected`].
    pub fn rejected_count(&self) -> usize {
        self.tip_rejected_exact.len()
            + self.tip_rejected_same_effects.len()
            + self
                .chain_rejected_same_effects
                .values()
                .map(EvictionList::len)
                .sum::<usize>()
    }

    // Reads.

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

    /// Whether `tx` spends an output or reveals a nullifier that a stored
    /// transaction does.
    pub fn has_spend_conflicts(&self, tx: &UnminedTx) -> bool {
        self.verified.has_spend_conflicts(tx)
    }

    /// Whether a transaction with the effects `tx_id` is stored, with any
    /// authorizing data.
    pub fn contains(&self, tx_id: &transaction::Hash) -> bool {
        self.verified.contains(tx_id)
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

    /// What storage holds, for the pool's metrics.
    pub fn stats(&self) -> StorageStats {
        StorageStats {
            transactions: self.verified.transaction_count(),
            serialized_size: self.verified.total_serialized_size(),
            cost: self.verified.total_cost(),
            weights: self.verified.weights().clone(),
            rejected: self.rejected_count(),
            eviction_cost_floor: self.eviction_cost_floor,
            evicted: self.evicted,
            below_eviction_cost: self.below_eviction_cost,
        }
    }
}
