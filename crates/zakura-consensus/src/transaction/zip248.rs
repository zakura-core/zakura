//! The [ZIP 248] consensus rules on a v7 transaction's bundles and value pool deltas.
//!
//! Decoding already enforces the rules on each bundle's own encoding. The rules here
//! relate a transaction's bundles to each other, to the epoch of its block, and to the
//! outputs it spends. The coinbase rules that relate the coinbase transaction to its
//! block live with the block's miner fee check.
//!
//! [ZIP 248]: https://zips.z.cash/zip-0248

use zakura_chain::{
    parameters::NetworkUpgrade,
    transaction::{
        v7::{BundleKind, ValueDeltas},
        Transaction,
    },
    transparent,
};

use crate::error::TransactionError;

/// Returns the network upgrade that activates `bundle` in v7 transactions, or `None` if
/// no upgrade this node knows activates it.
///
/// # Consensus
///
/// > For every `(bundleType, bundleVariant)` pair appearing in any of
/// > `mValuePoolDeltas`, `mEffectBundles`, or `mAuthBundles`, a full validator MUST
/// > verify that the pair is registered for v7 and activated for the consensus epoch of
/// > the block containing the transaction.
///
/// <https://zips.z.cash/zip-0248#bundle-availability>
pub(crate) fn activation(bundle: BundleKind) -> Option<NetworkUpgrade> {
    match bundle {
        BundleKind::Transparent
        | BundleKind::Coinbase
        | BundleKind::Sapling
        | BundleKind::Orchard
        | BundleKind::Ironwood
        | BundleKind::Fee => Some(NetworkUpgrade::Nu7_1),
        // NU7.1 does not deploy ZIP 233.
        BundleKind::Nsm => None,
    }
}

/// Checks the ZIP 248 rules on a v7 transaction's bundles and value pool deltas that
/// need only the transaction and the network upgrade of its block. Earlier transaction
/// versions pass.
///
/// These run before the spent outputs are loaded, so a transaction that fails them costs
/// no state lookups.
pub(crate) fn check_bundles_and_deltas(
    tx: &Transaction,
    network_upgrade: NetworkUpgrade,
) -> Result<(), TransactionError> {
    let Some(fields) = tx.v7_fields() else {
        return Ok(());
    };
    let deltas = &fields.value_deltas;

    bundles_are_active(tx, network_upgrade)?;
    deltas_have_bundles(tx, deltas)?;
    delta_signs_are_valid(tx, deltas)?;
    deltas_balance(tx, deltas)
}

/// Checks the ZIP 248 rule that relates a v7 transaction's transparent delta to the
/// outputs its transparent inputs spend, given in input order. Earlier transaction
/// versions pass.
///
/// [`check_bundles_and_deltas`] runs first, so once this passes, the deltas balance and
/// the stated fee is the value the transaction leaves.
pub(crate) fn check_spent_outputs(
    tx: &Transaction,
    spent_outputs: &[transparent::Output],
) -> Result<(), TransactionError> {
    match tx.v7_fields() {
        Some(fields) => {
            transparent_delta_matches_spent_outputs(tx, &fields.value_deltas, spent_outputs)
        }
        None => Ok(()),
    }
}

/// Rejects a bundle that is not active in the block's epoch.
fn bundles_are_active(
    tx: &Transaction,
    network_upgrade: NetworkUpgrade,
) -> Result<(), TransactionError> {
    match tx
        .v7_bundles()
        .into_iter()
        .find(|bundle| activation(*bundle).is_none_or(|upgrade| network_upgrade < upgrade))
    {
        Some(inactive) => Err(TransactionError::V7BundleNotActive(inactive)),
        None => Ok(()),
    }
}

/// Rejects a transparent or coinbase value pool delta without its effecting bundle.
///
/// # Consensus
///
/// > Each entry in `mValuePoolDeltas` for variant 0 of the transparent, coinbase,
/// > Sapling, Orchard, or Ironwood bundle MUST have a corresponding entry in
/// > `mEffectBundles`.
///
/// A Sapling, Orchard, or Ironwood delta is its bundle's value balance, so decoding
/// already rejects one without its bundle.
fn deltas_have_bundles(tx: &Transaction, deltas: &ValueDeltas) -> Result<(), TransactionError> {
    if !deltas.transparent.is_zero()
        && !tx.has_transparent_inputs()
        && !tx.has_transparent_outputs()
    {
        return Err(TransactionError::V7DeltaWithoutBundle(
            BundleKind::Transparent,
        ));
    }
    if !deltas.coinbase.is_zero() && tx.v7_coinbase().is_none() {
        return Err(TransactionError::V7DeltaWithoutBundle(BundleKind::Coinbase));
    }
    Ok(())
}

/// Checks the sign rules on the fee and NSM value pool deltas.
///
/// # Consensus
///
/// > For coinbase transactions, the value of mValuePoolDeltas[FeeBundleId] MUST be
/// > nonnegative. [...] For non-coinbase transactions, the value of
/// > mValuePoolDeltas[FeeBundleId] MUST be nonpositive.
///
/// <https://zips.z.cash/zip-0248#cross-bundle-and-chain-context-rules>
///
/// > The value of mValuePoolDeltas[NSMBundleId], if present, MUST be nonpositive.
///
/// <https://zips.z.cash/zip-0233#changes-to-the-zcash-protocol-specification>
///
/// ZIP 2002 and ZIP 233 also bound each value's absolute value by MAX_MONEY, which the
/// delta's amount type already guarantees.
///
/// NU7.1 does not activate the NSM bundle, so [`bundles_are_active`] rejects an NSM
/// delta first. This rule holds once an upgrade activates it.
pub(crate) fn delta_signs_are_valid(
    tx: &Transaction,
    deltas: &ValueDeltas,
) -> Result<(), TransactionError> {
    let fee = i64::from(deltas.fee);
    if tx.is_coinbase() && fee < 0 {
        return Err(TransactionError::V7CoinbaseFeeRemovesValue);
    }
    if !tx.is_coinbase() && fee > 0 {
        return Err(TransactionError::V7FeeAddsValue);
    }
    if i64::from(deltas.nsm) > 0 {
        return Err(TransactionError::V7NsmAddsValue);
    }
    Ok(())
}

/// Checks that the value pool deltas sum to zero.
///
/// # Consensus
///
/// > For every transaction, the sum of all value pool delta values MUST equal 0.
///
/// This replaces, for v7 transactions, the implicit fee of earlier versions: a v7
/// transaction states its fee in its fee bundle.
fn deltas_balance(tx: &Transaction, deltas: &ValueDeltas) -> Result<(), TransactionError> {
    // Each delta is within ±MAX_MONEY, so the sum of seven fits in an i128 without
    // overflow.
    let sum: i128 = [
        deltas.transparent,
        deltas.coinbase,
        tx.sapling_value_balance().sapling_amount(),
        tx.orchard_value_balance().orchard_amount(),
        tx.ironwood_value_balance().ironwood_amount(),
        deltas.fee,
        deltas.nsm,
    ]
    .into_iter()
    .map(|delta| i128::from(i64::from(delta)))
    .sum();
    if sum == 0 {
        Ok(())
    } else {
        Err(TransactionError::V7UnbalancedValuePoolDeltas)
    }
}

/// Checks that the transparent delta is the value of the spent outputs minus the value
/// of the created outputs.
///
/// # Consensus
///
/// > A full validator MUST verify that the value pool delta for `bundleType = 0` equals
/// > the total value of the transparent inputs minus the total value of the transparent
/// > outputs.
fn transparent_delta_matches_spent_outputs(
    tx: &Transaction,
    deltas: &ValueDeltas,
    spent_outputs: &[transparent::Output],
) -> Result<(), TransactionError> {
    let delta = deltas.transparent;
    // Each value is within MAX_MONEY and a block bounds their count, so the sums fit
    // in an i128.
    let total = |outputs: &[transparent::Output]| -> i128 {
        outputs
            .iter()
            .map(|output| i128::from(i64::from(output.value())))
            .sum()
    };
    let spent = total(spent_outputs);
    let created = total(tx.outputs());
    if i128::from(i64::from(delta)) == spent - created {
        Ok(())
    } else {
        Err(TransactionError::V7TransparentDeltaMismatch)
    }
}
