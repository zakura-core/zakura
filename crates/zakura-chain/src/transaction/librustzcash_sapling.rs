//! Builds a transaction's librustzcash Sapling bundle from its parsed fields.
//!
//! The signature-hash data for a transaction is a librustzcash copy of it, made by
//! serializing the transaction and parsing it again. Parsing a Sapling spend
//! decompresses its `rk` point, a square root that was most of the copy's cost.
//! Zakura already parsed every Sapling field and keeps each `rk` decoded, so this
//! module builds the Sapling bundle from those fields. librustzcash still parses
//! the rest of the transaction.

use zcash_primitives::transaction::{Authorized, TransactionData, TxVersion};
use zcash_protocol::value::ZatBalance;

use crate::sapling::{self, AnchorVariant, ShieldedData};

use super::Transaction;

/// A librustzcash Sapling bundle with its authorizing data.
type LibrustzcashSaplingBundle =
    sapling_crypto::Bundle<sapling_crypto::bundle::Authorized, ZatBalance>;

impl Transaction {
    /// Splits this transaction into a copy without Sapling data and its librustzcash
    /// Sapling bundle, built from the parsed fields.
    ///
    /// Returns `None` if the transaction has no Sapling data, or if its value balance
    /// is out of librustzcash's range, so the caller parses the whole transaction and
    /// reports librustzcash's error for it.
    pub(super) fn split_librustzcash_sapling(
        &self,
    ) -> Option<(Transaction, LibrustzcashSaplingBundle)> {
        let bundle = match self {
            Transaction::V4 {
                sapling_shielded_data: Some(shielded_data),
                ..
            } => librustzcash_sapling_bundle(shielded_data)?,
            Transaction::V5 {
                sapling_shielded_data: Some(shielded_data),
                ..
            }
            | Transaction::V6 {
                sapling_shielded_data: Some(shielded_data),
                ..
            } => librustzcash_sapling_bundle(shielded_data)?,
            _ => return None,
        };

        let mut without_sapling = self.clone();
        match &mut without_sapling {
            Transaction::V4 {
                sapling_shielded_data,
                ..
            } => *sapling_shielded_data = None,
            Transaction::V5 {
                sapling_shielded_data,
                ..
            }
            | Transaction::V6 {
                sapling_shielded_data,
                ..
            } => *sapling_shielded_data = None,
            Transaction::V1 { .. } | Transaction::V2 { .. } | Transaction::V3 { .. } => {
                unreachable!("only V4 and later transactions have Sapling data")
            }
        }

        Some((without_sapling, bundle))
    }
}

/// Returns `data`, parsed from a transaction without Sapling data, with `sapling`
/// as its Sapling bundle.
pub(super) fn with_sapling_bundle(
    data: TransactionData<Authorized>,
    sapling: LibrustzcashSaplingBundle,
) -> TransactionData<Authorized> {
    match data.version() {
        TxVersion::V6 => TransactionData::from_parts_v6(
            data.consensus_branch_id(),
            data.lock_time(),
            data.expiry_height(),
            data.transparent_bundle().cloned(),
            Some(sapling),
            data.orchard_bundle().cloned(),
            data.ironwood_bundle().cloned(),
        ),
        version => TransactionData::from_parts(
            version,
            data.consensus_branch_id(),
            data.lock_time(),
            data.expiry_height(),
            data.transparent_bundle().cloned(),
            data.sprout_bundle().cloned(),
            Some(sapling),
            data.orchard_bundle().cloned(),
        ),
    }
}

/// The librustzcash Sapling bundle for `shielded_data`, or `None` if the value
/// balance is out of range.
fn librustzcash_sapling_bundle<A>(
    shielded_data: &ShieldedData<A>,
) -> Option<LibrustzcashSaplingBundle>
where
    A: AnchorVariant + Clone,
    sapling::Spend<sapling::PerSpendAnchor>: From<(sapling::Spend<A>, A::Shared)>,
{
    let spends = shielded_data
        .spends_per_anchor()
        .map(|spend| {
            sapling_crypto::bundle::SpendDescription::from_parts(
                spend.cv.unvalidated(),
                spend.per_spend_anchor.0,
                sapling_crypto::Nullifier(spend.nullifier.into()),
                spend.rk.into(),
                spend.zkproof.0,
                spend.spend_auth_sig,
            )
        })
        .collect();
    let outputs = shielded_data
        .outputs()
        .map(|output| {
            sapling_crypto::bundle::OutputDescription::from_parts(
                output.cv.unvalidated(),
                output.cm_u,
                zcash_note_encryption::EphemeralKeyBytes(output.ephemeral_key.into()),
                output.enc_ciphertext.0,
                output.out_ciphertext.0,
                output.zkproof.0,
            )
        })
        .collect();

    sapling_crypto::Bundle::from_parts(
        spends,
        outputs,
        ZatBalance::from_i64(shielded_data.value_balance.zatoshis()).ok()?,
        sapling_crypto::bundle::Authorized {
            binding_sig: shielded_data.binding_sig,
        },
    )
}
