//! Immutable transaction bytes with JSON generated only when the mining RPC serves them.

use std::sync::{Arc, OnceLock};

use serde::{Serialize, Serializer};
use serde_json::value::RawValue;
use zakura_chain::{
    amount,
    serialization::ZcashSerialize,
    transaction::{SerializedTransaction, Transaction},
};

use super::TransactionTemplate;

mod cache;
pub(crate) use cache::TransactionDataCache;

/// Clones share both bytes and first-use JSON; no global retention cache is involved.
#[derive(Clone)]
pub(crate) struct TransactionData(Arc<Payload>);

struct Payload {
    bytes: SerializedTransaction,
    json: OnceLock<Box<RawValue>>,
}

impl std::fmt::Debug for TransactionData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.bytes.fmt(f)
    }
}

impl PartialEq for TransactionData {
    fn eq(&self, other: &Self) -> bool {
        self.0.bytes == other.0.bytes
    }
}

impl Eq for TransactionData {}

impl TransactionData {
    pub(crate) fn bytes(&self) -> &SerializedTransaction {
        &self.0.bytes
    }

    fn json(&self) -> &RawValue {
        self.0.json.get_or_init(|| {
            let hex = crate::methods::hex_data::encode(self.as_ref());
            let mut json = String::with_capacity(hex.len() + 2);
            json.push('"');
            json.push_str(&hex);
            json.push('"');
            RawValue::from_string(json).expect("quoted internally generated hex is valid JSON")
        })
    }
}

impl From<SerializedTransaction> for TransactionData {
    fn from(bytes: SerializedTransaction) -> Self {
        Self(Arc::new(Payload {
            bytes,
            json: OnceLock::new(),
        }))
    }
}

impl From<Vec<u8>> for TransactionData {
    fn from(bytes: Vec<u8>) -> Self {
        Self::from(SerializedTransaction::from(bytes))
    }
}

impl From<&Transaction> for TransactionData {
    fn from(transaction: &Transaction) -> Self {
        Self::from(
            transaction
                .zcash_serialize_to_vec()
                .expect("writing to a Vec cannot fail"),
        )
    }
}

impl AsRef<[u8]> for TransactionData {
    fn as_ref(&self) -> &[u8] {
        self.0.bytes.as_ref()
    }
}

/// The one outbound transaction-field layout, used by ordinary and JSON-only serialization.
#[derive(Serialize)]
#[serde(rename = "TransactionTemplate", bound(serialize = "D: Serialize"))]
struct Fields<'a, C: amount::Constraint + Clone + Copy, D: Serialize> {
    data: D,
    #[serde(with = "hex")]
    hash: zakura_chain::transaction::Hash,
    #[serde(rename = "authdigest", with = "hex")]
    auth_digest: zakura_chain::transaction::AuthDigest,
    depends: &'a Vec<u16>,
    fee: &'a amount::Amount<C>,
    sigops: u32,
    required: bool,
}

fn fields<C: amount::Constraint + Clone + Copy, D: Serialize>(
    tx: &TransactionTemplate<C>,
    data: D,
) -> Fields<'_, C, D> {
    Fields {
        data,
        hash: tx.hash,
        auth_digest: tx.auth_digest,
        depends: &tx.depends,
        fee: &tx.fee,
        sigops: tx.sigops,
        required: tx.required,
    }
}

struct HexData<'a>(&'a TransactionData);

impl Serialize for HexData<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        crate::methods::hex_data::serialize(self.0, serializer)
    }
}

impl<C: amount::Constraint + Clone + Copy> Serialize for TransactionTemplate<C> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        fields(self, HexData(&self.data)).serialize(serializer)
    }
}

/// Used exclusively by the mining method's JSON response adapter.
pub(crate) struct JsonTransaction<'a, C: amount::Constraint + Clone + Copy>(
    pub &'a TransactionTemplate<C>,
);

impl<C: amount::Constraint + Clone + Copy> Serialize for JsonTransaction<'_, C> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        fields(self.0, self.0.data.json()).serialize(serializer)
    }
}

pub(crate) struct JsonTransactions<'a>(pub &'a [TransactionTemplate<amount::NonNegative>]);

impl Serialize for JsonTransactions<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(JsonTransaction))
    }
}

#[cfg(test)]
mod tests;
