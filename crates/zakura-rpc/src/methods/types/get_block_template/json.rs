//! JSON-only mining response views; ordinary serializers use the same field layout.

use super::{BlockTemplateResponse, DefaultRoots, GetBlockTemplateResponse, LongPollId};
use crate::{
    methods::types::transaction::json::{JsonTransaction, JsonTransactions},
    server::jsonrpc::{Id, MethodResponse, RpcResult},
};
use serde::{Serialize, Serializer};
use zakura_chain::{
    block,
    block::ChainHistoryBlockTxAuthCommitmentHash,
    serialization::DateTime32,
    work::difficulty::{CompactDifficulty, ExpandedDifficulty},
};

#[derive(Serialize)]
#[serde(rename = "BlockTemplateResponse")]
struct Fields<'a, T: Serialize, C: Serialize> {
    capabilities: &'a Vec<String>,
    version: &'a u32,
    #[serde(rename = "previousblockhash")]
    #[serde(with = "hex")]
    previous_block_hash: block::Hash,
    #[serde(rename = "blockcommitmentshash")]
    #[serde(with = "hex")]
    block_commitments_hash: ChainHistoryBlockTxAuthCommitmentHash,
    #[serde(rename = "lightclientroothash")]
    #[serde(with = "hex")]
    light_client_root_hash: ChainHistoryBlockTxAuthCommitmentHash,
    #[serde(rename = "finalsaplingroothash")]
    #[serde(with = "hex")]
    final_sapling_root_hash: ChainHistoryBlockTxAuthCommitmentHash,
    #[serde(rename = "defaultroots")]
    default_roots: &'a DefaultRoots,
    transactions: T,
    #[serde(rename = "coinbasetxn")]
    coinbase_txn: C,
    #[serde(rename = "longpollid")]
    long_poll_id: &'a LongPollId,
    #[serde(with = "hex")]
    target: ExpandedDifficulty,
    #[serde(rename = "mintime")]
    min_time: &'a DateTime32,
    mutable: &'a Vec<String>,
    #[serde(rename = "noncerange")]
    nonce_range: &'a String,
    #[serde(rename = "sigoplimit")]
    sigop_limit: &'a u32,
    #[serde(rename = "sizelimit")]
    size_limit: &'a u64,
    #[serde(rename = "curtime")]
    cur_time: &'a DateTime32,
    #[serde(with = "hex")]
    bits: CompactDifficulty,
    height: &'a u32,
    #[serde(rename = "maxtime")]
    max_time: &'a DateTime32,
    #[serde(rename = "workid")]
    work_id: &'a String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    #[serde(rename = "submitold")]
    submit_old: &'a Option<bool>,
}

fn fields<T: Serialize, C: Serialize>(
    template: &BlockTemplateResponse,
    transactions: T,
    coinbase_txn: C,
) -> Fields<'_, T, C> {
    Fields {
        capabilities: &template.capabilities,
        version: &template.version,
        previous_block_hash: template.previous_block_hash,
        block_commitments_hash: template.block_commitments_hash,
        light_client_root_hash: template.light_client_root_hash,
        final_sapling_root_hash: template.final_sapling_root_hash,
        default_roots: &template.default_roots,
        transactions,
        coinbase_txn,
        long_poll_id: &template.long_poll_id,
        target: template.target,
        min_time: &template.min_time,
        mutable: &template.mutable,
        nonce_range: &template.nonce_range,
        sigop_limit: &template.sigop_limit,
        size_limit: &template.size_limit,
        cur_time: &template.cur_time,
        bits: template.bits,
        height: &template.height,
        max_time: &template.max_time,
        work_id: &template.work_id,
        submit_old: &template.submit_old,
    }
}

pub(super) fn ordinary(template: &BlockTemplateResponse) -> impl Serialize + '_ {
    fields(template, &template.transactions, &template.coinbase_txn)
}

struct JsonTemplate<'a>(&'a BlockTemplateResponse);

impl Serialize for JsonTemplate<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        fields(
            self.0,
            JsonTransactions(&self.0.transactions),
            JsonTransaction(&self.0.coinbase_txn),
        )
        .serialize(serializer)
    }
}

/// Selects checked data fragments only at the actual JSON-RPC method boundary.
/// Proposal results and errors retain their ordinary response handling.
pub(crate) fn encode_response(
    id: Id<'_>,
    result: &RpcResult<GetBlockTemplateResponse>,
    max_response_size: usize,
) -> MethodResponse {
    match result {
        Ok(GetBlockTemplateResponse::TemplateMode(template)) => {
            MethodResponse::response(id, &Ok(JsonTemplate(template)), max_response_size)
        }
        _ => MethodResponse::response(id, result, max_response_size),
    }
}

#[cfg(test)]
mod tests;
