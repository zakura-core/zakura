//! Zebra's internal peer message response format.

use std::{
    fmt,
    ops::Deref,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};
use zakura_node_services::block_memory::BlockHandle;

#[cfg(any(test, feature = "proptest-impl"))]
use zakura_chain::block::Block;

use zakura_chain::{
    block::{self},
    serialization::ZcashSerialize,
    transaction::{UnminedTx, UnminedTxId},
};

use crate::{meta_addr::MetaAddr, protocol::internal::InventoryResponse, PeerSocketAddr};

#[cfg(any(test, feature = "proptest-impl"))]
use proptest::prelude::*;
#[cfg(any(test, feature = "proptest-impl"))]
use proptest_derive::Arbitrary;

use InventoryResponse::*;

/// A received body together with the connection's optional timing observation.
#[derive(Clone, Debug)]
pub struct DeliveredBlock {
    block: BlockHandle,
    observation: Option<Arc<BlockObservation>>,
}

#[derive(Debug)]
struct BlockObservation {
    recorded: OnceLock<()>,
    slot: crate::peer::RequestSlot,
    missed: Option<Arc<tokio::time::Instant>>,
    registry: Option<crate::peer_registry::PeerRegistryUpdater>,
    duration: Duration,
    /// The serialized size of the blocks in the reply.
    wire_bytes: u64,
    received_at: Instant,
}

impl DeliveredBlock {
    /// Returns the body with its original decoded-memory ownership.
    pub fn into_block(self) -> BlockHandle {
        self.block
    }

    /// Credits a legacy delivery once its exact body has passed verification: clears the miss
    /// the connection had when the reply arrived, and records the reply's answer time, which
    /// moves the peer's expected answer time only if the reply was small enough to arrive in
    /// one round trip.
    /// Clones share the same observation. Other transports carry no observation.
    pub fn record_verified(&self) {
        if let Some(observation) = &self.observation {
            observation.recorded.get_or_init(|| {
                if let Some(missed) = &observation.missed {
                    observation.slot.clear_miss_at_receipt(missed);
                }
                if let Some(registry) = &observation.registry {
                    registry.record_delivery_sample(
                        crate::peer_registry::DeliverySampleKind::Block {
                            wire_bytes: observation.wire_bytes,
                        },
                        observation.duration,
                        observation.received_at,
                    );
                }
            });
        }
    }

    /// Credits a competing delivery only if it matches the complete verified body.
    pub fn record_verified_copy(&self, verified: &BlockHandle) {
        if self.observation.is_some() && self.block == *verified {
            self.record_verified();
        }
    }
}

impl Deref for DeliveredBlock {
    type Target = BlockHandle;
    fn deref(&self) -> &BlockHandle {
        &self.block
    }
}

impl<T: Into<BlockHandle>> From<T> for DeliveredBlock {
    fn from(block: T) -> Self {
        Self {
            block: block.into(),
            observation: None,
        }
    }
}

impl PartialEq for DeliveredBlock {
    fn eq(&self, other: &Self) -> bool {
        self.block == other.block
    }
}
impl Eq for DeliveredBlock {}

/// A response to a network request, represented in internal format.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(any(test, feature = "proptest-impl"), derive(Arbitrary))]
pub enum Response {
    /// The request does not have a response.
    ///
    /// Either:
    ///  * the request does not need a response, or
    ///  * we have no useful data to provide in response to the request,
    ///    and the request was not an inventory request.
    ///
    /// (Inventory requests provide a list of missing hashes if none of the hashes were available.)
    Nil,

    /// A list of peers, used to respond to `GetPeers`.
    ///
    /// The list contains `0..=MAX_META_ADDR` peers.
    //
    // TODO: make this into a HashMap<PeerSocketAddr, MetaAddr> - a unique list of peer addresses (#2244)
    Peers(Vec<MetaAddr>),

    /// A pong response containing the round-trip latency for a peer.
    ///
    /// Returned after a ping/pong exchange to measure RTT.
    Pong(Duration),

    /// An ordered list of block hashes.
    ///
    /// The list contains zero or more block hashes.
    //
    // TODO: make this into an IndexMap - an ordered unique list of hashes (#2244)
    BlockHashes(Vec<block::Hash>),

    /// An ordered list of block headers.
    ///
    /// The list contains zero or more block headers.
    //
    // TODO: make this into an IndexMap - an ordered unique list of headers (#2244)
    ///
    /// A header read from state keeps its read admission through each holder.
    BlockHeaders(
        #[cfg_attr(
            any(test, feature = "proptest-impl"),
            proptest(strategy = "header_responses()")
        )]
        Vec<zakura_node_services::block_memory::HeaderHandle>,
    ),

    /// A list of unmined transaction IDs.
    ///
    /// v4 transactions use a legacy transaction ID, and
    /// v5 transactions use a witnessed transaction ID.
    ///
    /// The list contains zero or more transaction IDs.
    //
    // TODO: make this into a HashSet - a unique list (#2244)
    TransactionIds(Vec<UnminedTxId>),

    /// A list of found blocks, and missing block hashes.
    ///
    /// Each list contains zero or more entries.
    ///
    /// When Zebra doesn't have a block or transaction, it always sends `notfound`.
    /// `zcashd` sometimes sends no response, and sometimes sends `notfound`.
    //
    // TODO: make this into a HashMap<block::Hash, InventoryResponse<zakura_node_services::block_memory::BlockHandle, ()>> - a unique list (#2244)
    Blocks(
        #[cfg_attr(
            any(test, feature = "proptest-impl"),
            proptest(strategy = "block_responses()")
        )]
        Vec<InventoryResponse<(DeliveredBlock, Option<PeerSocketAddr>), block::Hash>>,
    ),

    /// A list of found unmined transactions, and missing unmined transaction IDs.
    ///
    /// Each list contains zero or more entries.
    //
    // TODO: make this into a HashMap<UnminedTxId, InventoryResponse<UnminedTx, ()>> - a unique list (#2244)
    Transactions(Vec<InventoryResponse<(UnminedTx, Option<PeerSocketAddr>), UnminedTxId>>),
}

impl fmt::Display for Response {
    #[allow(clippy::unwrap_in_result)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&match self {
            Response::Nil => "Nil".to_string(),

            Response::Peers(peers) => format!("Peers {{ peers: {} }}", peers.len()),

            Response::Pong(duration) => format!("Pong {{ latency: {duration:?} }}"),

            Response::BlockHashes(hashes) => format!("BlockHashes {{ hashes: {} }}", hashes.len()),
            Response::BlockHeaders(headers) => {
                format!("BlockHeaders {{ headers: {} }}", headers.len())
            }
            Response::TransactionIds(ids) => format!("TransactionIds {{ ids: {} }}", ids.len()),

            // Display heights for single-block responses (which Zebra requests and expects)
            Response::Blocks(blocks) if blocks.len() == 1 => {
                match blocks.first().expect("len is 1") {
                    Available((block, _)) => format!(
                        "Block {{ height: {}, hash: {} }}",
                        block
                            .coinbase_height()
                            .as_ref()
                            .map(|h| h.0.to_string())
                            .unwrap_or_else(|| "None".into()),
                        block.hash(),
                    ),
                    Missing(hash) => format!("Block {{ missing: {hash} }}"),
                }
            }
            Response::Blocks(blocks) => format!(
                "Blocks {{ blocks: {}, missing: {} }}",
                blocks.iter().filter(|r| r.is_available()).count(),
                blocks.iter().filter(|r| r.is_missing()).count()
            ),

            Response::Transactions(transactions) => format!(
                "Transactions {{ transactions: {}, missing: {} }}",
                transactions.iter().filter(|r| r.is_available()).count(),
                transactions.iter().filter(|r| r.is_missing()).count()
            ),
        })
    }
}

impl Response {
    /// Share one transfer-time sample across the available bodies in this reply.
    /// Any verified body can confirm the delivery, once for the whole batch.
    pub(crate) fn observe_block_delivery(
        &mut self,
        slot: &crate::peer::RequestSlot,
        registry: Option<crate::peer_registry::PeerRegistryUpdater>,
        duration: Duration,
    ) {
        let Self::Blocks(blocks) = self else { return };
        let missed = slot.miss_at_receipt();
        if missed.is_none() && registry.is_none() {
            return;
        }
        // Only the registry reads the size, so a reply that can only clear a miss is not measured.
        let wire_bytes = if registry.is_some() {
            blocks
                .iter()
                .filter_map(|entry| match entry {
                    Available((block, _)) => Some(block.zcash_serialized_size()),
                    Missing(_) => None,
                })
                .map(|bytes| u64::try_from(bytes).unwrap_or(u64::MAX))
                .fold(0, u64::saturating_add)
        } else {
            0
        };
        let observation = Arc::new(BlockObservation {
            recorded: OnceLock::new(),
            slot: slot.clone(),
            missed,
            registry,
            duration,
            wire_bytes,
            received_at: Instant::now(),
        });
        for entry in blocks {
            if let Available((block, _)) = entry {
                block.observation = Some(observation.clone());
            }
        }
    }

    /// Returns the Zebra internal response type as a string.
    pub fn command(&self) -> &'static str {
        match self {
            Response::Nil => "Nil",

            Response::Peers(_) => "Peers",

            Response::Pong(_) => "Pong",

            Response::BlockHashes(_) => "BlockHashes",
            Response::BlockHeaders(_) => "BlockHeaders",
            Response::TransactionIds(_) => "TransactionIds",

            Response::Blocks(_) => "Blocks",
            Response::Transactions(_) => "Transactions",
        }
    }

    /// Returns true if the response is a block or transaction inventory download.
    pub fn is_inventory_download(&self) -> bool {
        matches!(self, Response::Blocks(_) | Response::Transactions(_))
    }
}

#[cfg(any(test, feature = "proptest-impl"))]
fn header_responses() -> impl Strategy<Value = Vec<zakura_node_services::block_memory::HeaderHandle>>
{
    any::<Vec<block::CountedHeader>>().prop_map(|headers| {
        headers
            .into_iter()
            .map(|counted| counted.header.into())
            .collect()
    })
}

#[cfg(any(test, feature = "proptest-impl"))]
fn block_responses(
) -> impl Strategy<Value = Vec<InventoryResponse<(DeliveredBlock, Option<PeerSocketAddr>), block::Hash>>>
{
    any::<Vec<InventoryResponse<(Arc<Block>, Option<PeerSocketAddr>), block::Hash>>>().prop_map(
        |responses| {
            responses
                .into_iter()
                .map(|response| match response {
                    Available((block, peer)) => Available((block.into(), peer)),
                    Missing(hash) => Missing(hash),
                })
                .collect()
        },
    )
}
