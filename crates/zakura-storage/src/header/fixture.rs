//! Fixture chains for the header adapter.
//!
//! Every fixture starts at the Regtest genesis block. Regtest checks only the
//! shape of each proof-of-work solution, so a fixture header is the genesis
//! header with a new parent, nonce and time, as in the header-chain fuzz
//! harness, and it still passes the real `prepare_headers`.

use std::{num::NonZeroU64, sync::Arc};

use chrono::{DateTime, Utc};
use zakura_chain::{
    block::{self, genesis::regtest_genesis_block},
    parameters::Network,
};
use zakura_header_chain::{
    EngineSnapshot, Frontier, HeaderWorkAuthority, SourceId, TargetCompletion,
};
use zakura_node_services::header_chain::{PrepareHeaderTarget, TargetEntry};

/// The Regtest network and its genesis block, the anchor of every fixture.
#[derive(Clone, Debug)]
pub struct HeaderFixture {
    network: Network,
    genesis: Arc<block::Block>,
}

impl HeaderFixture {
    /// The default Regtest network, anchored at its genesis block.
    pub fn regtest() -> Self {
        HeaderFixture {
            network: Network::new_regtest(Default::default()),
            genesis: regtest_genesis_block(),
        }
    }

    /// The fixture's network.
    pub fn network(&self) -> &Network {
        &self.network
    }

    /// The anchor's header.
    pub fn genesis_header(&self) -> Arc<block::Header> {
        self.genesis.header.clone()
    }

    /// The anchor, genesis at height 0.
    pub fn anchor(&self) -> Frontier {
        Frontier::new(block::Height(0), self.genesis.hash())
    }

    /// `count` headers extending `parent`, parent first. `branch` goes in each
    /// nonce, so two branches from the same parent never share a header.
    /// Each header's time is genesis plus its height in seconds, so times
    /// rise along every branch.
    ///
    /// # Panics
    ///
    /// If a height overflows, which a fixture never reaches.
    pub fn extend(&self, parent: Frontier, count: u32, branch: u8) -> Vec<Arc<block::Header>> {
        let mut headers = Vec::new();
        let mut parent_hash = parent.hash;
        for offset in 1..=count {
            let height = parent
                .height
                .0
                .checked_add(offset)
                .expect("fixture heights stay small");
            let mut header = *self.genesis.header;
            header.previous_block_hash = parent_hash;
            header.nonce.0[0] = branch;
            header.nonce.0[1..5].copy_from_slice(&height.to_le_bytes());
            header.time = self
                .genesis
                .header
                .time
                .checked_add_signed(chrono::Duration::seconds(i64::from(height)))
                .expect("fixture times stay representable");
            parent_hash = header.hash();
            headers.push(Arc::new(header));
        }
        headers
    }

    /// The request a requester sends to prepare `headers`, which extend
    /// `parent`, as complete work toward their last header. Its owner is
    /// bound to `snapshot`'s header generation, session 1 and `request_id`.
    ///
    /// # Panics
    ///
    /// If `headers` is empty or longer than the transition cap, or
    /// `request_id` is 0.
    pub fn target_request(
        &self,
        snapshot: &EngineSnapshot,
        parent: Frontier,
        headers: &[Arc<block::Header>],
        request_id: u64,
    ) -> PrepareHeaderTarget {
        let last = headers.last().expect("a target has headers");
        let count = u32::try_from(headers.len()).expect("a fixture batch fits u32");
        let height = parent
            .height
            .0
            .checked_add(count)
            .expect("fixture heights stay small");
        let target = Frontier::new(block::Height(height), last.hash());
        PrepareHeaderTarget {
            source: SourceId::from_digest([7; 32]),
            owner: HeaderWorkAuthority::for_target(snapshot, target.hash)
                .bind(
                    1,
                    NonZeroU64::new(request_id).expect("request IDs start at 1"),
                )
                .into(),
            common_ancestor: parent,
            target,
            entries: headers
                .iter()
                .map(|header| TargetEntry {
                    header: header.clone(),
                    body_size: 0,
                    tree_aux: None,
                })
                .collect::<Vec<_>>()
                .try_into()
                .expect("a fixture batch fits the transition cap"),
            completion: TargetCompletion::TargetComplete {
                common_ancestor: parent,
            },
        }
    }
}

/// The scenario clock: `zakura_rt::time::utc_now()`, which a simulator run
/// derives from virtual time and other builds read from the host.
#[derive(Copy, Clone, Debug, Default)]
pub struct ScenarioClock;

impl zakura_header_chain::Clock for ScenarioClock {
    fn now(&self) -> DateTime<Utc> {
        zakura_rt::time::utc_now().into()
    }
}
