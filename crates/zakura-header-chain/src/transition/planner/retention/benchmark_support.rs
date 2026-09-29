//! Release-mode retention benchmark fixtures.

use std::{num::NonZeroUsize, sync::Arc};

use zakura_chain::block::genesis::regtest_genesis_block;

use super::*;
use crate::{HeaderValidationState, InsertResult, MemHeaderStore};

/// Reusable retained-chain fixture for release-mode retention benchmarks.
pub struct RetentionBenchmarkFixture {
    graph: MemHeaderStore,
    header_best: Frontier,
    finalized: Frontier,
    retained_non_finalized_nodes: usize,
}

impl RetentionBenchmarkFixture {
    /// Build a linear graph at `percent` of the V1 retained-node limit.
    pub fn at_v1_limit_percent(percent: usize) -> Result<Self, GraphError> {
        assert!(matches!(percent, 25 | 50 | 90 | 100));
        let block = regtest_genesis_block();
        let hash = block.hash();
        let work = block
            .header
            .difficulty_threshold
            .to_work()
            .expect("the regtest genesis target has canonical work");
        let finalized = Frontier::new(block::Height(0), hash);
        let mut graph = MemHeaderStore::new(finalized, block.header.clone(), work, work.as_u256())?;
        let retained_non_finalized_nodes =
            crate::MAX_NON_FINALIZED_NODES_V1.saturating_mul(percent) / 100;
        let mut header_best = finalized;
        for index in 0..retained_non_finalized_nodes {
            let mut header = *regtest_genesis_block().header;
            header.previous_block_hash = header_best.hash;
            let nonce_marker = (index % 251) as u8;
            header.nonce = [nonce_marker; 32].into();
            header_best = match graph.insert(
                Arc::new(header),
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )? {
                InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => {
                    frontier
                }
            };
        }
        Ok(Self {
            graph,
            header_best,
            finalized,
            retained_non_finalized_nodes,
        })
    }

    /// Measure the ordinary exact-limit check without graph traversal.
    pub fn ordinary_check(&mut self) -> Result<RetentionBenchmarkResult, GraphError> {
        self.enforce_with_limit(self.retained_non_finalized_nodes.max(1))
    }

    /// Measure refusal when the protected chain exceeds the node limit by one.
    pub fn protected_refusal(&mut self) -> Result<RetentionBenchmarkResult, GraphError> {
        self.enforce_with_limit(self.retained_non_finalized_nodes.saturating_sub(1).max(1))
    }

    fn enforce_with_limit(
        &mut self,
        node_limit: usize,
    ) -> Result<RetentionBenchmarkResult, GraphError> {
        let limits = EngineLimits {
            max_non_finalized_nodes: NonZeroUsize::new(node_limit)
                .expect("the benchmark node limit is nonzero"),
            ..EngineLimits::v1()
        };
        let plan = enforce_retention(
            &mut self.graph,
            self.header_best,
            self.finalized,
            [],
            limits,
        )?;
        Ok(RetentionBenchmarkResult {
            admission_refused: plan.admission_refused,
            protected_path_visits: plan.work.protected_path_visits,
            candidate_nodes_scanned: plan.work.candidate_nodes_scanned,
            evicted_nodes: plan.work.evicted_nodes,
            graph_workspaces: plan.work.graph_workspaces,
        })
    }
}

/// Structural result returned by release-mode retention benchmarks.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RetentionBenchmarkResult {
    /// Whether the protected chain refused admission.
    pub admission_refused: bool,
    /// Unique retained nodes visited while constructing the protected union.
    pub protected_path_visits: usize,
    /// Retained nodes scanned while constructing eviction candidates.
    pub candidate_nodes_scanned: usize,
    /// Retained nodes removed by eviction.
    pub evicted_nodes: usize,
    /// Graph-sized retention workspaces allocated during the attempt.
    pub graph_workspaces: usize,
}
