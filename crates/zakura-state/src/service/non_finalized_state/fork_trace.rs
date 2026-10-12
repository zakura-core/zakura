//! Tracing for competing chains and best-chain switches.
//!
//! Propagation traces show which blocks reached which nodes. They do not show
//! what a node *did* with two blocks at the same height — whether it reorganised
//! onto the second one, and how much work separated them. That is what turns a
//! competing block into a confirmed stale block rather than a transient fork.
//!
//! Two tables:
//!
//! - `fork_event` — one row when the best chain tip changes to a different
//!   chain, i.e. a reorganisation rather than an extension.
//! - `fork_snapshot` — the full set of tracked chains whenever more than one
//!   exists, with each chain's tip, cumulative work, and fork point.
//!
//! Both stay JSONL. `fork_snapshot` nests an array of chains per row, which is
//! neither flat enough for CSV to shrink (it measured slightly *larger*) nor
//! what the analyzers read — they parse these two tables line by line as JSON,
//! unlike `peer_message`, which they load through DuckDB.

use std::path::PathBuf;

use serde::Serialize;
use zakura_chain::block;
use zakura_jsonl_trace::{JsonlEventEmitter, JsonlTraceTable, JsonlTracer};

use super::Chain;

/// Best-chain switches.
pub const FORK_EVENT_TABLE: JsonlTraceTable =
    JsonlTraceTable::new("fork_event", "fork_event.jsonl");

/// Point-in-time views of every tracked chain.
pub const FORK_SNAPSHOT_TABLE: JsonlTraceTable =
    JsonlTraceTable::new("fork_snapshot", "fork_snapshot.jsonl");

/// What caused the chain set to change.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ForkTrigger {
    /// A block extended an existing chain.
    CommitBlock,
    /// A block started a new chain from a fork point.
    NewChain,
    /// The lowest non-finalized block was finalized.
    Finalize,
}

impl ForkTrigger {
    fn label(self) -> &'static str {
        match self {
            Self::CommitBlock => "commit_block",
            Self::NewChain => "new_chain",
            Self::Finalize => "finalize",
        }
    }
}

#[derive(Serialize)]
struct BestChainSwitched<'a> {
    event: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    previous_best_tip_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    previous_best_tip_height: Option<u32>,
    new_best_tip_hash: String,
    new_best_tip_height: u32,
    chain_count: usize,
    trigger: &'static str,
    /// Present only when both tips are known, so a reader does not have to
    /// re-derive the depth of the reorganisation.
    #[serde(skip_serializing_if = "Option::is_none")]
    fork_depth: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    marker: Option<&'a str>,
}

#[derive(Serialize)]
struct ForkSnapshot {
    event: &'static str,
    best_tip_hash: String,
    best_tip_height: u32,
    chain_count: usize,
    chains: Vec<ChainSummary>,
}

#[derive(Serialize)]
struct ChainSummary {
    tip_hash: String,
    tip_height: u32,
    /// Cumulative work as hex, which is how the analyzers read it before
    /// converting with `int(chain_work, 16)`.
    chain_work: String,
    block_count: usize,
    fork_height: u32,
}

zakura_jsonl_trace::impl_jsonl_trace_event!(BestChainSwitched<'_>, FORK_EVENT_TABLE);
zakura_jsonl_trace::impl_jsonl_trace_event!(ForkSnapshot, FORK_SNAPSHOT_TABLE);

/// A handle for recording fork activity. Cloning is cheap.
#[derive(Clone, Debug, Default)]
pub struct ForkTrace {
    emitter: Option<JsonlEventEmitter>,
}

impl ForkTrace {
    /// Create a tracer writing to `trace_dir`, or a no-op tracer if it is `None`.
    pub fn new(trace_dir: Option<PathBuf>) -> Self {
        let Some(trace_dir) = trace_dir else {
            return Self::noop();
        };

        let tracer = JsonlTracer::spawn(trace_dir);
        if !tracer.is_enabled() {
            return Self::noop();
        }

        Self {
            emitter: Some(JsonlEventEmitter::new(
                tracer,
                zakura_jsonl_trace::node_id(),
            )),
        }
    }

    /// Create a tracer from an existing emitter.
    #[cfg(test)]
    pub fn from_emitter(emitter: JsonlEventEmitter) -> Self {
        Self {
            emitter: emitter.is_enabled().then_some(emitter),
        }
    }

    /// Create a no-op tracer. Every record call returns immediately.
    pub fn noop() -> Self {
        Self { emitter: None }
    }

    /// Returns true when this tracer emits rows.
    pub fn is_enabled(&self) -> bool {
        self.emitter.is_some()
    }

    /// Record the outcome of a change to the chain set.
    ///
    /// `previous_best` is the best tip before the change. A `fork_event` row is
    /// written only when the tip moved to a different chain; a `fork_snapshot`
    /// row is written whenever more than one chain is being tracked, which is
    /// the only time the snapshot carries information.
    pub fn record_chain_set_change<'a>(
        &self,
        trigger: ForkTrigger,
        previous_best: Option<(block::Height, block::Hash)>,
        chains: impl FnOnce() -> Vec<&'a Chain>,
    ) {
        let Some(emitter) = &self.emitter else {
            return;
        };

        let current = chains();
        let Some(best) = current.last() else {
            return;
        };

        let new_tip_height = best.non_finalized_tip_height();
        let new_tip_hash = best.non_finalized_tip_hash();
        let chain_count = current.len();

        let switched = match previous_best {
            Some((_, previous_hash)) => previous_hash != new_tip_hash,
            None => true,
        };

        // A chain that simply extended is not a fork event. Only a tip that
        // moved to a different block is.
        if switched {
            let extended = previous_best.is_some_and(|(_, previous_hash)| {
                best.blocks
                    .values()
                    .next_back()
                    .is_some_and(|tip| tip.block.header.previous_block_hash == previous_hash)
            });

            if !extended {
                emitter.emit_event(|| BestChainSwitched {
                    event: "best_chain_switched",
                    previous_best_tip_hash: previous_best.map(|(_, hash)| hash.to_string()),
                    previous_best_tip_height: previous_best.map(|(height, _)| height.0),
                    new_best_tip_hash: new_tip_hash.to_string(),
                    new_best_tip_height: new_tip_height.0,
                    chain_count,
                    trigger: trigger.label(),
                    fork_depth: previous_best
                        .map(|(height, _)| height.0.saturating_sub(new_tip_height.0)),
                    marker: None,
                });
            }
        }

        if chain_count > 1 {
            emitter.emit_event(|| ForkSnapshot {
                event: "fork_snapshot",
                best_tip_hash: new_tip_hash.to_string(),
                best_tip_height: new_tip_height.0,
                chain_count,
                chains: current.iter().map(|chain| summarize(chain)).collect(),
            });
        }
    }
}

fn summarize(chain: &Chain) -> ChainSummary {
    ChainSummary {
        tip_hash: chain.non_finalized_tip_hash().to_string(),
        tip_height: chain.non_finalized_tip_height().0,
        chain_work: format!("{:x}", chain.partial_cumulative_work.as_u256()),
        block_count: chain.len(),
        fork_height: chain.non_finalized_root_height().0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A disabled tracer must not build any of the row state, because
    /// `record_chain_set_change` sits on the block commit path.
    #[test]
    fn a_disabled_tracer_never_walks_the_chain_set() {
        let trace = ForkTrace::noop();
        assert!(!trace.is_enabled());

        let walked = std::cell::Cell::new(false);
        trace.record_chain_set_change(ForkTrigger::CommitBlock, None, || {
            walked.set(true);
            Vec::new()
        });

        assert!(
            !walked.get(),
            "a disabled tracer does no work on the commit path"
        );
    }

    #[test]
    fn triggers_have_stable_labels() {
        // `trigger` is a column the switch-episode analysis reads.
        assert_eq!(ForkTrigger::CommitBlock.label(), "commit_block");
        assert_eq!(ForkTrigger::NewChain.label(), "new_chain");
        assert_eq!(ForkTrigger::Finalize.label(), "finalize");
    }

    #[test]
    fn switch_rows_carry_the_columns_the_analyzer_reads() {
        // `load_switch_events` keys on `event == "best_chain_switched"` and then
        // indexes these fields directly, so a rename breaks it at read time.
        let row = serde_json::to_value(BestChainSwitched {
            event: "best_chain_switched",
            previous_best_tip_hash: Some("prev".to_string()),
            previous_best_tip_height: Some(9),
            new_best_tip_hash: "new".to_string(),
            new_best_tip_height: 10,
            chain_count: 2,
            trigger: ForkTrigger::CommitBlock.label(),
            fork_depth: Some(0),
            marker: None,
        })
        .expect("switch row serializes");

        for column in [
            "event",
            "previous_best_tip_hash",
            "previous_best_tip_height",
            "new_best_tip_hash",
            "new_best_tip_height",
            "chain_count",
            "trigger",
        ] {
            assert!(row.get(column).is_some(), "missing column {column}");
        }
    }

    #[test]
    fn snapshot_rows_nest_one_entry_per_chain() {
        let row = serde_json::to_value(ForkSnapshot {
            event: "fork_snapshot",
            best_tip_hash: "tip".to_string(),
            best_tip_height: 10,
            chain_count: 1,
            chains: vec![ChainSummary {
                tip_hash: "tip".to_string(),
                tip_height: 10,
                chain_work: "ff".to_string(),
                block_count: 3,
                fork_height: 7,
            }],
        })
        .expect("snapshot row serializes");

        assert_eq!(row["best_tip_hash"], "tip");
        let chain = &row["chains"][0];
        assert_eq!(chain["tip_hash"], "tip");
        assert_eq!(chain["block_count"], 3);
        assert_eq!(chain["fork_height"], 7);
        // Read back with `int(chain_work, 16)`, so it must stay bare hex with
        // no `0x` prefix.
        assert_eq!(chain["chain_work"], "ff");
        assert_eq!(
            i64::from_str_radix(chain["chain_work"].as_str().expect("hex string"), 16)
                .expect("parses as hex"),
            255
        );
    }
}
