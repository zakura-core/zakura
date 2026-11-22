//! The task that keeps a [`RecentMinerSet`] in step with the best chain.

use std::time::Duration;

use tokio::task::JoinHandle;
use tower::Service;
use tracing::{info, warn, Instrument};
use zakura_chain::{
    block::{self, Height},
    miner_mesh::{MinerSetSnapshot, RecentMinerSet},
    parameters::Network,
};
use zakura_state::{ChainTipChange, ReadRequest, ReadResponse, TipAction, MAX_BLOCK_REORG_HEIGHT};

use crate::BoxError;

use super::chain_reads::{best_chain_hash, read_block};

/// How long the tracker waits after a failed state read before it reads again.
const READ_RETRY_DELAY: Duration = Duration::from_secs(5);

/// Starts the task that counts the gateways named in recent best-chain blocks
/// and reports the counts as metrics.
///
/// The task runs until the state stops publishing tip changes.
pub fn spawn<S>(state: S, tip_change: ChainTipChange) -> JoinHandle<()>
where
    S: Service<ReadRequest, Response = ReadResponse, Error = BoxError>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send,
{
    info!("counting the gateways that recent blocks name");
    let network = tip_change.network().clone();
    tokio::spawn(
        async move {
            let mut tracker = Tracker::new(network, state);
            run(&mut tracker, tip_change).await;
        }
        .in_current_span(),
    )
}

/// What the tracker does next.
enum Step {
    /// Follow the tip to where it moved.
    FollowTip(TipMove),
    /// Read the newest unread block in the window; no tip change is pending.
    Read(Height),
}

/// Follows tip changes first and reads older blocks between them, until the
/// state stops publishing tip changes.
///
/// Metrics are recorded after each tip change and whenever the tracker has
/// read all it can.
pub(super) async fn run<S>(tracker: &mut Tracker<S>, mut tip_change: ChainTipChange)
where
    S: Service<ReadRequest, Response = ReadResponse, Error = BoxError> + Clone + Send + 'static,
    S::Future: Send,
{
    loop {
        let step = match (tip_change.last_tip_change(), tracker.next_unread()) {
            (Some(action), _) => Step::FollowTip(action.into()),
            (None, Some(height)) => Step::Read(height),
            (None, None) => match tip_change.wait_for_tip_change().await {
                Ok(action) => Step::FollowTip(action.into()),
                // The state has shut down.
                Err(_) => return,
            },
        };

        match step {
            Step::FollowTip(tip) => {
                if let Err(error) = tracker.follow_tip(tip).await {
                    warn!(
                        %error,
                        tip_height = ?tip.height,
                        "could not compare the blocks read for the miner mesh with the best \
                         chain after a reset; the blocks a reorg could reach are read again",
                    );
                    tokio::time::sleep(READ_RETRY_DELAY).await;
                }
                record_metrics(&tracker.snapshot());
            }
            Step::Read(height) => {
                if let Err(error) = tracker.read_next().await {
                    warn!(
                        %error,
                        ?height,
                        "could not read a block to count miner mesh gateways; retrying",
                    );
                    tokio::time::sleep(READ_RETRY_DELAY).await;
                }
                if tracker.next_unread().is_none() {
                    record_metrics(&tracker.snapshot());
                }
            }
        }
    }
}

/// Where the best chain's tip moved.
#[derive(Copy, Clone, Debug)]
pub(super) struct TipMove {
    pub height: Height,
    pub hash: block::Hash,
    /// Whether blocks the tracker has read may have left the best chain: the
    /// state reports a reset after a reorg and when it skipped tip changes.
    pub may_have_reorged: bool,
}

impl From<TipAction> for TipMove {
    fn from(action: TipAction) -> Self {
        match action {
            TipAction::Grow { block } => Self {
                height: block.height,
                hash: block.hash,
                may_have_reorged: false,
            },
            TipAction::Reset { height, hash } => Self {
                height,
                hash,
                may_have_reorged: true,
            },
        }
    }
}

/// A [`RecentMinerSet`] and the state it reads blocks from.
pub(super) struct Tracker<S> {
    set: RecentMinerSet,
    state: S,
    /// The highest height whose block could not be read since the tip last
    /// moved.
    ///
    /// The block may be pruned or may have just left the best chain. The
    /// blocks below it are not read either until the tip moves, so a pruned
    /// block costs one failed read per tip change.
    unreadable_from: Option<Height>,
}

impl<S> Tracker<S>
where
    S: Service<ReadRequest, Response = ReadResponse, Error = BoxError> + Clone + Send + 'static,
    S::Future: Send,
{
    pub(super) fn new(network: Network, state: S) -> Self {
        Self {
            set: RecentMinerSet::new(network),
            state,
            unreadable_from: None,
        }
    }

    pub(super) fn snapshot(&self) -> MinerSetSnapshot {
        self.set.snapshot()
    }

    /// Moves the window to follow the tip.
    ///
    /// When the tip may have moved to another chain, every read block a reorg
    /// could have reached is compared with the best chain, and everything from
    /// the lowest block that differs upwards is forgotten. A block that still
    /// matches says nothing about the blocks below it: it may have been read
    /// from the new chain before the reset arrived.
    ///
    /// When the tip it followed before is still on the best chain, nothing
    /// below it was displaced, so one read settles the reset. This covers the
    /// resets the state reports for skipped tip changes, which are most of
    /// them while the node catches up.
    ///
    /// The checks see the best chain as it is now. A block read from a chain
    /// that was briefly best and is gone again by the next tip change stays
    /// counted until it leaves the window.
    ///
    /// If a read fails, every block a reorg could have reached is forgotten,
    /// so the counts never include a block the repair could not check.
    pub(super) async fn follow_tip(&mut self, tip: TipMove) -> Result<(), BoxError> {
        let previous_tip = self.set.snapshot().tip;
        self.set.set_tip(tip.height, tip.hash);
        // A block that could not be read may have just left the best chain,
        // so each new tip gives the reads below it another try.
        self.unreadable_from = None;
        if !tip.may_have_reorged {
            return Ok(());
        }

        // A reorg reaches at most `MAX_BLOCK_REORG_HEIGHT` below the tip it
        // replaced, which may be lower than the new tip. The genesis block is
        // never displaced.
        let lower_tip = previous_tip.map_or(tip.height, |(previous, _)| previous.min(tip.height));
        let lowest_reachable = Height(lower_tip.0.saturating_sub(MAX_BLOCK_REORG_HEIGHT).max(1));

        if let Some((previous_height, previous_hash)) = previous_tip {
            match best_chain_hash(&self.state, previous_height).await {
                Ok(best) if best == Some(previous_hash) => return Ok(()),
                Ok(_) => {}
                Err(error) => return Err(self.give_up_repair(lowest_reachable, error)),
            }
        }

        let reachable: Vec<_> = self
            .set
            .checked_blocks()
            .take_while(|(height, _)| *height >= lowest_reachable)
            .collect();

        let mut lowest_displaced = None;
        for (height, hash) in reachable {
            match best_chain_hash(&self.state, height).await {
                Ok(best) if best == Some(hash) => {}
                Ok(_) => lowest_displaced = Some(height),
                Err(error) => return Err(self.give_up_repair(lowest_reachable, error)),
            }
        }

        if let Some(height) = lowest_displaced {
            metrics::counter!("mining.mesh.reorg.repairs").increment(1);
            self.forget_from(height);
        }
        Ok(())
    }

    /// Returns the newest height in the window the tracker will read next.
    pub(super) fn next_unread(&self) -> Option<Height> {
        self.set
            .unchecked_heights()
            .next()
            .filter(|height| self.unreadable_from.is_none_or(|limit| *height > limit))
    }

    /// Reads the block at [`next_unread`](Self::next_unread) into the set, if
    /// there is one.
    pub(super) async fn read_next(&mut self) -> Result<(), BoxError> {
        let Some(height) = self.next_unread() else {
            return Ok(());
        };

        match read_block(&self.state, height).await? {
            Some(block) => self.set.record(height, block.hash, block.commitment),
            None => self.unreadable_from = Some(height),
        }
        Ok(())
    }

    /// Forgets every block from `lowest_reachable` up, after a repair could not
    /// tell which of them a reorg displaced, and returns `error`.
    fn give_up_repair(&mut self, lowest_reachable: Height, error: BoxError) -> BoxError {
        metrics::counter!("mining.mesh.reorg.repair_failures").increment(1);
        self.forget_from(lowest_reachable);
        error
    }

    /// Forgets `height` and every block above it.
    fn forget_from(&mut self, height: Height) {
        let below = height
            .previous()
            .expect("every caller passes a height a reorg can reach, which is at least 1");
        self.set.forget_above(below);
    }
}

/// Reports how many gateways are named and how much of the window is read.
fn record_metrics(snapshot: &MinerSetSnapshot) {
    // Each is at most the number of heights in a two-day window.
    metrics::gauge!("mining.mesh.gateways").set(snapshot.gateways.len() as f64);
    metrics::gauge!("mining.mesh.window.checked").set(snapshot.checked as f64);
    metrics::gauge!("mining.mesh.window.total").set(snapshot.window_len as f64);
}
