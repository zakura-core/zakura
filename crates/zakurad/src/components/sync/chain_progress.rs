//! A deadline renewed only by verified blocks or a changed committed tip.

use tokio::time::Instant;
use zakura_chain::block::{Hash, Height};

use super::BLOCK_VERIFY_TIMEOUT;

pub(super) struct ChainProgress {
    tip: Option<(Height, Hash)>,
    last_verified: Instant,
}

impl ChainProgress {
    pub(super) fn new(tip: Option<(Height, Hash)>) -> Self {
        Self {
            tip,
            last_verified: Instant::now(),
        }
    }

    pub(super) fn observe(&mut self, tip: Option<(Height, Hash)>, verified_block: bool) {
        if verified_block || tip != self.tip {
            self.last_verified = Instant::now();
            self.tip = tip;
        }
    }

    pub(super) fn deadline(&self) -> Instant {
        self.last_verified + BLOCK_VERIFY_TIMEOUT
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn rejected_activity_does_not_extend_the_deadline() {
        let tip = Some((Height(10), Hash([1; 32])));
        let mut progress = ChainProgress::new(tip);
        let deadline = progress.deadline();
        for _ in 0..10 {
            tokio::time::advance(BLOCK_VERIFY_TIMEOUT / 10).await;
            progress.observe(tip, false);
            assert_eq!(progress.deadline(), deadline);
        }
        assert!(Instant::now() >= deadline);
        progress.observe(tip, true);
        assert!(progress.deadline() > deadline);
        tokio::time::advance(BLOCK_VERIFY_TIMEOUT / 2).await;
        let before_tip_change = progress.deadline();
        progress.observe(Some((Height(11), Hash([2; 32]))), false);
        assert!(progress.deadline() > before_tip_change);
    }
}
