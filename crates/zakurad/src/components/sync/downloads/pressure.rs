//! Download throttling owned by admitted work, with reset-safe task lifetimes.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use tokio::sync::watch;
use zakura_chain::{block::Height, chain_tip::ChainTip, parameters::Network};

use super::{lookahead_heights, lookahead_limit_multiplier};

#[derive(Debug)]
pub(super) struct DownloadPressure {
    sender: watch::Sender<bool>,
    generation: Arc<()>,
    heights: BTreeMap<Height, usize>,
    network: Network,
    limit: usize,
}

impl DownloadPressure {
    pub(super) fn new(sender: watch::Sender<bool>, network: Network, limit: usize) -> Self {
        Self {
            sender,
            generation: Arc::new(()),
            heights: BTreeMap::new(),
            network,
            limit,
        }
    }

    pub(super) fn generation(&self) -> Arc<()> {
        self.generation.clone()
    }

    /// Invalidates both future admissions and cleanup from the old round.
    pub(super) fn reset(&mut self) {
        self.generation = Arc::new(());
        self.heights.clear();
        self.sender.send_replace(false);
    }

    fn update(&self, tip: Option<Height>) {
        let multiplier = tip.map_or(1, |tip| lookahead_limit_multiplier(&self.network, tip));
        let (_, pause, reset) = lookahead_heights(tip, self.limit, multiplier);
        let highest = self.heights.last_key_value().map(|(height, _)| *height);
        let paused = match highest {
            None => false,
            Some(height) if height > pause => true,
            Some(height) if height <= reset => false,
            Some(_) => *self.sender.borrow(),
        };
        self.sender.send_replace(paused);
    }
}

/// Holds one admitted height until its task completes or is cancelled.
pub(super) struct PressureLease<T: ChainTip> {
    pressure: Arc<Mutex<DownloadPressure>>,
    generation: Arc<()>,
    height: Height,
    tip: T,
}

impl<T: ChainTip> PressureLease<T> {
    pub(super) fn admit(
        pressure: Arc<Mutex<DownloadPressure>>,
        generation: Arc<()>,
        height: Height,
        tip: T,
    ) -> Option<Self> {
        {
            let mut state = pressure.lock().expect("pressure updates never panic");
            if !Arc::ptr_eq(&state.generation, &generation) {
                return None;
            }
            *state.heights.entry(height).or_default() += 1;
            state.update(tip.best_tip_height());
        }
        Some(Self {
            pressure,
            generation,
            height,
            tip,
        })
    }
}

impl<T: ChainTip> Drop for PressureLease<T> {
    fn drop(&mut self) {
        let mut state = self.pressure.lock().expect("pressure updates never panic");
        if !Arc::ptr_eq(&state.generation, &self.generation) {
            return;
        }
        let count = state
            .heights
            .get_mut(&self.height)
            .expect("an active lease owns a height entry");
        *count -= 1;
        if *count == 0 {
            state.heights.remove(&self.height);
        }
        state.update(self.tip.best_tip_height());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::chain_tip::mock::MockChainTip;

    #[test]
    fn low_tasks_cannot_clear_pressure_from_pending_high_tasks() {
        let (tip, sender) = MockChainTip::new();
        let height = Height(3_535_411);
        sender.send_best_tip_height(height);
        let (sender, receiver) = watch::channel(false);
        let pressure = Arc::new(Mutex::new(DownloadPressure::new(
            sender,
            Network::Mainnet,
            10,
        )));
        let generation = pressure.lock().unwrap().generation();
        let high = PressureLease::admit(
            pressure.clone(),
            generation.clone(),
            (height + 100).unwrap(),
            tip.clone(),
        )
        .unwrap();
        assert!(*receiver.borrow());
        let low = PressureLease::admit(pressure.clone(), generation, height, tip).unwrap();
        assert!(*receiver.borrow());
        drop(low);
        assert!(*receiver.borrow());
        drop(high);
        assert!(!*receiver.borrow());
    }

    #[test]
    fn reset_invalidates_old_admissions_and_cleanup() {
        let (tip, sender) = MockChainTip::new();
        let height = Height(3_535_411);
        sender.send_best_tip_height(height);
        let (sender, receiver) = watch::channel(false);
        let pressure = Arc::new(Mutex::new(DownloadPressure::new(
            sender,
            Network::Mainnet,
            10,
        )));
        let old_generation = pressure.lock().unwrap().generation();
        let old = PressureLease::admit(
            pressure.clone(),
            old_generation.clone(),
            (height + 100).unwrap(),
            tip.clone(),
        )
        .unwrap();
        pressure.lock().unwrap().reset();
        assert!(!*receiver.borrow());
        let generation = pressure.lock().unwrap().generation();
        let current = PressureLease::admit(
            pressure.clone(),
            generation,
            (height + 100).unwrap(),
            tip.clone(),
        )
        .unwrap();
        assert!(*receiver.borrow());
        assert!(PressureLease::admit(pressure, old_generation, height, tip).is_none());
        drop(old);
        assert!(*receiver.borrow());
        drop(current);
        assert!(!*receiver.borrow());
    }
}
