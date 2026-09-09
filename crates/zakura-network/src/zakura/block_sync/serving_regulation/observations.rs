//! Observations retain no work: one record per live response, removed by its last owner.

use super::*;

#[derive(Clone, Debug, Default)]
pub(super) struct ServingMetrics(Arc<StdMutex<Counts>>);

#[derive(Debug, Default)]
struct Counts {
    next_id: u64,
    waiting: usize,
    active: BTreeMap<u64, time::Instant>,
}

impl ServingMetrics {
    pub(super) fn waiting(&self) -> Waiting {
        self.0
            .lock()
            .expect("serving observations are not poisoned")
            .waiting += 1;
        Waiting(self.clone())
    }

    pub(super) fn active(&self) -> Arc<Active> {
        let mut counts = self
            .0
            .lock()
            .expect("serving observations are not poisoned");
        let id = counts.next_id;
        counts.next_id = id
            .checked_add(1)
            .expect("a node cannot serve u64::MAX responses in its lifetime");
        counts.active.insert(id, time::Instant::now());
        Arc::new(Active {
            metrics: self.clone(),
            id,
        })
    }

    pub(super) fn publish(&self) {
        let counts = self
            .0
            .lock()
            .expect("serving observations are not poisoned");
        let count = |value| f64::from(u32::try_from(value).unwrap_or(u32::MAX));
        metrics::gauge!("sync.block.serving.active").set(count(counts.active.len()));
        metrics::gauge!("sync.block.serving.waiting").set(count(counts.waiting));
        metrics::gauge!("sync.block.serving.oldest_age_seconds").set(
            counts
                .active
                .first_key_value()
                .map_or(0.0, |(_, started)| started.elapsed().as_secs_f64()),
        );
    }
}

#[derive(Debug)]
pub(super) struct Waiting(ServingMetrics);

impl Drop for Waiting {
    fn drop(&mut self) {
        self.0
             .0
            .lock()
            .expect("serving observations are not poisoned")
            .waiting -= 1;
    }
}

#[derive(Debug)]
pub(super) struct Active {
    metrics: ServingMetrics,
    id: u64,
}

impl Drop for Active {
    fn drop(&mut self) {
        self.metrics
            .0
            .lock()
            .expect("serving observations are not poisoned")
            .active
            .remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn observation_follows_the_last_response_owner_without_retaining_it() {
        let regulator = GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
        let session = regulator.session(ZakuraPeerId::new(vec![8; 32]).unwrap());
        let mut response = session.admit_now(1).unwrap().commit();
        let read = response.work_lease();
        let frame = response.frame_guard(9);
        drop(response);
        time::advance(Duration::from_secs(3)).await;
        let metrics = &regulator.inner.metrics;
        {
            let counts = metrics.0.lock().unwrap();
            assert_eq!(counts.active.len(), 1);
            assert_eq!(
                counts.active.first_key_value().unwrap().1.elapsed(),
                Duration::from_secs(3)
            );
        }
        drop(read);
        assert_eq!(metrics.0.lock().unwrap().active.len(), 1);
        drop(frame);
        assert!(metrics.0.lock().unwrap().active.is_empty());
        assert_eq!(regulator.snapshot().node_active, 0);
        let wait = metrics.waiting();
        assert_eq!(metrics.0.lock().unwrap().waiting, 1);
        drop(wait);
        assert_eq!(metrics.0.lock().unwrap().waiting, 0);
    }
}
