use metrics::{Gauge, IntoF64};

/// Compute a diagnostic value only when the gauge has a recording handler.
pub(super) fn set_lazy(gauge: Gauge, compute: impl FnOnce() -> f64) {
    struct DeferredValue<F>(F);

    impl<F: FnOnce() -> f64> IntoF64 for DeferredValue<F> {
        fn into_f64(self) -> f64 {
            (self.0)()
        }
    }

    // Gauge::set skips IntoF64 conversion for a no-op handle. Keep scans inside
    // that conversion so disabled metrics never acquire their registry locks.
    gauge.set(DeferredValue(compute));
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        sync::{atomic::Ordering, Arc},
    };

    use metrics::atomics::AtomicU64;

    use super::*;

    #[test]
    fn noop_recorder_skips_gauge_computation() {
        metrics::with_local_recorder(&metrics::NoopRecorder, || {
            set_lazy(metrics::gauge!("test.block_sync.diagnostic"), || {
                panic!("disabled metrics must not compute diagnostics")
            });
        });
    }

    #[test]
    fn enabled_gauge_computes_once_per_sample_and_records_zero() {
        let recorded = Arc::new(AtomicU64::new(0));
        let gauge = Gauge::from_arc(recorded.clone());
        let computations = Cell::new(0);

        for (index, value) in [7.5, 0.0].into_iter().enumerate() {
            set_lazy(gauge.clone(), || {
                computations.set(computations.get() + 1);
                value
            });

            assert_eq!(computations.get(), index + 1);
            assert_eq!(f64::from_bits(recorded.load(Ordering::Relaxed)), value);
        }
    }
}
