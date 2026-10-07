//! Measurements attached to a batch without changing its verification behavior.

use serde::Serialize;
use std::time::Instant;

/// Counts accepted items and work units in one pending cryptographic batch.
#[derive(Default)]
pub struct BatchObservation {
    items: u64,
    work_units: u64,
    first_queued: Option<Instant>,
}

impl BatchObservation {
    /// Record an item only after the verifier accepts it into the batch.
    pub fn queued(&mut self, work_units: usize) {
        if !super::enabled() {
            return;
        }
        self.items = self.items.saturating_add(1);
        self.work_units = self
            .work_units
            .saturating_add(u64::try_from(work_units).unwrap_or(u64::MAX));
        self.first_queued.get_or_insert_with(Instant::now);
    }

    /// Run the unchanged validation closure on its existing CPU worker.
    ///
    /// `submitted` is taken immediately before scheduling the CPU job. In-batch
    /// wait starts at the first accepted item; it excludes upstream service queues.
    pub fn verify(
        self,
        verifier: &'static str,
        unit: &'static str,
        mode: &'static str,
        submitted: Instant,
        validate: impl FnOnce() -> bool,
    ) -> bool {
        self.measure((verifier, unit, mode), submitted, validate, |result| {
            *result
        })
    }

    /// Measure a verifier without discarding its original success or error value.
    pub fn verify_result<T, E>(
        self,
        verifier: &'static str,
        unit: &'static str,
        mode: &'static str,
        submitted: Instant,
        validate: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        self.measure((verifier, unit, mode), submitted, validate, Result::is_ok)
    }

    fn measure<R>(
        self,
        labels: (&'static str, &'static str, &'static str),
        submitted: Instant,
        validate: impl FnOnce() -> R,
        succeeded: impl FnOnce(&R) -> bool,
    ) -> R {
        if self.first_queued.is_none() {
            return validate();
        }
        let started = Instant::now();
        let result = validate();
        let finished = Instant::now();
        if let Some(first_queued) = self.first_queued {
            super::emit(|| BatchEvent {
                event: "crypto_batch",
                verifier: labels.0,
                unit: labels.1,
                mode: labels.2,
                items: self.items,
                work_units: self.work_units,
                success: succeeded(&result),
                in_batch_wait_ms: submitted
                    .saturating_duration_since(first_queued)
                    .as_secs_f64()
                    * 1000.0,
                scheduling_ms: started.saturating_duration_since(submitted).as_secs_f64() * 1000.0,
                execution_ms: finished.saturating_duration_since(started).as_secs_f64() * 1000.0,
            });
        }
        result
    }
}

#[derive(Serialize)]
struct BatchEvent {
    event: &'static str,
    verifier: &'static str,
    unit: &'static str,
    mode: &'static str,
    items: u64,
    work_units: u64,
    success: bool,
    in_batch_wait_ms: f64,
    scheduling_ms: f64,
    execution_ms: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preserves_owned_error_and_success_values() {
        for enabled in [false, true] {
            let observation = BatchObservation {
                items: 1,
                work_units: 1,
                first_queued: enabled.then(Instant::now),
            };
            let error = Box::new(42);
            let identity = &*error as *const i32;
            let result: Result<(), Box<i32>> = observation.verify_result(
                "ed25519",
                "signatures",
                "fallback",
                Instant::now(),
                || Err(error),
            );
            assert_eq!(&*result.unwrap_err() as *const i32, identity);
        }
        let result: Result<String, ()> = BatchObservation::default().verify_result(
            "ed25519",
            "signatures",
            "batch",
            Instant::now(),
            || Ok("unchanged".into()),
        );
        assert_eq!(result.unwrap(), "unchanged");
    }

    #[test]
    fn instrumentation_preserves_validation_result_and_consumes_batch() {
        for expected in [true, false] {
            let observation = BatchObservation {
                items: 3,
                work_units: 12,
                first_queued: Some(Instant::now()),
            };
            let mut called = 0;
            assert_eq!(
                observation.verify("halo2", "actions", "batch", Instant::now(), || {
                    called += 1;
                    expected
                }),
                expected
            );
            assert_eq!(called, 1);
        }
    }
}
