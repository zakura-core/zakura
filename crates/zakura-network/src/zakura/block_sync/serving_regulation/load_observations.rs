//! Let load tests read timings from the actual shared admission lock.

use super::*;

impl GetBlocksServingRegulator {
    pub(in crate::zakura::block_sync) fn locks_for_test(
        &self,
    ) -> zakura_test::resources::LockSnapshot {
        self.inner.admission.session_lock_probe.snapshot()
    }
}
