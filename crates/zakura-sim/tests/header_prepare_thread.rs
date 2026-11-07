//! Header preparation stays on the run's thread (M4.6).
//!
//! Outside the simulator build, preparation validates a batch on rayon's
//! global pool. In it, preparation iterates on the caller's thread instead,
//! since every task shares the run's one OS thread. A wake from a pool
//! thread wouldn't trip the scheduler's thread assertion, because the
//! reactor polls its port futures with a no-op waker, so the witness is
//! rayon's own: its global pool starts on first use and can't be built
//! again after. This test is the only one in its binary, so nothing else in
//! the process could start it.
#![cfg(zakura_sim)]

mod support;

use support::inputs;
use zakura_sim::{
    harness::{
        header_node::Chain,
        header_sync::{Dialer, MirrorSpec, Storage, StorageScenario},
        run_prepared, Subsystem,
    },
    observe::{StorageFact, StorageRequest, StorageResult},
    MemorySink, Observation,
};

/// The node prepares a 160-header batch from `mirror-0`, whose chain was itself
/// prepared before the run, and commits it. Then rayon's global pool can
/// still be built, so no preparation in the process used it.
#[test]
fn preparation_never_starts_the_rayon_pool() {
    let scenario = StorageScenario {
        mirrors: vec![MirrorSpec {
            chain: Chain::main(160),
            dialer: Dialer::Mirror,
        }],
        ..StorageScenario::default()
    };
    for input in inputs(2, 256) {
        let ports = Storage::prepare(&scenario);
        let report = run_prepared::<Storage>(
            &scenario,
            ports.clone(),
            input,
            &mut MemorySink::default(),
            None,
        )
        .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(report.end.violation, None, "{:?}", report.end);
        let mut batch = None;
        let mut prepared = false;
        for entry in report.log.entries() {
            match &entry.obs {
                Observation::Storage {
                    instance: 0,
                    fact:
                        StorageFact::Submitted {
                            id,
                            request: StorageRequest::PrepareHeaderTarget { headers: 160, .. },
                            ..
                        },
                    ..
                } => batch = Some(*id),
                Observation::Storage {
                    instance: 0,
                    fact:
                        StorageFact::Completed {
                            id,
                            result: StorageResult::Prepared,
                            ..
                        },
                    ..
                } if Some(*id) == batch => prepared = true,
                _ => {}
            }
        }
        assert!(prepared, "the node prepared all 160 headers in one batch");
        assert_eq!(
            ports.node.engine().snapshot().frontiers.header_best,
            Chain::main(160).tip(&ports.fixture)
        );
    }
    assert!(
        rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build_global()
            .is_ok(),
        "preparation started rayon's global pool"
    );
}
