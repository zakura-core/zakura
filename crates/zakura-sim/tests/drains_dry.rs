//! E1's generator guard: every committed seed, a correct node cut at its
//! harness's budget, drains to a dry end, never to its bound.
#![cfg(zakura_sim)]

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    thread,
};

use zakura_sim::{harness, seed, HarnessFailure, NullSink};

/// Every saved Plan and seed under `tests/fixtures`, run by its harness as
/// a campaign would, ends without [`HarnessFailure::DrainBound`]: once the
/// budget cuts it, the drain finishes what the live phase started and runs
/// dry within the harness's drain bound. An unbounded generator that never
/// registers ([`zakura_sim::sim::Generators`]) keeps a task ready, so its
/// run hits the bound and fails here, naming the task. Machines have no
/// drain. The seeds run on a few threads, each run on its own.
#[test]
fn every_saved_plan_drains_dry() {
    let fixtures = seed::repo_root().join("crates/zakura-sim/tests/fixtures");
    let loaded = seed::load(&[fixtures], None).expect("the fixture directory lists");
    let seeds: Vec<_> = loaded
        .admitted
        .iter()
        .map(|seed| {
            let entry =
                harness::find(&seed.sidecar.harness).expect("admitted seeds' harnesses exist");
            (seed, entry)
        })
        .filter(|(_, entry)| !entry.machine)
        .collect();
    assert!(!seeds.is_empty(), "the fixtures hold seeds");
    let next = AtomicUsize::new(0);
    let bound = Mutex::new(Vec::new());
    let threads = thread::available_parallelism().map_or(1, |n| n.get().min(8));
    thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                while let Some((seed, entry)) = seeds.get(next.fetch_add(1, Ordering::Relaxed)) {
                    let run = (entry.run_bytes)(&seed.input, &mut NullSink, None);
                    let end = run.report().map(|report| &report.end);
                    if let Some(HarnessFailure::DrainBound { open }) =
                        end.and_then(|end| end.harness.as_ref())
                    {
                        bound
                            .lock()
                            .expect("no thread panics holding it")
                            .push(format!("{}: {open}", seed.path.display()));
                    }
                }
            });
        }
    });
    let bound = bound.into_inner().expect("every thread finished");
    assert!(
        bound.is_empty(),
        "drains cut off at their bound: {bound:#?}"
    );
}
