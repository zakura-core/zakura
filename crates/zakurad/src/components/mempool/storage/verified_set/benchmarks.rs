//! Criterion benchmarks of full-pool eviction planning and storage admission.
//!
//! Uses actual storage and dependency indexes with synthetic verified fixtures.
//! Downloading, cryptographic verification, and gossip are outside these timings.

use std::{
    collections::BinaryHeap,
    time::{Duration, Instant},
};

use criterion::{black_box, BenchmarkId, Criterion};
use zakura_chain::{transaction::MEMPOOL_TRANSACTION_COST_THRESHOLD, transparent::OutPoint};

use super::super::{fixtures::TxFactory, Storage};
use super::*;
use crate::components::mempool::config;

#[derive(Clone, Copy)]
enum Shape {
    Independent,
    Chain,
    Join,
    Triangle,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Self::Independent => "independent",
            Self::Chain => "chain",
            Self::Join => "join",
            Self::Triangle => "triangle",
        }
    }
}

struct Fixture {
    storage: Storage,
    spent: HashMap<transaction::Hash, Vec<OutPoint>>,
}

const BASE_FEE: u64 = 10_000;

impl Fixture {
    fn new(factory: &mut TxFactory, count: usize, shape: Shape) -> Self {
        let count_cost = u64::try_from(count).expect("benchmark count fits in u64");
        let mut fixture = Self {
            storage: Storage::new(&config::Config {
                tx_cost_limit: count_cost * MEMPOOL_TRANSACTION_COST_THRESHOLD,
                ..Default::default()
            }),
            spent: HashMap::new(),
        };
        for start in (0..count).step_by(MAX_MEMPOOL_PACKAGE_TRANSACTIONS) {
            let mut group: Vec<VerifiedUnminedTx> = Vec::new();
            for position in 0..MAX_MEMPOOL_PACKAGE_TRANSACTIONS.min(count - start) {
                let output = |tx: &VerifiedUnminedTx, index| {
                    OutPoint::from_usize(tx.transaction.id().mined_id(), index)
                };
                let spent = match (shape, position) {
                    (Shape::Chain, 1 | 2) => vec![output(&group[position - 1], 0)],
                    (Shape::Join, 2) => vec![output(&group[0], 0), output(&group[1], 0)],
                    (Shape::Triangle, 1) => vec![output(&group[0], 0)],
                    (Shape::Triangle, 2) => vec![output(&group[0], 1), output(&group[1], 0)],
                    _ => vec![],
                };
                let fee = if count - start < MAX_MEMPOOL_PACKAGE_TRANSACTIONS {
                    100 * BASE_FEE
                } else {
                    [BASE_FEE, 2 * BASE_FEE, 5 * BASE_FEE][position]
                };
                let tx = factory.tx_with(fee, 2, 0);
                fixture
                    .storage
                    .insert(tx.clone(), spent.clone(), None)
                    .unwrap();
                fixture.spent.insert(tx.transaction.id().mined_id(), spent);
                group.push(tx);
            }
        }
        assert_eq!(fixture.storage.total_cost(), fixture.storage.tx_cost_limit);
        fixture
    }

    fn admit(&mut self, incoming: &VerifiedUnminedTx, expected_success: bool) -> Duration {
        let before_cost = self.storage.total_cost();
        // Snapshot only the victims, outside the timed admission. No pool clone.
        let victims = self.storage.verified.select_eviction_victims(
            incoming,
            &HashSet::new(),
            self.storage.tx_cost_limit,
        );
        let mut ids = HashSet::new();
        for root in victims.roots.into_iter().flatten() {
            ids.extend(self.storage.verified.descendants(root, &HashSet::new()));
            ids.insert(root);
        }
        let mut saved: Vec<_> = ids
            .iter()
            .map(|id| {
                (
                    self.storage.transactions()[id].clone(),
                    self.spent[id].clone(),
                )
            })
            .collect();
        self.storage.clear_tip_rejections();
        let start = Instant::now();
        let (result, evicted) = black_box(self.storage.insert_with_evicted_ids(
            black_box(incoming.clone()),
            vec![],
            None,
        ));
        let elapsed = start.elapsed();
        assert_eq!(result.is_ok(), expected_success);
        assert_eq!(evicted.len(), ids.len());

        // Restore victims in dependency order, including their tie-break times.
        // Restoration and assertions are outside the timed admission.
        if expected_success {
            self.storage
                .verified
                .remove(&incoming.transaction.id().mined_id());
        }
        while !saved.is_empty() {
            let next = saved
                .iter()
                .position(|(_, spent)| {
                    spent
                        .iter()
                        .all(|outpoint| self.storage.verified.contains(&outpoint.hash))
                })
                .expect("the verified dependency graph is acyclic");
            let (tx, spent) = saved.swap_remove(next);
            let id = tx.transaction.id().mined_id();
            let time = tx.time;
            self.storage
                .verified
                .insert(tx, spent, &mut self.storage.pending_outputs, None)
                .unwrap();
            let affected = self.storage.verified.dependency_group([id]);
            self.storage.verified.remove_eviction_candidates(&affected);
            self.storage
                .verified
                .transactions
                .get_mut(&id)
                .unwrap()
                .time = time;
            self.storage.verified.insert_eviction_candidates(&affected);
        }
        assert_eq!(self.storage.total_cost(), before_cost);
        elapsed
    }
}

/// Benchmarks indexed selection against the previous full-heap implementation,
/// then measures actual storage admission with setup and restoration excluded.
pub fn mempool_eviction_benchmarks(c: &mut Criterion) {
    let mut factory = TxFactory::new();
    let rejected = factory.tx(BASE_FEE);
    let small = factory.tx(1_000_000);
    let max_bytes = usize::try_from(config::DEFAULT_MAX_TRANSACTION_BYTES)
        .expect("default transaction limit fits in usize");
    let padding = max_bytes - small.transaction.size();
    let probe = factory.tx_with(1_000_000, 0, padding);
    // CompactSize gains bytes when the proof length crosses its next boundary.
    let overhead = probe.transaction.size() - max_bytes;
    let large = factory.tx_with(1_000_000, 0, padding - overhead);
    assert_eq!(large.transaction.size(), max_bytes);
    let none = HashSet::new();
    let default_count = usize::try_from(
        config::Config::default().tx_cost_limit / MEMPOOL_TRANSACTION_COST_THRESHOLD,
    )
    .expect("default transaction count fits in usize");
    for shape in [
        Shape::Independent,
        Shape::Chain,
        Shape::Join,
        Shape::Triangle,
    ] {
        let mut group = c.benchmark_group(format!("mempool_eviction/{}", shape.name()));
        for count in [default_count / 8, default_count] {
            let mut fixture = Fixture::new(&mut factory, count, shape);
            for (name, tx, accepted) in [
                ("reject", &rejected, false),
                ("one", &small, true),
                ("250kb", &large, true),
            ] {
                assert_eq!(
                    fixture
                        .storage
                        .verified
                        .select_eviction_victims(tx, &none, fixture.storage.tx_cost_limit,)
                        .roots,
                    fixture
                        .storage
                        .verified
                        .select_eviction_victims_by_heap(tx, &none, fixture.storage.tx_cost_limit,)
                        .roots,
                );
                group.bench_with_input(
                    BenchmarkId::new(format!("indexed/{name}"), count),
                    tx,
                    |b, tx| {
                        b.iter(|| {
                            black_box(fixture.storage.verified.select_eviction_victims(
                                black_box(tx),
                                &none,
                                fixture.storage.tx_cost_limit,
                            ))
                        });
                    },
                );
                group.bench_with_input(
                    BenchmarkId::new(format!("heap_baseline/{name}"), count),
                    tx,
                    |b, tx| {
                        b.iter(|| {
                            black_box(fixture.storage.verified.select_eviction_victims_by_heap(
                                black_box(tx),
                                &none,
                                fixture.storage.tx_cost_limit,
                            ))
                        });
                    },
                );
                group.bench_with_input(
                    BenchmarkId::new(format!("admission/{name}"), count),
                    tx,
                    |b, tx| {
                        b.iter_custom(|iterations| {
                            (0..iterations).map(|_| fixture.admit(tx, accepted)).sum()
                        });
                    },
                );
            }
            fixture.storage.tx_cost_limit += large.cost();
            for (name, tx) in [("one", &small), ("250kb", &large)] {
                group.bench_with_input(
                    BenchmarkId::new(format!("admission_with_room/{name}"), count),
                    tx,
                    |b, tx| {
                        b.iter_custom(|iterations| {
                            (0..iterations).map(|_| fixture.admit(tx, true)).sum()
                        });
                    },
                );
            }
        }
        group.finish();
    }
}

// Frozen pre-index selector: a reproducible baseline for this regression.
impl VerifiedSet {
    fn select_eviction_victims_by_heap(
        &self,
        incoming: &VerifiedUnminedTx,
        incoming_ancestors: &HashSet<transaction::Hash>,
        tx_cost_limit: u64,
    ) -> EvictionVictims {
        let needed = self
            .total_cost
            .saturating_add(incoming.cost())
            .saturating_sub(tx_cost_limit);
        let incoming = Self::eviction_cost(incoming);

        // Victims and their descendants, plus the incoming transaction's ancestors.
        let mut unavailable = incoming_ancestors.clone();

        let candidate = |tx_id: transaction::Hash, unavailable: &HashSet<_>| {
            Reverse((
                self.package_score(&tx_id, unavailable),
                Reverse(self.transactions[&tx_id].time),
                tx_id,
            ))
        };
        let mut candidates: BinaryHeap<_> = self
            .transactions
            .keys()
            .filter(|tx_id| !unavailable.contains(*tx_id))
            .map(|&tx_id| candidate(tx_id, &unavailable))
            .collect();

        let mut victims = EvictionVictims::default();
        let mut roots = Vec::new();
        let mut freed = 0;

        while freed < needed {
            let Some(Reverse((score, _, root))) = candidates.pop() else {
                // Evicting every other package does not free enough space.
                return victims;
            };

            // Skip evicted transactions, and scores that an earlier victim changed.
            if unavailable.contains(&root) || score != self.package_score(&root, &unavailable) {
                continue;
            }

            victims.cheapest.get_or_insert(score);
            if !incoming.exceeds_by_increment(score) {
                return victims;
            }

            roots.push(root);
            freed += self.package(&root, &unavailable).cost();
            if freed >= needed {
                break;
            }

            let mut removed = self.descendants(root, &unavailable);
            removed.insert(root);

            let group = self.dependency_group([root]);
            unavailable.extend(removed);
            candidates.extend(
                group
                    .into_iter()
                    .filter(|tx_id| !unavailable.contains(tx_id))
                    .map(|tx_id| candidate(tx_id, &unavailable)),
            );
        }

        victims.roots = Some(roots);
        victims
    }
}
