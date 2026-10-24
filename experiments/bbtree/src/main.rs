//! Isolated comparison; see README.md for the limits of the Height stand-in.
#![allow(dead_code)]
use std::{
    alloc::{GlobalAlloc, Layout, System},
    collections::BTreeMap,
    hint::black_box,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
    time::Instant,
};

/// Same representation and ordering as zakura_chain::block::Height. No chain dependencies.
mod block {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub struct Height(pub u32);
}
mod std_height {
    include!(concat!(env!("OUT_DIR"), "/std_height.rs"));
}
mod bb_height {
    include!(concat!(env!("OUT_DIR"), "/bb_height.rs"));
}

struct CountingAllocator;
static COUNT: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
// Counts requested bytes cumulatively, not retained bytes, RSS, or allocator overhead.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNT.load(Relaxed) {
            ALLOCS.fetch_add(1, Relaxed);
            BYTES.fetch_add(layout.size(), Relaxed);
        }
        // SAFETY: forward the caller's unchanged allocation contract to System.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: ptr and layout come from this allocator, which delegates to System.
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn measure(n: u32, backend: &str, operation: &str, repetition: usize, mut body: impl FnMut()) {
    body(); // warm-up, not reported
    let start = Instant::now();
    body();
    let ns = start.elapsed().as_nanos();
    ALLOCS.store(0, Relaxed);
    BYTES.store(0, Relaxed);
    COUNT.store(true, Relaxed);
    body(); // separate, untimed allocation pass
    COUNT.store(false, Relaxed);
    println!(
        "map,{backend},{n},{operation},{repetition},{ns},{},{}",
        ALLOCS.load(Relaxed),
        BYTES.load(Relaxed)
    );
}

fn keys(n: u32) -> Vec<u64> {
    let mut keys: Vec<_> = (0..u64::from(n)).collect();
    let mut state = 0x5eed_cafe_1234_abcd_u64;
    for i in (1..keys.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let j = usize::try_from(state % u64::try_from(i + 1).unwrap()).unwrap();
        keys.swap(i, j);
    }
    keys
}

// Both concrete maps execute the same operations, with construction/destruction included
// only where named. Values are 32-byte roots/hashes, keys are integer IDs.
macro_rules! map_workloads {
    ($map:ty, $backend:expr, $n:expr, $rep:expr) => {{
        let n = $n;
        let shuffled = keys(n);
        let full: $map = shuffled.iter().map(|&k| (k, [7u8; 32])).collect();
        measure(n, $backend, "insert_drop", $rep, || {
            let mut map = <$map>::new();
            for &k in &shuffled {
                black_box(map.insert(black_box(k), [7; 32]));
            }
            black_box(map);
        });
        measure(n, $backend, "lookup_hit_miss", $rep, || {
            for &k in &shuffled {
                black_box(full.get(black_box(&k)));
                black_box(full.get(black_box(&(k + u64::from(n)))));
            }
        });
        measure(n, $backend, "ordered_forward_reverse", $rep, || {
            for pair in full.iter() {
                black_box(pair);
            }
            for pair in full.iter().rev() {
                black_box(pair);
            }
        });
        measure(n, $backend, "range_32", $rep, || {
            for &k in &shuffled {
                for pair in full.range(k..k.saturating_add(32)) {
                    black_box(pair);
                }
            }
        });
        measure(n, $backend, "clone_remove_drop", $rep, || {
            let mut map = full.clone();
            for k in &shuffled {
                black_box(map.remove(k));
            }
            black_box(map);
        });
        measure(n, $backend, "clone_mixed_drop", $rep, || {
            let mut map = full.clone();
            for (i, &k) in shuffled.iter().enumerate() {
                match i % 5 {
                    0 => {
                        black_box(map.remove(&k));
                    }
                    1 => {
                        black_box(map.insert(k, [8; 32]));
                    }
                    2 => {
                        black_box(map.range(k..).next());
                    }
                    _ => {
                        black_box(map.get(&k));
                    }
                }
            }
            black_box(map);
        });
    }};
}

fn correctness() {
    range_edges();
    use block::Height;
    let mut std = BTreeMap::new();
    let mut bb = bbtree::unsync::BBTreeMap::new();
    let mut real_std = std_height::HeightMap::default();
    let mut real_bb = bb_height::HeightMap::default();
    let mut oracle = BTreeMap::new();
    for (i, key) in keys(4096).into_iter().cycle().take(20000).enumerate() {
        let height = Height(u32::try_from(key).unwrap());
        match i % 7 {
            0 => {
                assert_eq!(std.remove(&key), bb.remove(&key));
                let expected = oracle.remove(&height);
                assert_eq!(real_std.remove(&height), expected);
                assert_eq!(real_bb.remove(&height), expected);
            }
            1 => {
                real_std.remove_through(height);
                real_bb.remove_through(height);
                oracle.retain(|k, _| *k > height);
            }
            _ => {
                assert_eq!(std.insert(key, i), bb.insert(key, i));
                let expected = oracle.insert(height, i);
                assert_eq!(real_std.insert(height, i), expected);
                assert_eq!(real_bb.insert(height, i), expected);
            }
        }
        assert_eq!(std.get(&key), bb.get(&key));
        assert_eq!(std.range(key..).next(), bb.range(key..).next());
        assert_eq!(std.range(..=key).next_back(), bb.range(..=key).next_back());
        assert_eq!(real_std.get(&height), oracle.get(&height));
        assert_eq!(real_bb.get(&height), oracle.get(&height));
        if i % 101 == 0 {
            assert_eq!(
                std.iter().collect::<Vec<_>>(),
                bb.iter().collect::<Vec<_>>()
            );
            let expected: Vec<_> = oracle.iter().collect();
            assert_eq!(real_std.range(..).collect::<Vec<_>>(), expected);
            assert_eq!(real_bb.range(..).collect::<Vec<_>>(), expected);
            assert_eq!(
                real_std.range(..).rev().collect::<Vec<_>>(),
                real_bb.range(..).rev().collect::<Vec<_>>()
            );
            let mut fork = real_bb.clone();
            fork.insert(height, usize::MAX);
            assert_eq!(real_bb.get(&height), oracle.get(&height));
        }
    }
    // Owned values, borrowed lookup, duplicates, entries, and set order.
    let mut strings = bbtree::unsync::BBTreeMap::new();
    assert_eq!(strings.insert("key".to_owned(), vec![1]), None);
    assert_eq!(strings.insert("key".to_owned(), vec![2]), Some(vec![1]));
    strings.entry("key".to_owned()).or_default().push(3);
    assert_eq!(strings.get("key"), Some(&vec![2, 3]));
    let set: bbtree::unsync::BBTreeSet<_> = [3, 1, 3, 2].into_iter().collect();
    assert_eq!(set.into_iter().collect::<Vec<_>>(), [1, 2, 3]);
    eprintln!("correctness: 20000 deterministic differential operations passed");
}

/// Exercise dense chunks, all bound kinds, invalid ranges, and snapshot isolation.
fn range_edges() {
    use block::Height;
    use std::ops::Bound::{Excluded, Included, Unbounded};
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<bb_height::HeightMap<std::sync::Arc<u64>>>();
    let mut std = std_height::HeightMap::default();
    let mut bb = bb_height::HeightMap::default();
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    for populated in [false, true] {
        if populated {
            for height in 0..192 {
                std.insert(Height(height), height);
                bb.insert(Height(height), height);
            }
        }
        for lower in [0, 1, 63, 64, 65, 127, 128, 191, 192] {
            for upper in [0, 1, 63, 64, 65, 127, 128, 191, 192] {
                for start in [Unbounded, Included(Height(lower)), Excluded(Height(lower))] {
                    for end in [Unbounded, Included(Height(upper)), Excluded(Height(upper))] {
                        let a = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            std.range((start, end))
                                .map(|(k, v)| (*k, *v))
                                .collect::<Vec<_>>()
                        }));
                        let b = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            bb.range((start, end))
                                .map(|(k, v)| (*k, *v))
                                .collect::<Vec<_>>()
                        }));
                        assert_eq!(a.is_err(), b.is_err());
                        if let (Ok(a), Ok(b)) = (a, b) {
                            assert_eq!(a, b);
                        }
                    }
                }
            }
        }
    }
    std::panic::set_hook(hook);
    for boundary in [0, 63, 64, 127, 128, 191, 192] {
        let mut a = std.clone();
        let mut b = bb.clone();
        a.remove_through(Height(boundary));
        b.remove_through(Height(boundary));
        assert_eq!(
            a.range(..).collect::<Vec<_>>(),
            b.range(..).collect::<Vec<_>>()
        );
        assert_eq!(bb.get(&Height(0)), Some(&0));
    }
}

fn main() {
    correctness();
    if std::env::args().any(|arg| arg == "--check") {
        return;
    }
    println!("suite,backend,size,operation,repetition,ns,allocations,requested_bytes");
    for rep in 0..9 {
        for n in [16, 256, 4096, 65536] {
            if rep % 2 == 0 {
                map_workloads!(BTreeMap<u64, [u8; 32]>, "std", n, rep);
                map_workloads!(bbtree::unsync::BBTreeMap<u64, [u8; 32]>, "bb", n, rep);
            } else {
                map_workloads!(bbtree::unsync::BBTreeMap<u64, [u8; 32]>, "bb", n, rep);
                map_workloads!(BTreeMap<u64, [u8; 32]>, "std", n, rep);
            }
        }
        for n in [64, 1000, 4096] {
            let run = |backend| {
                let rows = if backend == "std" {
                    std_height::benchmark::benchmark_height_map(n, 21)
                        .into_iter()
                        .map(|s| (s.name, s.nanos_per_op))
                        .collect::<Vec<_>>()
                } else {
                    bb_height::benchmark::benchmark_height_map(n, 21)
                        .into_iter()
                        .map(|s| (s.name, s.nanos_per_op))
                        .collect::<Vec<_>>()
                };
                for (name, ns) in rows {
                    println!("height,{backend},{n},{name},{rep},{ns},,");
                }
            };
            if rep % 2 == 0 {
                run("std");
                run("bb");
            } else {
                run("bb");
                run("std");
            }
        }
    }
}
