//! Tests for the simulator build's random draws: `rng()` and the seeded
//! hashed collections. They only build with `RUSTFLAGS="--cfg zakura_sim"`,
//! in a separate `CARGO_TARGET_DIR`.
#![cfg(zakura_sim)]

mod support;

use rand_10::{seq::SliceRandom, Rng, RngExt};
use support::run;
use zakura_rt::{
    collections::{hash_map::Entry, HashMap, HashSet},
    rand,
    sim::{DrawKind, Op},
};

#[test]
fn rng_values_come_from_one_seeded_stream() {
    let ((bytes, words), report) = run(async {
        let mut rng = rand::rng();
        let mut bytes = [0u8; 12];
        rng.fill_bytes(&mut bytes);
        // A second handle continues the same run's stream.
        let mut other = rand::rng();
        (bytes, [rng.next_u32(), other.next_u32()])
    });
    // The stand-in answers the one draw with 0, as an exhausted `Plan` does.
    assert_ne!(bytes, [0; 12]);
    assert_ne!(words[0], words[1]);
    assert_eq!(report.draws, [(DrawKind::Rng, 0)], "one seed for the run");
    assert!(report.points.is_empty(), "values aren't preemption points");
}

#[test]
fn rng_ext_and_shuffle_replay() {
    let shuffled = || {
        run(async {
            let mut values: Vec<u32> = (0..8).collect();
            values.shuffle(&mut rand::rng());
            (values, rand::rng().random_range(10..20))
        })
        .0
    };
    let first = shuffled();
    assert_eq!(first, shuffled());
    assert!((10..20).contains(&first.1));
}

#[test]
fn rng_outside_a_run_is_the_thread_rng() {
    let mut rng = rand::rng();
    let values: Vec<u64> = (0..4).map(|_| rng.next_u64()).collect();
    assert!(values.windows(2).any(|w| w[0] != w[1]));
}

#[test]
fn one_hash_seed_per_run_and_order_replays() {
    let order = || {
        run(async {
            let map: HashMap<u32, u32> = (0..64).map(|k| (k, k)).collect();
            let set = HashSet::from([1, 2, 3]);
            let other = HashMap::<u8, u8>::new();
            assert!(other.is_empty() && set.len() == 3);
            map.keys().copied().collect::<Vec<_>>()
        })
    };
    let (first, report) = order();
    assert_eq!(
        report.draws,
        [(DrawKind::HashSeed, 0)],
        "one seed for the run"
    );
    assert_eq!(first, order().0, "the same seed gives the same order");
}

#[test]
fn hash_map_newtype_keeps_std_behavior() {
    let mut map = HashMap::from([("a", 1), ("b", 2)]);
    map.extend([("c", 3)]);
    *map.entry("a").or_insert(0) += 10;
    if let Entry::Vacant(slot) = map.entry("d") {
        slot.insert(4);
    }
    assert_eq!(map["a"], 11);
    assert_eq!(map.len(), 4);
    let copy = map.clone();
    assert_eq!(copy, map);
    let mut keys: Vec<_> = map.into_keys().collect();
    keys.sort_unstable();
    assert_eq!(keys, ["a", "b", "c", "d"]);

    let mut set: HashSet<u8> = [1, 2].into_iter().collect();
    set.extend(&[3]);
    assert!(set.contains(&3));
    assert_eq!(set.iter().copied().sum::<u8>(), 6);
}

#[test]
fn self_referencing_mutations_borrow_check_like_std() {
    let mut map: HashMap<u32, usize> = HashMap::new();
    map.insert(1, map.len());
    map.shrink_to(2 * map.len());
    let mut set: HashSet<usize> = HashSet::new();
    set.insert(set.len());
    set.reserve(set.len());
    assert_eq!((map[&1], set.len()), (0, 1));
}

#[test]
fn unbiased_select_starts_where_the_plan_says() {
    // The stand-in answers each draw with how many came before, so after
    // one rng draw the select starts at branch 1 of 3.
    let (won, report) = run(async {
        rand::rng().next_u64();
        zakura_rt::select! {
            v = std::future::ready(0) => v,
            v = std::future::ready(1) => v,
            v = std::future::ready(2) => v,
        }
    });
    assert_eq!(won, 1);
    assert_eq!(report.draws[1], (DrawKind::Select, 3));
    assert_eq!(report.count(Op::Select), 1);
}

#[test]
fn biased_select_keeps_source_order_and_guards_and_else_work() {
    let ((biased, guarded, fallback), report) = run(async {
        let biased = zakura_rt::select! {
            biased;
            v = std::future::ready(0) => v,
            v = std::future::ready(1) => v,
        };
        let skip_first = true;
        let guarded = zakura_rt::select! {
            biased;
            v = std::future::ready(0), if !skip_first => v,
            v = std::future::ready(1) => v,
        };
        let fallback = zakura_rt::select! {
            v = std::future::ready(0), if false => v,
            else => 9,
        };
        (biased, guarded, fallback)
    });
    assert_eq!((biased, guarded, fallback), (0, 1, 9));
    assert_eq!(
        report.draws,
        [(DrawKind::Select, 1)],
        "only the unbiased select draws"
    );
}

#[tokio::test]
async fn select_outside_a_run_is_tokio() {
    let won = zakura_rt::select! {
        v = std::future::ready(1) => v,
        () = std::future::pending::<()>() => 0,
    };
    assert_eq!(won, 1);
}

#[test]
fn stream_map_starts_where_the_plan_says() {
    let (keys, report) = run(async {
        use tokio_stream::StreamExt;

        // Taking the run's rng seed first makes the map's draws 1, 2, 3, 4,
        // which start the polls at entries 1, 2, 0 and 1 of 3.
        let _ = rand::rng().next_u32();
        let mut map = zakura_rt::stream::StreamMap::new();
        for key in 0..3 {
            map.insert(key, tokio_stream::iter([key, key]));
        }
        let mut keys = Vec::new();
        for _ in 0..4 {
            keys.push(map.next().await.expect("every stream has two items").0);
        }
        keys
    });
    assert_eq!(keys, [1, 2, 0, 1]);
    assert_eq!(&report.draws[1..], [(DrawKind::StreamMap, 3); 4]);
}
