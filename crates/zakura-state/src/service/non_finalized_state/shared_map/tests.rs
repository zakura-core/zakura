use super::*;

#[test]
fn empty_maps_share_one_partition_until_the_first_write() {
    let mut index = SharedMap::<u32, u32>::default();
    let empty_partition = index.partitions[0].clone();
    assert!(
        index
            .partitions
            .iter()
            .all(|partition| Arc::ptr_eq(partition, &empty_partition)),
        "empty maps share one allocation across all partition slots"
    );
    let snapshot = index.clone();

    assert_eq!(index.remove(&0), None);
    assert!(index
        .partitions
        .iter()
        .all(|partition| Arc::ptr_eq(partition, &empty_partition)));

    assert_eq!(index.insert(0, 7), None);
    let touched = index.partition_index(&0);
    for (partition_index, partition) in index.partitions.iter().enumerate() {
        assert_eq!(
            Arc::ptr_eq(partition, &empty_partition),
            partition_index != touched
        );
    }
    assert_eq!(index.get(&0), Some(&7));
    assert_eq!(index.len(), 1);
    assert_eq!(index.iter().count(), 1);
    assert!(snapshot.is_empty());
    assert_eq!(snapshot.get(&0), None);
    assert!(snapshot
        .partitions
        .iter()
        .all(|partition| Arc::ptr_eq(partition, &empty_partition)));
}

#[test]
fn mutation_copies_only_the_touched_partition() {
    let mut index = SharedMap::default();
    index.insert(0_u32, 0_u32);
    let snapshot = index.clone();
    assert!(index
        .partitions
        .iter()
        .zip(&snapshot.partitions)
        .all(|(a, b)| Arc::ptr_eq(a, b)));

    index.insert(0, 1);
    let touched = index.partition_index(&0);
    for (partition, (current, retained)) in index
        .partitions
        .iter()
        .zip(&snapshot.partitions)
        .enumerate()
    {
        assert_eq!(Arc::ptr_eq(current, retained), partition != touched);
    }
    assert_eq!(snapshot.get(&0), Some(&0));
    assert_eq!(index.get(&0), Some(&1));
    assert_eq!(index.len(), 1);
}

#[test]
fn removing_an_absent_key_keeps_partitions_shared() {
    let mut index = SharedMap::default();
    index.insert(0_u32, 0_u32);
    let snapshot = index.clone();

    assert_eq!(index.remove(&1), None);
    assert!(index
        .partitions
        .iter()
        .zip(&snapshot.partitions)
        .all(|(a, b)| Arc::ptr_eq(a, b)));
    assert_eq!(index.len(), 1);
}

#[test]
fn mutations_and_retained_snapshots_match_independent_maps() {
    let mut index = SharedMap::default();
    let mut expected = HashMap::new();
    let mut readers = Vec::new();
    for step in 0..3000_u32 {
        if step % 100 == 0 {
            readers.push((index.clone(), expected.clone()));
        }
        let key = step % 271;
        if step % 5 == 0 {
            assert_eq!(index.remove(&key), expected.remove(&key));
        } else {
            assert_eq!(index.insert(key, step), expected.insert(key, step));
        }
        assert_eq!(index.len(), expected.len());
        assert_eq!(index.is_empty(), expected.is_empty());
        assert_eq!(
            index
                .iter()
                .map(|(k, v)| (*k, *v))
                .collect::<HashMap<_, _>>(),
            expected
        );
        for (reader, expected_reader) in &readers {
            assert_eq!(reader.len(), expected_reader.len());
            for (key, value) in expected_reader {
                assert_eq!(reader.get(key), Some(value));
                assert!(reader.contains_key(key));
            }
        }
    }
    // Equality is by contents, even with independently randomized partition placement.
    let rebuilt: SharedMap<_, _> = expected.iter().map(|(k, v)| (*k, *v)).collect();
    assert_eq!(index, rebuilt);
    for (key, value) in expected {
        assert_eq!(index.remove(&key), Some(value));
    }
    assert!(index.is_empty());
    assert_eq!(index.iter().count(), 0);
    assert_ne!(index, rebuilt);
}
