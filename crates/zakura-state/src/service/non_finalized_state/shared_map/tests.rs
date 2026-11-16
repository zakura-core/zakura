use super::*;

/// Returns whether `a` and `b` share the group that holds partition `partition`.
fn share_group<K, V>(a: &SharedMap<K, V>, b: &SharedMap<K, V>, partition: usize) -> bool {
    Arc::ptr_eq(
        &a.groups[partition / PARTITIONS_PER_GROUP],
        &b.groups[partition / PARTITIONS_PER_GROUP],
    )
}

#[test]
fn empty_maps_share_one_partition_until_the_first_write() {
    let mut index = SharedMap::<u32, u32>::default();
    let empty_partition = index.partitions().next().unwrap().clone();
    assert!(
        index
            .partitions()
            .all(|partition| Arc::ptr_eq(partition, &empty_partition)),
        "empty maps share one allocation across all partition slots"
    );
    assert_eq!(index.partitions().count(), PARTITIONS);
    let snapshot = index.clone();

    assert_eq!(index.remove(&0), None);
    assert!(index
        .partitions()
        .all(|partition| Arc::ptr_eq(partition, &empty_partition)));

    assert_eq!(index.insert(0, 7), None);
    let touched = index.partition_index(&0);
    for (partition_index, partition) in index.partitions().enumerate() {
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
        .partitions()
        .all(|partition| Arc::ptr_eq(partition, &empty_partition)));
}

#[test]
fn a_snapshot_shares_every_partition_through_one_pointer() {
    let mut index = SharedMap::default();
    for key in 0..1000_u32 {
        index.insert(key, key);
    }
    let group_counts = |index: &SharedMap<u32, u32>| -> Vec<usize> {
        index.groups.iter().map(Arc::strong_count).collect()
    };
    let partition_counts = |index: &SharedMap<u32, u32>| -> Vec<usize> {
        index.partitions().map(Arc::strong_count).collect()
    };
    let (groups_before, partitions_before) = (group_counts(&index), partition_counts(&index));

    let snapshot = index.clone();

    assert!(Arc::ptr_eq(&index.groups, &snapshot.groups));
    assert_eq!(index.shared_partitions(&snapshot), PARTITIONS);
    // Taking a snapshot touches the group list only, not the groups or partitions.
    assert_eq!(Arc::strong_count(&index.groups), 2);
    assert_eq!(group_counts(&index), groups_before);
    assert_eq!(partition_counts(&index), partitions_before);
}

#[test]
fn mutation_copies_only_the_touched_partition() {
    let mut index = SharedMap::default();
    for key in 0..1000_u32 {
        index.insert(key, 0_u32);
    }
    let snapshot = index.clone();
    assert_eq!(index.shared_partitions(&snapshot), PARTITIONS);

    index.insert(0, 1);
    let touched = index.partition_index(&0);
    for (partition, (current, retained)) in
        index.partitions().zip(snapshot.partitions()).enumerate()
    {
        assert_eq!(Arc::ptr_eq(current, retained), partition != touched);
    }
    // Only the touched partition's group is copied; the other groups stay shared.
    for group in 0..GROUPS {
        let partition = group * PARTITIONS_PER_GROUP;
        assert_eq!(
            share_group(&index, &snapshot, partition),
            group != touched / PARTITIONS_PER_GROUP
        );
    }
    assert_eq!(snapshot.get(&0), Some(&0));
    assert_eq!(index.get(&0), Some(&1));
    assert_eq!(index.len(), 1000);
    assert_eq!(snapshot.len(), 1000);

    // A second write to the copied group copies nothing above its partition.
    let touched_group = index.groups[touched / PARTITIONS_PER_GROUP].clone();
    let neighbour = (0..1000_u32)
        .find(|key| {
            let partition = index.partition_index(key);
            partition != touched
                && partition / PARTITIONS_PER_GROUP == touched / PARTITIONS_PER_GROUP
        })
        .expect("1000 keys reach every partition of a group");
    drop(touched_group);
    let group_before = Arc::as_ptr(&index.groups[touched / PARTITIONS_PER_GROUP]);
    index.insert(neighbour, 2);
    assert_eq!(
        Arc::as_ptr(&index.groups[touched / PARTITIONS_PER_GROUP]),
        group_before
    );
    assert_eq!(index.shared_partitions(&snapshot), PARTITIONS - 2);
    assert_eq!(snapshot.get(&neighbour), Some(&0));
}

#[test]
fn removing_an_absent_key_keeps_partitions_shared() {
    let mut index = SharedMap::default();
    index.insert(0_u32, 0_u32);
    let snapshot = index.clone();

    assert_eq!(index.remove(&1), None);
    assert!(Arc::ptr_eq(&index.groups, &snapshot.groups));
    assert_eq!(index.shared_partitions(&snapshot), PARTITIONS);
    assert_eq!(index.len(), 1);
}

#[test]
fn retaining_every_entry_keeps_partitions_shared() {
    let mut index = SharedMap::default();
    for key in 0..1000_u32 {
        index.insert(key, key);
    }
    let snapshot = index.clone();

    for partition in 0..PARTITIONS {
        index.retain_partition(partition, |_, _| true);
    }
    assert!(Arc::ptr_eq(&index.groups, &snapshot.groups));

    let touched = index.partition_index(&0);
    index.retain_partition(touched, |key, _| *key != 0);
    assert_eq!(index.get(&0), None);
    assert_eq!(snapshot.get(&0), Some(&0));
    assert_eq!(index.len(), 999);
    assert_eq!(snapshot.len(), 1000);
    assert_eq!(index.shared_partitions(&snapshot), PARTITIONS - 1);
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
