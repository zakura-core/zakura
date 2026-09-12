//! The expected budget is the sum of live allocations, independent of pool counters.

use super::*;

#[test]
fn response_authorization_funds_allocation_before_retaining_it() {
    use crate::zakura::CloseCause;
    use tokio_util::sync::CancellationToken;
    use zakura_test::allocations::measure;

    let node = ResponseMemory::new(4096, 4096);
    let memory = node.connection();
    let scope =
        ResponseScope::with_memory(CancellationToken::new(), CloseCause::new(), memory.clone());
    let (authorization, allocated) = measure(|| scope.authorize().unwrap());
    let funded = node.reserved_for_test();
    assert_eq!(funded, u64::try_from(allocated.retained_bytes).unwrap());
    assert!(funded > 0);

    let writer = authorization.write_permission();
    drop(authorization);
    assert_eq!(node.reserved_for_test(), funded);
    drop(writer);
    assert_eq!(node.reserved_for_test(), 0);

    let full = memory.try_reserve(4096).unwrap();
    let (denied, allocated) = measure(|| scope.authorize());
    assert!(matches!(denied, Err(ResponseAdmissionError::MemoryFull)));
    assert_eq!(allocated.requested_bytes, 0);
    drop(full);
    assert_eq!(node.reserved_for_test(), 0);
}

#[test]
fn response_authorization_transitions_do_not_allocate_unfunded_storage() {
    use crate::zakura::CloseCause;
    use tokio_util::sync::CancellationToken;
    use zakura_test::allocations::measure;

    let node = ResponseMemory::new(4096, 4096);
    let scope = ResponseScope::with_memory(
        CancellationToken::new(),
        CloseCause::new(),
        node.connection(),
    );
    let mut authorization = scope.authorize().unwrap();
    let writer = authorization.write_permission();
    let funded_before = node.reserved_for_test();
    let (_, allocated) = measure(|| {
        assert!(writer.publish(|| {}));
        assert!(writer.try_start(|| true));
        authorization.finish();
    });
    let additionally_funded = node.reserved_for_test() - funded_before;
    assert!(
        u64::try_from(allocated.peak_live_bytes).unwrap() <= additionally_funded,
        "transitions retained {} extra bytes with {additionally_funded} extra bytes funded",
        allocated.peak_live_bytes,
    );
}

proptest! {
    #[test]
    fn response_metadata_histories_account_for_every_retained_owner(
        node_limit in 1u64..512,
        connection_limit in 1u64..256,
        actions in prop::collection::vec((any::<bool>(), 0usize..16, 0usize..4, 0u64..300), 1..192),
    ) {
        let node = ResponseMemory::new(node_limit, connection_limit);
        let mut connections: [_; 4] = std::array::from_fn(|_| node.connection());
        let mut permits: [Option<_>; 16] = std::array::from_fn(|_| None);
        let mut retained: [Option<(usize, u64)>; 16] = [None; 16];

        for (allocate, index, connection, bytes) in actions {
            let node_used: u64 = retained.iter().flatten().map(|(_, bytes)| *bytes).sum();
            let connection_used: u64 = retained.iter().flatten()
                .filter(|(owner, _)| *owner == connection).map(|(_, bytes)| *bytes).sum();
            // A new service or receiver keeps the same connection's context.
            connections[connection] = connections[connection].clone();
            if allocate && retained[index].is_none() {
                let allowed = bytes != 0 && bytes <= node_limit - node_used
                    && bytes <= connection_limit - connection_used;
                permits[index] = connections[connection].try_reserve(bytes);
                prop_assert_eq!(permits[index].is_some(), allowed);
                if allowed { retained[index] = Some((connection, bytes)); }
            } else if !allocate {
                drop(permits[index].take());
                retained[index] = None;
            }
            let expected_node: u64 = retained.iter().flatten().map(|(_, bytes)| *bytes).sum();
            prop_assert_eq!(node.reserved_for_test(), expected_node);
            prop_assert!(expected_node <= node_limit);
            for (owner, memory) in connections.iter().enumerate() {
                let expected_connection: u64 = retained.iter().flatten()
                    .filter(|(connection, _)| *connection == owner).map(|(_, bytes)| *bytes).sum();
                prop_assert_eq!(memory.reserved_for_test(), expected_connection);
                prop_assert!(expected_connection <= connection_limit);
            }
        }

        drop(permits);
        prop_assert_eq!(node.reserved_for_test(), 0);
        for memory in connections { prop_assert_eq!(memory.reserved_for_test(), 0); }
    }
}
