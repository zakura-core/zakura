//! The expected budget is the sum of live allocations, independent of pool counters.

use super::*;

#[test]
fn response_authorization_funds_allocation_before_retaining_it() {
    use crate::zakura::CloseCause;
    use tokio_util::sync::CancellationToken;
    use zakura_test::allocations::measure;

    let pool_setup = ResponseMemory::node_setup_bytes_for_test();
    let connection_setup = ResponseMemory::setup_bytes_for_test();
    let scope_setup = ResponseScope::setup_bytes_for_test();
    let setup = pool_setup + connection_setup + scope_setup;
    let (node, allocated) = measure(|| ResponseMemory::new(setup + 4096, setup + 4096));
    assert_eq!(node.reserved_for_test(), pool_setup);
    assert!(allocated.retained_bytes > 0);
    assert!(u64::try_from(allocated.peak_live_bytes).unwrap() <= pool_setup);
    // Include cold token/cause creation and both funded constructors. Moving
    // first-use locks into setup must not hide their allocations from the probe.
    let ((memory, cancel, cause), allocated) = measure(|| {
        let memory = node.try_connection().unwrap();
        (memory, CancellationToken::new(), CloseCause::new())
    });
    assert_eq!(node.reserved_for_test(), pool_setup + connection_setup);
    assert!(allocated.retained_bytes > 0);
    assert!(u64::try_from(allocated.peak_live_bytes).unwrap() <= connection_setup);
    let (scope, allocated) =
        measure(|| ResponseScope::try_with_memory(&cancel, &cause, memory.clone()).unwrap());
    assert_eq!(node.reserved_for_test(), setup);
    assert!(allocated.retained_bytes > 0);
    assert!(u64::try_from(allocated.peak_live_bytes).unwrap() <= scope_setup);
    let (authorization, allocated) = measure(|| scope.authorize().unwrap());
    let funded = node.reserved_for_test() - setup;
    assert_eq!(funded, u64::try_from(allocated.retained_bytes).unwrap());
    assert_eq!(funded, u64::try_from(allocated.peak_live_bytes).unwrap());
    assert!(funded > 0);

    let writer = authorization.write_permission();
    drop(authorization);
    assert_eq!(node.reserved_for_test(), setup + funded);
    drop(writer);
    assert_eq!(node.reserved_for_test(), setup);

    let full = memory.try_reserve(4096).unwrap();
    let (denied, allocated) = measure(|| scope.authorize());
    assert!(matches!(denied, Err(ResponseAdmissionError::MemoryFull)));
    assert_eq!(allocated.requested_bytes, 0);
    let (denied, allocated) = measure(|| node.try_connection());
    assert!(denied.is_none());
    assert_eq!(allocated.requested_bytes, 0);
    // These tokens have not been locked yet. Failed setup must not initialize them.
    let cold_cancel = CancellationToken::new();
    let cold_cause = CloseCause::new();
    let (denied, allocated) =
        measure(|| ResponseScope::try_with_memory(&cold_cancel, &cold_cause, memory.clone()));
    assert!(matches!(denied, Err(ResponseAdmissionError::MemoryFull)));
    assert_eq!(allocated.requested_bytes, 0);
    assert_eq!(node.reserved_for_test(), setup + 4096);
    drop(full);
    assert_eq!(node.reserved_for_test(), setup);
    drop(scope);
    assert_eq!(node.reserved_for_test(), pool_setup + connection_setup);
    drop(memory);
    assert_eq!(node.reserved_for_test(), pool_setup);
}

#[test]
fn cold_response_setup_and_unfinished_cleanup_fit_the_fixed_allowances() {
    use crate::zakura::CloseCause;
    use tokio_util::sync::CancellationToken;
    use zakura_test::allocations::measure;

    let (funded, allocated) = measure(|| {
        let node = ResponseMemory::default();
        let memory = node.try_connection().unwrap();
        let cancel = CancellationToken::new();
        let cause = CloseCause::new();
        let scope = ResponseScope::try_with_memory(&cancel, &cause, memory).unwrap();
        let owner = scope.authorize().unwrap();
        let writer = owner.write_permission();
        let funded = node.reserved_for_test();
        assert!(writer.publish(|| {}));
        assert!(writer.try_start(|| true));
        drop((owner, scope));
        assert!(cancel.is_cancelled());
        assert_eq!(node.reserved_for_test(), funded);
        drop(writer);
        assert_eq!(
            node.reserved_for_test(),
            ResponseMemory::node_setup_bytes_for_test()
        );
        drop((cancel, cause, node));
        funded
    });
    assert!(allocated.peak_live_bytes > 0);
    assert!(u64::try_from(allocated.peak_live_bytes).unwrap() <= funded);
    assert_eq!(allocated.retained_bytes, 0);
}

#[test]
fn response_authorization_transitions_do_not_allocate_unfunded_storage() {
    use crate::zakura::CloseCause;
    use tokio_util::sync::CancellationToken;
    use zakura_test::allocations::measure;

    let setup = ResponseMemory::node_setup_bytes_for_test()
        + ResponseMemory::setup_bytes_for_test()
        + ResponseScope::setup_bytes_for_test();
    let node = ResponseMemory::new(setup + 4096, setup + 4096);
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
        let setup = ResponseMemory::setup_bytes_for_test();
        let pool_setup = ResponseMemory::node_setup_bytes_for_test();
        let node = ResponseMemory::new(pool_setup + 4 * setup + node_limit, setup + connection_limit);
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
            prop_assert_eq!(node.reserved_for_test(), pool_setup + 4 * setup + expected_node);
            prop_assert!(expected_node <= node_limit);
            for (owner, memory) in connections.iter().enumerate() {
                let expected_connection: u64 = retained.iter().flatten()
                    .filter(|(connection, _)| *connection == owner).map(|(_, bytes)| *bytes).sum();
                prop_assert_eq!(memory.reserved_for_test(), setup + expected_connection);
                prop_assert!(expected_connection <= connection_limit);
            }
        }

        drop(permits);
        prop_assert_eq!(node.reserved_for_test(), pool_setup + 4 * setup);
        for memory in &connections { prop_assert_eq!(memory.reserved_for_test(), setup); }
        drop(connections);
        prop_assert_eq!(node.reserved_for_test(), pool_setup);
    }
}
