use super::*;

impl ResponseScope {
    pub(crate) fn setup_bytes_for_test() -> u64 {
        SCOPE_SETUP_BYTES
    }

    pub(crate) fn with_memory(
        connection_cancel: CancellationToken,
        cause: CloseCause,
        memory: ConnectionResponseMemory,
    ) -> Self {
        Self::try_with_memory(&connection_cancel, &cause, memory)
            .expect("the fixture funds scope setup")
    }

    pub(crate) fn authorize(&self) -> Result<ResponseAuthorization, ResponseAdmissionError> {
        self.authorize_with_metadata(0)
    }

    pub(crate) fn new(connection_cancel: CancellationToken, cause: CloseCause) -> Self {
        Self::with_memory(
            connection_cancel,
            cause,
            crate::zakura::regulation::ResponseMemory::default().connection(),
        )
    }
}

impl ResponseAuthorization {
    /// Counter/window fixtures do not publish writes. Writer tests use a real scope.
    pub(crate) fn for_test() -> Self {
        ResponseScope::new(CancellationToken::new(), CloseCause::new())
            .authorize()
            .unwrap()
    }
}

#[test]
fn retirement_fences_prepared_and_queued_writes_without_closing() {
    for queued in [false, true] {
        let connection = CancellationToken::new();
        let scope = ResponseScope::new(connection.clone(), CloseCause::new());
        let authorization = scope.authorize().unwrap();
        let permission = authorization.write_permission();
        if queued {
            assert!(permission.publish(|| {}));
        }
        assert!(scope.retire());
        assert_eq!(
            scope.authorize().unwrap_err(),
            ResponseAdmissionError::Retired
        );
        assert!(!permission.publish(|| panic!("a retired receiver cannot publish")));
        assert!(!permission.try_start(|| panic!("a retired receiver cannot start")));
        drop(authorization);
        assert!(!connection.is_cancelled());
    }
}

#[test]
fn owner_drop_fences_unstarted_writes_and_closes_started_exchanges() {
    for started in [false, true] {
        let connection = CancellationToken::new();
        let cause = CloseCause::new();
        let scope = ResponseScope::new(connection.clone(), cause.clone());
        let authorization = scope.authorize().unwrap();
        let permission = authorization.write_permission();
        assert!(permission.publish(|| {}));
        if started {
            assert!(permission.try_start(|| true));
        }
        drop(authorization);
        assert!(!permission.try_start(|| panic!("the response owner has gone")));
        assert_eq!(connection.is_cancelled(), started);
        assert_eq!(scope.retire(), !started);
        if started {
            assert_eq!(cause.get_or("unset"), "unfinished_response_authorization");
        }
    }
}

#[test]
fn only_validated_endings_release_started_exchanges() {
    for finish_both in [false, true] {
        let connection = CancellationToken::new();
        let scope = ResponseScope::new(connection.clone(), CloseCause::new());
        let mut first = scope.authorize().unwrap();
        let mut second = scope.authorize().unwrap();
        let first_write = first.write_permission();
        let second_write = second.write_permission();
        for write in [&first_write, &second_write] {
            assert!(write.publish(|| {}));
            assert!(write.try_start(|| true));
        }
        first.finish();
        first.finish();
        assert!(!first_write.try_start(|| panic!("a finished exchange cannot restart")));
        if finish_both {
            second.finish();
        }
        assert_eq!(scope.retire(), finish_both);
        assert_eq!(connection.is_cancelled(), !finish_both);
    }
}

#[test]
fn failed_work_claim_leaves_the_connection_reusable() {
    let connection = CancellationToken::new();
    let scope = ResponseScope::new(connection.clone(), CloseCause::new());
    let authorization = scope.authorize().unwrap();
    let permission = authorization.write_permission();
    assert!(permission.publish(|| {}));
    assert!(!permission.try_start(|| false));
    assert!(scope.retire());
    assert!(!connection.is_cancelled());
}

#[test]
fn retirement_and_first_write_have_one_winner() {
    for _ in 0..128 {
        let connection = CancellationToken::new();
        let scope = ResponseScope::new(connection.clone(), CloseCause::new());
        let authorization = scope.authorize().unwrap();
        let permission = authorization.write_permission();
        assert!(permission.publish(|| {}));
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let writer_barrier = barrier.clone();
        let writer = std::thread::spawn(move || {
            writer_barrier.wait();
            permission.try_start(|| true)
        });
        barrier.wait();
        let reusable = scope.retire();
        let started = writer.join().unwrap();
        assert_eq!(connection.is_cancelled(), started);
        assert_eq!(reusable, !started);
    }
}

#[test]
fn publication_is_complete_before_retirement_returns() {
    use std::sync::mpsc;
    use std::time::Duration;

    let scope = ResponseScope::new(CancellationToken::new(), CloseCause::new());
    let authorization = scope.authorize().unwrap();
    let permission = authorization.write_permission();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (published_tx, published_rx) = mpsc::channel();
    let publisher = std::thread::spawn(move || {
        assert!(permission.publish(|| {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            published_tx.send(()).unwrap();
        }));
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let retirement = std::thread::spawn(move || {
        assert!(scope.retire());
        published_rx
            .try_recv()
            .expect("retirement waits for publication");
        scope
    });
    release_tx.send(()).unwrap();
    publisher.join().unwrap();
    let scope = retirement.join().unwrap();
    assert_eq!(
        scope.authorize().unwrap_err(),
        ResponseAdmissionError::Retired
    );
    assert!(!authorization.write_permission().try_start(|| true));
}
