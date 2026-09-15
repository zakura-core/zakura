use super::*;

#[tokio::test]
async fn writer_failure_reaches_supervision_while_read_clones_hold_the_join_handle() {
    use tower::ServiceExt;

    let network = Network::new_regtest(Default::default());
    let (_state, read, _tip, _tip_change) = crate::service::init_test_services(&network).await;
    let mut read_clone = read.clone();
    let observer = read.clone();
    let supervisor = tokio::spawn(async move { observer.wait_for_writer_failure().await });
    tokio::task::yield_now().await;
    read.block_write_failure.set(BlockWriteTaskFailure::runtime(
        "incoherent ready VCT auxiliary window",
        "missing exact roots",
    ));
    let error = tokio::time::timeout(Duration::from_secs(5), supervisor)
        .await
        .expect("writer failure wakes supervision without joining the writer")
        .expect("the supervision task returns normally");
    assert!(error.to_string().contains("missing exact roots"));
    assert!(read_clone.ready().await.is_err());
    assert!(read
        .wait_for_writer_failure()
        .await
        .to_string()
        .contains("missing exact roots"));
}

#[tokio::test]
async fn writer_failure_wakes_all_waiters_and_survives_late_subscription() {
    let failure = Arc::new(BlockWriteFailure::default());
    let first = failure.clone();
    let second = failure.clone();
    let first = tokio::spawn(async move { first.wait().await });
    let second = tokio::spawn(async move { second.wait().await });
    tokio::task::yield_now().await;
    failure.set(BlockWriteTaskFailure::runtime(
        "incoherent roots",
        "wrong hash",
    ));
    failure.set(BlockWriteTaskFailure::panic());
    tokio::time::timeout(Duration::from_secs(5), async {
        for observed in [
            first.await.unwrap(),
            second.await.unwrap(),
            failure.wait().await,
        ] {
            assert!(observed
                .to_string()
                .contains("incoherent roots: wrong hash"));
        }
    })
    .await
    .expect("writer failure wakes every supervisor");
}

#[test]
fn abnormal_writer_exits_expose_stable_health_failures() {
    let failure = BlockWriteTaskFailure::runtime("runtime context", "store failure");
    let exit = BlockWriteTaskExit::HeaderChainRuntimeFailed(failure.clone());

    assert_eq!(
        exit.failure()
            .expect("a runtime failure is visible to every state clone")
            .to_string(),
        failure.to_string()
    );
    assert!(BlockWriteTaskExit::Completed.failure().is_none());
}

#[test]
fn header_chain_finalization_errors_become_failed_writer_exits() {
    let error = CommitBlockError::HeaderChainError {
        error: "durable header transition failed".to_owned(),
    }
    .into();

    let BlockWriteTaskExit::HeaderChainRuntimeFailed(failure) =
        header_chain_finalization_failure(error)
    else {
        panic!("a header-chain finalization failure must not report clean shutdown");
    };
    assert!(failure
        .to_string()
        .contains("header-chain reorg-limit finalization failed"));
}

#[test]
#[should_panic(expected = "unexpected finalized block commit error")]
fn legacy_finalization_invariant_failures_remain_explicit_panics() {
    let error = CommitCheckpointVerifiedError::from(CommitBlockError::WriteTaskExited);
    let _ = header_chain_finalization_failure(error);
}
