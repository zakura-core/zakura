use super::*;

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
