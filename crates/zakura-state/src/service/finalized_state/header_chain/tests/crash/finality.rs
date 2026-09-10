use super::*;

pub(super) fn crash_fixture_finality_advance_reopens_complete_before_or_after() {
    for (index, target) in FaultPoint::ALL.into_iter().enumerate() {
        let cache = tempfile::tempdir().expect("the test cache directory is created");
        let db_config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            debug_skip_non_finalized_state_backup_task: true,
            ..Config::default()
        };
        let (engine_config, anchor, metadata) = fixture();
        let network = engine_config.network().clone();
        let db = open(&db_config, &network);
        let store = HeaderChainStore::new(db.clone());
        store
            .initialize(metadata, anchor.clone())
            .expect("the empty schema initializes");
        let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
        let mut child_header = *anchor.header;
        child_header.previous_block_hash = anchor.hash;
        child_header.time += chrono::Duration::seconds(1);
        let child_header = Arc::new(child_header);
        let child = VerifiedHeaderRef {
            height: anchor
                .height
                .next()
                .expect("the genesis anchor has a next height"),
            hash: child_header.hash(),
            header: child_header,
        };
        let mut grandchild_header = *child.header;
        grandchild_header.previous_block_hash = child.hash;
        grandchild_header.time += chrono::Duration::seconds(1);
        let grandchild_header = Arc::new(grandchild_header);
        let grandchild = VerifiedHeaderRef {
            height: child.height.next().expect("the child has a next height"),
            hash: grandchild_header.hash(),
            header: grandchild_header,
        };
        let (runtime, _) = store
            .startup_reconciled(
                &engine_config,
                anchor_frontier,
                Vec::new(),
                vec![child.clone(), grandchild.clone()],
            )
            .expect("the verified suffix reconciles before the faulted finality transition");
        let before = runtime.publisher().snapshot();
        let new_finalized = Frontier::new(child.height, child.hash);
        let proof = runtime
            .verified_projection()
            .expect("the verified projection is readable")
            .into_iter()
            .take_while(|frontier| frontier.height <= new_finalized.height)
            .map(|frontier| frontier.hash)
            .collect::<Vec<_>>();
        let marker = u8::try_from(index + 0x60).expect("the fault-point list fits in u8");
        let evidence = zakura_header_chain::full_state_finality_evidence(
            before.state_version,
            new_finalized,
            &proof,
        );
        let authority = Authority(evidence);
        let context = TransitionContext {
            config: &engine_config,
            clock: &SystemClock,
            full_state_authority: Some(&authority),
            retention_references: &[],
        };
        let request = TransitionRequest {
            expected_version: before.state_version,
            event: TransitionEvent::FullStateFinalized(FullStateFinalized {
                full_state_transition_id: evidence,
                new_finalized,
                verified_path_proof: proof,
            }),
        };
        let marker_key = [marker; 4];
        let mut full_state_batch = DiskWriteBatch::new();
        stage_full_state_canonical_hash(&runtime.store, &mut full_state_batch, anchor_frontier);
        stage_full_state_canonical_hash(&runtime.store, &mut full_state_batch, new_finalized);
        runtime
            .store
            .put_raw(
                &mut full_state_batch,
                ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT,
                marker_key,
                [marker],
            )
            .expect("the paired finality marker can be staged");
        let memory_swapped = Arc::new(AtomicBool::new(false));
        let swap_probe = memory_swapped.clone();
        let result = runtime.apply_combined_with_fault(
            request,
            &context,
            full_state_batch,
            move || swap_probe.store(true, Ordering::SeqCst),
            |point| {
                if point == target {
                    Err(HeaderChainStoreError::InjectedCrash(point))
                } else {
                    Ok(())
                }
            },
        );
        assert!(matches!(
            result,
            Err(HeaderChainStoreError::InjectedCrash(point)) if point == target
        ));

        let observation = observe_transition_crash(
            target,
            runtime,
            db,
            &db_config,
            &network,
            &engine_config,
            &before,
            &memory_swapped,
            Some(marker_key),
        );
        let committed = target.commit_completed();
        let durable = &observation.durable;
        assert_eq!(
            durable.frontiers.finalized,
            if committed {
                new_finalized
            } else {
                anchor_frontier
            },
            "{target:?}"
        );
        assert_eq!(
            durable.frontiers.header_best,
            Frontier::new(grandchild.height, grandchild.hash),
            "{target:?}"
        );
        assert_eq!(
            observation
                .reopened
                .store
                .header_node(anchor.hash)
                .expect("the old anchor row read succeeds")
                .is_none(),
            committed,
            "{target:?}"
        );
        assert!(observation
            .reopened
            .store
            .header_node(child.hash)
            .expect("the new anchor row read succeeds")
            .is_some());
        assert!(observation
            .reopened
            .store
            .header_node(grandchild.hash)
            .expect("the retained suffix row read succeeds")
            .is_some());
        let durable_metadata = observation
            .reopened
            .store
            .metadata()
            .expect("the finality metadata read succeeds");
        assert_eq!(durable_metadata.work_origin, anchor_frontier, "{target:?}");
        assert_eq!(
            observation
                .reopened
                .store
                .finality_history()
                .expect("the finality history read succeeds")
                .len(),
            1 + usize::from(committed),
            "{target:?}"
        );
        assert_eq!(
            observation.startup.current.frontiers.finalized,
            if committed {
                new_finalized
            } else {
                anchor_frontier
            },
            "{target:?}"
        );
        assert_eq!(
            observation
                .reopened
                .store
                .metadata()
                .expect("the reopened metadata is readable")
                .work_origin,
            anchor_frontier,
            "{target:?}"
        );
        assert_eq!(
            observation
                .reopened
                .store
                .finality_history()
                .expect("the reopened finality history is readable")
                .len(),
            1 + usize::from(committed),
            "{target:?}"
        );
        let reopened_anchor = if committed {
            new_finalized
        } else {
            anchor_frontier
        };
        let lease = observation
            .reopened
            .reader()
            .validation_context(reopened_anchor.hash)
            .expect("the reopened anchor context read succeeds")
            .expect("the reopened anchor is retained");
        assert_eq!(
            lease.predecessors().len(),
            if committed { 2 } else { 1 },
            "{target:?}"
        );
        let (next_header, expected_height) = if committed {
            (grandchild.header.clone(), grandchild.height)
        } else {
            (child.header.clone(), child.height)
        };
        let rules = HeaderRules::for_validation_lease(&lease)
            .expect("the authenticated custom-network policy is valid");
        let prepared = zakura_header_chain::prepare_headers(
            HeaderBatchInput::new(std::slice::from_ref(&next_header)),
            lease.parent(),
            &rules,
            &SystemClock,
        )
        .expect("the first post-anchor child validates after reopen");
        assert_eq!(prepared.headers()[0].height, expected_height, "{target:?}");
    }
}

pub(super) fn crash_fixture_operator_reason_changes_reopen_complete_before_or_after() {
    for reconsider in [false, true] {
        for (index, target) in FaultPoint::ALL.into_iter().enumerate() {
            let cache = tempfile::tempdir().expect("the test cache directory is created");
            let db_config = Config {
                cache_dir: cache.path().to_owned(),
                ephemeral: false,
                debug_skip_non_finalized_state_backup_task: true,
                ..Config::default()
            };
            let (engine_config, anchor, metadata) = fixture();
            let network = engine_config.network().clone();
            let db = open(&db_config, &network);
            let store = HeaderChainStore::new(db.clone());
            store
                .initialize(metadata, anchor.clone())
                .expect("the empty schema initializes");
            let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
            let child_height = anchor
                .height
                .next()
                .expect("the genesis anchor has a next height");
            let mut first_header = *anchor.header;
            first_header.previous_block_hash = anchor.hash;
            first_header.time += chrono::Duration::seconds(1);
            let first_header = Arc::new(first_header);
            let mut second_header = *first_header;
            second_header.nonce.0[0] ^= 1;
            let second_header = Arc::new(second_header);
            let (lower_header, higher_header) = if first_header.hash().0 < second_header.hash().0 {
                (first_header, second_header)
            } else {
                (second_header, first_header)
            };
            let lower = Frontier::new(child_height, lower_header.hash());
            let higher = Frontier::new(child_height, higher_header.hash());
            let verified_lower = VerifiedHeaderRef {
                height: child_height,
                hash: lower.hash,
                header: lower_header,
            };
            let (runtime, _) = store
                .startup_reconciled(
                    &engine_config,
                    anchor_frontier,
                    Vec::new(),
                    vec![verified_lower],
                )
                .expect("the lower raw-hash branch reconciles from full state");
            let lease = runtime
                .reader()
                .validation_context(anchor.hash)
                .expect("the anchor validation context is coherent")
                .expect("the initialized anchor is retained");
            let rules = HeaderRules::for_validation_lease(&lease)
                .expect("the authenticated regtest policy is valid");
            let headers = [higher_header];
            let batch = zakura_header_chain::prepare_headers(
                HeaderBatchInput::new(&headers),
                lease.parent(),
                &rules,
                &SystemClock,
            )
            .expect("the equal-work competitor prepares through production validation");
            let before_insert = runtime.publisher().snapshot();
            let owner = header_owner(&before_insert, higher.hash, 1, 1);
            let context = TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: None,
                retention_references: &[],
            };
            runtime
                .apply(
                    TransitionRequest {
                        expected_version: before_insert.state_version,
                        event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                            owner,
                            source: SourceId::from_digest([0xc1; 32]),
                            parent_hash: anchor.hash,
                            target_tip_hash: higher.hash,
                            completion: TargetCompletion::TargetComplete {
                                common_ancestor: anchor_frontier,
                            },
                            batch,
                            aux: Vec::new(),
                        })),
                    },
                    &context,
                )
                .expect("the higher raw-hash competitor commits");
            assert_eq!(runtime.publisher().snapshot().frontiers.header_best, higher);
            assert_eq!(
                runtime.publisher().snapshot().frontiers.verified_best,
                lower
            );

            let invalidation_id = OperatorInvalidationId::new([0xd1; 16]);
            let mut reason_hasher = sha2::Sha256::new();
            use sha2::Digest as _;
            reason_hasher.update(b"zakura-operator-invalidation-v1");
            reason_hasher.update(higher.hash.0);
            reason_hasher.update(invalidation_id.bytes());
            let operator_reason_digest: [u8; 32] = reason_hasher.finalize().into();
            let invalidation_evidence = reconsider.then_some(EvidenceId::from_digest([0xd3; 32]));
            if let Some(invalidation_evidence) = invalidation_evidence {
                let before_invalidation = runtime.publisher().snapshot();
                let invalidation_authority = Authority(invalidation_evidence);
                runtime
                    .apply(
                        TransitionRequest {
                            expected_version: before_invalidation.state_version,
                            event: TransitionEvent::OperatorInvalidate(OperatorInvalidate {
                                target: higher.hash,
                                id: invalidation_id,
                                operator_reason_digest,
                                evidence: invalidation_evidence,
                            }),
                        },
                        &TransitionContext {
                            config: &engine_config,
                            clock: &SystemClock,
                            full_state_authority: Some(&invalidation_authority),
                            retention_references: &[],
                        },
                    )
                    .expect("the exact operator reason is installed before reconsideration");
                assert_eq!(runtime.publisher().snapshot().frontiers.header_best, lower);
            }

            let before = runtime.publisher().snapshot();
            let marker = u8::try_from(index + if reconsider { 0xa0 } else { 0x80 })
                .expect("the fault-point list fits in u8");
            let evidence = EvidenceId::from_digest([marker; 32]);
            let event = if reconsider {
                TransitionEvent::OperatorReconsider(OperatorReconsider {
                    target: higher.hash,
                    id: invalidation_id,
                    invalidation_evidence,
                    evidence,
                })
            } else {
                TransitionEvent::OperatorInvalidate(OperatorInvalidate {
                    target: higher.hash,
                    id: invalidation_id,
                    operator_reason_digest,
                    evidence,
                })
            };
            let operator_authority = Authority(evidence);
            let operator_context = TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: Some(&operator_authority),
                retention_references: &[],
            };
            let marker_key = [marker; 4];
            let mut full_state_batch = DiskWriteBatch::new();
            runtime
                .store
                .put_raw(
                    &mut full_state_batch,
                    ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT,
                    marker_key,
                    [marker],
                )
                .expect("the paired operator marker can be staged");
            let memory_swapped = Arc::new(AtomicBool::new(false));
            let swap_probe = memory_swapped.clone();
            let result = runtime.apply_combined_with_fault(
                TransitionRequest {
                    expected_version: before.state_version,
                    event,
                },
                &operator_context,
                full_state_batch,
                move || swap_probe.store(true, Ordering::SeqCst),
                |point| {
                    if point == target {
                        Err(HeaderChainStoreError::InjectedCrash(point))
                    } else {
                        Ok(())
                    }
                },
            );
            assert!(matches!(
                result,
                Err(HeaderChainStoreError::InjectedCrash(point)) if point == target
            ));

            let observation = observe_transition_crash(
                target,
                runtime,
                db,
                &db_config,
                &network,
                &engine_config,
                &before,
                &memory_swapped,
                Some(marker_key),
            );
            let committed = target.commit_completed();
            let selected_after = if reconsider { higher } else { lower };
            let selected_before = if reconsider { lower } else { higher };
            let reason_after = !reconsider;
            let durable = &observation.durable;
            let committed_version = before
                .state_version
                .checked_next()
                .expect("the short fixture state version can advance");
            assert_eq!(
                durable.state_version,
                if committed {
                    committed_version
                } else {
                    before.state_version
                },
                "{target:?}, reconsider={reconsider}"
            );
            assert_eq!(
                durable.frontiers.header_best,
                if committed {
                    selected_after
                } else {
                    selected_before
                },
                "{target:?}, reconsider={reconsider}"
            );
            assert_eq!(
                durable.frontiers.verified_best, lower,
                "{target:?}, reconsider={reconsider}"
            );
            assert_eq!(
                observation
                    .reopened
                    .store
                    .header_node(higher.hash)
                    .expect("the target node read succeeds")
                    .expect("the operator target remains retained")
                    .eligibility
                    .direct_reasons
                    .iter()
                    .any(|reason| matches!(reason, EligibilityReason::OperatorInvalid { id, .. } if *id == invalidation_id)),
                if committed { reason_after } else { reconsider },
                "{target:?}, reconsider={reconsider}"
            );
            assert_eq!(
                observation.startup.current.frontiers.header_best,
                if committed {
                    selected_after
                } else {
                    selected_before
                },
                "{target:?}, reconsider={reconsider}"
            );
            assert_eq!(
                observation.startup.current.state_version,
                if committed {
                    committed_version
                } else {
                    before.state_version
                },
                "{target:?}, reconsider={reconsider}"
            );
            assert_eq!(
                observation.startup.current.frontiers.verified_best, lower,
                "{target:?}, reconsider={reconsider}"
            );
            assert_eq!(
                observation
                    .reopened
                    .store
                    .header_node(higher.hash)
                    .expect("the reopened target node read succeeds")
                    .expect("the reopened operator target remains retained")
                    .eligibility
                    .direct_reasons
                    .iter()
                    .any(|reason| matches!(reason, EligibilityReason::OperatorInvalid { id, .. } if *id == invalidation_id)),
                if committed { reason_after } else { reconsider },
                "{target:?}, reconsider={reconsider}"
            );
        }
    }
}
