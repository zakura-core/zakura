use super::*;

pub(super) fn crash_fixture_verified_grow_and_reset_reopen_complete_before_or_after() {
    for reset in [false, true] {
        for (index, target) in FaultPoint::ALL.into_iter().enumerate() {
            let cache = tempfile::tempdir().expect("the test cache directory is created");
            let db_config = Config {
                cache_dir: cache.path().to_owned(),
                ephemeral: false,
                debug_skip_non_finalized_state_backup_task: true,
                ..Config::default()
            };
            let (engine_config, anchor, metadata) = fixture();
            let network = engine_config.network.clone();
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
            let mut incumbent_header = *anchor.header;
            incumbent_header.previous_block_hash = anchor.hash;
            incumbent_header.time += chrono::Duration::seconds(1);
            incumbent_header.nonce.0[0] ^= 1;
            let incumbent_header = Arc::new(incumbent_header);
            let incumbent = VerifiedHeaderRef {
                height: child_height,
                hash: incumbent_header.hash(),
                header: incumbent_header,
            };
            let mut replacement_header = *anchor.header;
            replacement_header.previous_block_hash = anchor.hash;
            replacement_header.time += chrono::Duration::seconds(1);
            replacement_header.nonce.0[0] ^= 2;
            let replacement_header = Arc::new(replacement_header);
            let replacement = VerifiedHeaderRef {
                height: child_height,
                hash: replacement_header.hash(),
                header: replacement_header,
            };
            assert_ne!(incumbent.hash, replacement.hash);

            let (runtime, _) = if reset {
                store
                    .startup_reconciled(
                        &engine_config,
                        anchor_frontier,
                        Vec::new(),
                        vec![incumbent.clone()],
                    )
                    .expect("the incumbent verified path reconciles")
            } else {
                store
                    .startup(&engine_config)
                    .expect("the initialized store audits")
            };
            let before = runtime.publisher().snapshot();
            let old_verified = before.frontiers.verified_best;
            let event_header = if reset {
                replacement.clone()
            } else {
                incumbent.clone()
            };
            let event_frontier = Frontier::new(event_header.height, event_header.hash);
            let marker = u8::try_from(index + if reset { 0xd0 } else { 0xb0 })
                .expect("the fault-point list fits in u8");
            let evidence = EvidenceId::from_digest([marker; 32]);
            let authority = Authority(evidence);
            let context = TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: Some(&authority),
                retention_references: &[],
            };
            let request = TransitionRequest {
                expected_version: before.state_version,
                event: TransitionEvent::VerifiedChainChanged(VerifiedChainChanged {
                    full_state_transition_id: evidence,
                    old_tip: old_verified,
                    new_path: vec![event_header],
                    cause: if reset {
                        VerifiedChangeCause::Reset
                    } else {
                        VerifiedChangeCause::Grow
                    },
                }),
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
                .expect("the paired verified-path marker can be staged");
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
            let committed_version = before
                .state_version
                .checked_next()
                .expect("the short fixture state version can advance");
            let header_best_after = if reset {
                [incumbent.hash, replacement.hash]
                    .into_iter()
                    .max_by_key(|hash| hash.0)
                    .map(|hash| Frontier::new(child_height, hash))
                    .expect("the two-child fixture is nonempty")
            } else {
                event_frontier
            };
            let durable = &observation.durable;
            assert_eq!(
                durable.state_version,
                if committed {
                    committed_version
                } else {
                    before.state_version
                },
                "{target:?}, reset={reset}"
            );
            assert_eq!(
                durable.frontiers.verified_best,
                if committed {
                    event_frontier
                } else {
                    old_verified
                },
                "{target:?}, reset={reset}"
            );
            assert_eq!(
                durable.frontiers.header_best,
                if committed {
                    header_best_after
                } else {
                    before.frontiers.header_best
                },
                "{target:?}, reset={reset}"
            );
            let event_node = observation
                .reopened
                .store
                .header_node(event_frontier.hash)
                .expect("the event node read succeeds");
            assert_eq!(event_node.is_some(), committed, "{target:?}, reset={reset}");
            if let Some(event_node) = event_node {
                assert!(matches!(
                    event_node.body_validation_state,
                    BodyValidationState::Verified {
                        evidence: node_evidence
                    } if node_evidence == evidence
                ));
            }
            assert_eq!(
                observation
                    .reopened
                    .store
                    .verified_projection()
                    .expect("the verified projection is readable"),
                if committed {
                    vec![anchor_frontier, event_frontier]
                } else if reset {
                    vec![
                        anchor_frontier,
                        Frontier::new(incumbent.height, incumbent.hash),
                    ]
                } else {
                    vec![anchor_frontier]
                },
                "{target:?}, reset={reset}"
            );
            assert_eq!(
                observation.startup.current.frontiers.verified_best,
                if committed {
                    event_frontier
                } else {
                    old_verified
                },
                "{target:?}, reset={reset}"
            );
            assert_eq!(
                observation
                    .reopened
                    .store
                    .header_node(event_frontier.hash)
                    .expect("the reopened event node read succeeds")
                    .is_some(),
                committed,
                "{target:?}, reset={reset}"
            );
        }
    }
}

pub(super) fn crash_fixture_body_retry_restarts_reopen_complete_before_or_after() {
    for operator_retry in [false, true] {
        for (index, target) in FaultPoint::ALL.into_iter().enumerate() {
            let cache = tempfile::tempdir().expect("the test cache directory is created");
            let db_config = Config {
                cache_dir: cache.path().to_owned(),
                ephemeral: false,
                debug_skip_non_finalized_state_backup_task: true,
                ..Config::default()
            };
            let (engine_config, anchor, metadata) = fixture();
            let network = engine_config.network.clone();
            let db = open(&db_config, &network);
            let store = HeaderChainStore::new(db.clone());
            store
                .initialize(metadata, anchor.clone())
                .expect("the empty schema initializes");
            let (runtime, _) = store
                .startup(&engine_config)
                .expect("the initial store audits");
            let initial = runtime.publisher().snapshot();
            let started_at = Utc
                .timestamp_opt(1_000, 0)
                .single()
                .expect("the fixture timestamp is valid");
            let old = BodyUnavailableSummary {
                started_at,
                attempts: 10,
                suppliers: 2,
                supplier_set_digest: [0x31; 32],
                alarmed: true,
                next_probe_at: Utc
                    .timestamp_opt(1_600, 0)
                    .single()
                    .expect("the fixture probe timestamp is valid"),
            };
            let seed_evidence = EvidenceId::from_digest(
                [u8::try_from(index + 0x60).expect("the fault-point list fits in u8"); 32],
            );
            let seed_authority = Authority(seed_evidence);
            let seed_context = TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: Some(&seed_authority),
                retention_references: &[],
            };
            runtime
                .apply(
                    TransitionRequest {
                        expected_version: initial.state_version,
                        event: TransitionEvent::BodyEvidence(BodyEvidence::Transient(
                            TransientBodyFailure {
                                hash: anchor.hash,
                                evidence: seed_evidence,
                                kind: TransientBodyFailureKind::Timeout,
                                availability: old,
                            },
                        )),
                    },
                    &seed_context,
                )
                .expect("the persistent body alarm fixture commits");
            let before = runtime.publisher().snapshot();
            assert_eq!(before.alarms.header_best_body_unavailable, Some(old));

            let marker = u8::try_from(index + if operator_retry { 0x90 } else { 0x70 })
                .expect("the fault-point list fits in u8");
            let fresh_at = started_at + chrono::Duration::minutes(20);
            let fresh = BodyUnavailableSummary {
                started_at: if operator_retry {
                    fresh_at
                } else {
                    old.started_at
                },
                attempts: if operator_retry { 0 } else { old.attempts },
                suppliers: if operator_retry {
                    old.suppliers
                } else {
                    old.suppliers.saturating_add(1)
                },
                supplier_set_digest: if operator_retry {
                    old.supplier_set_digest
                } else {
                    [0x32; 32]
                },
                alarmed: !operator_retry,
                next_probe_at: fresh_at,
            };
            let evidence = EvidenceId::from_digest([marker; 32]);
            let authority = Authority(evidence);
            let context = TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: Some(&authority),
                retention_references: &[],
            };
            let event = if operator_retry {
                TransitionEvent::OperatorBodyRetry(zakura_header_chain::OperatorBodyRetry {
                    hash: anchor.hash,
                    evidence,
                    availability: fresh,
                })
            } else {
                TransitionEvent::BodySupplierDiscovered(
                    zakura_header_chain::BodySupplierDiscovered {
                        hash: anchor.hash,
                        evidence,
                        availability: fresh,
                    },
                )
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
                .expect("the paired retry marker can be staged");
            let memory_swapped = Arc::new(AtomicBool::new(false));
            let swap_probe = memory_swapped.clone();
            let result = runtime.apply_combined_with_fault(
                TransitionRequest {
                    expected_version: before.state_version,
                    event,
                },
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
            let committed_version = before
                .state_version
                .checked_next()
                .expect("the short fixture state version can advance");
            let durable = &observation.durable;
            assert_eq!(
                durable.state_version,
                if committed {
                    committed_version
                } else {
                    before.state_version
                },
                "{target:?}, operator_retry={operator_retry}"
            );
            assert_eq!(
                durable.frontiers, before.frontiers,
                "{target:?}, operator_retry={operator_retry}"
            );
            assert_eq!(
                durable.header_generation, before.header_generation,
                "{target:?}, operator_retry={operator_retry}"
            );
            assert_eq!(
                durable.verified_generation, before.verified_generation,
                "{target:?}, operator_retry={operator_retry}"
            );
            assert_eq!(
                durable.alarms.header_best_body_unavailable,
                if committed {
                    if operator_retry {
                        None
                    } else {
                        Some(fresh)
                    }
                } else {
                    Some(old)
                },
                "{target:?}, operator_retry={operator_retry}"
            );
            assert_eq!(
                observation
                    .reopened
                    .store
                    .header_node(anchor.hash)
                    .expect("the retry node read succeeds")
                    .expect("the selected retry node remains retained")
                    .body_validation_state,
                BodyValidationState::Unavailable(if committed { fresh } else { old }),
                "{target:?}, operator_retry={operator_retry}"
            );
            assert_eq!(
                observation.startup.current.state_version,
                if committed {
                    committed_version
                } else {
                    before.state_version
                },
                "{target:?}, operator_retry={operator_retry}"
            );
            assert_eq!(
                observation
                    .startup
                    .current
                    .alarms
                    .header_best_body_unavailable,
                if committed {
                    if operator_retry {
                        None
                    } else {
                        Some(fresh)
                    }
                } else {
                    Some(old)
                },
                "{target:?}, operator_retry={operator_retry}"
            );
            assert_eq!(
                observation
                    .reopened
                    .store
                    .header_node(anchor.hash)
                    .expect("the reopened retry node read succeeds")
                    .expect("the reopened selected retry node remains retained")
                    .body_validation_state,
                BodyValidationState::Unavailable(if committed { fresh } else { old }),
                "{target:?}, operator_retry={operator_retry}"
            );
        }
    }
}

pub(super) fn crash_fixture_body_conclusions_reopen_complete_before_or_after() {
    for consensus_invalid in [false, true] {
        for (index, target) in FaultPoint::ALL.into_iter().enumerate() {
            let cache = tempfile::tempdir().expect("the test cache directory is created");
            let db_config = Config {
                cache_dir: cache.path().to_owned(),
                ephemeral: false,
                debug_skip_non_finalized_state_backup_task: true,
                ..Config::default()
            };
            let (engine_config, anchor, metadata) = fixture();
            let network = engine_config.network.clone();
            let db = open(&db_config, &network);
            let store = HeaderChainStore::new(db.clone());
            store
                .initialize(metadata, anchor.clone())
                .expect("the empty schema initializes");
            let (runtime, _) = store
                .startup(&engine_config)
                .expect("the initial store audits");
            let initial = runtime.publisher().snapshot();
            let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
            let lease = runtime
                .reader()
                .validation_context(anchor.hash)
                .expect("the anchor validation context is coherent")
                .expect("the initialized anchor is retained");
            let rules = HeaderRules::for_validation_lease(&lease)
                .expect("the authenticated regtest policy is valid");
            let marker = u8::try_from(index + if consensus_invalid { 0xc0 } else { 0x40 })
                .expect("the fault-point list fits in u8");
            let mut child_header = *anchor.header;
            child_header.previous_block_hash = anchor.hash;
            child_header.time += chrono::Duration::seconds(1);
            child_header.nonce.0[0] = marker;
            let child_header = Arc::new(child_header);
            let headers = [child_header.clone()];
            let batch = zakura_header_chain::prepare_headers(
                HeaderBatchInput::new(&headers),
                lease.parent(),
                &rules,
                &SystemClock,
            )
            .expect("the body-conclusion fixture header passes production validation");
            let child = Frontier::new(
                anchor
                    .height
                    .next()
                    .expect("the genesis anchor has a next height"),
                child_header.hash(),
            );
            let owner = header_owner(&initial, child.hash, 41, 42);
            let insertion_context = TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: None,
                retention_references: &[],
            };
            runtime
                .apply(
                    TransitionRequest {
                        expected_version: initial.state_version,
                        event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                            owner,
                            source: SourceId::from_digest([marker.wrapping_add(1); 32]),
                            parent_hash: anchor.hash,
                            target_tip_hash: child.hash,
                            completion: TargetCompletion::TargetComplete {
                                common_ancestor: anchor_frontier,
                            },
                            batch,
                            aux: Vec::new(),
                        })),
                    },
                    &insertion_context,
                )
                .expect("the selected body-conclusion fixture commits");
            let before = runtime.publisher().snapshot();
            assert_eq!(before.frontiers.header_best, child);
            assert_eq!(
                runtime
                    .store
                    .header_node(child.hash)
                    .expect("the child node read succeeds")
                    .expect("the selected child is retained")
                    .body_validation_state,
                BodyValidationState::Unknown
            );

            let evidence = EvidenceId::from_digest([marker.wrapping_add(2); 32]);
            let rule = BodyRuleId::new("aud14.commitment_matching_invalid");
            let source = SourceId::from_digest([marker.wrapping_add(3); 32]);
            let event = if consensus_invalid {
                TransitionEvent::BodyEvidence(BodyEvidence::ConsensusInvalid(
                    zakura_header_chain::ConsensusBodyInvalid {
                        hash: child.hash,
                        evidence,
                        rule: rule.clone(),
                        source,
                    },
                ))
            } else {
                TransitionEvent::BodyEvidence(BodyEvidence::Verified(VerifiedBodyEvidence {
                    hash: child.hash,
                    evidence,
                }))
            };
            let authority = Authority(evidence);
            let context = TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: Some(&authority),
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
                .expect("the paired body-conclusion marker can be staged");
            let memory_swapped = Arc::new(AtomicBool::new(false));
            let swap_probe = memory_swapped.clone();
            let result = runtime.apply_combined_with_fault(
                TransitionRequest {
                    expected_version: before.state_version,
                    event,
                },
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
            let committed_version = before
                .state_version
                .checked_next()
                .expect("the short fixture state version can advance");
            let committed_header_generation = before
                .header_generation
                .checked_next()
                .expect("the short fixture header generation can advance");
            let selected_after = if consensus_invalid {
                anchor_frontier
            } else {
                child
            };
            let durable = &observation.durable;
            assert_eq!(
                durable.state_version,
                if committed {
                    committed_version
                } else {
                    before.state_version
                },
                "{target:?}, consensus_invalid={consensus_invalid}"
            );
            assert_eq!(
                durable.frontiers.header_best,
                if committed { selected_after } else { child },
                "{target:?}, consensus_invalid={consensus_invalid}"
            );
            assert_eq!(
                durable.header_generation,
                if committed && consensus_invalid {
                    committed_header_generation
                } else {
                    before.header_generation
                },
                "{target:?}, consensus_invalid={consensus_invalid}"
            );
            assert_eq!(
                durable.frontiers.verified_best, before.frontiers.verified_best,
                "{target:?}, consensus_invalid={consensus_invalid}"
            );
            assert_eq!(
                durable.verified_generation, before.verified_generation,
                "{target:?}, consensus_invalid={consensus_invalid}"
            );
            let child_node = observation
                .reopened
                .store
                .header_node(child.hash)
                .expect("the body-conclusion node read succeeds")
                .expect("the body-conclusion child remains retained");
            let expected_body = if committed {
                if consensus_invalid {
                    BodyValidationState::ConsensusInvalid {
                        evidence,
                        rule: rule.clone(),
                    }
                } else {
                    BodyValidationState::Verified { evidence }
                }
            } else {
                BodyValidationState::Unknown
            };
            assert_eq!(
                child_node.body_validation_state, expected_body,
                "{target:?}, consensus_invalid={consensus_invalid}"
            );
            if committed {
                let audit = observation
                    .reopened
                    .store
                    .audit_snapshot()
                    .expect("the audit snapshot opens");
                assert!(
                    audit
                        .full_state_attests_to_body_validation_state(child.hash, &expected_body)
                        .expect("the body evidence authority row decodes"),
                    "{target:?}, consensus_invalid={consensus_invalid}"
                );
            }
            assert!(
                child_node.eligibility.direct_reasons.is_empty(),
                "body invalidity controls eligibility without a header reason"
            );
            let tombstone = observation
                .reopened
                .store
                .get_value::<zakura_header_chain::ConsensusInvalidBodyTombstone>(
                    HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE,
                    child.hash.0,
                )
                .expect("the durable tombstone row decodes");
            assert_eq!(
                tombstone,
                (committed && consensus_invalid).then(|| {
                    zakura_header_chain::ConsensusInvalidBodyTombstone {
                        hash: child.hash,
                        height: child.height,
                        evidence,
                        rule: rule.clone(),
                    }
                }),
                "{target:?}, consensus_invalid={consensus_invalid}"
            );
            assert_eq!(
                observation.startup.current.frontiers.header_best,
                if committed { selected_after } else { child },
                "{target:?}, consensus_invalid={consensus_invalid}"
            );
            assert_eq!(
                observation.startup.current.state_version,
                if committed {
                    committed_version
                } else {
                    before.state_version
                },
                "{target:?}, consensus_invalid={consensus_invalid}"
            );
            let reopened_child = observation
                .reopened
                .store
                .header_node(child.hash)
                .expect("the reopened body-conclusion node read succeeds")
                .expect("the reopened body-conclusion child remains retained");
            assert_eq!(
                reopened_child.body_validation_state, expected_body,
                "{target:?}, consensus_invalid={consensus_invalid}"
            );
            assert!(
                reopened_child.eligibility.direct_reasons.is_empty(),
                "recovery does not synthesize a redundant header reason"
            );
        }
    }
}
