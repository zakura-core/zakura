use super::*;
use proptest::prelude::*;
use zakura_header_chain::{AuxObservationV1, AuxVerificationFactV1, TreeAuxRecordV1};

struct Sequence {
    runtime: Option<HeaderChainRuntime>,
    config: EngineConfig,
    db_config: Config,
    _directory: tempfile::TempDir,
    serial: u64,
}

impl Sequence {
    fn new(bucket: usize) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let db_config = Config {
            cache_dir: directory.path().to_owned(),
            ephemeral: false,
            debug_skip_non_finalized_state_backup_task: true,
            ..Config::default()
        };
        let (mut config, anchor, metadata) = fixture();
        config.limits.max_aux_deliveries_per_header = NonZeroUsize::new(bucket).unwrap();
        config.limits.max_aux_deliveries_total = NonZeroUsize::new(3 * bucket + 2).unwrap();
        let store = HeaderChainStore::new(open(&db_config, config.network()));
        store.initialize(metadata, anchor).unwrap();
        let (runtime, _) = store.startup(&config).unwrap();
        Self {
            runtime: Some(runtime),
            config,
            db_config,
            _directory: directory,
            serial: 1,
        }
    }

    fn runtime(&self) -> &HeaderChainRuntime {
        self.runtime.as_ref().unwrap()
    }

    fn identity(&mut self) -> EvidenceId {
        self.serial += 1;
        let mut bytes = [0; 32];
        bytes[..8].copy_from_slice(&self.serial.to_le_bytes());
        EvidenceId::from_digest(bytes)
    }

    fn selected(&self, offset: u32) -> Option<HeaderNode> {
        let snapshot = self.runtime().publisher().snapshot();
        let height = block::Height(snapshot.frontiers.finalized.height.0 + offset);
        self.runtime()
            .reader()
            .coherent_selected_node(height)
            .unwrap()
    }

    fn dump(&self) -> Vec<(String, Vec<u8>, Vec<u8>)> {
        let mut rows = Vec::new();
        for name in STATE_COLUMN_FAMILIES_IN_CODE {
            for (key, value) in self.runtime().store.scan_raw(name).unwrap() {
                rows.push((name.to_string(), key, value));
            }
        }
        rows.sort();
        rows
    }

    fn apply(&self, event: TransitionEvent) -> bool {
        let before = self.dump();
        let snapshot = self.runtime().publisher().snapshot();
        let authenticated: Vec<_> = self
            .runtime()
            .store
            .load_aux_deliveries()
            .unwrap()
            .into_iter()
            .filter_map(|row| {
                let delivery = row.delivery();
                let engine = self.runtime().transition_engine.lock().unwrap();
                engine
                    .aux_deliveries(delivery.header_hash)
                    .iter()
                    .find(|row| row.delivery_id == delivery.delivery_id)
                    .filter(|delivery| delivery.is_authenticated())
                    .copied()
            })
            .collect();
        let authority = event.idempotency_key().map(Authority);
        let mut canonical = DiskWriteBatch::new();
        if let TransitionEvent::VerifiedChainChanged(change) = &event {
            let header = change.new_path.last().unwrap();
            stage_full_state_canonical_hash(
                &self.runtime().store,
                &mut canonical,
                Frontier::new(header.height, header.hash),
            );
        }
        let result = self.runtime().apply_combined(
            TransitionRequest {
                expected_version: snapshot.state_version,
                event,
            },
            &TransitionContext {
                config: &self.config,
                clock: &SystemClock,
                full_state_authority: authority
                    .as_ref()
                    .map(|authority| authority as &dyn FullStateEvidenceAuthority),
                retention_references: &[],
            },
            canonical,
            || {},
        );
        let committed = match result {
            Ok(ApplyResult::Committed) => true,
            Ok(ApplyResult::NoChange(_) | ApplyResult::Stale(_))
            | Err(HeaderChainStoreError::Transition(
                TransitionFailure::AuxiliaryLimitExceeded | TransitionFailure::ConflictingReplay,
            )) => {
                assert_eq!(
                    self.dump(),
                    before,
                    "refusal or no-change must preserve every durable row"
                );
                assert_eq!(self.runtime().publisher().snapshot(), snapshot);
                false
            }
            other => panic!("unexpected sequence outcome: {other:?}"),
        };
        for delivery in authenticated {
            if let Some(node) = self
                .runtime()
                .store
                .header_node(delivery.header_hash)
                .unwrap()
            {
                assert!(
                    node.aux_delivery_ids.contains(&delivery.delivery_id),
                    "admission cannot replace authenticated input on a retained header"
                );
            }
        }
        self.check();
        committed
    }

    fn check(&self) {
        let rows = self.runtime().store.load_aux_deliveries().unwrap();
        let bucket = self.config.limits.max_aux_deliveries_per_header.get();
        let occupied: usize = (0..3)
            .filter_map(|offset| self.selected(offset))
            .map(|node| node.aux_delivery_ids.len())
            .sum();
        // This oracle uses durable rows and the published selected window, not engine capacity helpers.
        assert!(
            rows.len() + 3 * bucket - occupied <= self.config.limits.max_aux_deliveries_total.get()
        );
        for row in &rows {
            let delivery = row.delivery();
            let node = self
                .runtime()
                .store
                .header_node(delivery.header_hash)
                .unwrap()
                .unwrap();
            assert!(node.aux_delivery_ids.len() <= bucket);
            assert!(node.aux_delivery_ids.contains(&delivery.delivery_id));
            assert_eq!(
                node.aux_delivery_ids.len(),
                rows.iter()
                    .filter(|row| row.delivery().header_hash == node.hash)
                    .count()
            );
        }
        assert!(audit_store(&self.runtime().store, &self.config)
            .unwrap()
            .is_clean());
    }

    fn grow(&mut self, fork: bool) {
        let snapshot = self.runtime().publisher().snapshot();
        let parent = if fork {
            snapshot.frontiers.finalized
        } else {
            snapshot.frontiers.header_best
        };
        let count = if fork {
            snapshot.frontiers.header_best.height.0 - parent.height.0 + 2
        } else {
            6
        };
        self.identity();
        let lease = self
            .runtime()
            .reader()
            .validation_context(parent.hash)
            .unwrap()
            .unwrap();
        let rules = HeaderRules::for_validation_lease(&lease).unwrap();
        let mut header = self
            .runtime()
            .store
            .header_node(parent.hash)
            .unwrap()
            .unwrap()
            .header;
        let mut headers = Vec::new();
        for index in 0..count {
            let mut next = *header;
            next.previous_block_hash = header.hash();
            next.time += chrono::Duration::seconds(1);
            next.nonce.0[..8].copy_from_slice(&(self.serial + u64::from(index)).to_le_bytes());
            header = Arc::new(next);
            headers.push(header.clone());
        }
        self.serial += u64::from(count);
        let batch = zakura_header_chain::prepare_headers(
            HeaderBatchInput::new(&headers),
            parent,
            &rules,
            &SystemClock,
        )
        .unwrap();
        let target = headers.last().unwrap().hash();
        assert!(
            self.apply(TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                owner: header_owner(&snapshot, target, 1, self.serial),
                source: SourceId::from_digest([0x51; 32]),
                parent_hash: parent.hash,
                target_tip_hash: target,
                completion: TargetCompletion::TargetComplete {
                    common_ancestor: parent
                },
                batch,
                aux: Vec::new(),
            })))
        );
        if fork {
            assert_eq!(
                self.runtime()
                    .publisher()
                    .snapshot()
                    .frontiers
                    .header_best
                    .hash,
                target
            );
        }
    }

    fn deliver(&mut self, offset: u32, repair: bool) {
        let Some(node) = self.selected(offset) else {
            return;
        };
        let id = self.identity();
        let snapshot = self.runtime().publisher().snapshot();
        let target = Frontier::new(node.height, node.hash);
        let parent = Frontier::new(node.height.previous().unwrap(), node.parent_hash);
        let owner = body_owner(&snapshot, 2, self.serial);
        let reader = self.runtime().reader();
        let context = reader
            .vct_repair_context(owner, node.height)
            .unwrap()
            .unwrap();
        let lease = reader.validation_context(parent.hash).unwrap().unwrap();
        let rules = HeaderRules::for_validation_lease(&lease).unwrap();
        let batch = zakura_header_chain::prepare_headers(
            HeaderBatchInput::new(&[node.header]),
            parent,
            &rules,
            &SystemClock,
        )
        .unwrap();
        let work_owner = if repair {
            owner.into()
        } else {
            header_owner(&snapshot, node.hash, 3, self.serial)
        };
        let source = SourceId::from_digest(id.digest());
        let delivery = AuxDelivery::new(
            id,
            node.hash,
            source,
            work_owner,
            zakura_header_chain::BodySizeHint::Unknown,
            Some(TreeAuxRecordV1 {
                height: node.height,
                sapling_root: Default::default(),
                orchard_root: Default::default(),
                ironwood_root: Default::default(),
                sapling_tx_count: self.serial,
                orchard_tx_count: 0,
                ironwood_tx_count: 0,
                auth_data_root: [0; 32].into(),
            }),
        );
        let completion = if repair {
            TargetCompletion::SelectedAuxiliaryRepair {
                common_ancestor: parent,
                selected_target: target,
                episode: context.episode,
            }
        } else {
            TargetCompletion::TargetComplete {
                common_ancestor: parent,
            }
        };
        let prior_rows = self.runtime().store.load_aux_deliveries().unwrap();
        let committed = self.apply(TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
            owner: work_owner,
            source,
            parent_hash: parent.hash,
            target_tip_hash: target.hash,
            completion,
            batch,
            aux: vec![delivery],
        })));
        if repair && context.admission_capacity_available {
            assert!(
                committed,
                "eligible selected repair must fit when preflight grants capacity"
            );
        } else if repair && committed {
            // Preflight does not predict space that settlement can reclaim from other branches.
            assert!(prior_rows.iter().any(|row| self
                .runtime()
                .store
                .header_node(row.delivery().header_hash)
                .unwrap()
                .is_none()));
        }
    }

    fn observe(&mut self, offset: u32, authenticate: bool) {
        let Some(node) = self.selected(offset) else {
            return;
        };
        if self.selected(offset + 1).is_none() {
            return;
        }
        let rows = self
            .runtime()
            .reader()
            .coherent_aux_deliveries(&node)
            .unwrap();
        let Some(delivery) = rows
            .into_iter()
            .find(|delivery| delivery.is_unauthenticated())
        else {
            return;
        };
        self.identity();
        let snapshot = self.runtime().publisher().snapshot();
        let verification = if authenticate {
            AuxVerificationFactV1::current_delivery_verified()
        } else {
            AuxVerificationFactV1::current_delivery_failed(1)
        };
        let observation = AuxObservationV1::from_vct(
            body_owner(&snapshot, 4, self.serial),
            vec![delivery],
            verification,
            Some([0x81; 32].into()),
        )
        .unwrap();
        assert!(self.apply(TransitionEvent::AuxEvidence(Box::new(
            zakura_header_chain::AuxEvidence::observed(observation)
        ))));
    }

    fn finalize(&mut self) {
        let Some(node) = self.selected(1) else { return };
        let snapshot = self.runtime().publisher().snapshot();
        let target = Frontier::new(node.height, node.hash);
        let evidence =
            zakura_header_chain::checkpoint_finality_evidence(snapshot.state_version, target);
        assert!(self.apply(TransitionEvent::VerifiedChainChanged(
            VerifiedChainChanged {
                full_state_transition_id: evidence,
                old_tip: snapshot.frontiers.verified_best,
                new_path: vec![zakura_header_chain::VerifiedHeaderRef {
                    height: node.height,
                    hash: node.hash,
                    header: node.header
                }],
                cause: VerifiedChangeCause::CheckpointFinalizedGrow,
            }
        )));
        assert_eq!(
            self.runtime().publisher().snapshot().frontiers.finalized,
            target
        );
    }

    fn reopen(&mut self) {
        let before = self.dump();
        let snapshot = self.runtime().publisher().snapshot();
        drop(self.runtime.take());
        let (runtime, _) = HeaderChainStore::new(open(&self.db_config, self.config.network()))
            .startup(&self.config)
            .unwrap();
        self.runtime = Some(runtime);
        assert_eq!(
            self.dump(),
            before,
            "clean reopen preserves durable input and indexes"
        );
        assert_eq!(self.runtime().publisher().snapshot(), snapshot);
        self.check();
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn auxiliary_capacity_survives_generated_fork_and_reopen_sequences(
        bucket in 2usize..5,
        operations in prop::collection::vec((0u8..8, 1u32..6), 1..32),
    ) {
        let _guard = zakura_test::init();
        let mut sequence = Sequence::new(bucket);
        sequence.grow(false);
        for offset in [1, 2] {
            for _ in 0..bucket { sequence.deliver(offset, true); }
        }
        sequence.deliver(3, false);
        sequence.deliver(4, false);
        assert_eq!(sequence.runtime().reader().speculative_auxiliary_capacity().unwrap(), 0);
        sequence.deliver(5, true);
        sequence.observe(1, true);
        sequence.observe(1, false);
        sequence.deliver(1, true);
        sequence.reopen();
        sequence.deliver(1, true);
        sequence.grow(true);
        for (operation, offset) in operations {
            match operation {
                0 => sequence.deliver(offset, false),
                1 => sequence.deliver(offset, true),
                2 => sequence.observe(offset, false),
                3 => sequence.observe(offset, true),
                4 => sequence.grow(true),
                5 => sequence.finalize(),
                6 => sequence.reopen(),
                7 => sequence.grow(false),
                _ => unreachable!(),
            }
        }
        sequence.grow(false);
        sequence.deliver(1, true);
        sequence.deliver(2, true);
        sequence.finalize();
        sequence.reopen();
    }
}
