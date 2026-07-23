//! Real-writer operation harness with an independent retained-DAG oracle.

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU64,
    sync::LazyLock,
};

use zakura_chain::{
    block,
    work::difficulty::{Work, U256},
};
use zakura_header_chain::{
    audit_store, AlarmSet, ApplyResult, BodyValidationState, BranchId, ChainScore, CheckpointSet,
    EngineConfig, EngineMetadata, EngineMode, EvidenceId, FinalityEpoch, Frontier, FrontierSet,
    FullStateEvidenceAuthority, FullStateFinalized, HeaderBatchInput, HeaderChainDiskVersion,
    HeaderGeneration, HeaderNode, HeaderRules, HeaderValidationState, InsertHeaders, SourceId,
    StateVersion, StoreAuditRead, StoreRead, SuffixWork, SystemClock, TargetCompletion,
    TransitionContext, TransitionEvent, TransitionRequest, TrustedAnchor, VerifiedChainChanged,
    VerifiedChangeCause, VerifiedGeneration, VerifiedHeaderRef, WorkCoordinate, WorkOwner,
};

use super::{
    super::{
        super::{
            super::super::{
                constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
                Config,
            },
            DiskDb, STATE_COLUMN_FAMILIES_IN_CODE,
        },
        HeaderChainRuntime, HeaderChainStore, HEADER_AUX_DELIVERY, HEADER_CANDIDATE, HEADER_CHILD,
        HEADER_DEFERRED, HEADER_ELIGIBILITY_ROOT, HEADER_ENGINE_META, HEADER_FINALITY_HISTORY,
        HEADER_HEIGHT_HASH, HEADER_NODE_BY_HASH, HEADER_SELECTED, HEADER_VALIDATION_CONTEXT,
        HEADER_VERIFIED,
    },
    fabricate::{FabHeader, Universe},
};

const HEADER_FAMILIES: [&str; 12] = [
    HEADER_NODE_BY_HASH,
    HEADER_CHILD,
    HEADER_HEIGHT_HASH,
    HEADER_SELECTED,
    HEADER_VERIFIED,
    HEADER_CANDIDATE,
    HEADER_ELIGIBILITY_ROOT,
    HEADER_AUX_DELIVERY,
    HEADER_DEFERRED,
    HEADER_FINALITY_HISTORY,
    HEADER_VALIDATION_CONTEXT,
    HEADER_ENGINE_META,
];

#[derive(Copy, Clone, Debug)]
pub(super) enum Source {
    Trunk,
    Branch(usize),
}

#[derive(Copy, Clone, Debug)]
pub(super) enum Anchor {
    Natural,
    Genesis,
    TrunkAt(u32),
}

#[derive(Clone, Debug)]
pub(super) enum Op {
    InsertHeaders {
        source: Source,
        offset: usize,
        len: usize,
        anchor: Anchor,
    },
    Verify {
        source: Source,
        index: usize,
    },
    Finalize {
        count: usize,
    },
    MalformedVerify {
        source: Source,
        index: usize,
    },
    Reopen,
}

#[derive(Clone, Debug)]
struct ModelNode {
    parent: block::Hash,
    height: block::Height,
    block_work: Work,
    cumulative_work: U256,
    body_verified: bool,
}

struct Authority(EvidenceId);

impl FullStateEvidenceAuthority for Authority {
    fn authorizes(&self, evidence: EvidenceId) -> bool {
        evidence == self.0
    }
}

pub(super) struct Harness {
    universe: &'static Universe,
    config: EngineConfig,
    db_config: Config,
    runtime: Option<HeaderChainRuntime>,
    model: HashMap<block::Hash, ModelNode>,
    finalized: Frontier,
    verified_path: Vec<Frontier>,
    next_request_id: u64,
    rejections: usize,
    _tempdir: tempfile::TempDir,
}

impl Harness {
    pub fn new() -> Self {
        let universe = universe();
        let tempdir = tempfile::tempdir().expect("the persistent harness directory is created");
        let db_config = Config {
            cache_dir: tempdir.path().to_owned(),
            ephemeral: false,
            debug_skip_non_finalized_state_backup_task: true,
            ..Config::default()
        };
        let frontier = Frontier::new(block::Height(0), universe.genesis.hash());
        let config = EngineConfig::new(
            EngineMode::Integrated,
            universe.network.clone(),
            TrustedAnchor {
                frontier,
                header: universe.genesis.header.clone(),
            },
            CheckpointSet::default(),
        )
        .expect("the coherence engine configuration is valid");
        let anchor_work = universe
            .genesis
            .header
            .difficulty_threshold
            .to_work()
            .expect("the genesis target has exact work");
        let anchor = HeaderNode::from_durable_parts(
            universe.genesis.header.clone(),
            frontier.hash,
            universe.genesis.header.previous_block_hash,
            frontier.height,
            anchor_work,
            WorkCoordinate::new(frontier.hash, anchor_work.as_u256()),
            HeaderValidationState::Valid,
            Default::default(),
            BodyValidationState::Unknown,
            Vec::new(),
        )
        .expect("the genesis node is internally coherent");
        let metadata = EngineMetadata {
            disk_format: HeaderChainDiskVersion(1),
            mode: EngineMode::Integrated,
            network_id: config.network.kind(),
            anchor_manifest_digest: config.trust_anchor_digest(),
            work_origin: frontier,
            state_version: StateVersion::new(1),
            header_generation: HeaderGeneration::new(1),
            verified_generation: VerifiedGeneration::new(1),
            finality_epoch: FinalityEpoch::new(0),
            frontiers: FrontierSet {
                finalized: frontier,
                header_best: frontier,
                verified_best: frontier,
            },
            header_best_score: ChainScore::new(SuffixWork::zero(), frontier.hash),
            oldest_retained_height: frontier.height,
            alarms: AlarmSet::default(),
            last_transition_id: EvidenceId::from_digest([0; 32]),
        };
        let db = open(&db_config, &config.network);
        let store = HeaderChainStore::new(db);
        store
            .initialize(metadata, anchor)
            .expect("the fresh coherence schema initializes");
        let (runtime, report) = store
            .startup(&config)
            .expect("the initialized coherence store audits");
        assert!(report.repairs.is_empty());

        let mut model = HashMap::new();
        model.insert(
            frontier.hash,
            ModelNode {
                parent: universe.genesis.header.previous_block_hash,
                height: frontier.height,
                block_work: anchor_work,
                cumulative_work: U256::zero(),
                body_verified: false,
            },
        );
        let harness = Self {
            universe,
            config,
            db_config,
            runtime: Some(runtime),
            model,
            finalized: frontier,
            verified_path: vec![frontier],
            next_request_id: 1,
            rejections: 0,
            _tempdir: tempdir,
        };
        harness.assert_coherent();
        harness
    }

    pub fn run_all(&mut self, operations: &[Op]) {
        for operation in operations {
            self.run(operation);
        }
    }

    pub fn rejections(&self) -> usize {
        self.rejections
    }

    fn run(&mut self, operation: &Op) {
        match *operation {
            Op::InsertHeaders {
                source,
                offset,
                len,
                anchor,
            } => self.insert_headers(source, offset, len, anchor),
            Op::Verify { source, index } => self.verify(source, index),
            Op::Finalize { count } => self.finalize(count),
            Op::MalformedVerify { source, index } => self.malformed_verify(source, index),
            Op::Reopen => self.reopen(),
        }
        self.assert_coherent();
    }

    fn insert_headers(&mut self, source: Source, offset: usize, len: usize, anchor: Anchor) {
        let rows = self.rows(source);
        if offset >= rows.len() || len == 0 {
            return;
        }
        let rows = rows[offset..(offset + len).min(rows.len())].to_vec();
        let anchor_hash = self.resolve_anchor(source, offset, anchor);
        let before = self.logical_dump();
        let Some(lease) = self
            .runtime()
            .reader()
            .validation_context(anchor_hash)
            .expect("the coherence lease read succeeds")
        else {
            assert_eq!(self.logical_dump(), before);
            self.rejections += 1;
            return;
        };
        let rules = HeaderRules::for_validation_lease(self.config.network.clone(), &lease)
            .expect("the authenticated custom-network policy is valid");
        let headers: Vec<_> = rows.iter().map(|row| row.header.clone()).collect();
        let Ok(batch) = zakura_header_chain::prepare_headers(
            HeaderBatchInput::new(&headers),
            &lease,
            &rules,
            &SystemClock,
        ) else {
            assert_eq!(
                self.logical_dump(),
                before,
                "a rejected prepared range must not mutate any header family"
            );
            self.rejections += 1;
            return;
        };
        let snapshot = self.runtime().publisher().snapshot();
        let target = rows.last().expect("the nonempty range has a target");
        let request_id = NonZeroU64::new(self.next_request_id).expect("request IDs start at one");
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .expect("the bounded coherence sequence cannot exhaust request IDs");
        let result = self
            .runtime()
            .apply(
                TransitionRequest {
                    expected_version: snapshot.state_version,
                    event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                        owner: WorkOwner {
                            state_version: snapshot.state_version,
                            header_generation: snapshot.header_generation,
                            verified_generation: None,
                            branch: BranchId::new(snapshot.frontiers.finalized.hash, target.hash),
                            session_id: 1,
                            request_id,
                        },
                        source: SourceId::from_digest([0x51; 32]),
                        parent_hash: anchor_hash,
                        target_tip_hash: target.hash,
                        completion: TargetCompletion::TargetComplete {
                            common_ancestor: Frontier::new(lease.parent.height, lease.parent.hash),
                        },
                        batch,
                        aux: Vec::new(),
                    })),
                },
                &TransitionContext {
                    config: &self.config,
                    clock: &SystemClock,
                    full_state_authority: None,
                    startup_capability: None,
                    retention_references: &[],
                },
            )
            .expect("a prepared current coherence range reaches the transition writer");
        assert!(matches!(
            result,
            ApplyResult::Committed(_) | ApplyResult::NoChange(_)
        ), "prepared range returned {result:?} for source={source:?}, offset={offset}, len={len}, anchor={anchor:?}");
        self.apply_model(&rows);
    }

    fn verify(&mut self, source: Source, index: usize) {
        let path = self.path_rows(source, index);
        let suffix_start = if self.finalized == self.config.bootstrap_anchor.frontier {
            0
        } else {
            let Some(position) = path
                .iter()
                .position(|header| header.hash == self.finalized.hash)
            else {
                return;
            };
            position + 1
        };
        let suffix = path[suffix_start..].to_vec();
        if suffix.is_empty() {
            return;
        }
        let snapshot = self.runtime().publisher().snapshot();
        let evidence = self.next_evidence(0x60);
        let authority = Authority(evidence);
        let new_path = suffix
            .iter()
            .map(|header| VerifiedHeaderRef {
                height: header.height,
                hash: header.hash,
                header: header.header.clone(),
            })
            .collect();
        let result = self
            .runtime()
            .apply(
                TransitionRequest {
                    expected_version: snapshot.state_version,
                    event: TransitionEvent::VerifiedChainChanged(VerifiedChainChanged {
                        full_state_transition_id: evidence,
                        old_tip: *self
                            .verified_path
                            .last()
                            .expect("the verified model path includes finality"),
                        new_path,
                        cause: VerifiedChangeCause::Reset,
                    }),
                },
                &TransitionContext {
                    config: &self.config,
                    clock: &SystemClock,
                    full_state_authority: Some(&authority),
                    startup_capability: None,
                    retention_references: &[],
                },
            )
            .expect("authenticated body-valid full-state evidence reaches the writer");
        assert!(matches!(
            result,
            ApplyResult::Committed(_) | ApplyResult::NoChange(_)
        ));
        self.apply_model(&suffix);
        for header in &suffix {
            self.model
                .get_mut(&header.hash)
                .expect("the verified suffix is retained by the model")
                .body_verified = true;
        }
        self.verified_path = std::iter::once(self.finalized)
            .chain(
                suffix
                    .iter()
                    .map(|header| Frontier::new(header.height, header.hash)),
            )
            .collect();
    }

    fn finalize(&mut self, count: usize) {
        if count == 0 || self.verified_path.len() <= 1 {
            return;
        }
        let advance = count.min(self.verified_path.len() - 1);
        let new_finalized = self.verified_path[advance];
        let proof = self.verified_path[..=advance]
            .iter()
            .map(|frontier| frontier.hash)
            .collect();
        let snapshot = self.runtime().publisher().snapshot();
        let evidence = self.next_evidence(0x70);
        let authority = Authority(evidence);
        let result = self
            .runtime()
            .apply(
                TransitionRequest {
                    expected_version: snapshot.state_version,
                    event: TransitionEvent::FullStateFinalized(FullStateFinalized {
                        full_state_transition_id: evidence,
                        new_finalized,
                        verified_path_proof: proof,
                    }),
                },
                &TransitionContext {
                    config: &self.config,
                    clock: &SystemClock,
                    full_state_authority: Some(&authority),
                    startup_capability: None,
                    retention_references: &[],
                },
            )
            .expect("authenticated full-state finality reaches the writer");
        assert!(matches!(result, ApplyResult::Committed(_)));

        let retained: HashSet<_> = self
            .model
            .keys()
            .copied()
            .filter(|hash| self.descends_from(*hash, new_finalized.hash))
            .collect();
        self.model.retain(|hash, _| retained.contains(hash));
        self.finalized = new_finalized;
        self.verified_path.drain(..advance);
        assert_eq!(
            self.verified_path.first().copied(),
            Some(self.finalized),
            "the verified projection is rebased to new finality"
        );
    }

    fn malformed_verify(&mut self, source: Source, index: usize) {
        let rows = self.rows(source);
        let header = rows[index.min(rows.len() - 1)].clone();
        if header.header.previous_block_hash == self.finalized.hash {
            return;
        }
        let before_rows = self.logical_dump();
        let before_snapshot = self.runtime().publisher().snapshot();
        let evidence = self.next_evidence(0x80);
        let authority = Authority(evidence);
        let result = self.runtime().apply(
            TransitionRequest {
                expected_version: before_snapshot.state_version,
                event: TransitionEvent::VerifiedChainChanged(VerifiedChainChanged {
                    full_state_transition_id: evidence,
                    old_tip: *self
                        .verified_path
                        .last()
                        .expect("the verified model path includes finality"),
                    new_path: vec![VerifiedHeaderRef {
                        height: header.height,
                        hash: header.hash,
                        header: header.header,
                    }],
                    cause: VerifiedChangeCause::Reset,
                }),
            },
            &TransitionContext {
                config: &self.config,
                clock: &SystemClock,
                full_state_authority: Some(&authority),
                startup_capability: None,
                retention_references: &[],
            },
        );
        assert!(
            result.is_err(),
            "a full-state reset whose first header is not a finalized child must fail"
        );
        assert_eq!(
            self.logical_dump(),
            before_rows,
            "a rejected unlinked full-state reset must mutate no header family"
        );
        assert_eq!(
            self.runtime().publisher().snapshot(),
            before_snapshot,
            "a rejected unlinked full-state reset must not publish"
        );
        self.rejections += 1;
    }

    fn reopen(&mut self) {
        let before = self.logical_dump();
        drop(
            self.runtime
                .take()
                .expect("the coherence runtime is present before reopen"),
        );
        let store = HeaderChainStore::new(open(&self.db_config, &self.config.network));
        let (runtime, report) = store
            .startup(&self.config)
            .expect("the coherent persistent store reopens");
        assert!(
            report.repairs.is_empty(),
            "a clean writer sequence must not depend on startup repair"
        );
        self.runtime = Some(runtime);
        assert_eq!(
            self.logical_dump(),
            before,
            "a clean reopen must not rewrite any logical row"
        );
    }

    fn apply_model(&mut self, rows: &[FabHeader]) {
        for row in rows {
            if let Some(existing) = self.model.get(&row.hash) {
                assert_eq!(existing.parent, row.header.previous_block_hash);
                assert_eq!(existing.height, row.height);
                assert_eq!(existing.block_work, row.work());
                continue;
            }
            let parent = self
                .model
                .get(&row.header.previous_block_hash)
                .expect("production accepted only a model-retained parent");
            assert_eq!(
                row.height,
                parent
                    .height
                    .next()
                    .expect("the bounded model parent has a next height")
            );
            let cumulative_work = parent
                .cumulative_work
                .checked_add(row.work().as_u256())
                .expect("the bounded coherence universe cannot overflow work");
            self.model.insert(
                row.hash,
                ModelNode {
                    parent: row.header.previous_block_hash,
                    height: row.height,
                    block_work: row.work(),
                    cumulative_work,
                    body_verified: false,
                },
            );
        }
    }

    fn descends_from(&self, mut hash: block::Hash, ancestor: block::Hash) -> bool {
        loop {
            if hash == ancestor {
                return true;
            }
            let Some(node) = self.model.get(&hash) else {
                return false;
            };
            if node.height <= self.model[&ancestor].height {
                return false;
            }
            hash = node.parent;
        }
    }

    fn assert_coherent(&self) {
        let runtime = self.runtime();
        let plan = audit_store(&runtime.store, &self.config)
            .expect("the production exhaustive audit accepts every writer state");
        assert!(
            plan.is_clean(),
            "a production writer mutation must not require startup repairs: {:?}",
            plan.repairs
        );
        let durable = runtime
            .store
            .snapshot()
            .expect("the durable coherence snapshot is readable");
        assert_eq!(runtime.publisher().snapshot(), durable);
        assert_eq!(plan.metadata.snapshot(), durable);

        let stored_nodes = runtime
            .store
            .all_nodes()
            .expect("the exhaustive node rows are readable");
        assert_eq!(stored_nodes.len(), self.model.len());
        for node in stored_nodes {
            let expected = self
                .model
                .get(&node.hash)
                .expect("the writer retained no node absent from the model");
            assert_eq!(node.parent_hash, expected.parent);
            assert_eq!(node.height, expected.height);
            assert_eq!(node.block_work, expected.block_work);
            assert_eq!(
                matches!(node.body, BodyValidationState::Verified { .. }),
                expected.body_verified
            );
            assert_eq!(
                node.work_coordinate().cumulative_work(),
                expected
                    .cumulative_work
                    .checked_add(
                        self.universe
                            .genesis
                            .header
                            .difficulty_threshold
                            .to_work()
                            .expect("genesis work is exact")
                            .as_u256()
                    )
                    .expect("the bounded model coordinate cannot overflow")
            );
        }

        let child_parents: HashSet<_> = self.model.values().map(|node| node.parent).collect();
        let finalized_work = self.model[&self.finalized.hash].cumulative_work;
        let candidate_hashes: HashSet<_> = self
            .model
            .keys()
            .copied()
            .filter(|hash| !child_parents.contains(hash))
            .collect();
        let (expected_score, expected_tip) = candidate_hashes
            .iter()
            .map(|hash| {
                let node = &self.model[hash];
                (
                    ChainScore::new(
                        SuffixWork::new(
                            node.cumulative_work
                                .checked_sub(finalized_work)
                                .expect("retained work is never below finalized work"),
                        ),
                        *hash,
                    ),
                    Frontier::new(node.height, *hash),
                )
            })
            .max_by_key(|(score, _)| *score)
            .expect("the model always retains the finalized candidate");
        assert_eq!(durable.header_best_score, expected_score);
        assert_eq!(durable.frontiers.header_best, expected_tip);
        assert_eq!(durable.frontiers.finalized, self.finalized);
        assert_eq!(
            durable.frontiers.verified_best,
            *self
                .verified_path
                .last()
                .expect("the verified model path includes finality")
        );
        assert_eq!(
            runtime
                .store
                .selected_projection()
                .expect("the selected projection is readable"),
            self.path_to(expected_tip.hash)
        );
        assert_eq!(
            runtime
                .store
                .verified_projection()
                .expect("the verified projection is readable"),
            self.verified_path
        );
    }

    fn path_to(&self, mut hash: block::Hash) -> Vec<Frontier> {
        let mut path = Vec::new();
        loop {
            let node = &self.model[&hash];
            path.push(Frontier::new(node.height, hash));
            if hash == self.finalized.hash {
                break;
            }
            hash = node.parent;
        }
        path.reverse();
        path
    }

    fn path_rows(&self, source: Source, index: usize) -> Vec<FabHeader> {
        match source {
            Source::Trunk => {
                self.universe.trunk[..=index.min(self.universe.trunk.len() - 1)].to_vec()
            }
            Source::Branch(branch) => {
                let branch = branch % self.universe.branches.len();
                let branch_def = &self.universe.branches[branch];
                let trunk_tip = if branch == 3 {
                    self.universe.branches[0].fork_parent.0
                } else {
                    branch_def.fork_parent.0
                };
                let fork_height =
                    usize::try_from(trunk_tip.0).expect("the bounded fork height fits in usize");
                let mut path = self.universe.trunk[..fork_height].to_vec();
                if branch == 3 {
                    path.extend(self.universe.branches[0].headers[..2].iter().cloned());
                }
                path.extend(
                    branch_def.headers[..=index.min(branch_def.headers.len() - 1)]
                        .iter()
                        .cloned(),
                );
                path
            }
        }
    }

    fn rows(&self, source: Source) -> &[FabHeader] {
        match source {
            Source::Trunk => &self.universe.trunk,
            Source::Branch(index) => {
                &self.universe.branches[index % self.universe.branches.len()].headers
            }
        }
    }

    fn resolve_anchor(&self, source: Source, offset: usize, anchor: Anchor) -> block::Hash {
        match anchor {
            Anchor::Natural if offset > 0 => self.rows(source)[offset - 1].hash,
            Anchor::Natural => match source {
                Source::Trunk => self.universe.genesis.hash(),
                Source::Branch(index) => {
                    self.universe.branches[index % self.universe.branches.len()]
                        .fork_parent
                        .1
                }
            },
            Anchor::Genesis => self.universe.genesis.hash(),
            Anchor::TrunkAt(height) => {
                self.universe
                    .trunk_at(
                        height.clamp(
                            1,
                            u32::try_from(self.universe.trunk.len())
                                .expect("the bounded trunk length fits in u32"),
                        ),
                    )
                    .hash
            }
        }
    }

    fn runtime(&self) -> &HeaderChainRuntime {
        self.runtime
            .as_ref()
            .expect("the coherence runtime is absent only during reopen")
    }

    fn next_evidence(&mut self, domain: u8) -> EvidenceId {
        let counter =
            u8::try_from(self.next_request_id).expect("the bounded operation sequence fits in u8");
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .expect("the bounded coherence sequence cannot exhaust identities");
        let mut digest = [counter; 32];
        digest[0] = domain;
        EvidenceId::from_digest(digest)
    }

    fn logical_dump(&self) -> Vec<(usize, Vec<u8>, Vec<u8>)> {
        let mut rows = Vec::new();
        for (family, name) in HEADER_FAMILIES.iter().enumerate() {
            for (key, value) in self
                .runtime()
                .store
                .scan_raw(name)
                .expect("the logical header rows are readable")
            {
                rows.push((family, key, value));
            }
        }
        rows.sort();
        rows
    }
}

fn universe() -> &'static Universe {
    static UNIVERSE: LazyLock<Universe> = LazyLock::new(Universe::new);
    &UNIVERSE
}

fn open(config: &Config, network: &zakura_chain::parameters::Network) -> DiskDb {
    DiskDb::new(
        config,
        STATE_DATABASE_KIND,
        &state_database_format_version_in_code(),
        network,
        STATE_COLUMN_FAMILIES_IN_CODE
            .iter()
            .map(ToString::to_string),
        false,
    )
    .expect("the coherence database opens")
}
