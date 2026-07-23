//! Bounded logical-row replay used by the header recovery fuzz target.

use std::collections::BTreeMap;

use zakura_chain::{
    block::{self, genesis::regtest_genesis_block},
    parameters::{testnet::RegtestParameters, Network},
};
use zakura_header_chain::{
    AlarmSet, BodyRuleId, ChainScore, CheckpointSet, EngineConfig, EngineMetadata, EngineMode,
    EvidenceId, FinalityEpoch, FinalityRecord, FinalitySource, Frontier, FrontierSet,
    FullStateEvidenceAuthority, HeaderChainDiskVersion, HeaderGeneration, HeaderNode,
    HeaderValidationState, MigratedPinRefutation, RecoveryRepair, StateVersion, StoreRead,
    SuffixWork, SystemClock, TransitionContext, TransitionEvent, TransitionRequest, TrustedAnchor,
    VerifiedGeneration, WorkCoordinate,
};

use super::{
    super::{
        super::super::constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
        disk_format::{
            header_chain::HeaderFinalityKey, header_chain_values::HeaderFinalityRecordDisk,
            IntoDisk,
        },
        DiskDb, DiskWriteBatch, STATE_COLUMN_FAMILIES_IN_CODE,
    },
    HeaderChainStore, HeaderChainStoreError, HEADER_AUX_DELIVERY, HEADER_CANDIDATE, HEADER_CHILD,
    HEADER_DEFERRED, HEADER_ELIGIBILITY_ROOT, HEADER_ENGINE_META, HEADER_FINALITY_HISTORY,
    HEADER_HEIGHT_HASH, HEADER_NODE_BY_HASH, HEADER_SELECTED, HEADER_VALIDATION_CONTEXT,
    HEADER_VERIFIED,
};
use crate::Config;

const MAX_INPUT_BYTES: usize = 256;
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

/// Stable result counters from one logical recovery-row replay.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RecoveryRowsReplaySummary {
    /// Number of complete four-byte mutations applied.
    pub mutations: usize,
    /// Number of reconstructible categories repaired before publication.
    pub repairs: usize,
    /// Whether the mutated store failed closed before publisher construction.
    pub rejected: bool,
    /// Supported headers-only-to-integrated migrations completed.
    pub mode_migrations: usize,
    /// Mode migrations rejected before publication for a mismatched full-state pin.
    pub mode_migration_rejections: usize,
    /// Migrated trust pins durably refuted and rejected again on reopen.
    pub migrated_pin_refutations: usize,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct LogicalRow {
    family: u8,
    key: Vec<u8>,
    value: Vec<u8>,
}

/// Replay bounded raw row mutations through the production startup audit and repair path.
pub fn replay_recovery_rows_bytes(data: &[u8]) -> RecoveryRowsReplaySummary {
    let (config, anchor, metadata) = fixture();
    let db = open(&config.network);
    let store = HeaderChainStore::new(db.clone());
    store
        .initialize(metadata.clone(), anchor)
        .expect("the fixed authenticated recovery fixture initializes");

    let mut rows = logical_dump(&store);
    let mut mutations = 0;
    let mut mode_operations = Vec::new();
    for operation in data[..data.len().min(MAX_INPUT_BYTES)].chunks_exact(4) {
        if recovery_opcode(operation[0]) == 8 {
            if mode_operations.len() < 2 {
                mode_operations.push(
                    operation
                        .try_into()
                        .expect("chunks_exact yields four-byte operations"),
                );
            }
            continue;
        }
        mutate_rows(&mut rows, operation);
        mutations += 1;
    }
    canonicalize(&mut rows);
    install_dump(&store, &rows);
    let installed = logical_dump(&store);
    assert_eq!(
        installed, rows,
        "the logical mutation dump installs exactly"
    );

    let (mode_migrations, mode_migration_rejections, migrated_pin_refutations) =
        replay_mode_operations(&mode_operations);
    match store.startup(&config) {
        Ok((runtime, report)) => {
            assert!(report.publication_allowed);
            assert_eq!(
                report.current.frontiers, metadata.frontiers,
                "recovery never publishes a frontier absent from authenticated source rows"
            );
            let repairs = report.repairs.len();
            assert!(report.repairs.iter().all(is_reconstructible));
            let current = report.current.clone();
            drop(runtime);
            let (reopened, reopened_report) = HeaderChainStore::new(db)
                .startup(&config)
                .expect("a successful recovery reopens coherently");
            assert!(
                reopened_report.repairs.is_empty(),
                "one recovery transaction leaves no residual repair"
            );
            assert_eq!(reopened.publisher().snapshot(), current);
            RecoveryRowsReplaySummary {
                mutations,
                repairs,
                rejected: false,
                mode_migrations,
                mode_migration_rejections,
                migrated_pin_refutations,
            }
        }
        Err(_) => {
            let after = logical_dump(&HeaderChainStore::new(db));
            assert_eq!(
                after, installed,
                "failed startup performs no logical mutation before publication"
            );
            RecoveryRowsReplaySummary {
                mutations,
                repairs: 0,
                rejected: true,
                mode_migrations,
                mode_migration_rejections,
                migrated_pin_refutations,
            }
        }
    }
}

fn mutate_rows(rows: &mut Vec<LogicalRow>, operation: &[u8]) {
    let opcode = recovery_opcode(operation[0]);
    if opcode == 0 {
        remove_reconstructible_row(rows, operation[1]);
        return;
    }
    if opcode == 7 {
        let family = operation[1] % family_count();
        let key_length = usize::from(operation[2] % 72);
        let value_length = usize::from(operation[3] % 96);
        rows.push(LogicalRow {
            family,
            key: vec![operation[3]; key_length],
            value: vec![operation[2]; value_length],
        });
        return;
    }
    let Some(index) = row_index(rows, operation[1]) else {
        return;
    };
    match opcode {
        1 => mutate_byte(&mut rows[index].key, operation[2], operation[3]),
        2 => truncate(&mut rows[index].key, operation[2]),
        3 => rows[index].key.push(operation[3]),
        4 => mutate_byte(&mut rows[index].value, operation[2], operation[3]),
        5 => truncate(&mut rows[index].value, operation[2]),
        6 => rows[index].value.push(operation[3]),
        0 | 7 => unreachable!("handled before selecting a row"),
        8 => unreachable!("mode operations are separated before row mutation"),
        _ => unreachable!("the opcode is reduced to a known operation"),
    }
}

fn recovery_opcode(byte: u8) -> u8 {
    match byte {
        8 | 72 => 8,
        _ => byte % 8,
    }
}

fn replay_mode_operations(operations: &[[u8; 4]]) -> (usize, usize, usize) {
    let mut migrations = 0;
    let mut rejections = 0;
    let mut refutations = 0;
    for operation in operations {
        let (integrated_config, anchor, mut metadata) = fixture();
        let mut headers_only_config = integrated_config.clone();
        headers_only_config.mode = EngineMode::HeadersOnly;
        metadata.mode = EngineMode::HeadersOnly;
        let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
        let db = open(&integrated_config.network);
        let store = HeaderChainStore::new(db.clone());
        store
            .initialize(metadata, anchor)
            .expect("the bounded headers-only migration fixture initializes");
        let record = FinalityRecord {
            previous: anchor_frontier,
            current: anchor_frontier,
            source: FinalitySource::MigratedHeadersOnly,
            epoch: FinalityEpoch::new(0),
        };
        let mut batch = DiskWriteBatch::new();
        store
            .put_value(
                &mut batch,
                HEADER_FINALITY_HISTORY,
                HeaderFinalityKey(record.epoch).as_bytes(),
                &HeaderFinalityRecordDisk(record),
            )
            .expect("the fixed migrated-pin record encodes");
        db.write(batch)
            .expect("the fixed migrated-pin record commits");
        let before = logical_dump(&store);

        let supplied_pin = if operation[3] & 1 == 0 {
            anchor_frontier
        } else {
            Frontier::new(anchor_frontier.height, block::Hash([operation[2]; 32]))
        };
        let result = store
            .clone()
            .migrate_headers_only_to_integrated(&integrated_config, supplied_pin);
        if supplied_pin != anchor_frontier {
            assert!(matches!(
                result,
                Err(HeaderChainStoreError::Incoherent(
                    "integrated migration requires full-state verification through the preserved pin"
                ))
            ));
            assert_eq!(
                logical_dump(&store),
                before,
                "a rejected mode migration performs no durable mutation"
            );
            rejections += 1;
            continue;
        }

        let (runtime, report) = result.expect("the exact authenticated pin permits migration");
        assert_eq!(report.current.mode, EngineMode::Integrated);
        assert!(report.publication_allowed);
        assert!(matches!(
            runtime.store.finality_history().as_deref(),
            Ok([FinalityRecord {
                source: FinalitySource::MigratedHeadersOnly,
                ..
            }])
        ));
        migrations += 1;

        if operation[3] & 2 == 0 {
            drop(runtime);
            let (reopened, reopened_report) = HeaderChainStore::new(db)
                .startup(&integrated_config)
                .expect("the successful mode migration reopens coherently");
            assert_eq!(reopened_report.current.mode, EngineMode::Integrated);
            assert_eq!(reopened.publisher().snapshot(), report.current);
            continue;
        }

        let evidence = EvidenceId::from_digest([operation[2]; 32]);
        let authority = FuzzAuthority(evidence);
        let snapshot = runtime.publisher().snapshot();
        let context = TransitionContext {
            config: &integrated_config,
            clock: &SystemClock,
            full_state_authority: Some(&authority),
            retention_references: &[],
        };
        let result = runtime.apply(
            TransitionRequest {
                expected_version: snapshot.state_version,
                event: TransitionEvent::MigratedPinRefutation(MigratedPinRefutation {
                    full_state_transition_id: evidence,
                    pin: anchor_frontier,
                    invalid_header: anchor_frontier,
                    rule: BodyRuleId::new("fuzz.migrated-pin-refutation"),
                }),
            },
            &context,
        );
        assert!(matches!(
            result,
            Err(HeaderChainStoreError::MigratedPinRefuted { pin }) if pin == anchor_frontier
        ));
        assert_eq!(
            runtime.publisher().snapshot(),
            snapshot,
            "fail-closed pin refutation is never published as a usable frontier"
        );
        drop(runtime);
        assert!(matches!(
            HeaderChainStore::new(db).startup(&integrated_config),
            Err(HeaderChainStoreError::MigratedPinRefuted { pin }) if pin == anchor_frontier
        ));
        refutations += 1;
    }
    (migrations, rejections, refutations)
}

struct FuzzAuthority(EvidenceId);

impl FullStateEvidenceAuthority for FuzzAuthority {
    fn authorizes(&self, evidence: EvidenceId) -> bool {
        evidence == self.0
    }
}

fn remove_reconstructible_row(rows: &mut Vec<LogicalRow>, selector: u8) {
    let families = [
        HEADER_CHILD,
        HEADER_HEIGHT_HASH,
        HEADER_SELECTED,
        HEADER_VERIFIED,
        HEADER_CANDIDATE,
        HEADER_DEFERRED,
    ];
    let family = family_id(families[usize::from(selector) % families.len()]);
    if let Some(index) = rows.iter().position(|row| row.family == family) {
        rows.remove(index);
    } else {
        rows.push(LogicalRow {
            family,
            key: vec![selector; expected_key_width(family)],
            value: Vec::new(),
        });
    }
}

fn mutate_byte(bytes: &mut [u8], selector: u8, mutation: u8) {
    if bytes.is_empty() {
        return;
    }
    let index = usize::from(selector) % bytes.len();
    bytes[index] ^= mutation | 1;
}

fn truncate(bytes: &mut Vec<u8>, selector: u8) {
    if !bytes.is_empty() {
        bytes.truncate(usize::from(selector) % bytes.len());
    }
}

fn row_index(rows: &[LogicalRow], selector: u8) -> Option<usize> {
    let family = selector % family_count();
    rows.iter().position(|row| row.family == family)
}

fn canonicalize(rows: &mut Vec<LogicalRow>) {
    let mut unique = BTreeMap::new();
    for row in rows.drain(..) {
        unique.insert((row.family, row.key), row.value);
    }
    *rows = unique
        .into_iter()
        .map(|((family, key), value)| LogicalRow { family, key, value })
        .collect();
}

fn logical_dump(store: &HeaderChainStore) -> Vec<LogicalRow> {
    let mut rows = Vec::new();
    for (family, name) in HEADER_FAMILIES.iter().enumerate() {
        let family = u8::try_from(family).expect("the header family count fits in one byte");
        for (key, value) in store
            .scan_raw(name)
            .expect("the local logical dump remains readable from RocksDB")
        {
            rows.push(LogicalRow { family, key, value });
        }
    }
    canonicalize(&mut rows);
    rows
}

fn install_dump(store: &HeaderChainStore, rows: &[LogicalRow]) {
    let mut batch = DiskWriteBatch::new();
    for family in HEADER_FAMILIES {
        for (key, _) in store
            .scan_raw(family)
            .expect("the baseline logical rows are readable")
        {
            store
                .delete_raw(&mut batch, family, key)
                .expect("the fixed header column family is open");
        }
    }
    for row in rows {
        let family = HEADER_FAMILIES[usize::from(row.family)];
        store
            .put_raw(&mut batch, family, &row.key, &row.value)
            .expect("the fixed header column family is open");
    }
    store
        .db
        .write(batch)
        .expect("the bounded logical mutation batch commits");
}

fn family_id(name: &str) -> u8 {
    let index = HEADER_FAMILIES
        .iter()
        .position(|candidate| *candidate == name)
        .expect("the named header family is in the fuzz family table");
    u8::try_from(index).expect("the header family count fits in one byte")
}

fn family_count() -> u8 {
    u8::try_from(HEADER_FAMILIES.len()).expect("the header family count fits in one byte")
}

fn expected_key_width(family: u8) -> usize {
    match HEADER_FAMILIES[usize::from(family)] {
        HEADER_CHILD | HEADER_CANDIDATE | HEADER_AUX_DELIVERY => 64,
        HEADER_HEIGHT_HASH => 36,
        HEADER_SELECTED | HEADER_VERIFIED => 4,
        HEADER_ELIGIBILITY_ROOT => 65,
        HEADER_DEFERRED => 44,
        HEADER_FINALITY_HISTORY => 8,
        HEADER_NODE_BY_HASH | HEADER_VALIDATION_CONTEXT => 32,
        HEADER_ENGINE_META => 0,
        _ => unreachable!("all header families have fixed version-one key widths"),
    }
}

fn is_reconstructible(repair: &RecoveryRepair) -> bool {
    matches!(
        repair,
        RecoveryRepair::ChildIndex
            | RecoveryRepair::HeightIndex
            | RecoveryRepair::DeferredIndex
            | RecoveryRepair::CandidateIndex
            | RecoveryRepair::SelectedProjection
            | RecoveryRepair::VerifiedProjection
            | RecoveryRepair::InheritedEligibility
            | RecoveryRepair::RetentionMetadata
            | RecoveryRepair::BodyAvailabilityAlarm
    )
}

fn open(network: &Network) -> DiskDb {
    DiskDb::new(
        &Config::ephemeral(),
        STATE_DATABASE_KIND,
        &state_database_format_version_in_code(),
        network,
        STATE_COLUMN_FAMILIES_IN_CODE
            .iter()
            .map(ToString::to_string),
        false,
    )
    .expect("the ephemeral recovery fuzz database opens")
}

fn fixture() -> (EngineConfig, HeaderNode, EngineMetadata) {
    let network = Network::new_regtest(RegtestParameters::default());
    let block = regtest_genesis_block();
    let frontier = Frontier::new(block::Height(0), block.hash());
    let config = EngineConfig::new(
        EngineMode::Integrated,
        network,
        TrustedAnchor {
            frontier,
            header: block.header.clone(),
        },
        CheckpointSet::default(),
    )
    .expect("the fixed regtest engine configuration is coherent");
    let work = block
        .header
        .difficulty_threshold
        .to_work()
        .expect("the regtest genesis target has exact work");
    let node = HeaderNode::from_durable_parts(
        block.header.clone(),
        frontier.hash,
        block.header.previous_block_hash,
        frontier.height,
        work,
        WorkCoordinate::new(frontier.hash, work.as_u256()),
        HeaderValidationState::Valid,
        zakura_header_chain::EligibilityState::default(),
        zakura_header_chain::BodyValidationState::Unknown,
        Vec::new(),
    )
    .expect("the canonical genesis fields agree");
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
    (config, node, metadata)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use super::*;

    #[test]
    fn recovery_rows_regression_corpus_replays_green() {
        let corpus =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../fuzz/header-chain/corpus/recovery_rows");
        let mut entries: Vec<_> = fs::read_dir(&corpus)
            .expect("the checked-in recovery corpus exists")
            .map(|entry| entry.expect("the corpus entry is readable").path())
            .collect();
        entries.sort();
        assert!(!entries.is_empty(), "the recovery corpus is not empty");
        for path in entries {
            let data = fs::read(&path).expect("the recovery corpus file is readable");
            let first = replay_recovery_rows_bytes(&data);
            let second = replay_recovery_rows_bytes(&data);
            assert_eq!(
                first,
                second,
                "recovery replay is deterministic for {}",
                path.display()
            );
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .expect("corpus filenames are valid UTF-8");
            match name {
                "clean" => {
                    assert_eq!(first.mutations, 0);
                    assert_eq!(first.repairs, 0);
                    assert!(!first.rejected);
                }
                "reconstructible_indexes" => {
                    assert!(first.repairs > 0);
                    assert!(!first.rejected);
                }
                "malformed_key" | "truncated_authority" => {
                    assert_eq!(first.repairs, 0);
                    assert!(first.rejected);
                }
                "mode_migration" => {
                    assert_eq!(first.mutations, 0);
                    assert_eq!(first.mode_migrations, 1);
                    assert_eq!(first.mode_migration_rejections, 0);
                    assert_eq!(first.migrated_pin_refutations, 0);
                    assert!(!first.rejected);
                }
                "mode_migration_rejection" => {
                    assert_eq!(first.mutations, 0);
                    assert_eq!(first.mode_migrations, 0);
                    assert_eq!(first.mode_migration_rejections, 1);
                    assert_eq!(first.migrated_pin_refutations, 0);
                    assert!(!first.rejected);
                }
                "migrated_pin_refutation" => {
                    assert_eq!(first.mutations, 0);
                    assert_eq!(first.mode_migrations, 1);
                    assert_eq!(first.mode_migration_rejections, 0);
                    assert_eq!(first.migrated_pin_refutations, 1);
                    assert!(!first.rejected);
                }
                other => panic!("new recovery corpus seed {other} needs an expected outcome"),
            }
        }
    }

    #[test]
    fn reconstructible_and_authoritative_rows_take_distinct_paths() {
        let repair = replay_recovery_rows_bytes(&[0, 2, 0, 0]);
        assert!(!repair.rejected);
        assert!(repair.repairs > 0);

        let rejection = replay_recovery_rows_bytes(&[5, 0, 0, 0]);
        assert!(rejection.rejected);
        assert_eq!(rejection.repairs, 0);
    }

    #[test]
    fn mode_migration_requires_the_exact_pin_and_refutation_fails_closed() {
        let migrated = replay_recovery_rows_bytes(&[8, 0, 7, 0]);
        assert_eq!(migrated.mode_migrations, 1);
        assert_eq!(migrated.mode_migration_rejections, 0);
        assert_eq!(migrated.migrated_pin_refutations, 0);

        let rejected = replay_recovery_rows_bytes(&[8, 0, 7, 1]);
        assert_eq!(rejected.mode_migrations, 0);
        assert_eq!(rejected.mode_migration_rejections, 1);

        let refuted = replay_recovery_rows_bytes(&[8, 0, 7, 2]);
        assert_eq!(refuted.mode_migrations, 1);
        assert_eq!(refuted.migrated_pin_refutations, 1);
    }
}
