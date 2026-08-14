//! Versioned engine resource limits.

use std::{
    collections::BTreeMap,
    num::{NonZeroU32, NonZeroUsize},
    str::FromStr,
    sync::Arc,
};

use sha2::{Digest, Sha256};
use thiserror::Error;
use zakura_chain::{
    block,
    parameters::{
        constants::MAX_BLOCK_REORG_HEIGHT, Network, NetworkKind, NetworkUpgrade,
        MAX_NON_FINALIZED_CHAIN_FORKS,
    },
};

use crate::Frontier;

/// Exact v1 maximum number of retained non-finalized header nodes.
pub const MAX_NON_FINALIZED_NODES_V1: usize = 65_536;
/// Exact v1 maximum number of staged unknown targets across all peers.
pub const MAX_STAGED_TARGETS_V1: usize = 16;
/// Exact v1 maximum prepared headers admitted by one transition.
pub const MAX_HEADERS_PER_TRANSITION_V1: usize = 4_000;
/// Exact v1 maximum auxiliary deliveries retained for one header.
pub const MAX_AUX_DELIVERIES_PER_HEADER_V1: usize = 16;
/// Exact v1 maximum auxiliary deliveries retained across the graph.
pub const MAX_AUX_DELIVERIES_TOTAL_V1: usize = MAX_NON_FINALIZED_NODES_V1;
/// Full-state fork policy sets the exact v1 candidate-tip cap.
pub const MAX_CANDIDATE_TIPS_V1: usize = MAX_NON_FINALIZED_CHAIN_FORKS;
/// Exact v1 maximum active retained-path references supplied to one transition.
pub const MAX_RETENTION_REFERENCES_V1: usize = MAX_STAGED_TARGETS_V1 + MAX_CANDIDATE_TIPS_V1;

/// Header-engine integration and finality mode.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum EngineMode {
    /// Only full state advances finality.
    Integrated,
    /// The engine turns a selected header 1,000 blocks deep into a disclosed local trust pin.
    HeadersOnly,
}

/// Exact trusted bootstrap header and its hash-qualified height.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrustedAnchor {
    /// Exact configured frontier.
    pub frontier: Frontier,
    /// Canonical anchor header, still subject to observable validation.
    pub header: Arc<block::Header>,
}

/// The local checkpoint map authenticates both height and hash.
/// The map rejects height-only and hash-only entries.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CheckpointSet(BTreeMap<block::Height, block::Hash>);

impl CheckpointSet {
    /// Construct a checkpoint set, rejecting conflicting duplicates.
    pub fn new(checkpoints: impl IntoIterator<Item = Frontier>) -> Result<Self, EngineConfigError> {
        let mut result = BTreeMap::new();
        for checkpoint in checkpoints {
            if result
                .insert(checkpoint.height, checkpoint.hash)
                .is_some_and(|old| old != checkpoint.hash)
            {
                return Err(EngineConfigError::ConflictingCheckpoint(checkpoint.height));
            }
        }
        Ok(Self(result))
    }

    /// Return the configured hash at `height`.
    pub fn hash(&self, height: block::Height) -> Option<block::Hash> {
        self.0.get(&height).copied()
    }

    /// Iterate checkpoints in ascending height order.
    pub fn iter(&self) -> impl Iterator<Item = Frontier> + '_ {
        self.0
            .iter()
            .map(|(height, hash)| Frontier::new(*height, *hash))
    }
}

/// One release-authenticated settled network-upgrade pin.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct SettledUpgradePin {
    /// Production network identity.
    pub network: NetworkKind,
    /// Settled upgrade identity.
    pub upgrade: NetworkUpgrade,
    /// Exact activation frontier.
    pub activation: Frontier,
}

/// This release compiles immutable settled pins.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SettledUpgradeManifest {
    pins: Vec<SettledUpgradePin>,
    digest: [u8; 32],
}

impl SettledUpgradeManifest {
    /// Construct and validate the exact specification-v1.3 release manifest.
    pub fn for_release() -> Result<Self, EngineConfigError> {
        let pins = vec![
            SettledUpgradePin {
                network: NetworkKind::Mainnet,
                upgrade: NetworkUpgrade::Nu6_2,
                activation: Frontier::new(
                    block::Height(3_364_600),
                    block::Hash::from_str(
                        "0000000000806344c408a4cfdf472f4132c632edbdc24cf2f3f672061da8b865",
                    )
                    .map_err(|_| EngineConfigError::MalformedSettledPin(NetworkKind::Mainnet))?,
                ),
            },
            SettledUpgradePin {
                network: NetworkKind::Testnet,
                upgrade: NetworkUpgrade::Nu6_2,
                activation: Frontier::new(
                    block::Height(4_052_000),
                    block::Hash::from_str(
                        "0010cb912b0188da5bc055ee67e3f77d30cd27611369d865974a5bf0b1ec2912",
                    )
                    .map_err(|_| EngineConfigError::MalformedSettledPin(NetworkKind::Testnet))?,
                ),
            },
        ];
        Self::new(pins)
    }

    fn new(mut pins: Vec<SettledUpgradePin>) -> Result<Self, EngineConfigError> {
        pins.sort_unstable_by_key(|pin| match pin.network {
            NetworkKind::Mainnet => 0_u8,
            NetworkKind::Testnet => 1_u8,
            NetworkKind::Regtest => 2_u8,
        });
        if pins.iter().any(|pin| pin.network == NetworkKind::Regtest) {
            return Err(EngineConfigError::InvalidSettledNetwork);
        }
        if pins
            .windows(2)
            .any(|pair| pair[0].network == pair[1].network)
        {
            return Err(EngineConfigError::DuplicateSettledPin);
        }
        let mut hasher = Sha256::new();
        hasher.update(b"zakura-settled-upgrade-manifest-v1");
        for pin in &pins {
            hasher.update(match pin.network {
                NetworkKind::Mainnet => b"mainnet".as_slice(),
                NetworkKind::Testnet => b"testnet".as_slice(),
                NetworkKind::Regtest => b"regtest".as_slice(),
            });
            hasher.update(b"nu6.2");
            hasher.update(pin.activation.height.0.to_le_bytes());
            hasher.update(pin.activation.hash.0);
        }
        Ok(Self {
            pins,
            digest: hasher.finalize().into(),
        })
    }

    /// Return the immutable manifest digest stored with engine metadata.
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    /// Return a production network's mandatory pin or `None` for a custom network.
    pub fn pin_for_network(&self, network: &Network) -> Option<SettledUpgradePin> {
        let production_kind = match network {
            Network::Mainnet => Some(NetworkKind::Mainnet),
            Network::Testnet(_) if network.is_default_testnet() => Some(NetworkKind::Testnet),
            Network::Testnet(_) => None,
        }?;
        self.pins
            .iter()
            .find(|pin| pin.network == production_kind)
            .copied()
    }

    /// Iterate every release-authenticated production pin.
    pub fn iter(&self) -> impl Iterator<Item = SettledUpgradePin> + '_ {
        self.pins.iter().copied()
    }
}

/// Immutable pure-engine configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineConfig {
    /// Finality authority mode.
    pub mode: EngineMode,
    /// Authenticated network parameters.
    pub network: Network,
    /// Exact trusted bootstrap anchor.
    bootstrap_anchor: TrustedAnchor,
    /// Optional authenticated local checkpoints.
    local_checkpoints: CheckpointSet,
    /// Mandatory release-authenticated settled pins.
    settled_manifest: SettledUpgradeManifest,
    /// Frozen engine resource limits.
    pub limits: EngineLimits,
    /// Cached digest of the immutable trust-anchor fields.
    trust_anchor_digest: [u8; 32],
    /// Transition verification uses these cached, sorted, immutable trust pins.
    trust_pins: Arc<[Frontier]>,
}

impl EngineConfig {
    /// Construct a configuration with the mandatory compiled settled manifest.
    pub fn new(
        mode: EngineMode,
        network: Network,
        bootstrap_anchor: TrustedAnchor,
        local_checkpoints: CheckpointSet,
    ) -> Result<Self, EngineConfigError> {
        let actual_anchor = crate::validation::validate_trusted_anchor_observables(
            &bootstrap_anchor.header,
            &network,
            bootstrap_anchor.frontier.height,
        )
        .map_err(EngineConfigError::InvalidTrustedAnchor)?;
        if actual_anchor != bootstrap_anchor.frontier.hash {
            return Err(EngineConfigError::AnchorHashMismatch {
                expected: bootstrap_anchor.frontier.hash,
                actual: actual_anchor,
            });
        }
        let settled_manifest = SettledUpgradeManifest::for_release()?;
        if matches!(network, Network::Mainnet) || network.is_default_testnet() {
            settled_manifest
                .pin_for_network(&network)
                .ok_or(EngineConfigError::MissingSettledPin(network.kind()))?;
        }
        validate_trust_pin_consistency(
            &settled_manifest,
            &network,
            bootstrap_anchor.frontier,
            &local_checkpoints,
        )?;
        let trust_anchor_digest =
            trust_anchor_digest(&settled_manifest, &bootstrap_anchor, &local_checkpoints);
        let trust_pins = trust_pins(&settled_manifest, &network, &local_checkpoints);
        Ok(Self {
            mode,
            network,
            bootstrap_anchor,
            local_checkpoints,
            settled_manifest,
            limits: EngineLimits::v1(),
            trust_anchor_digest,
            trust_pins,
        })
    }

    /// Return the exact trusted bootstrap anchor.
    pub const fn bootstrap_anchor(&self) -> &TrustedAnchor {
        &self.bootstrap_anchor
    }

    /// Return the authenticated local checkpoint set.
    pub const fn local_checkpoints(&self) -> &CheckpointSet {
        &self.local_checkpoints
    }

    /// Return the mandatory release-authenticated settled pins.
    pub const fn settled_manifest(&self) -> &SettledUpgradeManifest {
        &self.settled_manifest
    }

    /// Digest binding every absolute trust anchor used by validation and startup.
    pub const fn trust_anchor_digest(&self) -> [u8; 32] {
        self.trust_anchor_digest
    }

    /// This method returns the cached trust pins for transition verification.
    pub(crate) fn trust_pins(&self) -> Arc<[Frontier]> {
        self.trust_pins.clone()
    }

    #[cfg(test)]
    pub(crate) fn replace_local_checkpoints(&mut self, local_checkpoints: CheckpointSet) {
        self.local_checkpoints = local_checkpoints;
        self.trust_anchor_digest = trust_anchor_digest(
            &self.settled_manifest,
            &self.bootstrap_anchor,
            &self.local_checkpoints,
        );
        self.trust_pins = trust_pins(
            &self.settled_manifest,
            &self.network,
            &self.local_checkpoints,
        );
    }
}

fn trust_anchor_digest(
    settled_manifest: &SettledUpgradeManifest,
    bootstrap_anchor: &TrustedAnchor,
    local_checkpoints: &CheckpointSet,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-header-chain-trust-anchors-v1");
    hasher.update(settled_manifest.digest());
    hasher.update(bootstrap_anchor.frontier.height.0.to_le_bytes());
    hasher.update(bootstrap_anchor.frontier.hash.0);
    for checkpoint in local_checkpoints.iter() {
        hasher.update(checkpoint.height.0.to_le_bytes());
        hasher.update(checkpoint.hash.0);
    }
    hasher.finalize().into()
}

fn trust_pins(
    settled_manifest: &SettledUpgradeManifest,
    network: &Network,
    local_checkpoints: &CheckpointSet,
) -> Arc<[Frontier]> {
    let mut pins: BTreeMap<_, _> = local_checkpoints
        .iter()
        .map(|pin| (pin.height, pin.hash))
        .collect();
    if let Some(pin) = settled_manifest.pin_for_network(network) {
        pins.entry(pin.activation.height)
            .or_insert(pin.activation.hash);
    }
    pins.into_iter()
        .map(|(height, hash)| Frontier::new(height, hash))
        .collect::<Vec<_>>()
        .into()
}

/// Ensure independent trust sources agree whenever they pin the same height.
///
/// Exact duplicate frontiers are valid. Different hashes at one height are
/// rejected because no canonical chain can satisfy both trust requirements.
fn validate_trust_pin_consistency(
    settled_manifest: &SettledUpgradeManifest,
    network: &Network,
    bootstrap_anchor: Frontier,
    local_checkpoints: &CheckpointSet,
) -> Result<(), EngineConfigError> {
    let settled = settled_manifest
        .pin_for_network(network)
        .into_iter()
        .map(|pin| pin.activation);
    let mut pins_by_height = BTreeMap::new();
    // Source order does not express precedence: the bootstrap anchor, applicable
    // settled pin, and local checkpoints must all agree at overlapping heights.
    for pin in std::iter::once(bootstrap_anchor)
        .chain(settled)
        .chain(local_checkpoints.iter())
    {
        if pins_by_height
            .insert(pin.height, pin.hash)
            .is_some_and(|expected| expected != pin.hash)
        {
            return Err(EngineConfigError::ConflictingTrustPin(pin.height));
        }
    }
    Ok(())
}

/// Invalid immutable engine or trust-anchor configuration.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum EngineConfigError {
    /// A supplied trusted header failed a directly observable validation rule.
    #[error("trusted anchor failed {0}")]
    InvalidTrustedAnchor(&'static str),
    /// The canonical trusted header did not match its configured hash.
    #[error("trusted anchor header hashes to {actual:?}, expected {expected:?}")]
    AnchorHashMismatch {
        /// Configured hash.
        expected: block::Hash,
        /// Locally computed hash.
        actual: block::Hash,
    },
    /// Two local checkpoints name different hashes at one height.
    #[error("conflicting local checkpoint at {0:?}")]
    ConflictingCheckpoint(block::Height),
    /// Two independently authenticated trust sources name different hashes at one height.
    #[error("conflicting trust pin at {0:?}")]
    ConflictingTrustPin(block::Height),
    /// A compiled settled hash failed canonical parsing.
    #[error("malformed compiled settled pin for {0:?}")]
    MalformedSettledPin(NetworkKind),
    /// A manifest contains more than one pin for a production identity.
    #[error("duplicate settled-upgrade production identity")]
    DuplicateSettledPin,
    /// Settled production pins cannot use the Regtest identity.
    #[error("settled-upgrade manifest cannot contain a Regtest pin")]
    InvalidSettledNetwork,
    /// A production configuration has no mandatory settled pin.
    #[error("missing mandatory settled pin for {0:?}")]
    MissingSettledPin(NetworkKind),
}

/// Immutable resource bounds for one header-chain engine version.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct EngineLimits {
    /// Irreversible local finality depth.
    pub local_finality_depth: NonZeroU32,
    /// Maximum retained eligible and ineligible candidate tips.
    pub max_candidate_tips: NonZeroUsize,
    /// Maximum retained non-finalized DAG nodes.
    pub max_non_finalized_nodes: NonZeroUsize,
    /// Maximum prepared headers accepted before any batch-proportional work.
    pub max_headers_per_transition: NonZeroUsize,
    /// Maximum fixed-size auxiliary records retained for one header.
    pub max_aux_deliveries_per_header: NonZeroUsize,
    /// Maximum fixed-size auxiliary records retained across the graph.
    pub max_aux_deliveries_total: NonZeroUsize,
    /// Maximum active retained-path references admitted by one transition.
    pub max_retention_references: NonZeroUsize,
}

impl EngineLimits {
    /// Return the exact limits frozen by specification version 1.3.
    pub fn v1() -> Self {
        Self {
            local_finality_depth: NonZeroU32::new(MAX_BLOCK_REORG_HEIGHT)
                .expect("the v1 local finality depth is nonzero"),
            max_candidate_tips: NonZeroUsize::new(MAX_CANDIDATE_TIPS_V1)
                .expect("the v1 candidate-tip limit is nonzero"),
            max_non_finalized_nodes: NonZeroUsize::new(MAX_NON_FINALIZED_NODES_V1)
                .expect("the v1 node limit is nonzero"),
            max_headers_per_transition: NonZeroUsize::new(MAX_HEADERS_PER_TRANSITION_V1)
                .expect("the v1 per-transition header limit is nonzero"),
            max_aux_deliveries_per_header: NonZeroUsize::new(MAX_AUX_DELIVERIES_PER_HEADER_V1)
                .expect("the v1 per-header auxiliary limit is nonzero"),
            max_aux_deliveries_total: NonZeroUsize::new(MAX_AUX_DELIVERIES_TOTAL_V1)
                .expect("the v1 aggregate auxiliary limit is nonzero"),
            max_retention_references: NonZeroUsize::new(MAX_RETENTION_REFERENCES_V1)
                .expect("the v1 retained-path reference limit is nonzero"),
        }
    }
}

impl Default for EngineLimits {
    fn default() -> Self {
        Self::v1()
    }
}

const _: () = assert!(MAX_BLOCK_REORG_HEIGHT == 1_000);
const _: () = assert!(MAX_CANDIDATE_TIPS_V1 == 10);
const _: () = assert!(MAX_NON_FINALIZED_NODES_V1 == 65_536);
const _: () = assert!(MAX_STAGED_TARGETS_V1 == 16);
const _: () = assert!(MAX_HEADERS_PER_TRANSITION_V1 == 4_000);
const _: () = assert!(MAX_AUX_DELIVERIES_PER_HEADER_V1 == 16);
const _: () = assert!(MAX_AUX_DELIVERIES_TOTAL_V1 == 65_536);
const _: () = assert!(MAX_RETENTION_REFERENCES_V1 == 26);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use zakura_chain::{
        block::{genesis::regtest_genesis_block, Block},
        parameters::testnet::RegtestParameters,
        serialization::ZcashDeserialize,
    };

    #[test]
    fn engine_limits_v1_match_the_frozen_specification() {
        let limits = EngineLimits::v1();
        assert_eq!(limits.local_finality_depth.get(), 1_000);
        assert_eq!(limits.max_candidate_tips.get(), 10);
        assert_eq!(limits.max_non_finalized_nodes.get(), 65_536);
        assert_eq!(
            limits.max_retention_references.get(),
            MAX_STAGED_TARGETS_V1 + limits.max_candidate_tips.get(),
            "one atomic transition can retain every active header target and full-state fork tip"
        );
    }

    #[test]
    fn release_manifest_pins_exact_v1_3_production_tuples() {
        let manifest = SettledUpgradeManifest::for_release().expect("compiled pins are valid");
        let pins: Vec<_> = manifest.iter().collect();
        assert_eq!(pins.len(), 2);

        let mainnet = manifest
            .pin_for_network(&Network::Mainnet)
            .expect("mainnet has a mandatory pin");
        assert_eq!(mainnet.upgrade, NetworkUpgrade::Nu6_2);
        assert_eq!(mainnet.activation.height, block::Height(3_364_600));
        assert_eq!(mainnet.activation.hash.0[0], 0x65);
        assert_eq!(mainnet.activation.hash.0[31], 0x00);
        assert_eq!(
            mainnet.activation.hash.to_string(),
            "0000000000806344c408a4cfdf472f4132c632edbdc24cf2f3f672061da8b865"
        );

        let testnet = manifest
            .pin_for_network(&Network::new_default_testnet())
            .expect("default testnet has a mandatory pin");
        assert_eq!(testnet.upgrade, NetworkUpgrade::Nu6_2);
        assert_eq!(testnet.activation.height, block::Height(4_052_000));
        assert_eq!(testnet.activation.hash.0[0], 0x12);
        assert_eq!(testnet.activation.hash.0[31], 0x00);
        assert_eq!(
            testnet.activation.hash.to_string(),
            "0010cb912b0188da5bc055ee67e3f77d30cd27611369d865974a5bf0b1ec2912"
        );

        let regtest = Network::new_regtest(RegtestParameters::default());
        assert_eq!(manifest.pin_for_network(&regtest), None);
        assert_eq!(
            manifest.digest(),
            SettledUpgradeManifest::for_release()
                .expect("compiled pins are deterministic")
                .digest()
        );
    }

    #[test]
    fn production_config_always_installs_the_release_manifest() {
        for (network, bytes) in [
            (
                Network::Mainnet,
                zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES.as_slice(),
            ),
            (
                Network::new_default_testnet(),
                zakura_test::vectors::BLOCK_TESTNET_GENESIS_BYTES.as_slice(),
            ),
        ] {
            let block = Arc::<Block>::zcash_deserialize(bytes)
                .expect("the production genesis vector is canonical");
            let config = EngineConfig::new(
                EngineMode::Integrated,
                network.clone(),
                TrustedAnchor {
                    frontier: Frontier::new(block::Height(0), block.hash()),
                    header: block.header.clone(),
                },
                CheckpointSet::default(),
            )
            .expect("the production genesis anchor passes every direct check");
            assert!(config.settled_manifest.pin_for_network(&network).is_some());
        }
    }

    #[test]
    fn engine_config_rejects_conflicting_trust_sources() {
        let regtest_block = regtest_genesis_block();
        let regtest_network = Network::new_regtest(RegtestParameters::default());
        let regtest_anchor = TrustedAnchor {
            frontier: Frontier::new(block::Height(0), regtest_block.hash()),
            header: regtest_block.header.clone(),
        };
        EngineConfig::new(
            EngineMode::HeadersOnly,
            regtest_network.clone(),
            regtest_anchor.clone(),
            CheckpointSet::new([regtest_anchor.frontier])
                .expect("the matching bootstrap checkpoint is unique"),
        )
        .expect("identical bootstrap and local trust pins agree");
        assert_eq!(
            EngineConfig::new(
                EngineMode::HeadersOnly,
                regtest_network,
                regtest_anchor,
                CheckpointSet::new([Frontier::new(block::Height(0), block::Hash([9; 32]))])
                    .expect("the conflicting bootstrap checkpoint is unique"),
            ),
            Err(EngineConfigError::ConflictingTrustPin(block::Height(0)))
        );

        let mainnet_block = Arc::<Block>::zcash_deserialize(
            zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES.as_slice(),
        )
        .expect("the mainnet genesis vector is canonical");
        let mainnet_anchor = TrustedAnchor {
            frontier: Frontier::new(block::Height(0), mainnet_block.hash()),
            header: mainnet_block.header.clone(),
        };
        let manifest = SettledUpgradeManifest::for_release().expect("compiled pins are valid");
        let settled = manifest
            .pin_for_network(&Network::Mainnet)
            .expect("mainnet has a settled pin")
            .activation;
        let matching = EngineConfig::new(
            EngineMode::Integrated,
            Network::Mainnet,
            mainnet_anchor.clone(),
            CheckpointSet::new([settled]).expect("the matching settled checkpoint is unique"),
        )
        .expect("identical settled and local trust pins agree");
        assert_eq!(
            matching
                .trust_pins()
                .iter()
                .filter(|pin| pin.height == settled.height)
                .count(),
            1,
            "the effective trust pins contain one hash per height"
        );

        let conflicting_hash = block::Hash([0x5c; 32]);
        assert_ne!(conflicting_hash, settled.hash);
        assert_eq!(
            EngineConfig::new(
                EngineMode::Integrated,
                Network::Mainnet,
                mainnet_anchor,
                CheckpointSet::new([Frontier::new(settled.height, conflicting_hash)])
                    .expect("the conflicting settled checkpoint is unique"),
            ),
            Err(EngineConfigError::ConflictingTrustPin(settled.height))
        );

        validate_trust_pin_consistency(
            &manifest,
            &Network::Mainnet,
            settled,
            &CheckpointSet::default(),
        )
        .expect("identical bootstrap and settled trust pins agree");
        assert_eq!(
            validate_trust_pin_consistency(
                &manifest,
                &Network::Mainnet,
                Frontier::new(settled.height, conflicting_hash),
                &CheckpointSet::default(),
            ),
            Err(EngineConfigError::ConflictingTrustPin(settled.height))
        );
    }

    #[test]
    fn trusted_anchor_still_runs_every_directly_observable_check() {
        let sapling =
            Arc::<Block>::zcash_deserialize(zakura_test::vectors::MAINNET_BLOCKS[&419_200])
                .expect("the Mainnet Sapling activation vector is canonical");
        let make_config = |network: Network, height: block::Height, header: Arc<block::Header>| {
            let frontier_hash =
                crate::validate_encoding_version_hash(&header).unwrap_or(block::Hash([0; 32]));
            EngineConfig::new(
                EngineMode::Integrated,
                network,
                TrustedAnchor {
                    frontier: Frontier::new(height, frontier_hash),
                    header,
                },
                CheckpointSet::default(),
            )
        };
        make_config(
            Network::Mainnet,
            block::Height(419_200),
            sapling.header.clone(),
        )
        .expect("the real production activation anchor passes every direct check");

        let mut bad_version = *sapling.header;
        bad_version.version = 3;
        assert_eq!(
            make_config(
                Network::Mainnet,
                block::Height(419_200),
                Arc::new(bad_version)
            ),
            Err(EngineConfigError::InvalidTrustedAnchor(
                "canonical header version and hash"
            ))
        );

        let mut bad_commitment = *sapling.header;
        bad_commitment.commitment_bytes.0 = [0xff; 32];
        assert_eq!(
            make_config(
                Network::Mainnet,
                block::Height(419_200),
                Arc::new(bad_commitment)
            ),
            Err(EngineConfigError::InvalidTrustedAnchor(
                "height-dependent commitment structure"
            ))
        );

        let mut bad_target = *sapling.header;
        bad_target.difficulty_threshold =
            zakura_chain::work::difficulty::CompactDifficulty::from_le_bytes([0; 4]);
        assert_eq!(
            make_config(
                Network::Mainnet,
                block::Height(419_200),
                Arc::new(bad_target)
            ),
            Err(EngineConfigError::InvalidTrustedAnchor(
                "compact target and network limit"
            ))
        );

        let target = crate::validate_compact_target(&sapling.header, &Network::Mainnet)
            .expect("the vector target is valid");
        let mut bad_hash = *sapling.header;
        bad_hash.nonce.0[0] = bad_hash.nonce.0[0].wrapping_add(1);
        assert!(
            crate::validate_hash_filter(bad_hash.hash(), target).is_err(),
            "the deterministic nonce mutation no longer satisfies production work"
        );
        assert_eq!(
            make_config(Network::Mainnet, block::Height(419_200), Arc::new(bad_hash)),
            Err(EngineConfigError::InvalidTrustedAnchor(
                "header hash filter"
            ))
        );

        let regtest = Network::new_regtest(RegtestParameters::default());
        let mut wrong_solution_shape = *regtest_genesis_block().header;
        wrong_solution_shape.solution = zakura_chain::work::equihash::Solution::for_proposal();
        assert_eq!(
            make_config(regtest, block::Height(0), Arc::new(wrong_solution_shape)),
            Err(EngineConfigError::InvalidTrustedAnchor(
                "Equihash solution shape or proof"
            ))
        );
    }

    #[test]
    fn engine_config_binds_and_validates_every_trust_anchor() {
        let block = regtest_genesis_block();
        let network = Network::new_regtest(RegtestParameters::default());
        let anchor = TrustedAnchor {
            frontier: Frontier::new(block::Height(0), block.hash()),
            header: block.header.clone(),
        };
        let plain = EngineConfig::new(
            EngineMode::HeadersOnly,
            network.clone(),
            anchor.clone(),
            CheckpointSet::default(),
        )
        .expect("the fixture anchor is canonical");
        let checkpointed = EngineConfig::new(
            EngineMode::HeadersOnly,
            network.clone(),
            anchor.clone(),
            CheckpointSet::new([Frontier::new(block::Height(10), block::Hash([9; 32]))])
                .expect("the fixture checkpoint set has unique heights"),
        )
        .expect("the fixture checkpoint is hash-qualified");
        assert_ne!(
            plain.trust_anchor_digest(),
            checkpointed.trust_anchor_digest()
        );
        assert_eq!(
            checkpointed.trust_anchor_digest(),
            trust_anchor_digest(
                checkpointed.settled_manifest(),
                checkpointed.bootstrap_anchor(),
                checkpointed.local_checkpoints(),
            ),
            "the cached digest must preserve the canonical trust-anchor transcript",
        );

        let mut replaced = plain.clone();
        replaced.replace_local_checkpoints(checkpointed.local_checkpoints().clone());
        assert_eq!(
            replaced.trust_anchor_digest(),
            checkpointed.trust_anchor_digest(),
            "test-only checkpoint replacement must refresh the cached digest",
        );
        assert_eq!(
            replaced.trust_pins().as_ref(),
            checkpointed.trust_pins().as_ref()
        );
        assert!(
            !Arc::ptr_eq(&plain.trust_pins(), &replaced.trust_pins()),
            "test-only checkpoint replacement must refresh the cached pins"
        );
        assert!(
            Arc::ptr_eq(&checkpointed.trust_pins(), &checkpointed.trust_pins()),
            "successive plans can share the immutable trust-pin allocation"
        );

        let mismatched = TrustedAnchor {
            frontier: Frontier::new(block::Height(0), block::Hash([1; 32])),
            ..anchor
        };
        assert!(matches!(
            EngineConfig::new(
                EngineMode::HeadersOnly,
                network,
                mismatched,
                CheckpointSet::default()
            ),
            Err(EngineConfigError::AnchorHashMismatch { .. })
        ));
    }
}
