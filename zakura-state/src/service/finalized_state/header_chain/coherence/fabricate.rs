//! Deterministic real-difficulty fork universe used by the coherence harness.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use zakura_chain::{
    block::{self, Block, Height},
    parameters::{testnet, Network, Network::Mainnet, NetworkUpgrade},
    serialization::ZcashDeserializeInto,
    work::difficulty::{CompactDifficulty, ParameterDifficulty, U256},
};
use zakura_header_chain::{AdjustedDifficulty, POW_ADJUSTMENT_BLOCK_SPAN};
use zakura_test::vectors::MAINNET_BLOCKS;

type DifficultyContext = Vec<(CompactDifficulty, DateTime<Utc>)>;

#[derive(Copy, Clone, Debug)]
enum Spacing {
    Fast,
    Slow,
}

impl Spacing {
    fn duration(self, network: &Network, height: Height) -> Duration {
        let target = NetworkUpgrade::target_spacing_for_height(network, height);
        let duration = match self {
            Self::Fast => target / 16,
            Self::Slow => target * 4,
        };
        Duration::seconds(duration.num_seconds().max(1))
    }
}

#[derive(Clone, Debug)]
pub(super) struct FabHeader {
    pub height: Height,
    pub hash: block::Hash,
    pub header: Arc<block::Header>,
}

impl FabHeader {
    pub fn work(&self) -> zakura_chain::work::difficulty::Work {
        self.header
            .difficulty_threshold
            .to_work()
            .expect("fabricated compact targets always have exact work")
    }
}

fn fabricate_headers(
    network: &Network,
    anchor: (Height, block::Hash),
    mut context: DifficultyContext,
    spacings: &[Spacing],
    nonce_seed: u8,
) -> Vec<FabHeader> {
    let template = mainnet_block(1);
    let (mut previous_height, mut previous_hash) = anchor;
    let mut nonce_tag = nonce_seed;

    spacings
        .iter()
        .map(|spacing| {
            let height = previous_height
                .next()
                .expect("the bounded universe stays within the height range");
            let previous_time = context
                .first()
                .expect("the anchor difficulty context is nonempty")
                .1;
            let time = previous_time + spacing.duration(network, height);
            let difficulty = AdjustedDifficulty::new_from_header_time(
                time,
                previous_height,
                network,
                context.iter().copied(),
            )
            .expected_difficulty_threshold();

            let mut header = *template.header;
            header.previous_block_hash = previous_hash;
            header.time = time;
            header.difficulty_threshold = difficulty;
            header.nonce.0[0] = header.nonce.0[0].wrapping_add(nonce_tag);
            nonce_tag = nonce_tag.wrapping_add(1);
            let header = Arc::new(header);
            let hash = header.hash();

            previous_height = height;
            previous_hash = hash;
            context.insert(0, (header.difficulty_threshold, header.time));
            context.truncate(POW_ADJUSTMENT_BLOCK_SPAN);

            FabHeader {
                height,
                hash,
                header,
            }
        })
        .collect()
}

fn extend_context(mut context: DifficultyContext, headers: &[FabHeader]) -> DifficultyContext {
    for header in headers {
        context.insert(0, (header.header.difficulty_threshold, header.header.time));
    }
    context.truncate(POW_ADJUSTMENT_BLOCK_SPAN);
    context
}

fn total_work(headers: &[FabHeader]) -> U256 {
    headers.iter().fold(U256::zero(), |sum, header| {
        sum.checked_add(header.work().as_u256())
            .expect("the bounded universe work cannot overflow")
    })
}

#[derive(Clone, Debug)]
pub(super) struct BranchDef {
    pub fork_parent: (Height, block::Hash),
    pub headers: Vec<FabHeader>,
}

pub(super) const FORK_HEIGHT: u32 = 50;
pub(super) const TRUNK_LEN: usize = 60;

pub(super) struct Universe {
    pub network: Network,
    pub genesis: Arc<Block>,
    pub trunk: Vec<FabHeader>,
    pub branches: Vec<BranchDef>,
}

impl Universe {
    pub fn new() -> Self {
        let genesis = mainnet_block(0);
        let network = testnet::Parameters::build()
            .with_network_name("HeaderCoherenceTest")
            .expect("the test network name is valid")
            .with_genesis_hash(genesis.hash())
            .expect("the test genesis hash is valid")
            .with_target_difficulty_limit(Mainnet.target_difficulty_limit())
            .expect("the mainnet difficulty limit is valid")
            .with_activation_heights(testnet::ConfiguredActivationHeights {
                canopy: Some(1),
                ..Default::default()
            })
            .expect("the test activation heights are valid")
            .with_disable_pow(true)
            .clear_funding_streams()
            .clear_checkpoints()
            .expect("genesis-only checkpoints are valid")
            .to_network()
            .expect("the coherence test network is valid");
        let genesis_anchor = (Height(0), genesis.hash());
        let genesis_context = vec![(genesis.header.difficulty_threshold, genesis.header.time)];
        let trunk = fabricate_headers(
            &network,
            genesis_anchor,
            genesis_context.clone(),
            &[Spacing::Fast; TRUNK_LEN],
            0x10,
        );
        let fork_index = usize::try_from(FORK_HEIGHT).expect("the fork height fits in usize") - 1;
        let fork_parent = (trunk[fork_index].height, trunk[fork_index].hash);
        let fork_context = extend_context(genesis_context, &trunk[..=fork_index]);

        let branch_a = BranchDef {
            fork_parent,
            headers: fabricate_headers(
                &network,
                fork_parent,
                fork_context.clone(),
                &[Spacing::Fast; 26],
                0x40,
            ),
        };
        let branch_b = BranchDef {
            fork_parent,
            headers: fabricate_headers(
                &network,
                fork_parent,
                fork_context.clone(),
                &[Spacing::Slow; 30],
                0x80,
            ),
        };
        let b_prefix = branch_b.headers[..4].to_vec();
        let b_prefix_tip = (b_prefix[3].height, b_prefix[3].hash);
        let b_prefix_context = extend_context(fork_context.clone(), &b_prefix);
        let a_work = total_work(&branch_a.headers);
        let mut continuation_len = 1;
        let branch_b_ext = loop {
            assert!(
                continuation_len <= 64,
                "the fast continuation should out-work branch A"
            );
            let continuation = fabricate_headers(
                &network,
                b_prefix_tip,
                b_prefix_context.clone(),
                &vec![Spacing::Fast; continuation_len],
                0xc0,
            );
            let mut headers = b_prefix.clone();
            headers.extend(continuation);
            if total_work(&headers) > a_work {
                let continuation = fabricate_headers(
                    &network,
                    b_prefix_tip,
                    b_prefix_context.clone(),
                    &vec![Spacing::Fast; continuation_len + 2],
                    0xc0,
                );
                let mut headers = b_prefix;
                headers.extend(continuation);
                break BranchDef {
                    fork_parent,
                    headers,
                };
            }
            continuation_len += 1;
        };
        let c_parent = (branch_a.headers[1].height, branch_a.headers[1].hash);
        let c_context = extend_context(fork_context, &branch_a.headers[..2]);
        let branch_c = BranchDef {
            fork_parent: c_parent,
            headers: fabricate_headers(&network, c_parent, c_context, &[Spacing::Fast; 5], 0xe0),
        };

        assert!(branch_b.headers.len() > branch_a.headers.len());
        assert!(total_work(&branch_a.headers) > total_work(&branch_b.headers));
        assert!(total_work(&branch_b_ext.headers) > total_work(&branch_a.headers));
        assert_eq!(
            branch_b.headers[..4]
                .iter()
                .map(|header| header.hash)
                .collect::<Vec<_>>(),
            branch_b_ext.headers[..4]
                .iter()
                .map(|header| header.hash)
                .collect::<Vec<_>>()
        );

        Self {
            network,
            genesis,
            trunk,
            branches: vec![branch_a, branch_b, branch_b_ext, branch_c],
        }
    }

    pub fn trunk_at(&self, height: u32) -> &FabHeader {
        &self.trunk[usize::try_from(height).expect("the bounded height fits in usize") - 1]
    }
}

fn mainnet_block(height: u32) -> Arc<Block> {
    MAINNET_BLOCKS
        .get(&height)
        .expect("the requested mainnet test vector exists")
        .zcash_deserialize_into::<Arc<Block>>()
        .expect("the mainnet test vector deserializes")
}
