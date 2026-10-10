//! Historical inputs for client-side Tachyon proof construction.

use std::sync::Arc;

use zakura_chain::{block::Height, parameters::NetworkUpgrade, tachyon};

use crate::{
    service::{finalized_state::ZakuraDb, non_finalized_state::Chain},
    BoxError, HashOrHeight, TachyonBlock,
};

/// Reads one block and its anchors from one selected chain. Appending finalized
/// blocks does not change these height-bounded lookups; a concurrent body prune
/// fails explicitly instead of producing an incomplete proof input.
pub(crate) fn block(
    chain: Option<Arc<Chain>>,
    db: &ZakuraDb,
    hash_or_height: HashOrHeight,
) -> Result<Option<TachyonBlock>, BoxError> {
    let Some(height) =
        hash_or_height.height_or_else(|hash| super::height_by_hash(chain.as_ref(), db, hash))
    else {
        return Ok(None);
    };
    let Some(hash) = super::hash_by_height(chain.as_ref(), db, height) else {
        return Ok(None);
    };
    if matches!(hash_or_height, HashOrHeight::Hash(requested) if requested != hash) {
        return Ok(None);
    }

    let activation_height = NetworkUpgrade::NuTachyon
        .activation_height(&db.network())
        .filter(|activation| height >= *activation)
        .ok_or("NuTachyon is not active at the requested block")?;
    let block = super::block(chain.as_ref(), db, hash.into())
        .ok_or("Tachyon block data unavailable: body pruned or not retained; use an archive node or previously saved sync data")?;

    let anchor_at = |height| {
        chain
            .as_ref()
            .and_then(|chain| {
                chain
                    .tachyon_anchors_by_height
                    .range(..=height)
                    .next_back()
                    .map(|(_, anchor)| *anchor)
            })
            .or_else(|| db.tachyon_anchor_by_height(height))
            .ok_or_else(|| {
                BoxError::from("Tachyon anchor history unavailable at the requested block")
            })
    };
    let anchor_before = if height == activation_height {
        tachyon::Anchor::from(zcash_tachyon::Anchor::default())
    } else {
        anchor_at(Height(
            height
                .0
                .checked_sub(1)
                .expect("active non-initial block has a parent"),
        ))?
    };

    let finalized = db.tip().is_some_and(|(tip, _)| height <= tip);
    if finalized && db.hash(height) != Some(hash) {
        return Err("selected chain changed during Tachyon read; retry the request".into());
    }

    Ok(Some(TachyonBlock {
        block,
        height,
        activation_height,
        finalized,
        anchor_before,
        anchor_after: anchor_at(height)?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        request::{CheckpointVerifiedBlock, FinalizedBlock, Treestate},
        service::finalized_state::{DiskWriteBatch, FinalizedState},
        tests::FakeChainHelper,
        Config, ContextuallyVerifiedBlock,
    };
    use zakura_chain::{
        block::{genesis::regtest_genesis_block, ChainHistoryBlockTxAuthCommitmentHash},
        parameters::{testnet::ConfiguredActivationHeights, Network},
    };

    #[test]
    fn tachyon_sync_reads_sparse_historical_anchors_and_reports_pruned_bodies() {
        let _guard = zakura_test::init();
        let network = Network::new_regtest(
            ConfiguredActivationHeights {
                nu5: Some(1),
                nu_tachyon: Some(2),
                ..Default::default()
            }
            .into(),
        );
        let mut state = FinalizedState::new(&Config::ephemeral(), &network).unwrap();
        let initial = tachyon::Anchor::from(zcash_tachyon::Anchor::default());

        let mut parent = regtest_genesis_block();
        state
            .commit_finalized_direct(parent.clone().into(), None, None, "Tachyon sync tests")
            .unwrap();
        // Empty regtest blocks have no stamps, so the sparse anchor index has
        // just the activation entry. Preserve the real history commitments.
        for _ in 1..=4 {
            let child = parent.make_fake_child();
            let commitment = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
                &state.db.history_tree().hash().unwrap_or([0; 32].into()),
                &child.auth_data_root(),
            );
            parent = child.set_block_commitment(commitment.into());
            state
                .commit_finalized_direct(parent.clone().into(), None, None, "Tachyon sync tests")
                .unwrap();
        }

        assert!(block(None, &state.db, Height(1).into())
            .unwrap_err()
            .to_string()
            .contains("not active"));
        assert!(block(None, &state.db, Height(5).into()).unwrap().is_none());
        assert!(block(
            None,
            &state.db,
            zakura_chain::block::Hash([0xff; 32]).into()
        )
        .unwrap()
        .is_none());
        let at_activation = block(None, &state.db, Height(2).into()).unwrap().unwrap();
        assert_eq!(at_activation.anchor_before, initial);
        assert_eq!(at_activation.anchor_after, initial);
        let historical = block(None, &state.db, Height(3).into()).unwrap().unwrap();
        assert_eq!(historical.anchor_before, initial);
        assert_eq!(historical.anchor_after, initial);
        assert!(historical.finalized);
        assert_eq!(
            block(None, &state.db, historical.block.hash().into()).unwrap(),
            Some(historical)
        );

        // A live response uses the selected chain, not a competing sibling.
        let siblings = parent.make_fake_child().make_fake_siblings(2);
        let mut chain = Chain::new(
            &network,
            Height(4),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            initial,
            state.db.history_tree(),
            Default::default(),
        );
        let child = Arc::new(
            ContextuallyVerifiedBlock::with_block_and_spent_utxos(
                &network,
                siblings[0].clone().into(),
                Default::default(),
            )
            .unwrap(),
        );
        chain.height_by_hash.insert(child.hash, child.height);
        chain.blocks.insert(child.height, child);
        let chain = Arc::new(chain);
        let live = block(Some(chain.clone()), &state.db, Height(5).into())
            .unwrap()
            .unwrap();
        assert_eq!(live.block.hash(), siblings[0].hash());
        assert_eq!(live.anchor_before, initial);
        assert_eq!(live.anchor_after, initial);
        assert!(!live.finalized);
        assert!(block(Some(chain), &state.db, siblings[1].hash().into())
            .unwrap()
            .is_none());

        // Expiry removes the anchor-validity index, not historical sync inputs.
        let mut epoch_boundary = FinalizedBlock::from_checkpoint_verified(
            CheckpointVerifiedBlock::from(parent),
            Treestate::default(),
        );
        epoch_boundary.height = Height(2 + 2 * tachyon::EPOCH_LENGTH);
        let mut batch = DiskWriteBatch::new();
        batch.prepare_tachyon_tachygram_batch(&state.db, &epoch_boundary);
        state.db.write_batch(batch).unwrap();
        assert_eq!(state.db.tachyon_anchor_height(&initial), None);
        assert_eq!(
            block(None, &state.db, Height(3).into())
                .unwrap()
                .unwrap()
                .anchor_after,
            initial
        );

        let mut batch = DiskWriteBatch::new();
        batch.prepare_prune_batch(&state.db, Height(1), Height(4));
        state.db.write_batch(batch).unwrap();
        assert!(block(None, &state.db, Height(3).into())
            .unwrap_err()
            .to_string()
            .contains("pruned"));
        let retained = block(None, &state.db, Height(4).into()).unwrap().unwrap();
        assert_eq!(
            retained.anchor_before, initial,
            "pruning the parent body must not lose its anchor"
        );
    }
}
