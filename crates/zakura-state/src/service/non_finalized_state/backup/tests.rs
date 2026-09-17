//! Cross-build database upgrade test, driven by scripts/test-tachyon-db-upgrade.sh.

use std::{path::PathBuf, sync::Arc};

use zakura_chain::{block::Block, parameters::Network, serialization::ZcashDeserializeInto};

use crate::{
    constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
    service::finalized_state::{DatabaseWriterMetadata, FinalizedState},
    Config,
};

#[cfg(not(zcash_unstable = "nutachyon"))]
use super::write_semantically_verified_backup_block;
#[cfg(zcash_unstable = "nutachyon")]
use {super::read_non_finalized_blocks_from_backup, zakura_chain::block::Height};

#[test]
#[ignore = "requires an ordinary build followed by a Tachyon build on the same fixture"]
fn cross_build_database_upgrade() {
    let _init_guard = zakura_test::init();
    let config = Config {
        cache_dir: PathBuf::from(
            std::env::var_os("ZAKURA_DB_UPGRADE_FIXTURE").expect("fixture directory is set"),
        ),
        ephemeral: false,
        ..Config::default()
    };
    let network = Network::Mainnet;
    let backup_dir = config.non_finalized_state_backup_dir(&network).unwrap();
    let ordinary_path = config.db_path(STATE_DATABASE_KIND, 28, &network);
    let tachyon_path = config.db_path(STATE_DATABASE_KIND, 29, &network);
    let writer = DatabaseWriterMetadata::new(
        "Zakura",
        if cfg!(zcash_unstable = "nutachyon") {
            "tachyon-test"
        } else {
            "ordinary-test"
        },
        "",
    );
    let block = |height: u32| -> Arc<Block> {
        zakura_test::vectors::MAINNET_BLOCKS[&height]
            .zcash_deserialize_into()
            .unwrap()
    };

    #[cfg(not(zcash_unstable = "nutachyon"))]
    {
        assert!(
            !ordinary_path.exists(),
            "producer requires an unused fixture directory"
        );
        assert_eq!(state_database_format_version_in_code().major, 28);
        let mut state =
            FinalizedState::new_with_database_writer_metadata(&config, &network, writer.clone())
                .unwrap();
        for height in 0..10 {
            state
                .commit_finalized_direct(block(height).into(), None, None, "cross-build fixture")
                .unwrap();
        }
        write_semantically_verified_backup_block(&backup_dir, &block(10).into()).unwrap();
        assert_eq!(state.db.database_writer_metadata().unwrap(), Some(writer));
        assert_eq!(stored_balance_width(&state), 48);
        assert!(ordinary_path.exists());
        assert!(!tachyon_path.exists());
        state.db.shutdown(true);
    }

    #[cfg(zcash_unstable = "nutachyon")]
    {
        assert!(
            ordinary_path.exists(),
            "ordinary build must create the fixture first"
        );
        assert!(!tachyon_path.exists());
        let backup_before: Vec<_> = std::fs::read_dir(&backup_dir)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                let bytes = std::fs::read(&path).unwrap();
                (path, bytes)
            })
            .collect();
        assert_eq!(backup_before.len(), 1);
        {
            use crate::{service::finalized_state::STATE_COLUMN_FAMILIES_IN_CODE, ZakuraDb};
            let old = ZakuraDb::new(
                &config,
                STATE_DATABASE_KIND,
                &semver::Version::new(28, 2, 0),
                &network,
                true,
                STATE_COLUMN_FAMILIES_IN_CODE
                    .iter()
                    .map(ToString::to_string),
                true,
            )
            .unwrap();
            assert_eq!(
                old.database_writer_metadata().unwrap(),
                Some(DatabaseWriterMetadata::new("Zakura", "ordinary-test", ""))
            );
        }
        let mut state =
            FinalizedState::new_with_database_writer_metadata(&config, &network, writer.clone())
                .unwrap();
        assert_eq!(state_database_format_version_in_code().major, 29);
        assert!(
            !ordinary_path.exists(),
            "upgrade reuses the ordinary database"
        );
        assert!(tachyon_path.exists());
        assert_eq!(
            state.db.format_version_on_disk().unwrap(),
            Some(state_database_format_version_in_code())
        );
        assert_eq!(
            state.db.database_writer_metadata().unwrap(),
            Some(writer.clone())
        );
        assert_eq!(state.db.finalized_tip_height(), Some(Height(9)));
        for height in 0..10 {
            assert_eq!(
                state.db.block(Height(height).into()).unwrap(),
                block(height)
            );
            if height > 0 {
                assert!(state.db.block_info(Height(height).into()).is_some());
            }
        }
        assert_eq!(state.db.finalized_value_pool().tachyon_amount(), 0);
        let restored: Vec<_> =
            read_non_finalized_blocks_from_backup(&backup_dir, &state.db).collect();
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].block, block(10));
        for (path, bytes) in backup_before {
            assert_eq!(std::fs::read(path).unwrap(), bytes);
        }
        assert_eq!(stored_balance_width(&state), 48);
        state
            .commit_finalized_direct(block(10).into(), None, None, "post-upgrade write")
            .unwrap();
        assert_eq!(stored_balance_width(&state), 56);
        state.db.shutdown(true);
        drop(state);
        let mut reopened =
            FinalizedState::new_with_database_writer_metadata(&config, &network, writer.clone())
                .unwrap();
        assert_eq!(reopened.db.finalized_tip_height(), Some(Height(10)));
        assert_eq!(
            reopened.db.database_writer_metadata().unwrap(),
            Some(writer)
        );
        reopened.db.shutdown(true);
    }
}

fn stored_balance_width(state: &FinalizedState) -> usize {
    let db = state.db.header_chain_disk_db();
    let cf = db.cf_handle("tip_chain_value_pool").unwrap();
    db.raw_get_cf(&cf, &[]).unwrap().unwrap().len()
}
