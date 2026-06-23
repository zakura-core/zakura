//! Contains code that interfaces with the zcash_note_encryption crate from
//! librustzcash.

use crate::{
    block::Height,
    parameters::{Network, NetworkUpgrade},
    transaction::Transaction,
};

/// Returns true if all Sapling, Orchard, or Ironwood outputs, if any, decrypt successfully with
/// an all-zeroes outgoing viewing key.
pub fn decrypts_successfully(tx: &Transaction, network: &Network, height: Height) -> bool {
    let nu = NetworkUpgrade::current(network, height);

    let Ok(tx) = tx.to_librustzcash(nu) else {
        return false;
    };

    let null_sapling_ovk = sapling_crypto::keys::OutgoingViewingKey([0u8; 32]);

    // Note that, since this function is used to validate coinbase transactions, we can ignore
    // the "grace period" mentioned in ZIP-212.
    let zip_212_enforcement = if nu >= NetworkUpgrade::Canopy {
        sapling_crypto::note_encryption::Zip212Enforcement::On
    } else {
        sapling_crypto::note_encryption::Zip212Enforcement::Off
    };

    if let Some(bundle) = tx.sapling_bundle() {
        for output in bundle.shielded_outputs().iter() {
            let recovery = sapling_crypto::note_encryption::try_sapling_output_recovery(
                &null_sapling_ovk,
                output,
                zip_212_enforcement,
            );
            if recovery.is_none() {
                return false;
            }
        }
    }

    let null_orchard_ovk = orchard::keys::OutgoingViewingKey::from([0u8; 32]);

    if let Some(bundle) = tx.orchard_bundle() {
        for act in bundle.actions() {
            let Some((coinbase_note, _, _)) = zcash_note_encryption::try_output_recovery_with_ovk(
                &orchard::note_encryption::OrchardDomain::for_action(act),
                &null_orchard_ovk,
                act,
                act.cv_net(),
                &act.encrypted_note().out_ciphertext,
            ) else {
                return false;
            };

            if !is_valid_orchard_coinbase_note_version(coinbase_note.version()) {
                return false;
            }
        }
    }

    #[cfg(zcash_unstable = "nu6.3")]
    if let Some(bundle) = tx.ironwood_bundle() {
        for act in bundle.actions() {
            let Some((coinbase_note, _, _)) = zcash_note_encryption::try_output_recovery_with_ovk(
                &orchard::note_encryption::OrchardDomain::for_action(act),
                &null_orchard_ovk,
                act,
                act.cv_net(),
                &act.encrypted_note().out_ciphertext,
            ) else {
                return false;
            };

            if !is_valid_ironwood_coinbase_note_version(coinbase_note.version()) {
                return false;
            }
        }
    }

    true
}

fn is_valid_orchard_coinbase_note_version(note_version: orchard::NoteVersion) -> bool {
    note_version == orchard::NoteVersion::V2
}

#[cfg(zcash_unstable = "nu6.3")]
fn is_valid_ironwood_coinbase_note_version(note_version: orchard::NoteVersion) -> bool {
    note_version == orchard::NoteVersion::V3
}

#[cfg(test)]
mod tests {
    use super::*;

    use group::ff::PrimeField;
    use halo2::pasta::pallas;
    use rand::SeedableRng;
    use rand_chacha::ChaCha20Rng;
    use zcash_protocol::value::{ZatBalance, Zatoshis};

    #[cfg(zcash_unstable = "nu6.3")]
    use crate::parameters::testnet::{ConfiguredActivationHeights, RegtestParameters};
    use crate::{
        amount::{Amount, NegativeAllowed},
        orchard::{
            self as zebra_orchard, AuthorizedAction, EncryptedNote, ShieldedData, ValueCommitment,
            WrappedNoteKey,
        },
        parameters::NetworkUpgrade,
        primitives::Halo2Proof,
        serialization::AtLeastOne,
        transaction::LockTime,
    };

    fn null_ovk() -> orchard::keys::OutgoingViewingKey {
        orchard::keys::OutgoingViewingKey::from([0u8; 32])
    }

    fn orchard_recipient() -> orchard::Address {
        let fvk = orchard::keys::FullViewingKey::from(
            &orchard::keys::SpendingKey::from_bytes([0; 32]).unwrap(),
        );
        fvk.address_at(0u32, orchard::keys::Scope::External)
    }

    fn mainnet_nu5_height() -> Height {
        NetworkUpgrade::Nu5
            .activation_height(&Network::Mainnet)
            .expect("NU5 is active on mainnet")
    }

    fn orchard_shielded_data(protocol: orchard::BundleProtocol) -> ShieldedData {
        let value = Zatoshis::const_from_u64(10_000);
        let mut builder = orchard::builder::Builder::new(
            protocol,
            orchard::builder::BundleType::Coinbase,
            orchard::Anchor::empty_tree(),
        );

        builder
            .add_output(
                Some(null_ovk()),
                orchard_recipient(),
                orchard::value::NoteValue::from_raw(value.into()),
                [0u8; 512],
            )
            .expect("can add an Orchard-style coinbase output");

        let (bundle, _) = builder
            .build::<ZatBalance>(&mut ChaCha20Rng::from_seed([0; 32]))
            .expect("can build an Orchard-style coinbase bundle")
            .expect("builder with one output creates a bundle");

        let actions = bundle
            .actions()
            .iter()
            .map(|action| {
                let encrypted_note = action.encrypted_note();

                AuthorizedAction {
                    action: zebra_orchard::Action {
                        cv: ValueCommitment::try_from(action.cv_net().to_bytes())
                            .expect("builder creates valid value commitments"),
                        nullifier: action
                            .nullifier()
                            .to_bytes()
                            .try_into()
                            .expect("builder creates valid nullifiers"),
                        rk: <[u8; 32]>::from(action.rk().clone()).into(),
                        cm_x: pallas::Base::from_repr(action.cmx().to_bytes())
                            .expect("builder creates valid note commitment x-coordinates"),
                        ephemeral_key: encrypted_note
                            .epk_bytes
                            .try_into()
                            .expect("builder creates valid ephemeral keys"),
                        enc_ciphertext: EncryptedNote::from(encrypted_note.enc_ciphertext),
                        out_ciphertext: WrappedNoteKey::from(encrypted_note.out_ciphertext),
                    },
                    spend_auth_sig: [0u8; 64].into(),
                }
            })
            .collect::<Vec<_>>();

        ShieldedData {
            flags: zebra_orchard::Flags::ENABLE_OUTPUTS,
            value_balance: Amount::<NegativeAllowed>::try_from(-10_000)
                .expect("test value is in range"),
            shared_anchor: zebra_orchard::tree::Root::default(),
            proof: Halo2Proof(vec![
                0u8;
                orchard::Proof::expected_proof_size(actions.len())
            ]),
            actions: AtLeastOne::try_from(actions).expect("test bundle has one action"),
            binding_sig: [0u8; 64].into(),
        }
    }

    fn orchard_coinbase_transaction() -> (Transaction, Height) {
        let height = mainnet_nu5_height();
        let orchard_shielded_data = orchard_shielded_data(orchard::BundleProtocol::OrchardPreNu6_3);

        (
            Transaction::V5 {
                network_upgrade: NetworkUpgrade::Nu5,
                lock_time: LockTime::unlocked(),
                expiry_height: height,
                inputs: Vec::new(),
                outputs: Vec::new(),
                sapling_shielded_data: None,
                orchard_shielded_data: Some(orchard_shielded_data),
            },
            height,
        )
    }

    #[test]
    fn orchard_coinbase_output_decrypts_with_v2_note() {
        let (transaction, height) = orchard_coinbase_transaction();

        assert!(decrypts_successfully(
            &transaction,
            &Network::Mainnet,
            height
        ));
    }

    #[test]
    #[cfg(zcash_unstable = "nu6.3")]
    fn ironwood_coinbase_output_rejects_v2_orchard_note() {
        let (transaction, height) = orchard_coinbase_transaction();
        let Transaction::V5 {
            lock_time,
            expiry_height,
            inputs,
            outputs,
            sapling_shielded_data,
            orchard_shielded_data: Some(orchard_shielded_data),
            ..
        } = transaction
        else {
            panic!("test transaction is V5 with Orchard shielded data");
        };
        let transaction = Transaction::V6 {
            network_upgrade: NetworkUpgrade::Nu6_3,
            lock_time,
            expiry_height,
            inputs,
            outputs,
            sapling_shielded_data,
            orchard_shielded_data: None,
            ironwood_shielded_data: Some(orchard_shielded_data),
        };
        let network = Network::new_regtest(RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                before_overwinter: Some(1),
                overwinter: Some(2),
                sapling: Some(3),
                blossom: Some(4),
                heartwood: Some(5),
                canopy: Some(6),
                nu5: Some(7),
                nu6: Some(8),
                nu6_1: Some(9),
                nu6_2: Some(10),
                nu6_3: Some(height.0),
                nu7: None,
            },
            ..Default::default()
        });

        assert!(!decrypts_successfully(&transaction, &network, height));
    }

    #[test]
    #[cfg(zcash_unstable = "nu6.3")]
    fn ironwood_coinbase_output_decrypts_with_v3_note() {
        let height = Height(11);
        let ironwood_shielded_data =
            orchard_shielded_data(orchard::BundleProtocol::IronwoodPostNu6_3);
        let activation_heights = ConfiguredActivationHeights {
            before_overwinter: Some(1),
            overwinter: Some(2),
            sapling: Some(3),
            blossom: Some(4),
            heartwood: Some(5),
            canopy: Some(6),
            nu5: Some(7),
            nu6: Some(8),
            nu6_1: Some(9),
            nu6_2: Some(10),
            nu6_3: Some(height.0),
            nu7: None,
        };
        let network = Network::new_regtest(RegtestParameters {
            activation_heights,
            ..Default::default()
        });
        let transaction = Transaction::V6 {
            network_upgrade: NetworkUpgrade::Nu6_3,
            lock_time: LockTime::unlocked(),
            expiry_height: height,
            inputs: Vec::new(),
            outputs: Vec::new(),
            sapling_shielded_data: None,
            orchard_shielded_data: None,
            ironwood_shielded_data: Some(ironwood_shielded_data),
        };

        assert!(decrypts_successfully(&transaction, &network, height));
    }
}
