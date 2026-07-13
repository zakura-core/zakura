//! Contains code that interfaces with the zcash_note_encryption crate from
//! librustzcash.

use crate::{
    block::Height,
    parameters::{Network, NetworkUpgrade},
    transaction::Transaction,
};

/// Returns true if all Sapling, Orchard, or Ironwood outputs, if any, decrypt
/// successfully with an all-zeroes outgoing viewing key.
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

    if let Some(bundle) = tx.orchard_bundle() {
        for act in bundle.actions() {
            if !orchard_action_decrypts_successfully(act) {
                return false;
            }
        }
    }

    if let Some(bundle) = tx.ironwood_bundle() {
        for act in bundle.actions() {
            if !ironwood_action_decrypts_successfully(act) {
                return false;
            }
        }
    }

    true
}

fn orchard_action_decrypts_successfully<A>(act: &orchard::Action<A>) -> bool {
    zcash_note_encryption::try_output_recovery_with_ovk(
        &orchard::note_encryption::OrchardDomain::for_action(act),
        &orchard::keys::OutgoingViewingKey::from([0u8; 32]),
        act,
        act.cv_net(),
        &act.encrypted_note().out_ciphertext,
    )
    .is_some()
}

fn ironwood_action_decrypts_successfully<A>(act: &orchard::Action<A>) -> bool {
    zcash_note_encryption::try_output_recovery_with_ovk(
        &orchard::note_encryption::IronwoodDomain::for_action(act),
        &orchard::keys::OutgoingViewingKey::from([0u8; 32]),
        act,
        act.cv_net(),
        &act.encrypted_note().out_ciphertext,
    )
    .is_some()
}

#[cfg(test)]
mod tests {
    use group::{ff::PrimeField, prime::PrimeCurveAffine, GroupEncoding};
    use halo2::pasta::pallas;
    use orchard::{
        note::{
            ExtractedNoteCommitment, NoteVersion, Nullifier, RandomSeed, TransmittedNoteCiphertext,
        },
        note_encryption::IronwoodNoteEncryption,
        primitives::redpallas::{SpendAuth, VerificationKey},
        value::{NoteValue, ValueCommitment},
        Action, Address, Note,
    };
    use rand_core::OsRng;
    use zcash_note_encryption::Domain;

    use crate::{
        at_least_one,
        block::Height,
        orchard as chain_orchard,
        parameters::{
            testnet::{ConfiguredActivationHeights, RegtestParameters},
            Network, NetworkUpgrade,
        },
        primitives::Halo2Proof,
        transaction::{arbitrary::v5_transactions, LockTime, Transaction},
        transparent,
    };

    #[test]
    fn orchard_and_ironwood_domains_accept_only_their_plaintext_versions() {
        let vector = zakura_test::vectors::ORCHARD_NOTE_ENCRYPTION_ZERO_VECTOR
            .first()
            .expect("test vectors are non-empty");
        let v2_action = v2_action_from_test_vector(vector);
        let v3_action = v3_action_from_test_vector(vector);

        for (domain_name, plaintext_lead_byte, action, expected) in [
            ("Orchard", 0x02, &v2_action, true),
            ("Orchard", 0x03, &v3_action, false),
            ("Ironwood", 0x02, &v2_action, false),
            ("Ironwood", 0x03, &v3_action, true),
        ] {
            let actual = match domain_name {
                "Orchard" => super::orchard_action_decrypts_successfully(action),
                "Ironwood" => super::ironwood_action_decrypts_successfully(action),
                _ => unreachable!("test cases are exhaustive"),
            };

            assert_eq!(
                actual, expected,
                "{domain_name} domain result for note plaintext {plaintext_lead_byte:#04x}"
            );
        }
    }

    #[test]
    fn decrypts_successfully_routes_v6_ironwood_outputs_through_ironwood_domain() {
        let vector = zakura_test::vectors::ORCHARD_NOTE_ENCRYPTION_ZERO_VECTOR
            .first()
            .expect("test vectors are non-empty");
        let v3_shielded_data = local_shielded_data_from_action(&v3_action_from_test_vector(vector));
        let network = nu6_3_network();
        let height = Height(1);

        let ironwood_tx = v6_coinbase(height, None, Some(v3_shielded_data.clone()));
        assert!(
            super::decrypts_successfully(&ironwood_tx, &network, height),
            "V6 Ironwood outputs must use the V3 note plaintext domain"
        );

        let orchard_tx = v6_coinbase(height, Some(v3_shielded_data), None);
        assert!(
            !super::decrypts_successfully(&orchard_tx, &network, height),
            "the same V3 ciphertext must not be accepted as an Orchard output"
        );
    }

    fn v2_action_from_test_vector(v: &zakura_test::vectors::TestVector) -> Action<()> {
        Action::from_parts(
            Nullifier::from_bytes(&v.rho).expect("test vector has a valid nullifier"),
            test_rk(),
            ExtractedNoteCommitment::from_bytes(&v.cmx)
                .expect("test vector has a valid note commitment"),
            TransmittedNoteCiphertext {
                epk_bytes: v.ephemeral_key,
                enc_ciphertext: v.c_enc,
                out_ciphertext: v.c_out,
            },
            ValueCommitment::from_bytes(&v.cv_net)
                .expect("test vector has a valid value commitment"),
            (),
        )
        .expect("test vector fields form a valid action")
    }

    fn v3_action_from_test_vector(v: &zakura_test::vectors::TestVector) -> Action<()> {
        let rho = orchard::note::Rho::from_bytes(&v.rho).expect("test vector has valid rho");
        let rseed = RandomSeed::from_bytes(v.rseed, &rho)
            .expect("test vector has a valid note random seed");
        let address = test_vector_address(v);
        let note = Note::from_parts(
            address,
            NoteValue::from_raw(v.v),
            rho,
            rseed,
            NoteVersion::V3,
        )
        .expect("test vector components form a V3 note");
        let cmx = ExtractedNoteCommitment::from(note.commitment());
        let cv_net = ValueCommitment::from_bytes(&v.cv_net)
            .expect("test vector has a valid value commitment");
        let ovk = orchard::keys::OutgoingViewingKey::from([0; 32]);
        let encryptor = IronwoodNoteEncryption::new(Some(ovk), note, v.memo);
        let mut rng = OsRng;

        Action::from_parts(
            Nullifier::from_bytes(&v.rho).expect("test vector has a valid nullifier"),
            test_rk(),
            cmx,
            TransmittedNoteCiphertext {
                epk_bytes: orchard::note_encryption::IronwoodDomain::epk_bytes(encryptor.epk()).0,
                enc_ciphertext: encryptor.encrypt_note_plaintext(),
                out_ciphertext: encryptor.encrypt_outgoing_plaintext(&cv_net, &cmx, &mut rng),
            },
            cv_net,
            (),
        )
        .expect("test vector fields form a valid action")
    }

    fn local_shielded_data_from_action(action: &Action<()>) -> chain_orchard::ShieldedData {
        let mut shielded_data = v5_transactions(Network::new_default_testnet().block_iter())
            .find_map(|tx| tx.orchard_shielded_data().cloned())
            .expect("test vectors include an Orchard transaction");
        let mut local_action = shielded_data.actions.first().action.clone();

        local_action.cv = chain_orchard::ValueCommitment::try_from(action.cv_net().to_bytes())
            .expect("test vector has a valid value commitment");
        local_action.nullifier =
            chain_orchard::Nullifier::try_from((*action.nullifier()).to_bytes())
                .expect("test vector has a valid nullifier");
        local_action.cm_x = pallas::Base::from_repr((*action.cmx()).to_bytes())
            .expect("test vector has a valid note commitment");
        local_action.ephemeral_key =
            chain_orchard::keys::EphemeralPublicKey::try_from(action.encrypted_note().epk_bytes)
                .expect("test vector has a valid ephemeral key");
        local_action.enc_ciphertext = action.encrypted_note().enc_ciphertext.into();
        local_action.out_ciphertext = action.encrypted_note().out_ciphertext.into();

        let spend_auth_sig = shielded_data.actions.first().spend_auth_sig;
        shielded_data.flags = chain_orchard::Flags::ENABLE_OUTPUTS;
        shielded_data.proof = Halo2Proof(vec![0; ::orchard::Proof::expected_proof_size(1)]);
        shielded_data.actions = at_least_one![chain_orchard::AuthorizedAction::from_parts(
            local_action,
            spend_auth_sig,
        )];
        shielded_data
    }

    fn nu6_3_network() -> Network {
        Network::new_regtest(RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                nu6_3: Some(1),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    fn v6_coinbase(
        height: Height,
        orchard_shielded_data: Option<chain_orchard::ShieldedData>,
        ironwood_shielded_data: Option<chain_orchard::ShieldedData>,
    ) -> Transaction {
        Transaction::V6 {
            network_upgrade: NetworkUpgrade::Nu6_3,
            lock_time: LockTime::unlocked(),
            expiry_height: height,
            inputs: vec![transparent::Input::Coinbase {
                height,
                data: vec![],
                sequence: u32::MAX,
            }],
            outputs: vec![],
            sapling_shielded_data: None,
            orchard_shielded_data,
            ironwood_shielded_data,
        }
    }

    fn test_vector_address(v: &zakura_test::vectors::TestVector) -> Address {
        let mut bytes = [0; 43];
        bytes[..11].copy_from_slice(&v.default_d);
        bytes[11..].copy_from_slice(&v.default_pk_d);
        Address::from_raw_address_bytes(&bytes).expect("test vector has a valid address")
    }

    fn test_rk() -> VerificationKey<SpendAuth> {
        VerificationKey::<SpendAuth>::try_from(pallas::Affine::generator().to_bytes())
            .expect("Pallas generator is a valid RedPallas verification key")
    }
}
