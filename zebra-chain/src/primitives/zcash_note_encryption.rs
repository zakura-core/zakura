//! Contains code that interfaces with the zcash_note_encryption crate from
//! librustzcash.

use crate::{
    block::Height,
    parameters::{Network, NetworkUpgrade},
    transaction::Transaction,
};

/// Returns true if all Sapling or Orchard outputs, if any, decrypt successfully with
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
    use group::{prime::PrimeCurveAffine, GroupEncoding};
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

    #[test]
    fn orchard_and_ironwood_domains_accept_only_their_plaintext_versions() {
        let vector = zebra_test::vectors::ORCHARD_NOTE_ENCRYPTION_ZERO_VECTOR
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

    fn v2_action_from_test_vector(v: &zebra_test::vectors::TestVector) -> Action<()> {
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

    fn v3_action_from_test_vector(v: &zebra_test::vectors::TestVector) -> Action<()> {
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

    fn test_vector_address(v: &zebra_test::vectors::TestVector) -> Address {
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
