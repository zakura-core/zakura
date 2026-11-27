//! Fuzz driver for a record's extension area and its ML-DSA-65 signature:
//! the bytes a node reads from a peer's record before it trusts the record.
//!
//! `qa/fuzz/p2p`'s `record_signature` target and this module's tests call
//! [`replay_record_signature_bytes`].

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::OnceLock,
};

use zakura_quic::NodeSecretKey;

use super::{
    super::protocol::{ZakuraNodeRecord, ZakuraNodeRecordBody, ZakuraServiceId},
    parse, verify, write_key, write_signature, SignatureExtensions, RECORD_EXTENSION_ML_DSA_65_KEY,
    RECORD_EXTENSION_ML_DSA_65_SIGNATURE,
};
use crate::zakura::ZakuraNetworkId;

/// An accepted extension area has one reading, and it is the area's bytes.
const CANONICAL: &str = "record-signature/canonical-encoding";
/// A record's ML-DSA-65 signature verifies only over the record it was made
/// for.
const TAMPER: &str = "record-signature/tamper-refused";

/// The most edits one input applies to the witness.
const MAX_WITNESS_EDITS: usize = 8;

/// The longest area a replay builds: past the largest area a record's body
/// cap admits, so lengths around the cap are reached.
const MAX_AREA_BYTES: usize = 9_000;

/// What one input reached.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordSignatureReplay {
    /// The input was read as an extension area.
    Raw {
        /// The area holds a key and a signature in their canonical place.
        parsed: bool,
    },
    /// The witness record's area was verified after the input's edits.
    Witness {
        /// The edits changed the area.
        edited: bool,
        /// The ML-DSA-65 signature verified.
        verified: bool,
    },
}

/// An honest record and what its ML-DSA-65 signature verifies under.
struct Witness {
    /// The record's author.
    secret: NodeSecretKey,
    record: ZakuraNodeRecord,
    /// The record's pre-image without its extension area.
    fields: Vec<u8>,
}

/// The honest witness, signed once per process with a fixed hedge: key
/// generation and signing cost more than the verification an input pays for.
fn witness() -> &'static Witness {
    static WITNESS: OnceLock<Witness> = OnceLock::new();
    WITNESS.get_or_init(|| {
        let secret = NodeSecretKey::from_bytes(&[0x42; 32]);
        // Two TLVs this release does not know, ahead of the key.
        let unknown = [tlv(0x7001, &[0xaa, 0xbb, 0xcc]), tlv(0x7002, &[])].concat();
        let body = ZakuraNodeRecordBody {
            keys: secret.key_list().clone(),
            direct_addrs: vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(45, 33, 30, 10)),
                8233,
            )],
            services: vec![ZakuraServiceId::discovery()],
            zakura_protocol_min: 5,
            zakura_protocol_max: 5,
            network_id: ZakuraNetworkId::Regtest,
            chain_id: [7; 32],
            sequence: 1,
            expires_at_unix_secs: 1_800_000_000,
            extensions: unknown,
        };
        let record = ZakuraNodeRecord::sign_with_randomness(body, &secret, [9; 32])
            .expect("the witness signs its own record");
        let pre_image = record
            .body
            .encode_for_signature()
            .expect("a signed record's body is in bounds");
        let fields = pre_image[..pre_image.len() - record.body.extensions.len()].to_vec();
        Witness {
            secret,
            record,
            fields,
        }
    })
}

fn tlv(kind: u16, value: &[u8]) -> Vec<u8> {
    let len = u16::try_from(value.len()).expect("a driver TLV value is under 64 KiB");
    let mut bytes = kind.to_le_bytes().to_vec();
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(value);
    bytes
}

/// Reads one extension area that `data` describes.
///
/// The first byte's low bit picks the layout. With it clear, the rest is an
/// extension area as a peer sends it. With it set, the rest is up to
/// [`MAX_WITNESS_EDITS`] four-byte edits of an honest record's area, so a
/// campaign explores near-valid areas without a corpus. Each input verifies
/// at most one ML-DSA-65 signature.
///
/// An accepted area is exactly its other TLVs, its key TLV and its signature
/// TLV, and signing can rebuild it. The witness verifies, and every edit of
/// its area is refused, as an attacker who can forge the Ed25519 signature
/// over the edited body would find.
pub fn replay_record_signature_bytes(data: &[u8]) -> RecordSignatureReplay {
    match data.split_first() {
        Some((&layout, rest)) if layout & 1 == 1 => replay_witness(rest),
        Some((_, rest)) => replay_raw(rest),
        None => replay_raw(&[]),
    }
}

/// Checks that an accepted `area` is canonical and returns its split.
fn check_canonical(area: &[u8]) -> Option<SignatureExtensions<'_>> {
    let extensions = parse(area).ok()?;
    let rebuilt = [
        extensions.other,
        &tlv(RECORD_EXTENSION_ML_DSA_65_KEY, extensions.key),
        &tlv(RECORD_EXTENSION_ML_DSA_65_SIGNATURE, extensions.signature),
    ]
    .concat();
    assert_eq!(
        rebuilt, area,
        "{CANONICAL}: an accepted area is not its three parts in order"
    );
    // The other TLVs are whole and hold neither type, so signing takes them
    // and gives an area that splits at the same place.
    let mut resigned = extensions.other.to_vec();
    assert_eq!(
        write_key(&mut resigned, &witness().secret),
        Ok(()),
        "{CANONICAL}: the other TLVs of an accepted area can't be signed again"
    );
    write_signature(
        &mut resigned,
        extensions
            .signature
            .try_into()
            .expect("parse checked the signature length"),
    );
    assert_eq!(
        parse(&resigned).map(|resigned| resigned.other),
        Ok(extensions.other),
        "{CANONICAL}: signing moved the split"
    );
    Some(extensions)
}

fn replay_raw(area: &[u8]) -> RecordSignatureReplay {
    let area = &area[..area.len().min(MAX_AREA_BYTES)];
    RecordSignatureReplay::Raw {
        parsed: check_canonical(area).is_some(),
    }
}

/// Applies `data`'s edits to the witness's area and verifies the result
/// against the witness's fields.
///
/// Each edit is four bytes: an operation, a little-endian `u16` and a value.
/// Operation 0 XORs the value into the area at the `u16`'s offset, 1 resizes
/// the area to the `u16`'s length, filling with the value, 2 inserts the
/// value at the offset, 3 removes the byte there, and 4 appends a copy of
/// the area's bytes from the offset on.
fn replay_witness(data: &[u8]) -> RecordSignatureReplay {
    let honest = witness();
    let mut area = honest.record.body.extensions.clone();
    let (edits, _) = data.as_chunks::<4>();
    for &[operation, low, high, value] in edits.iter().take(MAX_WITNESS_EDITS) {
        let at = usize::from(u16::from_le_bytes([low, high]));
        match operation % 5 {
            0 if !area.is_empty() => {
                let at = at % area.len();
                area[at] ^= value;
            }
            1 => area.resize(at % (MAX_AREA_BYTES + 1), value),
            2 if area.len() < MAX_AREA_BYTES => area.insert(at % (area.len() + 1), value),
            3 if !area.is_empty() => {
                area.remove(at % area.len());
            }
            4 if !area.is_empty() => {
                let tail = area[at % area.len()..].to_vec();
                area.extend_from_slice(&tail);
                area.truncate(MAX_AREA_BYTES);
            }
            _ => {}
        }
    }
    let edited = area != honest.record.body.extensions;
    check_canonical(&area);

    // The pre-image of the witness's fields with the edited area: what the
    // Ed25519 signature of a forged record would cover.
    let pre_image = [honest.fields.as_slice(), &area].concat();
    let verified = verify(&honest.record.body.keys, &area, &pre_image).is_ok();
    assert_eq!(
        verified, !edited,
        "{TAMPER}: the witness verified {verified} after edits that changed it: {edited}"
    );
    RecordSignatureReplay::Witness { edited, verified }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// The witness verifies untouched, and one edit of each kind is refused.
    #[test]
    fn the_witness_verifies_and_each_edit_is_refused() {
        assert_eq!(
            replay_record_signature_bytes(&[1]),
            RecordSignatureReplay::Witness {
                edited: false,
                verified: true
            }
        );
        // Two flips of one bit cancel, so the area is the witness's again.
        assert_eq!(
            replay_record_signature_bytes(&[1, 0, 9, 0, 1, 0, 9, 0, 1]),
            RecordSignatureReplay::Witness {
                edited: false,
                verified: true
            }
        );
        let area_len = u16::try_from(witness().record.body.extensions.len()).unwrap();
        let [low, high] = (area_len - 1).to_le_bytes();
        let edits: [&[u8]; 6] = [
            // A bit of an unknown TLV, which the signature covers.
            &[1, 0, 4, 0, 1],
            // The signature's last byte.
            &[1, 0, low, high, 1],
            // One byte short.
            &[1, 1, low, high, 0],
            // A byte inserted ahead of everything.
            &[1, 2, 0, 0, 0],
            // The first byte removed.
            &[1, 3, 0, 0, 0],
            // The whole area twice.
            &[1, 4, 0, 0, 0],
        ];
        for edit in edits {
            assert_eq!(
                replay_record_signature_bytes(edit),
                RecordSignatureReplay::Witness {
                    edited: true,
                    verified: false
                },
                "{edit:?}"
            );
        }
    }

    /// A raw area parses only in its canonical form.
    #[test]
    fn a_raw_area_parses_only_when_canonical() {
        let honest = witness().record.body.extensions.clone();
        let raw = |area: &[u8]| replay_record_signature_bytes(&[&[0], area].concat());
        assert_eq!(raw(&honest), RecordSignatureReplay::Raw { parsed: true });
        for area in [&honest[..honest.len() - 1], &[][..], &honest[1..]] {
            assert_eq!(raw(area), RecordSignatureReplay::Raw { parsed: false });
        }
        assert_eq!(
            replay_record_signature_bytes(&[]),
            RecordSignatureReplay::Raw { parsed: false }
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Random inputs break neither property.
        #[test]
        fn random_inputs_keep_the_properties(data in proptest::collection::vec(any::<u8>(), 0..64)) {
            replay_record_signature_bytes(&data);
        }
    }
}
