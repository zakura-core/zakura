//! Keys, public-key encodings, and signatures with real ECDSA values.
//!
//! Each encoding either reaches a successful signature check or probes one rule that zcashd and
//! the Rust interpreter must apply identically: strict DER, high-S normalization, scalar overflow,
//! and libsecp256k1 public-key parsing.

use std::sync::LazyLock;

use arbitrary::{Result, Unstructured};
use secp256k1::{All, Message, PublicKey, Secp256k1, SecretKey};

/// The number of distinct signing keys.
pub const KEYS: usize = 3;

static SECP: LazyLock<Secp256k1<All>> = LazyLock::new(Secp256k1::new);

static SECRET_KEYS: LazyLock<[SecretKey; KEYS]> = LazyLock::new(|| {
    [0x11, 0x22, 0x33].map(|byte| SecretKey::from_slice(&[byte; 32]).expect("valid scalar"))
});

/// The secp256k1 group order.
const ORDER: [u8; 32] = [
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe,
    0xba, 0xae, 0xdc, 0xe6, 0xaf, 0x48, 0xa0, 0x3b, 0xbf, 0xd2, 0x5e, 0x8c, 0xd0, 0x36, 0x41, 0x41,
];

/// The secp256k1 field prime.
const PRIME: [u8; 32] = [
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe, 0xff, 0xff, 0xfc, 0x2f,
];

fn public_key(key: usize) -> PublicKey {
    PublicKey::from_secret_key(&SECP, &SECRET_KEYS[key])
}

/// Picks a key index.
pub fn key(u: &mut Unstructured) -> Result<usize> {
    u.choose_index(KEYS)
}

/// Encodes the public key of `key`, usually validly.
pub fn encode_public_key(u: &mut Unstructured, key: usize) -> Result<Vec<u8>> {
    let public = public_key(key);
    let compressed = public.serialize().to_vec();
    let uncompressed = public.serialize_uncompressed().to_vec();
    let odd = uncompressed[64] & 1 == 1;
    let with_header = |header: u8, bytes: &[u8]| [&[header][..], &bytes[1..]].concat();
    let mut encoded = match u.int_in_range(0..=19)? {
        0..=7 => compressed,
        8..=10 => uncompressed,
        11 => with_header(if odd { 7 } else { 6 }, &uncompressed),
        12 => with_header(if odd { 6 } else { 7 }, &uncompressed),
        13 => [&compressed[..], &[0]].concat(),
        14 => compressed[..32].to_vec(),
        15 => with_header(u.int_in_range(0..=8)?, &uncompressed),
        16 => [&[0x02][..], &PRIME].concat(),
        17 => [&uncompressed[..33], &PRIME].concat(),
        18 => Vec::new(),
        _ => {
            // An x coordinate whose cube plus seven is usually not a square.
            let mut off_curve = compressed;
            off_curve[32] ^= 1;
            off_curve
        }
    };
    if u.ratio(1, 64)? && !encoded.is_empty() {
        let index = u.choose_index(encoded.len())?;
        encoded[index] ^= u.arbitrary::<u8>()? | 1;
    }
    Ok(encoded)
}

/// Picks a signature hash type: usually canonical, otherwise one that differs from a canonical
/// type in the bits each transaction version treats differently, or any byte.
pub fn hash_type(u: &mut Unstructured) -> Result<u8> {
    Ok(match u.int_in_range(0..=9)? {
        0..=5 => *u.choose(&[0x01, 0x02, 0x03, 0x81, 0x82, 0x83])?,
        6..=8 => *u.choose(&[0x00, 0x04, 0x21, 0x41, 0x43, 0x80, 0x84, 0xc1, 0xc3, 0xff])?,
        _ => u.arbitrary()?,
    })
}

/// Encodes a signature by `key` over `digest`, followed by `hash_type`, usually validly.
pub fn encode_signature(
    u: &mut Unstructured,
    key: usize,
    mut digest: [u8; 32],
    hash_type: u8,
) -> Result<Vec<u8>> {
    let mut key = key;
    match u.int_in_range(0..=15)? {
        0 => key = (key + 1) % KEYS,
        1 => digest[0] ^= 1,
        _ => {}
    }
    let compact = SECP
        .sign_ecdsa(&Message::from_digest(digest), &SECRET_KEYS[key])
        .serialize_compact();
    let (r, s) = compact.split_at(32);
    let (mut r, mut s): (Vec<u8>, Vec<u8>) = (r.to_vec(), s.to_vec());
    let mut der = match u.int_in_range(0..=31)? {
        0..=2 => {
            s = sub(&ORDER, &s);
            der(&r, &s)
        }
        3 => {
            let scalar = special_scalar(u)?;
            if u.arbitrary()? {
                r = scalar;
            } else {
                s = scalar;
            }
            der(&r, &s)
        }
        // Each case below breaks strict DER or the signature size limits.
        4 => {
            let mut padded = vec![0];
            padded.extend(integer(&r));
            der_from_integers(&padded, &integer(&s))
        }
        5 => {
            let mut der = der(&r, &s);
            der.insert(1, 0x81);
            der
        }
        6 => {
            let mut der = der(&r, &s);
            der.push(u.arbitrary()?);
            if u.arbitrary()? {
                der[1] += 1;
            }
            der
        }
        7 => {
            let mut der = der(&r, &s);
            der[1] = der[1].wrapping_add(*u.choose(&[1, 0xff])?);
            der
        }
        8 => Vec::new(),
        9 => super::bytes(u, 80)?,
        10 => {
            let mut der = der(&r, &s);
            let index = u.choose_index(der.len())?;
            der[index] ^= u.arbitrary::<u8>()? | 1;
            der
        }
        _ => der(&r, &s),
    };
    if !u.ratio(1, 32)? {
        der.push(hash_type);
    }
    Ok(der)
}

/// Returns a scalar at or near an edge: 0, 1, n - 1, n, n + 1, p, or 2^256 - 1.
fn special_scalar(u: &mut Unstructured) -> Result<Vec<u8>> {
    Ok(match u.int_in_range(0..=6)? {
        0 => vec![0],
        1 => vec![1],
        2 => sub(&ORDER, &[1]),
        3 => ORDER.to_vec(),
        4 => add_one(&ORDER),
        5 => PRIME.to_vec(),
        _ => vec![0xff; 32],
    })
}

/// Returns the minimal DER INTEGER content for the unsigned big-endian `value`.
fn integer(value: &[u8]) -> Vec<u8> {
    let start = value
        .iter()
        .position(|&byte| byte != 0)
        .unwrap_or(value.len());
    let mut content = value[start..].to_vec();
    if content.first().is_none_or(|&byte| byte & 0x80 != 0) {
        content.insert(0, 0);
    }
    content
}

fn der(r: &[u8], s: &[u8]) -> Vec<u8> {
    der_from_integers(&integer(r), &integer(s))
}

fn der_from_integers(r: &[u8], s: &[u8]) -> Vec<u8> {
    let length = |bytes: &[u8]| u8::try_from(bytes.len()).expect("integers are at most 34 bytes");
    let mut der = vec![0x30, length(r) + length(s) + 4, 0x02, length(r)];
    der.extend(r);
    der.extend([0x02, length(s)]);
    der.extend(s);
    der
}

/// Returns `a - b` for big-endian `a >= b`, as 32 bytes.
fn sub(a: &[u8; 32], b: &[u8]) -> Vec<u8> {
    let mut b_padded = [0u8; 32];
    b_padded[32 - b.len()..].copy_from_slice(b);
    let mut out = [0u8; 32];
    let mut borrow = 0i16;
    for i in (0..32).rev() {
        let difference = i16::from(a[i]) - i16::from(b_padded[i]) - borrow;
        borrow = i16::from(difference < 0);
        // `rem_euclid(256)` is in 0..=255, so the cast is exact.
        out[i] = difference.rem_euclid(256) as u8;
    }
    out.to_vec()
}

/// Returns `a + 1` for big-endian `a < 2^256 - 1`, as 32 bytes.
fn add_one(a: &[u8; 32]) -> Vec<u8> {
    let mut out = *a;
    for byte in out.iter_mut().rev() {
        let (sum, carry) = byte.overflowing_add(1);
        *byte = sum;
        if !carry {
            break;
        }
    }
    out.to_vec()
}
