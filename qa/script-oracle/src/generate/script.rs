//! Spends built from signing templates and an opcode grammar.

use arbitrary::{Result, Unstructured};
use zcash_script::num;

use super::sign;
use crate::{hash160, p2sh_script_pubkey};

/// Returns the digest to sign for a script code and a signature hash type: the verifier's sighash,
/// or for a hash type the verifier rejects, the sighash a verifier that wrongly accepted it would
/// compute.
pub type Digest<'a> = &'a dyn Fn(&[u8], u8) -> [u8; 32];

/// A scriptSig and the scriptPubKey it spends.
#[derive(Clone, Debug)]
pub struct Spend {
    pub script_sig: Vec<u8>,
    pub script_pubkey: Vec<u8>,
    /// Whether accepting this spend requires a successful signature check.
    pub needs_signature: bool,
}

/// How a lock script is satisfied.
enum Unlock {
    /// A signature by the key.
    Signature { key: usize },
    /// A signature by the key, then the public key.
    KeyHash { key: usize, public_key: Vec<u8> },
    /// A dummy element, then signatures by the keys in order.
    Multisig { keys: Vec<usize>, required: usize },
    /// Grammar output.
    Fragment,
}

struct Lock {
    script: Vec<u8>,
    unlock: Unlock,
    needs_signature: bool,
}

/// A lock script and the scriptPubKey that commits to it.
pub struct Locked {
    lock: Lock,
    /// The P2SH scriptPubKey, or `None` when the lock script is the scriptPubKey.
    script_pubkey: Option<Vec<u8>>,
}

impl Locked {
    /// Returns the scriptPubKey.
    pub fn script_pubkey(&self) -> &[u8] {
        self.script_pubkey.as_deref().unwrap_or(&self.lock.script)
    }
}

/// Opcodes that execute without failing on their own.
const COMMON: &[u8] = &[
    0x61, 0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77,
    0x78, 0x79, 0x7a, 0x7b, 0x7c, 0x7d, 0x82, 0x87, 0x88, 0x8b, 0x8c, 0x8f, 0x90, 0x91, 0x92, 0x93,
    0x94, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f, 0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8,
    0xa9, 0xaa, 0xac, 0xad, 0xae, 0xaf, 0xb0, 0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8, 0xb9,
];

/// Reserved, disabled, and invalid opcodes.
const BAD: &[u8] = &[
    0x50, 0x62, 0x65, 0x66, 0x7e, 0x7f, 0x80, 0x81, 0x83, 0x84, 0x85, 0x86, 0x89, 0x8a, 0x8d, 0x8e,
    0x95, 0x96, 0x97, 0x98, 0x99, 0xab, 0xba, 0xc0, 0xfe, 0xff,
];

/// Numbers at script-number, lock-time, and multisig-count edges.
const NUMBERS: &[i64] = &[
    0,
    1,
    -1,
    2,
    16,
    17,
    20,
    21,
    -21,
    127,
    128,
    255,
    256,
    0x7fff_ffff,
    -0x7fff_ffff,
    0x8000_0000,
    499_999_999,
    500_000_000,
    500_000_001,
    0xffff_ffff,
    1 << 39,
];

/// Lengths at push-encoding, key, hash, and element-size edges.
const LENGTHS: &[usize] = &[0, 1, 20, 32, 33, 65, 75, 76, 255, 256, 519, 520, 521];

/// Picks a transaction lock time, usually at an edge.
pub fn lock_time(u: &mut Unstructured) -> Result<u32> {
    Ok(if u.ratio(3, 4)? {
        *u.choose(&[0, 1, 499_999_999, 500_000_000, 500_000_001, u32::MAX])?
    } else {
        u.arbitrary()?
    })
}

/// Generates spends from `Unstructured` input, signing with `digest`.
pub struct Builder<'u, 'data, 'digest> {
    u: &'u mut Unstructured<'data>,
    digest: Digest<'digest>,
}

impl<'u, 'data, 'digest> Builder<'u, 'data, 'digest> {
    pub fn new(u: &'u mut Unstructured<'data>, digest: Digest<'digest>) -> Self {
        Self { u, digest }
    }

    /// Generates a spend. Signatures commit to the lock script, which is the scriptPubKey or,
    /// for P2SH, the redeem script.
    pub fn spend(&mut self) -> Result<Spend> {
        let locked = self.locked()?;
        self.unlock(locked)
    }

    /// Generates the output a spend will spend. The verifier's digest may depend on its
    /// scriptPubKey, so [`Builder::unlock`] signs it separately.
    pub fn locked(&mut self) -> Result<Locked> {
        let lock = self.lock()?;
        let script_pubkey = if self.u.ratio(1, 3)? {
            Some(match self.u.int_in_range(0..=15)? {
                0 => p2sh_script_pubkey(&[]),
                // The same hash with a non-minimal push is not a P2SH scriptPubKey.
                1 => [&[0xa9, 0x4c, 0x14][..], &hash160(&lock.script), &[0x87]].concat(),
                _ => p2sh_script_pubkey(&lock.script),
            })
        } else {
            None
        };
        Ok(Locked {
            lock,
            script_pubkey,
        })
    }

    /// Generates a scriptSig for `locked`.
    pub fn unlock(&mut self, locked: Locked) -> Result<Spend> {
        let Locked {
            lock,
            script_pubkey,
        } = locked;
        let mut needs_signature = lock.needs_signature;
        let mut script_sig = self.unlock_script(&lock)?;
        if self.u.ratio(1, 16)? {
            let noise = self.fragment(&lock.script, 1)?;
            script_sig.splice(0..0, noise);
            needs_signature = false;
        }
        let script_pubkey = match script_pubkey {
            Some(script_pubkey) => {
                script_sig.extend(self.push(&lock.script)?);
                script_pubkey
            }
            None => lock.script,
        };
        if self.u.ratio(1, 64)? {
            script_sig = self.pad(script_sig)?;
        }
        Ok(Spend {
            script_sig,
            script_pubkey,
            needs_signature,
        })
    }

    fn lock(&mut self) -> Result<Lock> {
        let mut lock = match self.u.int_in_range(0..=9)? {
            0..=1 => {
                let key = sign::key(self.u)?;
                let mut script = self.public_key(key)?;
                let (tail, needs_signature): (&[u8], bool) = *self.u.choose(&[
                    (&[0xac][..], true),
                    (&[0xad, 0x51], true),
                    (&[0xac, 0x91], false),
                ])?;
                script.extend(tail);
                Lock {
                    script,
                    unlock: Unlock::Signature { key },
                    needs_signature,
                }
            }
            2..=3 => {
                let key = sign::key(self.u)?;
                let public_key = sign::encode_public_key(self.u, key)?;
                let hashed = if self.u.ratio(1, 16)? {
                    let other = sign::key(self.u)?;
                    sign::encode_public_key(self.u, other)?
                } else {
                    public_key.clone()
                };
                let script = [&[0x76, 0xa9, 0x14][..], &hash160(&hashed), &[0x88, 0xac]].concat();
                Lock {
                    script,
                    unlock: Unlock::KeyHash { key, public_key },
                    needs_signature: true,
                }
            }
            4..=6 => self.multisig()?,
            _ => Lock {
                script: self.fragment(&[], 0)?,
                unlock: Unlock::Fragment,
                needs_signature: false,
            },
        };
        if self.u.ratio(1, 8)? {
            let prefix = [self.number()?, vec![0xb1, 0x75]].concat();
            lock.script.splice(0..0, prefix);
        }
        if self.u.ratio(1, 16)? {
            let noise = self.fragment(&[], 1)?;
            lock.script.extend(noise);
            lock.needs_signature = false;
        }
        if self.u.ratio(1, 64)? {
            lock.script = self.pad(lock.script)?;
        }
        Ok(lock)
    }

    fn multisig(&mut self) -> Result<Lock> {
        let key_count = if self.u.ratio(1, 16)? {
            self.u.int_in_range(15..=20)?
        } else {
            self.u.int_in_range(0..=4)?
        };
        let keys = (0..key_count)
            .map(|_| sign::key(self.u))
            .collect::<Result<Vec<_>>>()?;
        let required = self.u.int_in_range(0..=key_count)?;
        let mut script = if self.u.ratio(1, 8)? {
            self.number()?
        } else {
            small_number(required)
        };
        for &key in &keys {
            script.extend(self.public_key(key)?);
        }
        let exact_count = !self.u.ratio(1, 8)?;
        script.extend(if exact_count {
            small_number(key_count)
        } else {
            self.number()?
        });
        let verify = self.u.arbitrary()?;
        script.extend(if verify { &[0xaf, 0x51][..] } else { &[0xae] });
        Ok(Lock {
            script,
            needs_signature: exact_count && required > 0,
            unlock: Unlock::Multisig { keys, required },
        })
    }

    fn unlock_script(&mut self, lock: &Lock) -> Result<Vec<u8>> {
        let code = &lock.script;
        Ok(match &lock.unlock {
            &Unlock::Signature { key } => self.signature(key, code)?,
            Unlock::KeyHash { key, public_key } => {
                [self.signature(*key, code)?, self.push(public_key)?].concat()
            }
            Unlock::Multisig { keys, required } => {
                let mut script = if self.u.ratio(15, 16)? {
                    vec![0x00]
                } else {
                    self.number()?
                };
                // Sign with `required` keys in key order, so a correct spend verifies.
                let mut remaining = *required;
                for (index, &key) in keys.iter().enumerate() {
                    let left = keys.len() - index;
                    if remaining > 0 && (remaining == left || self.u.arbitrary()?) {
                        script.extend(self.signature(key, code)?);
                        remaining -= 1;
                    }
                }
                script
            }
            Unlock::Fragment => self.fragment(code, 0)?,
        })
    }

    fn public_key(&mut self, key: usize) -> Result<Vec<u8>> {
        let public_key = sign::encode_public_key(self.u, key)?;
        self.push(&public_key)
    }

    fn signature(&mut self, key: usize, code: &[u8]) -> Result<Vec<u8>> {
        let hash_type = sign::hash_type(self.u)?;
        let digest = (self.digest)(code, hash_type);
        let signature = sign::encode_signature(self.u, key, digest, hash_type)?;
        self.push(&signature)
    }

    /// Generates grammar output: data pushes, opcodes, nested conditionals, and runs of opcodes at
    /// the op-count and stack-depth limits. Signatures commit to `code`.
    fn fragment(&mut self, code: &[u8], depth: usize) -> Result<Vec<u8>> {
        let mut script = Vec::new();
        for _ in 0..self.u.int_in_range(0..=10)? {
            if script.len() > 4_000 {
                break;
            }
            match self.u.int_in_range(0..=19)? {
                0..=1 => {
                    let key = sign::key(self.u)?;
                    script.extend(self.signature(key, code)?);
                }
                2 => {
                    let key = sign::key(self.u)?;
                    script.extend(self.public_key(key)?);
                }
                3..=4 => script.extend(self.number()?),
                5 => {
                    let len = *self.u.choose(LENGTHS)?;
                    let byte = self.u.arbitrary()?;
                    script.extend(self.push(&vec![byte; len])?);
                }
                6..=11 => script.push(*self.u.choose(COMMON)?),
                12 => script.push(*self.u.choose(BAD)?),
                13 => script.push(self.u.arbitrary()?),
                14..=15 if depth < 3 => {
                    script.push(*self.u.choose(&[0x63, 0x64])?);
                    script.extend(self.fragment(code, depth + 1)?);
                    if self.u.arbitrary()? {
                        script.push(0x67);
                        script.extend(self.fragment(code, depth + 1)?);
                    }
                    if !self.u.ratio(1, 16)? {
                        script.push(0x68);
                    }
                }
                16 => {
                    let (run, low, high): (&[u8], _, _) = *self.u.choose(&[
                        (&[0x61][..], 195, 205),
                        (&[0x51], 995, 1_002),
                        (&[0x51, 0x6b], 495, 502),
                    ])?;
                    script.extend(run.repeat(self.u.int_in_range(low..=high)?));
                }
                17 => script.push(*self.u.choose(&[0x63, 0x64, 0x67, 0x68])?),
                // A truncated push ends the script.
                18 => {
                    let opcode = *self.u.choose(&[0x05, 0x4b, 0x4c, 0x4d, 0x4e])?;
                    script.push(opcode);
                    script.push(0x01);
                    break;
                }
                _ => {
                    let lock = self.multisig()?;
                    script.extend(lock.script);
                }
            }
        }
        Ok(script)
    }

    /// Pushes `data` with the minimal push opcode, or sometimes a longer one.
    fn push(&mut self, data: &[u8]) -> Result<Vec<u8>> {
        let minimal = match data.len() {
            0..=75 => 0,
            76..=255 => 1,
            256..=65_535 => 2,
            _ => 3,
        };
        let width = if self.u.ratio(7, 8)? {
            minimal
        } else {
            self.u.int_in_range(minimal..=3)?
        };
        Ok(encode_push(width, data))
    }

    /// Pushes a number: a small-integer opcode, an edge value, or a non-minimal encoding.
    fn number(&mut self) -> Result<Vec<u8>> {
        match self.u.int_in_range(0..=7)? {
            0..=2 => Ok(vec![*self.u.choose(&[
                0x00, 0x4f, 0x51, 0x52, 0x53, 0x54, 0x55, 0x58, 0x5f, 0x60,
            ])?]),
            3..=5 => {
                let value = *self.u.choose(NUMBERS)?;
                let sign = if self.u.arbitrary()? { -1 } else { 1 };
                self.push(&num::serialize(sign * value))
            }
            6 => {
                // Non-minimal encodings, including negative zero.
                let mut bytes = num::serialize(*self.u.choose(NUMBERS)?);
                bytes.push(*self.u.choose(&[0x00, 0x80])?);
                self.push(&bytes)
            }
            _ => {
                let bytes = super::bytes(self.u, 6)?;
                self.push(&bytes)
            }
        }
    }

    /// Prefixes `script` with pushes and drops, so it ends 1 byte below, at, or 1 byte above the
    /// 10,000-byte script size limit.
    fn pad(&mut self, script: Vec<u8>) -> Result<Vec<u8>> {
        let target: usize = self.u.int_in_range(9_999..=10_001)?;
        let mut remaining = target.saturating_sub(script.len());
        let mut padding = Vec::new();
        while remaining > 0 {
            let block = if remaining == 1 {
                vec![0x61]
            } else {
                // A push of width w has a (w + 1)-byte header, then the data, then OP_DROP.
                let width = match remaining {
                    2..=77 => 0,
                    78..=258 => 1,
                    _ => 2,
                };
                let data = (remaining - width - 2).min(520);
                [encode_push(width, &vec![0; data]), vec![0x75]].concat()
            };
            remaining -= block.len();
            padding.extend(block);
        }
        Ok([padding, script].concat())
    }
}

/// Encodes a push of `data` with a direct push (width 0), PUSHDATA1, PUSHDATA2, or PUSHDATA4.
fn encode_push(width: usize, data: &[u8]) -> Vec<u8> {
    let len = data.len();
    let header = match width {
        0 => vec![u8::try_from(len).expect("direct pushes hold at most 75 bytes")],
        1 => vec![
            0x4c,
            u8::try_from(len).expect("PUSHDATA1 holds at most 255 bytes"),
        ],
        2 => [
            &[0x4d][..],
            &u16::try_from(len)
                .expect("PUSHDATA2 holds at most 65,535 bytes")
                .to_le_bytes(),
        ]
        .concat(),
        _ => [
            &[0x4e][..],
            &u32::try_from(len)
                .expect("generated pushes are small")
                .to_le_bytes(),
        ]
        .concat(),
    };
    [header, data.to_vec()].concat()
}

/// Returns the small-integer opcode for `n` up to 16, or a push of `n`.
fn small_number(n: usize) -> Vec<u8> {
    match n {
        0 => vec![0x00],
        // `n` is 1..=16, so the opcode is 0x51..=0x60.
        1..=16 => vec![0x50 + n as u8],
        _ => encode_push(
            0,
            &num::serialize(i64::try_from(n).expect("key counts are small")),
        ),
    }
}
