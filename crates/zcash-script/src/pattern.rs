//! Reusable bits of scripts, to avoid writing hex strings.
//!
//! Much of this comes from <https://gist.github.com/str4d/9d80f1b60e6787310897044502cb025b> – the
//! definitions here generally start their rustdoc with the corresponding `label_script` result from
//! that gist. If there are multiple matching labels, or if the `label_script` result doesn’t
//! accurately describe the definiton, then there is a separate “label” tag in the rustdoc that
//! matches the `label_script` result(s).
//!
//! Zcash Script doesn’t have a real type system, but many of these are annotated with some
//! indication of the type. Being scripts with holes, the types are more complicated than those
//! listed with the opcodes in interpreter.rs. Here’s the decoder ring:

//! * `Bool`, `PubKey`, `Signature`, and other capitalized WordsSmashedTogether – a individual stack
//!   value, with a particular shape
//! * `[]` – a comma-separated sequence of stack values
//! * `+` – a concatenation of stack sequences (useful with type variables that represent sequences)
//! * `*` – repetition `n*Signature` is a sequence of `n` Signature`s
//! * `->` – the left side of the arrow represents the type of the top elements of the stack before
//!   the script, and the right side is the type of the top elements of the stack after the script.
//!   Scripts never care about elements below these, and the remainder of the stack is unchanged.
//! * `∪` – a union of elements or stack sequence. If a union occurs to the left of an `->`, then
//!   the alternative to provide depends on some conditional in the script. This is one way to
//!   introduce a dependent type (described after this section).
//! * `?` – optional. Shorthand for `∪ []` and can also be applied to an individual element
//! * `-` – suffix removal. `x - y` first implies that one is a suffix of the other, then we want any
//!   prefix of `x` that doesn’t overlap with `y`. So `[1, 2, 3] - [2, 3]` would be `[1]`,
//!   `[2, 3] - [1, 2, 3]` would be `[]`, but `[1, 2, 3] - [2]` would be invalid.
//! * `💥` – terminates evaluation, if followed by `?`, it _may_ terminate evaluation (`💥?` is the
//!   behavior of `OP_*VERIFY`)
//! * vars – identifiers in the Rust function signature, indicates where those values are used. In
//!   the case of identifiers that represent `[Opcode]`, an occurrence in negative position
//!   represents the input type of that script, and an occurence in positive position represents the
//!   output type of that script. Identifiers that don’t show up in the rust function signature
//!   represent type variables – they are the same type in each occurrence.
//! * `_` – any type, each occurrence can represent a different type
//!
//! Some scripts have dependent types. This can be read as the script having potentially simpler
//! types if we statically know some of the stack values. In the current set of scripts, this is
//! always the top element, and always a `Bool`. These types are written out as two types with
//! `True` and `False` in place of the `Bool`. The non-dependent version can be inferred by unioning
//! the dependent ones, but it’s provided explicitly since union isn’t always trivial.

use alloc::vec::Vec;

use ripemd::Ripemd160;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    num, op, opcode, pv,
    script::{self, Evaluable},
    Opcode::{self, PushValue},
};

/// Pushes a value onto the stack that indicates whether the stack was previously empty.
///
/// type: `[] -> [Bool]`
pub const EMPTY_STACK_CHECK: [Opcode; 3] = [op::DEPTH, op::_0, op::EQUAL];

/// Pushes a value onto the stack, then immediately drops it.
///
/// type: `[] -> []`
pub fn ignored_value(v: opcode::PushValue) -> [Opcode; 2] {
    [PushValue(v), op::DROP]
}

// abstractions

/// Holds the two branches of a conditional, without the condition.
///
/// type: `(ti ∪ ei) + [Bool] -> (to ∪ eo)`
///   where
///     thn: `ti -> to`
///     els: `ei -> eo`
///
/// dependent type:
/// - `ti + [True] -> to`
/// - `ei + [False] -> eo`
pub fn branch(thn: &[Opcode], els: &[Opcode]) -> Vec<Opcode> {
    [&[op::IF], thn, &[op::ELSE], els, &[op::ENDIF]].concat()
}

/// Like `branch`, but also holds the conditional.
///
/// Example: `if_else(EMPTY_STACK_CHECK, [], [op::RETURN])`
///
/// type: `((ti ∪ ei) - co) + ci -> (co - (ti ∪ ei)) + (to ∪ eo)`
///   where
///     cond: `ci -> co + [Bool]`
///     thn: `ti -> to`
///     els: `ei -> eo`
///
/// The suffix removasl (`-`) in the type here is because `cond` can put elements on the stack, and
/// the branches must consume that output first (if they consume any input at all). And then on the
/// output side, if the output of cond was more than the branches consume, that residue is left on
/// the stack prior to the outputs of the branches.
pub fn if_else(cond: &[Opcode], thn: &[Opcode], els: &[Opcode]) -> Vec<Opcode> {
    let mut vec = cond.to_vec();
    vec.extend(branch(thn, els));
    vec
}

/// Errors that can happen when creating scripts containing `CHECK*SIG*` operations.
#[allow(missing_docs)]
#[derive(Copy, Clone, PartialEq, Eq, Debug, Error)]
pub enum Error {
    #[error("CHECKMULTISIG only supports 20 keys, but you provided {0}")]
    TooManyPubKeys(usize),
    #[error("PubKeys shouldn’t be longer than 65 bytes")]
    OverlongPubKey,
}

/// Generate an [`Opcode`] that would push the provided pubkey onto the stack.
pub fn push_pubkey(pubkey: &[u8]) -> Result<Opcode, Error> {
    pv::push_value(pubkey)
        .map(PushValue)
        .ok_or(Error::OverlongPubKey)
}

/// Performs a `sig_count`-of-`pks.len()` multisig.
///
/// if `verify`
///   type: `sig_count*Signature -> 💥?`
/// else
///   type: `sig_count*Signature -> [Bool]`
pub fn check_multisig(sig_count: u8, pks: &[&[u8]], verify: bool) -> Result<Vec<Opcode>, Error> {
    Ok([
        &[PushValue(push_num(sig_count.into()))],
        &pks.iter()
            .map(|pk| push_pubkey(pk))
            .collect::<Result<Vec<_>, _>>()?[..],
        &[
            PushValue(push_num(
                pks.len()
                    .try_into()
                    .map_err(|_| Error::TooManyPubKeys(pks.len()))?,
            )),
            if verify {
                op::CHECKMULTISIGVERIFY
            } else {
                op::CHECKMULTISIG
            },
        ],
    ]
    .concat())
}

/// Checks equality against some constant value.
///
/// if `verify`
///   type: `[_] -> 💥?`
/// else
///   type: `[_] -> [Bool]`
pub fn equals(expected: opcode::PushValue, verify: bool) -> [Opcode; 2] {
    [
        PushValue(expected),
        if verify { op::EQUALVERIFY } else { op::EQUAL },
    ]
}

/// Checks a signature against the provided pubkey.
///
/// if `verify`
///   type: `[Signature] -> 💥?`
/// else
///   type: `[Signature] -> [Bool]`
pub fn check_sig(pubkey: &[u8], verify: bool) -> Result<[Opcode; 2], Error> {
    Ok([
        push_pubkey(pubkey)?,
        if verify {
            op::CHECKSIGVERIFY
        } else {
            op::CHECKSIG
        },
    ])
}

/// Checks that the top of the stack has exactly the expected size.
///
/// type: `[a] -> [a] + 💥?`
pub fn size_check(expected: u32) -> Vec<Opcode> {
    [&[op::SIZE], &equals(push_num(expected.into()), true)[..]].concat()
}

/// “CLTV”
///
/// type: `[] -> ([lt] + 💥)?`
pub fn check_lock_time_verify(lt: u32) -> [Opcode; 3] {
    [
        PushValue(push_num(lt.into())),
        op::CHECKLOCKTIMEVERIFY,
        op::DROP,
    ]
}

/// Produce a minimal `PushValue` that encodes the provided number.
pub fn push_num(n: i64) -> opcode::PushValue {
    pv::push_value(&num::serialize(n)).expect("all i64 can be encoded as `PushValue`")
}

/// Produce a minimal `PushValue` that encodes the provided script. This is particularly useful with
/// P2SH.
pub fn push_script<T: Into<opcode::PossiblyBad> + opcode::Evaluable + Clone>(
    script: &script::Component<T>,
) -> Option<opcode::PushValue> {
    pv::push_value(&script.to_bytes())
}

/// Creates a `PushValue` from a 20-byte value (basically, RipeMD160 and other hashes).
///
/// __TODO__: Once const_generic_exprs lands, this should become `push_array<N>(a: &[u8; N])` with
///           `N` bounded by `opcode::push_value::LargeValue::MAX_SIZE`.
pub fn push_160b_hash(hash: &[u8; 20]) -> opcode::PushValue {
    pv::push_value(hash).expect("20 is a valid data size")
}

/// Creates a `PushValue` from a 32-byte value (basically, SHA-256 and other hashes).
///
/// __TODO__: Once const_generic_exprs lands, this should become `push_array<N>(a: &[u8; N])` with
///           `N` bounded by `opcode::push_value::LargeValue::MAX_SIZE`.
pub fn push_256b_hash(hash: &[u8; 32]) -> opcode::PushValue {
    pv::push_value(hash).expect("32 is a valid data size")
}

/// P2PK
///
/// label: Pay-to-(compressed-)pubkey inside P2SH
///
/// type: `[Signature] -> [Bool]`
pub fn pay_to_pubkey(pubkey: &[u8]) -> Result<[Opcode; 2], Error> {
    check_sig(pubkey, false)
}

/// P2PKH
///
/// type: `[Signature, PubKey] -> [Bool] ∪  💥`
pub fn pay_to_pubkey_hash(pk: &[u8]) -> Vec<Opcode> {
    [
        &[op::DUP, op::HASH160],
        &equals(
            push_160b_hash(&Ripemd160::digest(Sha256::digest(pk)).into()),
            true,
        )[..],
        &[op::CHECKSIG],
    ]
    .concat()
}

/// P2SH
///
/// type: `[_] -> [Bool]`
pub fn pay_to_script_hash<T: Into<opcode::PossiblyBad> + opcode::Evaluable + Clone>(
    redeem_script: &script::Component<T>,
) -> Vec<Opcode> {
    [
        &[op::HASH160],
        &equals(
            push_160b_hash(&Ripemd160::digest(Sha256::digest(redeem_script.to_bytes())).into()),
            false,
        )[..],
    ]
    .concat()
}

/// multisig with ignored value and empty stack check
///
/// label: 2-of-3 multisig with compressed pubkeys, a 4-byte ignored data value, and an empty stack
///   check
///
/// type: `n*Signature -> [Bool] ∪  💥`
pub fn check_multisig_empty(
    n: u8,
    x: &[&[u8]],
    ignored: opcode::PushValue,
) -> Result<Vec<Opcode>, Error> {
    Ok([
        &check_multisig(n, x, true)?,
        &ignored_value(ignored)[..],
        &EMPTY_STACK_CHECK,
    ]
    .concat())
}

/// Combined multisig
///
/// 1-of-3 and 2-of-2 combined multisig with compressed pubkeys
///
/// type: `m*Signature + n*Signature -> (m*Signature + 💥) ∪ [Bool]`
pub fn combined_multisig(m: u8, x: &[&[u8]], n: u8, y: &[&[u8]]) -> Result<Vec<Opcode>, Error> {
    Ok([
        &check_multisig(m, x, true)?[..],
        &check_multisig(n, y, false)?[..],
    ]
    .concat())
}

/// P2PKH with an ignored value
///
/// label:
/// - P2PKH inside P2SH with a 32-byte ignored data value
/// - P2PKH inside P2SH with a zero-value placeholder ignored data value
///
/// type: \[Signature, PubKey\] -> \[Bool\] ∪  💥
pub fn p2pkh_ignored(ignored: opcode::PushValue, pk: &[u8]) -> Vec<Opcode> {
    [&ignored_value(ignored)[..], &pay_to_pubkey_hash(pk)].concat()
}

/// P2PK with ignored value and empty stack check
///
/// label: Pay-to-(compressed-)pubkey inside P2SH with an empty stack check
///
/// type: `Signature -> [Bool] ∪  💥`
pub fn p2pk_empty(recipient_pk: &[u8], ignored: opcode::PushValue) -> Result<Vec<Opcode>, Error> {
    Ok([
        &check_sig(recipient_pk, true)?[..],
        &ignored_value(ignored)[..],
        &EMPTY_STACK_CHECK,
    ]
    .concat())
}

/// Hash160 HTLC
///
/// type: `[Signature, _?, Bool] -> ([Signature, lt?] + 💥) ∪ [Bool]`
///
/// dependent type:
/// - `[Signature, True] -> ([Signature, lt] + 💥) ∪ [Bool]`
/// - `[Signature, _, False] -> ([Signature] + 💥) ∪ [Bool]`
pub fn hash160_htlc(
    lt: u32,
    sender_pk: &[u8],
    recipient_hash: &[u8; 20],
    recipient_pk: &[u8],
) -> Result<Vec<Opcode>, Error> {
    Ok(branch(
        &[
            &check_lock_time_verify(lt)[..],
            &check_sig(sender_pk, false)?[..],
        ]
        .concat(),
        &[
            &[op::HASH160],
            &equals(push_160b_hash(recipient_hash), true)[..],
            &check_sig(recipient_pk, false)?[..],
        ]
        .concat(),
    ))
}

/// Hash160 HTLC with size check
/// type: `[Signature, a?, Bool] -> ([Signature, lt ∪ a?] + 💥) ∪ [Bool]`
///
/// dependent type:
/// - `[Signature, True] -> ([Signature, lt] + 💥) ∪ [Bool]`
/// - `[Signature, a, False] -> ([Signature, a?] + 💥) ∪ [Bool]`
pub fn hash160_htlc_size_check(
    lt: u32,
    sender_pk: &[u8],
    recipient_hash: &[u8; 20],
    recipient_pk: &[u8],
) -> Result<Vec<Opcode>, Error> {
    Ok(branch(
        &[
            &check_lock_time_verify(lt)[..],
            &check_sig(sender_pk, false)?[..],
        ]
        .concat(),
        &[
            &size_check(20)[..],
            &[op::HASH160],
            &equals(push_160b_hash(recipient_hash), true)[..],
            &check_sig(recipient_pk, false)?[..],
        ]
        .concat(),
    ))
}

/// Hash160 HTLC
///
/// type: `[Signature, PubKey, _?, Bool] -> ([Signature, lt?] + 💥) ∪ [Bool]`
///
/// dependent type:
/// - `[Signature, PubKey, _, True] -> ([Signature] + 💥) ∪ [Bool]`
/// - `[Signature, PubKey, False] -> ([Signature, lt] + 💥) ∪ [Bool]`
pub fn hash160_htlc_p2pkh(
    lt: u32,
    sender_pk: &[u8],
    recipient_hash: &[u8; 20],
    recipient_pk: &[u8],
) -> Vec<Opcode> {
    branch(
        &[
            &[op::HASH160],
            &equals(push_160b_hash(recipient_hash), true)[..],
            &pay_to_pubkey_hash(recipient_pk)[..],
        ]
        .concat(),
        &[
            &check_lock_time_verify(lt)[..],
            &pay_to_pubkey_hash(sender_pk)[..],
        ]
        .concat(),
    )
}

/// SHA-256 HTLC
///
/// type: `[Signature, _?, Bool] -> ([Signature, lt?] + 💥) ∪ [Bool]`
///
/// dependent type:
/// - `[Signature, True] -> ([Signature, lt] + 💥) ∪ [Bool]`
/// - `[Signature, _, False] -> ([Signature] + 💥) ∪ [Bool]`
pub fn sha256_htlc(
    lt: u32,
    sender_pk: &[u8],
    recipient_sha: &[u8; 32],
    recipient_pk: &[u8],
) -> Result<Vec<Opcode>, Error> {
    Ok(branch(
        &[
            &check_lock_time_verify(lt)[..],
            &check_sig(sender_pk, false)?[..],
        ]
        .concat(),
        &[
            &[op::SHA256],
            &equals(push_256b_hash(recipient_sha), true)[..],
            &check_sig(recipient_pk, false)?[..],
        ]
        .concat(),
    ))
}

/// SHA-256 HTLC
///
/// label:
/// - SHA-256 HTLC (2-byte CLTV)
/// - SHA-256 HTLC (3-byte CLTV)
///
/// type: `[Signature, _?, Bool] -> ([Signature, lt?] + 💥) ∪ [Bool]`
///
/// dependent type:
/// - `[Signature, PubKey, _, True] -> ([Signature] + 💥) ∪ [Bool]`
/// - `[Signature, PubKey, False] -> ([Signature, lt] + 💥) ∪ [Bool]`
pub fn sha256_htlc_p2pkh(
    lt: u32,
    sender_pk: &[u8],
    recipient_sha: &[u8; 32],
    recipient_pk: &[u8],
) -> Vec<Opcode> {
    branch(
        &[
            &[op::SHA256],
            &equals(push_256b_hash(recipient_sha), true)[..],
            &pay_to_pubkey_hash(recipient_pk)[..],
        ]
        .concat(),
        &[
            &check_lock_time_verify(lt)[..],
            &pay_to_pubkey_hash(sender_pk)[..],
        ]
        .concat(),
    )
}

/// SHA-256 HTLC with size check
///
/// type: `[Signature, a?, Bool] -> ([Signature, lt ∪ a?] + 💥) ∪ [Bool]`
///
/// dependent type:
/// - `[Signature, a, True] -> ([Signature, a?] + 💥) ∪ [Bool]`
/// - `[Signature, False] -> ([Signature, lt] + 💥) ∪ [Bool]`
pub fn sha256_htlc_size_check(
    lt: u32,
    sender_pk: &[u8],
    recipient_sha: &[u8; 32],
    recipient_pk: &[u8],
) -> Vec<Opcode> {
    branch(
        &[
            &size_check(20)[..],
            &[op::SHA256],
            &equals(push_256b_hash(recipient_sha), true)[..],
            &pay_to_pubkey_hash(recipient_pk)[..],
        ]
        .concat(),
        &[
            &check_lock_time_verify(lt)[..],
            &pay_to_pubkey_hash(sender_pk)[..],
        ]
        .concat(),
    )
}

/// One party has SHA-256 hashlock, other party can spend unconditionally
///
/// type: `[Signature, PubKey, _, Bool] -> ([Signature, PubKey] + 💥) ∪ [Bool]`
pub fn sha256_htlc_with_unconditional(
    sender_pk: &[u8],
    recipient_sha: &[u8; 32],
    recipient_pk: &[u8],
) -> Vec<Opcode> {
    branch(
        &[
            &[op::SHA256],
            &equals(push_256b_hash(recipient_sha), true)[..],
            &pay_to_pubkey_hash(recipient_pk)[..],
        ]
        .concat(),
        &[
            &ignored_value(pv::_1)[..],
            &[op::HASH160],
            &equals(
                push_160b_hash(&Ripemd160::digest(Sha256::digest(sender_pk)).into()),
                true,
            )[..],
            &[op::CHECKSIG],
        ]
        .concat(),
    )
}

/// Two-sided Hash160 HTLC with size checks
///
/// type: `[Signature, a, Bool] -> ([Signature] + [a, lt?]? + 💥) ∪ [Bool]`
///
/// dependent type:
/// - `[Signature, a, True] -> ([Signature] + [a, lt?]? + 💥) ∪ [Bool]`
/// - `[Signature, a, False] -> ([Signature, a?] + 💥) ∪ [Bool]`
pub fn dual_hash160_htlc_size_check(
    lt: u32,
    sender_hash: &[u8; 20],
    sender_pk: &[u8],
    recipient_hash: &[u8; 20],
    recipient_pk: &[u8],
) -> Result<Vec<Opcode>, Error> {
    // type: `[Signature, _] -> ([Signature, _?] + 💥) ∪ [Bool]`
    let verify = |hash, pk| {
        Ok([
            &size_check(20)[..],
            &[op::HASH160],
            &equals(push_160b_hash(hash), true)[..],
            &check_sig(pk, false)?[..],
        ]
        .concat())
    };
    Ok(branch(
        &[
            &check_lock_time_verify(lt)[..],
            &verify(sender_hash, sender_pk)?[..],
        ]
        .concat(),
        &verify(recipient_hash, recipient_pk)?,
    ))
}
