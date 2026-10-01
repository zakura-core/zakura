//! The individual opcodes that form a script.

#![allow(non_camel_case_types)]

use alloc::{borrow::ToOwned, string::String, vec::Vec};

pub mod push_value;

use core::cmp::{max, min};

use ripemd::Ripemd160;
use sha1::Sha1;
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::Opcode;
#[cfg(feature = "signature-validation")]
use crate::{external::pubkey::PubKey, signature};
use crate::{
    interpreter, num,
    script::{self, Asm},
};
use push_value::{
    LargeValue,
    SmallValue::{self, *},
};

/// Errors that occur while parsing an opcode.
#[allow(missing_docs)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Error)]
pub enum Error {
    #[error("expected {expected_bytes} bytes, but only {available_bytes} bytes available")]
    Read {
        expected_bytes: usize,
        available_bytes: usize,
    },

    /// __TODO__: `Option` can go away once C++ support is removed.
    #[error("disabled opcode encountered{}", .0.map_or("".to_owned(), |op| format!(": {:?}", op)))]
    Disabled(Option<Disabled>),

    // Max sizes
    #[error(
        "push size{} exceeded maxmimum ({} bytes)",
        .0.map_or("", |size| " ({size} bytes)"),
        push_value::LargeValue::MAX_SIZE
    )]
    PushSize(Option<usize>),
}

/// Definitions needed for evaluation of script types.
pub trait Evaluable {
    /// The length in bytes of this script value. This can be more efficient than
    /// `self.to_bytes().len()`.
    fn byte_len(&self) -> usize;

    /// Convert a script value into the bytes that would be included in a transaction.
    fn to_bytes(&self) -> Vec<u8>;

    /// Constrain an arbitrary `PossiblyBad` to the type the `impl` is defined on. This is used when
    /// parsing an arbitrary opcode – we first parse to the most general type `opcode::PossiblyBad`,
    /// and then ensure that it’s of the required type.
    fn restrict(pb: PossiblyBad) -> Result<Self, script::Error>
    where
        Self: Sized;

    /// Evaluate the provided script value.
    fn eval(
        &self,
        flags: interpreter::Flags,
        script: &script::Code,
        checker: &dyn interpreter::SignatureChecker,
        state: interpreter::State,
    ) -> Result<interpreter::State, interpreter::Error>;

    /// If the opcode is a `PushValue`, return that. Otherwise error. This is used to identify
    /// push-only script sigs.
    fn extract_push_value(&self) -> Result<&PushValue, script::Error>;

    /// Upper bound on the signature operations performed by this opcode. In the case of
    /// `OP_CHECKMULTISIG*`, it looks at the previous opcode (if provided) to possibly return a more
    /// accurate bound.
    fn sig_op_count(&self, last_opcode: Option<PossiblyBad>) -> u32;
}

/// Opcodes that represent constants to be pushed onto the stack.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PushValue {
    /// Constants that are represented by a single byte.
    SmallValue(SmallValue),
    /// Constants that contain data in addition to the opcode byte.
    LargeValue(LargeValue),
}

impl PushValue {
    /// Produce a minimal `PushValue` for the given data.
    pub(crate) fn from_slice(v: &[u8]) -> Option<PushValue> {
        match v {
            [] => Some(PushValue::SmallValue(OP_0)),
            [0x81] => Some(PushValue::SmallValue(OP_1NEGATE)),
            [1] => Some(PushValue::SmallValue(OP_1)),
            [2] => Some(PushValue::SmallValue(OP_2)),
            [3] => Some(PushValue::SmallValue(OP_3)),
            [4] => Some(PushValue::SmallValue(OP_4)),
            [5] => Some(PushValue::SmallValue(OP_5)),
            [6] => Some(PushValue::SmallValue(OP_6)),
            [7] => Some(PushValue::SmallValue(OP_7)),
            [8] => Some(PushValue::SmallValue(OP_8)),
            [9] => Some(PushValue::SmallValue(OP_9)),
            [10] => Some(PushValue::SmallValue(OP_10)),
            [11] => Some(PushValue::SmallValue(OP_11)),
            [12] => Some(PushValue::SmallValue(OP_12)),
            [13] => Some(PushValue::SmallValue(OP_13)),
            [14] => Some(PushValue::SmallValue(OP_14)),
            [15] => Some(PushValue::SmallValue(OP_15)),
            [16] => Some(PushValue::SmallValue(OP_16)),
            _ => LargeValue::from_slice(v).map(PushValue::LargeValue),
        }
    }

    /// Statically analyze a push value.
    pub(crate) fn analyze(
        &self,
        flags: &interpreter::Flags,
    ) -> Result<(), Vec<interpreter::Error>> {
        if flags.contains(interpreter::Flags::MinimalData) && !self.is_minimal_push() {
            Err(vec![interpreter::Error::MinimalData])
        } else {
            Ok(())
        }
    }

    /// Returns the numeric value represented by the opcode, if one exists.
    pub fn to_num(&self) -> Result<i64, num::Error> {
        match self {
            PushValue::LargeValue(lv) => lv.to_num(),
            PushValue::SmallValue(sv) => Ok(sv.to_num().into()),
        }
    }

    /// Get the [`interpreter::Stack`] element represented by this [`PushValue`].
    pub fn value(&self) -> Vec<u8> {
        match self {
            PushValue::LargeValue(pv) => pv.value().to_vec(),
            PushValue::SmallValue(pv) => pv.value(),
        }
    }

    /// Returns false if there is a smaller possible encoding of the provided value.
    pub(crate) fn is_minimal_push(&self) -> bool {
        match self {
            PushValue::LargeValue(lv) => lv.is_minimal_push(),
            PushValue::SmallValue(_) => true,
        }
    }

    /// The internal implementation of `Evaluable::eval`. `eval` has larger parameters, because it
    /// needs to be general. This only has the parameters required by the implementation. The
    /// `Evaluable` `impl` extracts the necessary components from the larger parameters and passes
    /// them through.
    fn eval_internal(
        &self,
        require_minimal: bool,
        mut stack: interpreter::Stack<Vec<u8>>,
    ) -> Result<interpreter::Stack<Vec<u8>>, interpreter::Error> {
        if require_minimal && !self.is_minimal_push() {
            Err(interpreter::Error::MinimalData)
        } else {
            stack.push(self.value());
            Ok(stack)
        }
    }
}

impl Evaluable for PushValue {
    fn byte_len(&self) -> usize {
        match self {
            PushValue::LargeValue(pv) => pv.byte_len(),
            PushValue::SmallValue(_) => 1,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        Vec::<u8>::from(self)
    }

    fn restrict(pb: PossiblyBad) -> Result<Self, script::Error> {
        Opcode::restrict(pb).and_then(|op| match op {
            Opcode::PushValue(pv) => Ok(pv),
            _ => Err(script::Error::SigPushOnly),
        })
    }

    fn eval(
        &self,
        flags: interpreter::Flags,
        _script: &script::Code,
        _checker: &dyn interpreter::SignatureChecker,
        mut state: interpreter::State,
    ) -> Result<interpreter::State, interpreter::Error> {
        state.stack =
            self.eval_internal(flags.contains(interpreter::Flags::MinimalData), state.stack)?;
        Ok(state)
    }

    fn extract_push_value(&self) -> Result<&PushValue, script::Error> {
        Ok(self)
    }

    fn sig_op_count(&self, _last_opcode: Option<PossiblyBad>) -> u32 {
        0
    }
}

impl From<SmallValue> for PushValue {
    fn from(value: SmallValue) -> Self {
        Self::SmallValue(value)
    }
}

impl From<LargeValue> for PushValue {
    fn from(value: LargeValue) -> Self {
        Self::LargeValue(value)
    }
}

impl Asm for PushValue {
    fn to_asm(&self, attempt_sighash_decode: bool) -> String {
        match self {
            PushValue::SmallValue(small_value) => small_value.to_asm(false),
            PushValue::LargeValue(large_value) => large_value.to_asm(attempt_sighash_decode),
        }
    }
}

impl core::fmt::Display for PushValue {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.to_asm(false))
    }
}

/// Control operations are evaluated regardless of whether the current branch is active.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[allow(missing_docs)]
#[repr(u8)]
pub enum Control {
    OP_IF = 0x63,
    OP_NOTIF = 0x64,
    OP_ELSE = 0x67,
    OP_ENDIF = 0x68,
}

impl Control {
    /// Decodes this opcode from its byte encoding.
    fn decode(b: u8) -> Option<Self> {
        match b {
            0x63 => Some(Self::OP_IF),
            0x64 => Some(Self::OP_NOTIF),
            0x67 => Some(Self::OP_ELSE),
            0x68 => Some(Self::OP_ENDIF),
            _ => None,
        }
    }

    /// Returns the byte encoding of this opcode.
    pub(crate) fn encode(self) -> u8 {
        // This is how you get the discriminant, but using `as` everywhere is too much code smell
        self as u8
    }

    /// `<expression> if [statements] [else [statements]] endif`
    pub(crate) fn eval(
        &self,
        mut stack: interpreter::Stack<Vec<u8>>,
        mut vexec: interpreter::Stack<bool>,
    ) -> Result<(interpreter::Stack<Vec<u8>>, interpreter::Stack<bool>), interpreter::Error> {
        match self {
            // <expression> if [statements] [else [statements]] endif
            Self::OP_IF | Self::OP_NOTIF => vexec.push(
                interpreter::should_exec(&vexec) && {
                    let value = interpreter::cast_to_bool(&stack.pop()?);
                    if self == &Self::OP_NOTIF {
                        !value
                    } else {
                        value
                    }
                },
            ),

            Self::OP_ELSE => vexec
                .last_mut()
                .map_err(|_| interpreter::Error::UnbalancedConditional)
                .map(|last| *last = !*last)?,

            Self::OP_ENDIF => {
                vexec
                    .pop()
                    .map_err(|_| interpreter::Error::UnbalancedConditional)?;
            }
        }
        Ok((stack, vexec))
    }
}

impl Asm for Control {
    fn to_asm(&self, _attempt_sighash_decode: bool) -> String {
        match self {
            Control::OP_IF => "OP_IF",
            Control::OP_NOTIF => "OP_NOTIF",
            Control::OP_ELSE => "OP_ELSE",
            Control::OP_ENDIF => "OP_ENDIF",
        }
        .into()
    }
}

impl core::fmt::Display for Control {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.to_asm(false))
    }
}

/// Normal operations are only executed when they are on an active branch.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[allow(missing_docs)]
#[repr(u8)]
pub enum Operation {
    // control
    OP_NOP = 0x61,
    OP_VERIFY = 0x69,
    OP_RETURN = 0x6a,

    // stack ops
    OP_TOALTSTACK = 0x6b,
    OP_FROMALTSTACK = 0x6c,
    OP_2DROP = 0x6d,
    OP_2DUP = 0x6e,
    OP_3DUP = 0x6f,
    OP_2OVER = 0x70,
    OP_2ROT = 0x71,
    OP_2SWAP = 0x72,
    OP_IFDUP = 0x73,
    OP_DEPTH = 0x74,
    OP_DROP = 0x75,
    OP_DUP = 0x76,
    OP_NIP = 0x77,
    OP_OVER = 0x78,
    OP_PICK = 0x79,
    OP_ROLL = 0x7a,
    OP_ROT = 0x7b,
    OP_SWAP = 0x7c,
    OP_TUCK = 0x7d,

    // splice ops
    OP_SIZE = 0x82,

    // bit logic
    OP_EQUAL = 0x87,
    OP_EQUALVERIFY = 0x88,

    // numeric
    OP_1ADD = 0x8b,
    OP_1SUB = 0x8c,
    OP_NEGATE = 0x8f,
    OP_ABS = 0x90,
    OP_NOT = 0x91,
    OP_0NOTEQUAL = 0x92,

    OP_ADD = 0x93,
    OP_SUB = 0x94,

    OP_BOOLAND = 0x9a,
    OP_BOOLOR = 0x9b,
    OP_NUMEQUAL = 0x9c,
    OP_NUMEQUALVERIFY = 0x9d,
    OP_NUMNOTEQUAL = 0x9e,
    OP_LESSTHAN = 0x9f,
    OP_GREATERTHAN = 0xa0,
    OP_LESSTHANOREQUAL = 0xa1,
    OP_GREATERTHANOREQUAL = 0xa2,
    OP_MIN = 0xa3,
    OP_MAX = 0xa4,

    OP_WITHIN = 0xa5,

    // crypto
    OP_RIPEMD160 = 0xa6,
    OP_SHA1 = 0xa7,
    OP_SHA256 = 0xa8,
    OP_HASH160 = 0xa9,
    OP_HASH256 = 0xaa,
    OP_CHECKSIG = 0xac,
    OP_CHECKSIGVERIFY = 0xad,
    OP_CHECKMULTISIG = 0xae,
    OP_CHECKMULTISIGVERIFY = 0xaf,

    // expansion
    OP_NOP1 = 0xb0,
    OP_CHECKLOCKTIMEVERIFY = 0xb1,
    OP_NOP3 = 0xb2,
    OP_NOP4 = 0xb3,
    OP_NOP5 = 0xb4,
    OP_NOP6 = 0xb5,
    OP_NOP7 = 0xb6,
    OP_NOP8 = 0xb7,
    OP_NOP9 = 0xb8,
    OP_NOP10 = 0xb9,
}

impl Operation {
    /// Decodes this opcode from its byte encoding.
    fn decode(b: u8) -> Option<Self> {
        match b {
            0x61 => Some(Self::OP_NOP),
            0x69 => Some(Self::OP_VERIFY),
            0x6a => Some(Self::OP_RETURN),
            0x6b => Some(Self::OP_TOALTSTACK),
            0x6c => Some(Self::OP_FROMALTSTACK),
            0x6d => Some(Self::OP_2DROP),
            0x6e => Some(Self::OP_2DUP),
            0x6f => Some(Self::OP_3DUP),
            0x70 => Some(Self::OP_2OVER),
            0x71 => Some(Self::OP_2ROT),
            0x72 => Some(Self::OP_2SWAP),
            0x73 => Some(Self::OP_IFDUP),
            0x74 => Some(Self::OP_DEPTH),
            0x75 => Some(Self::OP_DROP),
            0x76 => Some(Self::OP_DUP),
            0x77 => Some(Self::OP_NIP),
            0x78 => Some(Self::OP_OVER),
            0x79 => Some(Self::OP_PICK),
            0x7a => Some(Self::OP_ROLL),
            0x7b => Some(Self::OP_ROT),
            0x7c => Some(Self::OP_SWAP),
            0x7d => Some(Self::OP_TUCK),
            0x82 => Some(Self::OP_SIZE),
            0x87 => Some(Self::OP_EQUAL),
            0x88 => Some(Self::OP_EQUALVERIFY),
            0x8b => Some(Self::OP_1ADD),
            0x8c => Some(Self::OP_1SUB),
            0x8f => Some(Self::OP_NEGATE),
            0x90 => Some(Self::OP_ABS),
            0x91 => Some(Self::OP_NOT),
            0x92 => Some(Self::OP_0NOTEQUAL),
            0x93 => Some(Self::OP_ADD),
            0x94 => Some(Self::OP_SUB),
            0x9a => Some(Self::OP_BOOLAND),
            0x9b => Some(Self::OP_BOOLOR),
            0x9c => Some(Self::OP_NUMEQUAL),
            0x9d => Some(Self::OP_NUMEQUALVERIFY),
            0x9e => Some(Self::OP_NUMNOTEQUAL),
            0x9f => Some(Self::OP_LESSTHAN),
            0xa0 => Some(Self::OP_GREATERTHAN),
            0xa1 => Some(Self::OP_LESSTHANOREQUAL),
            0xa2 => Some(Self::OP_GREATERTHANOREQUAL),
            0xa3 => Some(Self::OP_MIN),
            0xa4 => Some(Self::OP_MAX),
            0xa5 => Some(Self::OP_WITHIN),
            0xa6 => Some(Self::OP_RIPEMD160),
            0xa7 => Some(Self::OP_SHA1),
            0xa8 => Some(Self::OP_SHA256),
            0xa9 => Some(Self::OP_HASH160),
            0xaa => Some(Self::OP_HASH256),
            0xac => Some(Self::OP_CHECKSIG),
            0xad => Some(Self::OP_CHECKSIGVERIFY),
            0xae => Some(Self::OP_CHECKMULTISIG),
            0xaf => Some(Self::OP_CHECKMULTISIGVERIFY),
            0xb0 => Some(Self::OP_NOP1),
            0xb1 => Some(Self::OP_CHECKLOCKTIMEVERIFY),
            0xb2 => Some(Self::OP_NOP3),
            0xb3 => Some(Self::OP_NOP4),
            0xb4 => Some(Self::OP_NOP5),
            0xb5 => Some(Self::OP_NOP6),
            0xb6 => Some(Self::OP_NOP7),
            0xb7 => Some(Self::OP_NOP8),
            0xb8 => Some(Self::OP_NOP9),
            0xb9 => Some(Self::OP_NOP10),
            _ => None,
        }
    }

    /// Returns the byte encoding of this opcode.
    pub(crate) fn encode(self) -> u8 {
        // This is how you get the discriminant, but using `as` everywhere is too much code smell
        self as u8
    }

    /// Statically analyze an operation.
    ///
    /// __NB__: [`Operation::OP_RETURN`] isn’t tracked by this function. That is functionally
    ///         more like a `break` then an error.
    pub(crate) fn analyze(
        &self,
        flags: &interpreter::Flags,
    ) -> Result<(), Vec<interpreter::Error>> {
        match self {
            Self::OP_CHECKLOCKTIMEVERIFY
                if !flags.contains(interpreter::Flags::CHECKLOCKTIMEVERIFY)
                    && flags.contains(interpreter::Flags::DiscourageUpgradableNOPs) =>
            {
                Err(vec![interpreter::Error::DiscourageUpgradableNOPs])
            }
            Self::OP_NOP1
            | Self::OP_NOP3
            | Self::OP_NOP4
            | Self::OP_NOP5
            | Self::OP_NOP6
            | Self::OP_NOP7
            | Self::OP_NOP8
            | Self::OP_NOP9
            | Self::OP_NOP10
                if flags.contains(interpreter::Flags::DiscourageUpgradableNOPs) =>
            {
                Err(vec![interpreter::Error::DiscourageUpgradableNOPs])
            }

            _ => Ok(()),
        }
    }

    /// The number of signature operations represented by this opcode. If the `last_opcode` is
    /// provided, we can sometimes give a more accurate value.
    pub(crate) fn sig_op_count(&self, last_opcode: Option<PossiblyBad>) -> u32 {
        match self {
            Self::OP_CHECKSIG | Self::OP_CHECKSIGVERIFY => 1,
            Self::OP_CHECKMULTISIG | Self::OP_CHECKMULTISIGVERIFY => {
                match last_opcode {
                    // Even with an accurate count, 0 keys is counted as 20 for some reason.
                    Some(PossiblyBad::Good(Opcode::PushValue(PushValue::SmallValue(sv))))
                        if push_value::SmallValue::OP_1 <= sv =>
                    {
                        u32::try_from(sv.to_num()).expect("`sv` is positive")
                    }
                    // Apparently it’s too much work to figure out if it’s one of the few
                    // `LargeValue`s that’s valid, so we assume the worst.
                    Some(_) => u32::from(interpreter::MAX_PUBKEY_COUNT),
                    // We don’t know what the previous opcode could be – assume the worst.
                    None => u32::from(interpreter::MAX_PUBKEY_COUNT),
                }
            }
            _ => 0,
        }
    }

    fn binbasic_num<R>(
        stack: &mut interpreter::Stack<Vec<u8>>,
        require_minimal: bool,
        op: impl FnOnce(i64, i64) -> Result<R, interpreter::Error>,
    ) -> Result<R, interpreter::Error> {
        stack.binfn(|x1, x2| {
            let bn2 = num::parse(&x2, require_minimal, None).map_err(interpreter::Error::Num)?;
            let bn1 = num::parse(&x1, require_minimal, None).map_err(interpreter::Error::Num)?;
            op(bn1, bn2)
        })
    }

    #[cfg(feature = "signature-validation")]
    fn is_compressed_or_uncompressed_pub_key(vch_pub_key: &[u8]) -> bool {
        match vch_pub_key.first() {
            Some(0x02 | 0x03) => vch_pub_key.len() == PubKey::COMPRESSED_SIZE,
            Some(0x04) => vch_pub_key.len() == PubKey::SIZE,
            _ => false, // not a public key
        }
    }

    #[cfg(feature = "signature-validation")]
    fn check_pub_key_encoding(
        vch_sig: &[u8],
        flags: interpreter::Flags,
    ) -> Result<(), interpreter::Error> {
        if flags.contains(interpreter::Flags::StrictEnc)
            && !Self::is_compressed_or_uncompressed_pub_key(vch_sig)
        {
            return Err(interpreter::Error::PubKeyType);
        };
        Ok(())
    }

    #[cfg(feature = "signature-validation")]
    fn is_sig_valid(
        vch_sig: &[u8],
        vch_pub_key: &[u8],
        flags: interpreter::Flags,
        script: &script::Code,
        checker: &dyn interpreter::SignatureChecker,
    ) -> Result<bool, interpreter::Error> {
        // Note how this makes the exact order of pubkey/signature evaluation distinguishable by
        // CHECKMULTISIG NOT if the STRICTENC flag is set. See the script_(in)valid tests for details.
        match signature::Decoded::from_bytes(
            vch_sig,
            flags.contains(interpreter::Flags::LowS),
            flags.contains(interpreter::Flags::StrictEnc),
        ) {
            signature::Validity::InvalidAbort(e) => Err(interpreter::Error::from(e)),
            signature::Validity::InvalidContinue => {
                // We still need to check the pubkey here, because it can cause an abort.
                Self::check_pub_key_encoding(vch_pub_key, flags)?;
                Ok(false)
            }
            signature::Validity::Valid(sig) => {
                Self::check_pub_key_encoding(vch_pub_key, flags)?;
                Ok(checker.check_sig(&sig, vch_pub_key, script))
            }
        }
    }

    #[cfg(not(feature = "signature-validation"))]
    fn is_sig_valid(
        _vch_sig: &[u8],
        _vch_pub_key: &[u8],
        _flags: interpreter::Flags,
        _script: &script::Code,
        _checker: &dyn interpreter::SignatureChecker,
    ) -> Result<bool, interpreter::Error> {
        Ok(false)
    }

    fn cast_from_bool(b: bool) -> Vec<u8> {
        static VCH_FALSE: [u8; 0] = [];
        static VCH_TRUE: [u8; 1] = [1];
        if b {
            VCH_TRUE.to_vec()
        } else {
            VCH_FALSE.to_vec()
        }
    }

    /// Evaluate a single operation.
    pub(crate) fn eval(
        &self,
        flags: interpreter::Flags,
        script: &script::Code,
        checker: &dyn interpreter::SignatureChecker,
        mut state: interpreter::State,
    ) -> Result<interpreter::State, interpreter::Error> {
        let require_minimal = flags.contains(interpreter::Flags::MinimalData);

        let parse_num = |v: &[u8], size: Option<usize>| -> Result<i64, interpreter::Error> {
            num::parse(v, require_minimal, size).map_err(interpreter::Error::Num)
        };

        let pop_num = |stack: &mut interpreter::Stack<Vec<u8>>,
                       size: Option<usize>|
         -> Result<i64, interpreter::Error> {
            stack.pop().and_then(|v| parse_num(&v, size))
        };

        let unfn_num = |stackin: &mut interpreter::Stack<Vec<u8>>,
                        op: &dyn Fn(i64) -> Vec<u8>|
         -> Result<(), interpreter::Error> {
            stackin.unop(|vch| parse_num(&vch, None).map(op))
        };

        let unop_num = |stack: &mut interpreter::Stack<Vec<u8>>,
                        op: &dyn Fn(i64) -> i64|
         -> Result<(), interpreter::Error> {
            unfn_num(stack, &|bn| num::serialize(op(bn)))
        };

        let binfn_num = |stack: &mut interpreter::Stack<Vec<u8>>,
                         op: &dyn Fn(i64, i64) -> Vec<u8>|
         -> Result<(), interpreter::Error> {
            Self::binbasic_num(stack, require_minimal, |bn1, bn2| Ok(op(bn1, bn2)))
                .map(|res| stack.push(res))
        };

        let binop_num = |stack: &mut interpreter::Stack<Vec<u8>>,
                         op: &dyn Fn(i64, i64) -> i64|
         -> Result<(), interpreter::Error> {
            binfn_num(stack, &|bn1, bn2| num::serialize(op(bn1, bn2)))
        };

        let binrel = |stack: &mut interpreter::Stack<Vec<u8>>,
                      op: &dyn Fn(i64, i64) -> bool|
         -> Result<(), interpreter::Error> {
            binfn_num(stack, &|bn1, bn2| Self::cast_from_bool(op(bn1, bn2)))
        };

        let unrel = |stack: &mut interpreter::Stack<Vec<u8>>,
                     op: &dyn Fn(i64) -> bool|
         -> Result<(), interpreter::Error> {
            unfn_num(stack, &|bn| Self::cast_from_bool(op(bn)))
        };

        match self {
            //
            // Control
            //
            Self::OP_NOP => Ok(()),

            // (lt -- lt bool)
            Self::OP_CHECKLOCKTIMEVERIFY => {
                // https://zips.z.cash/protocol/protocol.pdf#bips :
                //
                //   The following BIPs apply starting from the Zcash genesis block,
                //   i.e. any activation rules or exceptions for particular blocks in
                //   the Bitcoin block chain are to be ignored: [BIP-16], [BIP-30],
                //   [BIP-65], [BIP-66].
                //
                // So BIP 65, which defines CHECKLOCKTIMEVERIFY, is in practice always
                // enabled, and this `if` branch is dead code. In zcashd see
                // https://github.com/zcash/zcash/blob/a3435336b0c561799ac6805a27993eca3f9656df/src/main.cpp#L3151
                if flags.contains(interpreter::Flags::CHECKLOCKTIMEVERIFY) {
                    // Note that elsewhere numeric opcodes are limited to
                    // operands in the range -2**31+1 to 2**31-1, however it is
                    // legal for opcodes to produce results exceeding that
                    // range. This limitation is implemented by `ScriptNum`'s
                    // default 4-byte limit.
                    //
                    // If we kept to that limit we'd have a year 2038 problem,
                    // even though the `lock_time` field in transactions
                    // themselves is u32 which only becomes meaningless
                    // after the year 2106.
                    //
                    // Thus as a special case we tell `ScriptNum` to accept up
                    // to 5-byte bignums, which are good until 2**39-1, well
                    // beyond the 2**32-1 limit of the `lock_time` field itself.
                    let lock_time = state.stack.rget(0).and_then(|v| parse_num(v, Some(5)))?;

                    // In the rare event that the argument may be < 0 due to
                    // some arithmetic being done first, you can always use
                    // 0 MAX CHECKLOCKTIMEVERIFY.
                    if 0 <= lock_time {
                        // Actually compare the specified lock time with the transaction.
                        if !checker.check_lock_time(lock_time) {
                            Err(interpreter::Error::UnsatisfiedLockTime)
                        } else {
                            Ok(())
                        }
                    } else {
                        Err(interpreter::Error::NegativeLockTime)
                    }
                } else if flags.contains(interpreter::Flags::DiscourageUpgradableNOPs) {
                    Err(interpreter::Error::DiscourageUpgradableNOPs)
                } else {
                    Ok(())
                }
            }

            Self::OP_NOP1
            | Self::OP_NOP3
            | Self::OP_NOP4
            | Self::OP_NOP5
            | Self::OP_NOP6
            | Self::OP_NOP7
            | Self::OP_NOP8
            | Self::OP_NOP9
            | Self::OP_NOP10 => {
                // Do nothing, though if the caller wants to prevent people from using
                // these NOPs (as part of a standard tx rule, for example) they can
                // enable `DiscourageUpgradableNOPs` to turn these opcodes into errors.
                if flags.contains(interpreter::Flags::DiscourageUpgradableNOPs) {
                    Err(interpreter::Error::DiscourageUpgradableNOPs)
                } else {
                    Ok(())
                }
            }

            Self::OP_VERIFY => {
                // (true -- ) or
                // (false -- false) and return
                state.stack.pop().and_then(|v| {
                    if interpreter::cast_to_bool(&v) {
                        Ok(())
                    } else {
                        Err(interpreter::Error::Verify)
                    }
                })
            }

            Self::OP_RETURN => Err(interpreter::Error::OpReturn),

            //
            // Stack ops
            //
            Self::OP_TOALTSTACK => state.stack.pop().map(|v| state.altstack.push(v)),

            Self::OP_FROMALTSTACK => state.altstack.pop().map(|v| state.stack.push(v)),

            Self::OP_2DROP => state
                .stack
                .pop()
                .and_then(|_| state.stack.pop())
                .map(|_| ()),

            // (x1 x2 -- x1 x2 x1 x2)
            Self::OP_2DUP => state.stack.repush(1).and_then(|()| state.stack.repush(1)),

            // (x1 x2 x3 -- x1 x2 x3 x1 x2 x3)
            Self::OP_3DUP => {
                state.stack.repush(2)?;
                state.stack.repush(2)?;
                state.stack.repush(2)
            }

            // (x1 x2 x3 x4 -- x1 x2 x3 x4 x1 x2)
            Self::OP_2OVER => state.stack.repush(3).and_then(|()| state.stack.repush(3)),

            // (x1 x2 x3 x4 x5 x6 -- x3 x4 x5 x6 x1 x2)
            Self::OP_2ROT => state
                .stack
                .move_to_top(5)
                .and_then(|()| state.stack.move_to_top(5)),

            // (x1 x2 x3 x4 -- x3 x4 x1 x2)
            Self::OP_2SWAP => state
                .stack
                .move_to_top(3)
                .and_then(|()| state.stack.move_to_top(3)),

            // (x - 0 | x x)
            Self::OP_IFDUP => state.stack.rget(0).cloned().map(|v| {
                if interpreter::cast_to_bool(&v) {
                    state.stack.push(v)
                }
            }),

            // -- stacksize
            Self::OP_DEPTH => i64::try_from(state.stack.len())
                .map_err(|err| interpreter::Error::StackSize(Some(err)))
                .map(|n| state.stack.push(num::serialize(n))),

            // (x -- )
            Self::OP_DROP => state.stack.pop().map(|_| ()),

            // (x -- x x)
            Self::OP_DUP => state.stack.repush(0),

            // (x1 x2 -- x2)
            Self::OP_NIP => state.stack.rremove(1).map(|_| ()),

            // (x1 x2 -- x1 x2 x1)
            Self::OP_OVER => state.stack.repush(1),

            // (xn ... x2 x1 x0 n - xn ... x2 x1 x0 xn)
            // (xn ... x2 x1 x0 n - ... x2 x1 x0 xn)
            Self::OP_PICK | Self::OP_ROLL => state.stack.check_len(2).and_then(|()| {
                pop_num(&mut state.stack, None)
                    .and_then(|i| {
                        usize::try_from(i)
                            .map_err(|_| interpreter::Error::InvalidStackOperation(None))
                    })
                    .and_then(|n| {
                        if self == &Self::OP_ROLL {
                            state.stack.move_to_top(n)
                        } else {
                            state.stack.repush(n)
                        }
                    })
            }),

            // (x1 x2 x3 -- x2 x3 x1)
            Self::OP_ROT => state.stack.move_to_top(2),

            // (x1 x2 -- x2 x1)
            Self::OP_SWAP => state.stack.move_to_top(1),

            // (x1 x2 -- x2 x1 x2)
            Self::OP_TUCK => state.stack.rinsert(1, state.stack.rget(0)?.clone()),

            // (in -- in size)
            Self::OP_SIZE => state.stack.rget(0).cloned().map(|v| {
                state.stack.push(num::serialize(
                    i64::try_from(v.len()).expect("stack element size <= PushValue::MAX_SIZE"),
                ))
            }),

            //
            // Bitwise logic
            //
            // (x1 x2 - bool)
            Self::OP_EQUAL => state
                .stack
                .binop(|x1, x2| Ok(Self::cast_from_bool(x1 == x2))),
            Self::OP_EQUALVERIFY => state.stack.binfn(|x1, x2| {
                if x1 == x2 {
                    Ok(())
                } else {
                    Err(interpreter::Error::Verify)
                }
            }),

            //
            // Numeric
            //

            // (in -- out)
            Self::OP_1ADD => unop_num(&mut state.stack, &|x| x + 1),
            Self::OP_1SUB => unop_num(&mut state.stack, &|x| x - 1),
            Self::OP_NEGATE => unop_num(&mut state.stack, &|x| -x),
            Self::OP_ABS => unop_num(&mut state.stack, &|x| x.abs()),
            Self::OP_NOT => unrel(&mut state.stack, &|x| x == 0),
            Self::OP_0NOTEQUAL => unrel(&mut state.stack, &|x| x != 0),

            // (x1 x2 -- out)
            Self::OP_ADD => binop_num(&mut state.stack, &|x1, x2| x1 + x2),
            Self::OP_SUB => binop_num(&mut state.stack, &|x1, x2| x1 - x2),
            Self::OP_BOOLAND => binrel(&mut state.stack, &|x1, x2| x1 != 0 && x2 != 0),
            Self::OP_BOOLOR => binrel(&mut state.stack, &|x1, x2| x1 != 0 || x2 != 0),
            Self::OP_NUMEQUAL => binrel(&mut state.stack, &|x1, x2| x1 == x2),
            Self::OP_NUMEQUALVERIFY => {
                Self::binbasic_num(&mut state.stack, require_minimal, |x1, x2| {
                    if x1 == x2 {
                        Ok(())
                    } else {
                        Err(interpreter::Error::Verify)
                    }
                })
            }
            Self::OP_NUMNOTEQUAL => binrel(&mut state.stack, &|x1, x2| x1 != x2),
            Self::OP_LESSTHAN => binrel(&mut state.stack, &|x1, x2| x1 < x2),
            Self::OP_GREATERTHAN => binrel(&mut state.stack, &|x1, x2| x1 > x2),
            Self::OP_LESSTHANOREQUAL => binrel(&mut state.stack, &|x1, x2| x1 <= x2),
            Self::OP_GREATERTHANOREQUAL => binrel(&mut state.stack, &|x1, x2| x1 >= x2),
            Self::OP_MIN => binop_num(&mut state.stack, &min),
            Self::OP_MAX => binop_num(&mut state.stack, &max),

            // (x min max -- out)
            Self::OP_WITHIN => {
                // We have to check them in this order to make sure we get any errors in the same order
                // as the C++ impl.
                let x = state.stack.rremove(2).and_then(|v| parse_num(&v, None))?;
                let min = state.stack.rremove(1).and_then(|v| parse_num(&v, None))?;
                let max = pop_num(&mut state.stack, None)?;
                Ok(state.stack.push(Self::cast_from_bool(min <= x && x < max)))
            }

            //
            // Crypto
            //

            // (in -- hash)
            Self::OP_RIPEMD160
            | Self::OP_SHA1
            | Self::OP_SHA256
            | Self::OP_HASH160
            | Self::OP_HASH256 => {
                let vch = state.stack.pop()?;
                let mut vch_hash = vec![];
                if self == &Self::OP_RIPEMD160 {
                    vch_hash = Ripemd160::digest(vch).to_vec();
                } else if self == &Self::OP_SHA1 {
                    let mut hasher = Sha1::new();
                    hasher.update(vch);
                    vch_hash = hasher.finalize().to_vec();
                } else if self == &Self::OP_SHA256 {
                    vch_hash = Sha256::digest(vch).to_vec();
                } else if self == &Self::OP_HASH160 {
                    vch_hash = Ripemd160::digest(Sha256::digest(vch)).to_vec();
                } else if self == &Self::OP_HASH256 {
                    vch_hash = Sha256::digest(Sha256::digest(vch)).to_vec();
                }
                Ok(state.stack.push(vch_hash))
            }

            // (sig pubkey -- bool)
            Self::OP_CHECKSIG | Self::OP_CHECKSIGVERIFY => {
                let vch_pub_key = state.stack.pop()?;
                let vch_sig = state.stack.pop()?;

                Self::is_sig_valid(&vch_sig, &vch_pub_key, flags, script, checker).and_then(
                    |success| {
                        if self == &Self::OP_CHECKSIGVERIFY {
                            if success {
                                Ok(())
                            } else {
                                Err(interpreter::Error::Verify)
                            }
                        } else {
                            Ok(state.stack.push(Self::cast_from_bool(success)))
                        }
                    },
                )
            }

            // ([sig ...] num_of_signatures [pubkey ...] num_of_pubkeys -- bool)
            Self::OP_CHECKMULTISIG | Self::OP_CHECKMULTISIGVERIFY => {
                let mut keys_count = pop_num(&mut state.stack, None).and_then(|i| {
                    u8::try_from(i).map_err(|err| interpreter::Error::PubKeyCount(Some(err)))
                })?;
                if keys_count > interpreter::MAX_PUBKEY_COUNT {
                    return Err(interpreter::Error::PubKeyCount(None));
                };
                state.increment_op_count(keys_count)?;

                // NB: This is guaranteed u8-safe, because we are limited to 20 keys and
                //     20 signatures, plus a couple other fields. u8 also gives us total
                //     conversions to the other types we deal with here (`usize` and `i64`).
                let mut i: u8 = keys_count;
                let mut ikey: u8 = 0;
                assert!(i <= interpreter::MAX_PUBKEY_COUNT);

                let mut sigs_count = state
                    .stack
                    .rget(i.into())
                    .and_then(|v| parse_num(v, None))
                    .and_then(|i| {
                        u8::try_from(i).map_err(|err| interpreter::Error::SigCount(Some(err)))
                    })?;
                if sigs_count > keys_count {
                    return Err(interpreter::Error::SigCount(None));
                };
                i += 1;
                let mut isig = i;
                i += sigs_count;
                state.stack.check_len(usize::from(i) + 1)?;

                let mut success = true;
                while success && sigs_count > 0 {
                    let vch_sig = state.stack.rget(usize::from(isig))?;
                    let vch_pub_key = state.stack.rget(usize::from(ikey))?;

                    // Check signature
                    let ok: bool =
                        Self::is_sig_valid(vch_sig, vch_pub_key, flags, script, checker)?;

                    if ok {
                        isig += 1;
                        sigs_count -= 1;
                    }
                    ikey += 1;
                    keys_count -= 1;

                    // If there are more signatures left than keys left,
                    // then too many signatures have failed. Exit early,
                    // without checking any further signatures.
                    if sigs_count > keys_count {
                        success = false;
                    };
                }

                // Clean up stack of actual arguments
                for _ in 0..i {
                    state.stack.pop()?;
                }

                // A bug causes CHECKMULTISIG to consume one extra argument
                // whose contents were not checked in any way.
                //
                // Unfortunately this is a potential source of mutability,
                // so optionally verify it is exactly equal to zero prior
                // to removing it from the stack.
                if flags.contains(interpreter::Flags::NullDummy) && !state.stack.rget(0)?.is_empty()
                {
                    return Err(interpreter::Error::SigNullDummy);
                }
                state.stack.pop()?;

                if self == &Self::OP_CHECKMULTISIGVERIFY {
                    if success {
                        Ok(())
                    } else {
                        Err(interpreter::Error::Verify)
                    }
                } else {
                    Ok(state.stack.push(Self::cast_from_bool(success)))
                }
            }
        }
        .map(|()| state)
    }
}

impl From<&PushValue> for Vec<u8> {
    fn from(value: &PushValue) -> Self {
        match value {
            PushValue::SmallValue(v) => vec![(*v).encode()],
            PushValue::LargeValue(v) => v.into(),
        }
    }
}

impl Asm for Operation {
    fn to_asm(&self, _attempt_sighash_decode: bool) -> String {
        match self {
            Operation::OP_NOP => "OP_NOP",
            Operation::OP_VERIFY => "OP_VERIFY",
            Operation::OP_RETURN => "OP_RETURN",
            Operation::OP_TOALTSTACK => "OP_TOALTSTACK",
            Operation::OP_FROMALTSTACK => "OP_FROMALTSTACK",
            Operation::OP_2DROP => "OP_2DROP",
            Operation::OP_2DUP => "OP_2DUP",
            Operation::OP_3DUP => "OP_3DUP",
            Operation::OP_2OVER => "OP_2OVER",
            Operation::OP_2ROT => "OP_2ROT",
            Operation::OP_2SWAP => "OP_2SWAP",
            Operation::OP_IFDUP => "OP_IFDUP",
            Operation::OP_DEPTH => "OP_DEPTH",
            Operation::OP_DROP => "OP_DROP",
            Operation::OP_DUP => "OP_DUP",
            Operation::OP_NIP => "OP_NIP",
            Operation::OP_OVER => "OP_OVER",
            Operation::OP_PICK => "OP_PICK",
            Operation::OP_ROLL => "OP_ROLL",
            Operation::OP_ROT => "OP_ROT",
            Operation::OP_SWAP => "OP_SWAP",
            Operation::OP_TUCK => "OP_TUCK",
            Operation::OP_SIZE => "OP_SIZE",
            Operation::OP_EQUAL => "OP_EQUAL",
            Operation::OP_EQUALVERIFY => "OP_EQUALVERIFY",
            Operation::OP_1ADD => "OP_1ADD",
            Operation::OP_1SUB => "OP_1SUB",
            Operation::OP_NEGATE => "OP_NEGATE",
            Operation::OP_ABS => "OP_ABS",
            Operation::OP_NOT => "OP_NOT",
            Operation::OP_0NOTEQUAL => "OP_0NOTEQUAL",
            Operation::OP_ADD => "OP_ADD",
            Operation::OP_SUB => "OP_SUB",
            Operation::OP_BOOLAND => "OP_BOOLAND",
            Operation::OP_BOOLOR => "OP_BOOLOR",
            Operation::OP_NUMEQUAL => "OP_NUMEQUAL",
            Operation::OP_NUMEQUALVERIFY => "OP_NUMEQUALVERIFY",
            Operation::OP_NUMNOTEQUAL => "OP_NUMNOTEQUAL",
            Operation::OP_LESSTHAN => "OP_LESSTHAN",
            Operation::OP_GREATERTHAN => "OP_GREATERTHAN",
            Operation::OP_LESSTHANOREQUAL => "OP_LESSTHANOREQUAL",
            Operation::OP_GREATERTHANOREQUAL => "OP_GREATERTHANOREQUAL",
            Operation::OP_MIN => "OP_MIN",
            Operation::OP_MAX => "OP_MAX",
            Operation::OP_WITHIN => "OP_WITHIN",
            Operation::OP_RIPEMD160 => "OP_RIPEMD160",
            Operation::OP_SHA1 => "OP_SHA1",
            Operation::OP_SHA256 => "OP_SHA256",
            Operation::OP_HASH160 => "OP_HASH160",
            Operation::OP_HASH256 => "OP_HASH256",
            Operation::OP_CHECKSIG => "OP_CHECKSIG",
            Operation::OP_CHECKSIGVERIFY => "OP_CHECKSIGVERIFY",
            Operation::OP_CHECKMULTISIG => "OP_CHECKMULTISIG",
            Operation::OP_CHECKMULTISIGVERIFY => "OP_CHECKMULTISIGVERIFY",
            Operation::OP_NOP1 => "OP_NOP1",
            Operation::OP_CHECKLOCKTIMEVERIFY => "OP_NOP2",
            Operation::OP_NOP3 => "OP_NOP3",
            Operation::OP_NOP4 => "OP_NOP4",
            Operation::OP_NOP5 => "OP_NOP5",
            Operation::OP_NOP6 => "OP_NOP6",
            Operation::OP_NOP7 => "OP_NOP7",
            Operation::OP_NOP8 => "OP_NOP8",
            Operation::OP_NOP9 => "OP_NOP9",
            Operation::OP_NOP10 => "OP_NOP10",
        }
        .into()
    }
}

impl core::fmt::Display for Operation {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.to_asm(false))
    }
}

/// Opcodes that fail if they’re on an active branch.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[allow(missing_docs)]
pub enum Bad {
    OP_RESERVED,
    OP_VER,
    OP_VERIF,
    OP_VERNOTIF,
    OP_RESERVED1,
    OP_RESERVED2,
    Unknown(u8),
}

impl Bad {
    /// Bad opcodes are a bit complicated.
    ///
    /// - They only fail if they are evaluated, so we can’t statically fail scripts that contain them
    ///   (unlike [Disabled]).
    /// - [Bad::OP_RESERVED] counts as a push value for the purposes of
    ///   [interpreter::Flags::SigPushOnly] (but push-only sigs must necessarily evaluate
    ///   all of their opcodes, so what we’re preserving here is that we get
    ///   [`script::Error::SigPushOnly`] in this case instead of [`interpreter::Error::BadOpcode`]).
    /// - [Bad::OP_VERIF] and [Bad::OP_VERNOTIF] both _always_ get evaluated, so we need to special case
    ///   them when checking whether to throw [`interpreter::Error::BadOpcode`]
    fn eval(
        &self,
        mut state: interpreter::State,
    ) -> Result<interpreter::State, interpreter::Error> {
        // Note how OP_RESERVED does not count towards the opcode limit.
        if &Self::OP_RESERVED != self {
            state.increment_op_count(1)?;
        }
        if matches!(self, Self::OP_VERIF | Self::OP_VERNOTIF)
            || interpreter::should_exec(&state.vexec)
        {
            Err(interpreter::Error::BadOpcode)
        } else {
            Ok(state)
        }
    }
}

impl Bad {
    /// Decodes this opcode from its byte encoding.
    fn decode(value: u8) -> Self {
        match value {
            0x50 => Bad::OP_RESERVED,
            0x62 => Bad::OP_VER,
            0x65 => Bad::OP_VERIF,
            0x66 => Bad::OP_VERNOTIF,
            0x89 => Bad::OP_RESERVED1,
            0x8a => Bad::OP_RESERVED2,
            _ => Bad::Unknown(value),
        }
    }

    /// Returns the byte encoding of this opcode.
    pub(crate) fn encode(self) -> u8 {
        match self {
            Bad::OP_RESERVED => 0x50,
            Bad::OP_VER => 0x62,
            Bad::OP_VERIF => 0x65,
            Bad::OP_VERNOTIF => 0x66,
            Bad::OP_RESERVED1 => 0x89,
            Bad::OP_RESERVED2 => 0x8a,
            Bad::Unknown(byte) => byte,
        }
    }
}

/// When writing scripts, we don’t want to allow bad opcodes, so `Opcode` doesn’t include them.
/// However, when validating scripts, bad opcodes only cause a failure when they’re on an active
/// branch, so this type allows us to hold onto the bad opcodes when parsing.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PossiblyBad {
    /// An opcode that can be evaluated.
    Good(Opcode),
    /// An opcode that is valid in a script, but fails if evaluated.
    Bad(Bad),
}

impl PossiblyBad {
    /// This parses a single opcode from a byte stream.
    ///
    /// This always returns the unparsed bytes, because parsing failures don’t invalidate the
    /// remainder of the stream (if any).
    pub fn parse(script: &[u8]) -> (Result<Self, Error>, &[u8]) {
        match push_value::LargeValue::parse(script) {
            None => match script.split_first() {
                None => (
                    Err(Error::Read {
                        expected_bytes: 1,
                        available_bytes: 0,
                    }),
                    &[],
                ),
                Some((&leading_byte, remaining_code)) => (
                    Disabled::decode(leading_byte).map_or(
                        Ok(
                            if let Some(sv) = push_value::SmallValue::decode(leading_byte) {
                                Self::from(Opcode::from(PushValue::SmallValue(sv)))
                            } else if let Some(ctl) = Control::decode(leading_byte) {
                                Self::from(Opcode::Control(ctl))
                            } else if let Some(op) = Operation::decode(leading_byte) {
                                Self::from(Opcode::Operation(op))
                            } else {
                                Self::from(Bad::decode(leading_byte))
                            },
                        ),
                        |disabled| Err(Error::Disabled(Some(disabled))),
                    ),
                    remaining_code,
                ),
            },
            Some((res, remaining_code)) => (
                res.map(|v| Self::from(Opcode::from(PushValue::LargeValue(v)))),
                remaining_code,
            ),
        }
    }

    /// Statically analyze a possibly-bad opcode.
    pub(crate) fn analyze(
        &self,
        flags: &interpreter::Flags,
    ) -> Result<&Opcode, Vec<interpreter::Error>> {
        match self {
            Self::Good(op) => op.analyze(flags).map(|()| op),
            Self::Bad(_) => Err(vec![interpreter::Error::BadOpcode]),
        }
    }
}

impl Evaluable for PossiblyBad {
    fn byte_len(&self) -> usize {
        match self {
            Self::Good(op) => op.byte_len(),
            Self::Bad(_) => 1,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        Vec::<u8>::from(self)
    }

    fn restrict(pb: PossiblyBad) -> Result<Self, script::Error> {
        Ok(pb)
    }

    /// Eval a single [`Opcode`] … which may be [`Bad`].
    fn eval(
        &self,
        flags: interpreter::Flags,
        script: &script::Code,
        checker: &dyn interpreter::SignatureChecker,
        state: interpreter::State,
    ) -> Result<interpreter::State, interpreter::Error> {
        match self {
            Self::Bad(bad) => bad.eval(state),
            Self::Good(opcode) => opcode.eval(flags, script, checker, state),
        }
    }

    fn extract_push_value(&self) -> Result<&PushValue, script::Error> {
        match self {
            Self::Good(op) => op.extract_push_value(),
            Self::Bad(_) => Err(script::Error::Interpreter(
                Some(self.clone()),
                interpreter::Error::BadOpcode,
            )),
        }
    }

    fn sig_op_count(&self, last_opcode: Option<PossiblyBad>) -> u32 {
        match self {
            Self::Good(op) => op.sig_op_count(last_opcode),
            Self::Bad(_) => 0,
        }
    }
}

impl From<Opcode> for PossiblyBad {
    fn from(value: Opcode) -> Self {
        Self::Good(value)
    }
}

impl From<Bad> for PossiblyBad {
    fn from(value: Bad) -> Self {
        Self::Bad(value)
    }
}

impl From<PushValue> for PossiblyBad {
    fn from(value: PushValue) -> Self {
        Self::Good(Opcode::from(value))
    }
}

impl From<&PossiblyBad> for Vec<u8> {
    fn from(value: &PossiblyBad) -> Self {
        match value {
            PossiblyBad::Good(opcode) => opcode.into(),
            PossiblyBad::Bad(bad) => vec![bad.encode()],
        }
    }
}

/// Opcodes that are invalid in a script, regardless of whether they would ever be evaluated.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[allow(missing_docs)]
#[repr(u8)]
pub enum Disabled {
    // splice ops
    OP_CAT = 0x7e,
    OP_SUBSTR = 0x7f,
    OP_LEFT = 0x80,
    OP_RIGHT = 0x81,
    // bit logic
    OP_INVERT = 0x83,
    OP_AND = 0x84,
    OP_OR = 0x85,
    OP_XOR = 0x86,
    // numeric
    OP_2MUL = 0x8d,
    OP_2DIV = 0x8e,
    OP_MUL = 0x95,
    OP_DIV = 0x96,
    OP_MOD = 0x97,
    OP_LSHIFT = 0x98,
    OP_RSHIFT = 0x99,

    //crypto
    OP_CODESEPARATOR = 0xab,
}

impl Disabled {
    /// Decodes this opcode from its byte encoding.
    fn decode(b: u8) -> Option<Self> {
        match b {
            0x7e => Some(Self::OP_CAT),
            0x7f => Some(Self::OP_SUBSTR),
            0x80 => Some(Self::OP_LEFT),
            0x81 => Some(Self::OP_RIGHT),
            0x83 => Some(Self::OP_INVERT),
            0x84 => Some(Self::OP_AND),
            0x85 => Some(Self::OP_OR),
            0x86 => Some(Self::OP_XOR),
            0x8d => Some(Self::OP_2MUL),
            0x8e => Some(Self::OP_2DIV),
            0x95 => Some(Self::OP_MUL),
            0x96 => Some(Self::OP_DIV),
            0x97 => Some(Self::OP_MOD),
            0x98 => Some(Self::OP_LSHIFT),
            0x99 => Some(Self::OP_RSHIFT),
            0xab => Some(Self::OP_CODESEPARATOR),
            _ => None,
        }
    }

    /// Returns the byte encoding of this opcode.
    #[cfg(any(test, feature = "test-dependencies"))]
    pub(crate) fn encode(self) -> u8 {
        // This is how you get the discriminant, but using `as` everywhere is too much code smell
        self as u8
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec::Vec;

    use crate::{op, Opcode};

    use super::{push_value::LargeValue, Control, Disabled, Operation, PushValue, SmallValue};

    #[test]
    fn round_trip_opcode_encodings() {
        for op in [
            op::_0,
            op::_1NEGATE,
            op::_1,
            op::_2,
            op::_3,
            op::_4,
            op::_5,
            op::_6,
            op::_7,
            op::_8,
            op::_9,
            op::_10,
            op::_11,
            op::_12,
            op::_13,
            op::_14,
            op::_15,
            op::_16,
            op::NOP,
            op::IF,
            op::NOTIF,
            op::ELSE,
            op::ENDIF,
            op::VERIFY,
            op::RETURN,
            op::TOALTSTACK,
            op::FROMALTSTACK,
            op::_2DROP,
            op::_2DUP,
            op::_3DUP,
            op::_2OVER,
            op::_2ROT,
            op::_2SWAP,
            op::IFDUP,
            op::DEPTH,
            op::DROP,
            op::DUP,
            op::NIP,
            op::OVER,
            op::PICK,
            op::ROLL,
            op::ROT,
            op::SWAP,
            op::TUCK,
            op::SIZE,
            op::EQUAL,
            op::EQUALVERIFY,
            op::_1ADD,
            op::_1SUB,
            op::NEGATE,
            op::ABS,
            op::NOT,
            op::_0NOTEQUAL,
            op::ADD,
            op::SUB,
            op::BOOLAND,
            op::BOOLOR,
            op::NUMEQUAL,
            op::NUMEQUALVERIFY,
            op::NUMNOTEQUAL,
            op::LESSTHAN,
            op::GREATERTHAN,
            op::LESSTHANOREQUAL,
            op::GREATERTHANOREQUAL,
            op::MIN,
            op::MAX,
            op::WITHIN,
            op::RIPEMD160,
            op::SHA1,
            op::SHA256,
            op::HASH160,
            op::HASH256,
            op::CHECKSIG,
            op::CHECKSIGVERIFY,
            op::CHECKMULTISIG,
            op::CHECKMULTISIGVERIFY,
            op::NOP1,
            op::CHECKLOCKTIMEVERIFY,
            op::NOP3,
            op::NOP4,
            op::NOP5,
            op::NOP6,
            op::NOP7,
            op::NOP8,
            op::NOP9,
            op::NOP10,
        ] {
            match op {
                Opcode::PushValue(pv) => match pv {
                    PushValue::SmallValue(sv) => {
                        assert_eq!(SmallValue::decode(sv.encode()), Some(sv));
                    }
                    // LargeValues don't encode to a single opcode.
                    PushValue::LargeValue(lv) => {
                        assert_eq!(LargeValue::from_slice(&Vec::from(&lv)), Some(lv));
                    }
                },
                Opcode::Control(control) => {
                    assert_eq!(Control::decode(control.encode()), Some(control));
                }
                Opcode::Operation(operation) => {
                    assert_eq!(Operation::decode(operation.encode()), Some(operation));
                }
            }
        }

        // Disabled opcodes are not valid `Opcode`s.
        for op in [
            Disabled::OP_CAT,
            Disabled::OP_SUBSTR,
            Disabled::OP_LEFT,
            Disabled::OP_RIGHT,
            Disabled::OP_INVERT,
            Disabled::OP_AND,
            Disabled::OP_OR,
            Disabled::OP_XOR,
            Disabled::OP_2MUL,
            Disabled::OP_2DIV,
            Disabled::OP_MUL,
            Disabled::OP_DIV,
            Disabled::OP_MOD,
            Disabled::OP_LSHIFT,
            Disabled::OP_RSHIFT,
            Disabled::OP_CODESEPARATOR,
        ] {
            assert_eq!(Disabled::decode(op.encode()), Some(op));
        }
    }
}
