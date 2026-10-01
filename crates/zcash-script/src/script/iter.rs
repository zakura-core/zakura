//! Much of the code is common between the script components, so this provides operations on
//! iterators that can be shared.

use alloc::vec::Vec;

use crate::{
    interpreter, opcode,
    script::{self, Evaluable},
};

/// Evaluate an entire script.
pub fn eval_script<T: script::Evaluable, U: script::Evaluable>(
    sig: &T,
    pub_key: &U,
    flags: interpreter::Flags,
    checker: &dyn interpreter::SignatureChecker,
) -> Result<bool, (script::ComponentType, script::Error)> {
    if flags.contains(interpreter::Flags::SigPushOnly) && !sig.is_push_only() {
        Err((script::ComponentType::Sig, script::Error::SigPushOnly))
    } else {
        let data_stack = sig
            .eval(flags, checker, interpreter::Stack::new())
            .map_err(|e| (script::ComponentType::Sig, e))?;
        let pub_key_stack = pub_key
            .eval(flags, checker, data_stack.clone())
            .map_err(|e| (script::ComponentType::PubKey, e))?;
        if pub_key_stack
            .last()
            .is_ok_and(|v| interpreter::cast_to_bool(v))
        {
            if flags.contains(interpreter::Flags::P2SH) && pub_key.is_pay_to_script_hash() {
                // script_sig must be literals-only or validation fails
                if sig.is_push_only() {
                    data_stack
                        .split_last()
                        .map_err(|_| script::Error::MissingRedeemScript)
                        .and_then(|(pub_key_2, remaining_stack)| {
                            script::Code(pub_key_2.clone()).eval(flags, checker, remaining_stack)
                        })
                        .map(|p2sh_stack| {
                            if p2sh_stack
                                .last()
                                .is_ok_and(|v| interpreter::cast_to_bool(v))
                            {
                                Some(p2sh_stack)
                            } else {
                                None
                            }
                        })
                        .map_err(|e| (script::ComponentType::Redeem, e))
                } else {
                    Err((script::ComponentType::Sig, script::Error::SigPushOnly))
                }
            } else {
                Ok(Some(pub_key_stack))
            }
            .and_then(|mresult_stack| {
                match mresult_stack {
                    None => Ok(false),
                    Some(result_stack) => {
                        // The CLEANSTACK check is only performed after potential P2SH evaluation, as the
                        // non-P2SH evaluation of a P2SH script will obviously not result in a clean stack
                        // (the P2SH inputs remain).
                        if flags.contains(interpreter::Flags::CleanStack) {
                            // Disallow CLEANSTACK without P2SH, because Bitcoin did.
                            assert!(flags.contains(interpreter::Flags::P2SH));
                            if result_stack.len() == 1 {
                                Ok(true)
                            } else {
                                Err((script::ComponentType::Redeem, script::Error::CleanStack))
                            }
                        } else {
                            Ok(true)
                        }
                    }
                }
            })
        } else {
            Ok(false)
        }
    }
}

pub fn eval<T: Into<opcode::PossiblyBad> + opcode::Evaluable + Clone>(
    mut iter: impl Iterator<Item = Result<T, script::Error>>,
    flags: interpreter::Flags,
    script_code: &script::Code,
    stack: interpreter::Stack<Vec<u8>>,
    checker: &dyn interpreter::SignatureChecker,
) -> Result<interpreter::Stack<Vec<u8>>, script::Error> {
    iter.try_fold(interpreter::State::initial(stack), |state, elem| {
        elem.and_then(|op| {
            op.eval(flags, script_code, checker, state)
                .map_err(|e| script::Error::Interpreter(Some(op.clone().into()), e))
        })
    })
    .and_then(|final_state| match final_state.vexec.len() {
        0 => Ok(final_state.stack),
        n => Err(script::Error::UnclosedConditional(n)),
    })
}

/// Read one raw opcode and borrow its push data without applying execution limits.
fn read_opcode(bytes: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&opcode, mut bytes) = bytes.split_first()?;
    let size = match opcode {
        0x00..=0x4b => u32::from(opcode),
        0x4c => {
            let (&size, rest) = bytes.split_first()?;
            bytes = rest;
            u32::from(size)
        }
        0x4d => {
            let length = bytes.get(..2)?;
            let size = u32::from(u16::from_le_bytes([length[0], length[1]]));
            bytes = &bytes[2..];
            size
        }
        0x4e => {
            let length = bytes.get(..4)?;
            let size = u32::from_le_bytes([length[0], length[1], length[2], length[3]]);
            bytes = &bytes[4..];
            size
        }
        _ => 0,
    };
    let size = usize::try_from(size).ok()?;
    Some((opcode, bytes.get(..size)?, bytes.get(size..)?))
}

/// Count raw opcodes without applying execution limits or allocating push values.
/// Complete pushes may exceed 520 bytes. Truncated pushes stop the count.
pub fn sig_op_count(mut bytes: &[u8], accurate: bool) -> u32 {
    let mut count = 0u32;
    let mut previous = 0u8;
    while let Some((opcode, _, rest)) = read_opcode(bytes) {
        bytes = rest;
        count = count.saturating_add(match opcode {
            0xac | 0xad => 1,
            0xae | 0xaf if accurate && (0x51..=0x60).contains(&previous) => {
                u32::from(previous - 0x50)
            }
            0xae | 0xaf => 20,
            _ => 0,
        });
        previous = opcode;
    }
    count
}

/// Extract the last raw push and count its sigops in accurate mode.
pub fn p2sh_sig_op_count(mut bytes: &[u8]) -> u32 {
    let mut last_data = &[][..];
    while !bytes.is_empty() {
        let Some((opcode, data, rest)) = read_opcode(bytes) else {
            return 0;
        };
        if opcode > 0x60 {
            return 0;
        }
        last_data = data;
        bytes = rest;
    }
    sig_op_count(last_data, true)
}

#[cfg(test)]
mod sig_op_count_tests {
    use alloc::vec::Vec;

    use crate::script::Code;

    /// `sig_op_count` must match zcashd's `CScript::GetSigOpCount` even when the script contains
    /// opcodes this crate buckets under `Disabled` -- in particular OP_CODESEPARATOR (0xab), which
    /// is a normal, valid opcode in zcashd and must not stop the count.
    #[test]
    fn does_not_short_circuit_on_disabled_opcodes() {
        // OP_1 OP_CHECKMULTISIG: accurate counts the OP_N prefix (1); legacy counts 20.
        assert_eq!(Code(Vec::from([0x51u8, 0xae])).sig_op_count(true), 1);
        assert_eq!(Code(Vec::from([0x51u8, 0xae])).sig_op_count(false), 20);

        // OP_CODESEPARATOR (0xab) then OP_CHECKSIG: zcashd keeps counting -> 1.
        assert_eq!(Code(Vec::from([0xabu8, 0xac])).sig_op_count(true), 1);

        // OP_CAT (0x7e, truly disabled) then OP_CHECKSIG: zcashd still keeps counting -> 1.
        assert_eq!(Code(Vec::from([0x7eu8, 0xac])).sig_op_count(true), 1);

        // OP_CODESEPARATOR before 50 x OP_CHECKMULTISIG: each charged 20 (no OP_N prefix),
        // matching zcashd's GetSigOpCount(true) == 1000.
        let mut s = Vec::from([0xabu8]);
        s.extend([0xaeu8; 50]);
        assert_eq!(Code(s).sig_op_count(true), 1000);

        // A truncated push stops the count, like zcashd's GetOp returning false.
        assert_eq!(Code(Vec::from([0xacu8, 0x05, 0x01])).sig_op_count(true), 1);
    }
}
