//! Zcash transparent script implementations.

#![no_std]
#![doc(html_logo_url = "https://www.zfnd.org/images/zebra-icon.png")]
#![doc(html_root_url = "https://docs.rs/zcash_script/0.4.5")]
#![allow(clippy::unit_arg)]
#![allow(non_snake_case)]
#![forbid(unsafe_code)]
#![deny(missing_docs)]

#[macro_use]
extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

mod external;
pub mod interpreter;
pub mod num;
pub mod op;
pub mod opcode;
pub mod pattern;
pub mod pv;
pub mod script;
pub mod signature;
pub mod solver;

#[cfg(feature = "descriptor")]
pub mod descriptor;

#[cfg(any(test, feature = "test-dependencies"))]
pub mod test_vectors;

use alloc::string::String;
use alloc::vec::Vec;

use crate::script::Asm;

/// Script opcodes
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Opcode {
    /// Opcodes that represent constants to be pushed onto the stack.
    PushValue(opcode::PushValue),
    /// - always evaluated
    /// - can be cast to its discriminant
    Control(opcode::Control),
    /// - only evaluated on active branch
    /// - can be cast to its discriminant
    Operation(opcode::Operation),
}

impl Opcode {
    /// Statically analyze an opcode. That is, this identifies potential runtime errors without
    /// needing to evaluate the script.
    ///
    /// __NB__: [`op::RETURN`] isn’t tracked by this function because it’s functionally more like a
    ///         `break` then an error.
    pub(crate) fn analyze(
        &self,
        flags: &interpreter::Flags,
    ) -> Result<(), Vec<interpreter::Error>> {
        match self {
            Opcode::PushValue(pv) => pv.analyze(flags),
            Opcode::Operation(op) => op.analyze(flags),
            Opcode::Control(_) => Ok(()),
        }
    }
}

impl opcode::Evaluable for Opcode {
    fn byte_len(&self) -> usize {
        match self {
            Opcode::PushValue(pv) => pv.byte_len(),
            Opcode::Control(_) => 1,
            Opcode::Operation(_) => 1,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        Vec::<u8>::from(self)
    }

    fn restrict(pb: opcode::PossiblyBad) -> Result<Self, script::Error> {
        match pb {
            opcode::PossiblyBad::Good(op) => Ok(op),
            opcode::PossiblyBad::Bad(_) => Err(script::Error::Interpreter(
                Some(pb),
                interpreter::Error::BadOpcode,
            )),
        }
    }

    fn eval(
        &self,
        flags: interpreter::Flags,
        script: &script::Code,
        checker: &dyn interpreter::SignatureChecker,
        mut state: interpreter::State,
    ) -> Result<interpreter::State, interpreter::Error> {
        match self {
            Self::PushValue(pv) => {
                if interpreter::should_exec(&state.vexec) {
                    pv.eval(flags, script, checker, state)
                } else {
                    Ok(state)
                }
            }
            Self::Control(control) => {
                state.increment_op_count(1)?;
                (state.stack, state.vexec) = control.eval(state.stack, state.vexec)?;
                Ok(state)
            }
            Self::Operation(normal) => {
                state.increment_op_count(1)?;
                if interpreter::should_exec(&state.vexec) {
                    normal.eval(flags, script, checker, state)
                } else {
                    Ok(state)
                }
            }
        }
        .and_then(|final_state| {
            // Size limits
            if final_state.stack.len() + final_state.altstack().len() > interpreter::MAX_STACK_DEPTH
            {
                Err(interpreter::Error::StackSize(None))
            } else {
                Ok(final_state)
            }
        })
    }

    fn extract_push_value(&self) -> Result<&opcode::PushValue, script::Error> {
        match self {
            Self::PushValue(pv) => Ok(pv),
            _ => Err(script::Error::SigPushOnly),
        }
    }

    fn sig_op_count(&self, last_opcode: Option<opcode::PossiblyBad>) -> u32 {
        match self {
            Self::Operation(op) => op.sig_op_count(last_opcode),
            _ => 0,
        }
    }
}

impl From<opcode::PushValue> for Opcode {
    fn from(value: opcode::PushValue) -> Self {
        Opcode::PushValue(value)
    }
}

impl From<opcode::Control> for Opcode {
    fn from(value: opcode::Control) -> Self {
        Opcode::Control(value)
    }
}

impl From<opcode::Operation> for Opcode {
    fn from(value: opcode::Operation) -> Self {
        Opcode::Operation(value)
    }
}

impl From<&Opcode> for Vec<u8> {
    fn from(value: &Opcode) -> Self {
        match value {
            Opcode::PushValue(v) => v.into(),
            Opcode::Control(v) => vec![(*v).encode()],
            Opcode::Operation(v) => vec![(*v).encode()],
        }
    }
}

impl Asm for Opcode {
    fn to_asm(&self, attempt_sighash_decode: bool) -> String {
        match self {
            Opcode::PushValue(pv) => pv.to_asm(attempt_sighash_decode),
            Opcode::Operation(op) => op.to_asm(attempt_sighash_decode),
            Opcode::Control(c) => c.to_asm(attempt_sighash_decode),
        }
    }
}

impl core::fmt::Display for Opcode {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.to_asm(false))
    }
}

/// A Zcash script consists of a sig and a pubkey. The first type parameter is the type of opcodes
/// in the script sig, and the second is the type of opcodes in the script pubkey.
///
/// - Script<opcode::PossiblyBad, opcode::PossiblyBad> – from the chain
/// - Script<opcode::PushValue, Opcode> – authoring sig_push_only
/// - Script<Opcode, Opcode> – authoring non-sig_push_only
pub struct Script<Sig = opcode::PushValue, PubKey = Opcode> {
    /// The script sig.
    pub sig: script::Component<Sig>,
    /// The script pubkey.
    pub pub_key: script::Component<PubKey>,
}

impl<
        Sig: Into<opcode::PossiblyBad> + opcode::Evaluable + Clone,
        PubKey: Into<opcode::PossiblyBad> + opcode::Evaluable + Clone,
    > Script<Sig, PubKey>
{
    /// Evaluate an entire script.
    pub fn eval(
        &self,
        flags: interpreter::Flags,
        checker: &dyn interpreter::SignatureChecker,
    ) -> Result<bool, (script::ComponentType, script::Error)> {
        script::iter::eval_script(&self.sig, &self.pub_key, flags, checker)
    }
}

impl<T: Asm, U: Asm> Asm for Script<T, U> {
    fn to_asm(&self, include_sighash_suffix: bool) -> String {
        self.sig.to_asm(include_sighash_suffix) + " " + &self.pub_key.to_asm(false)
    }
}

/// Utilities useful for tests in other modules and crates.
#[cfg(any(test, feature = "test-dependencies"))]
pub mod testing {
    use hex::FromHex;

    use crate::{
        interpreter,
        pattern::{check_multisig, pay_to_script_hash, push_num, push_script},
        pv,
        script::{self, Evaluable},
        signature::HashType,
        test_vectors::{AnnError, TestVector},
        Script,
    };

    /// Ensures that flags represent a supported state. This avoids crashes in the C++ code, which
    /// break various tests.
    pub fn repair_flags(flags: interpreter::Flags) -> interpreter::Flags {
        // TODO: The C++ implementation fails an assert (interpreter.cpp:1097) if `CleanStack` is
        //       set without `P2SH`.
        if flags.contains(interpreter::Flags::CleanStack) {
            flags & interpreter::Flags::P2SH
        } else {
            flags
        }
    }

    /// A `usize` one larger than the longest allowed script, for testing bounds.
    pub const OVERFLOW_SCRIPT_SIZE: usize = script::Code::MAX_SIZE + 1;

    lazy_static::lazy_static! {
        /// The P2SH redeem script used for the static test case.
        pub static ref REDEEM_SCRIPT: script::Redeem = script::Component(check_multisig(
            2,
            &[
                &<[u8; 0x21]>::from_hex("03b2cc71d23eb30020a4893982a1e2d352da0d20ee657fa02901c432758909ed8f").expect("valid key"),
                &<[u8; 0x21]>::from_hex("029d1e9a9354c0d2aee9ffd0f0cea6c39bbf98c4066cf143115ba2279d0ba7dabe").expect("valid key"),
                &<[u8; 0x21]>::from_hex("03e32096b63fd57f3308149d238dcbb24d8d28aad95c0e4e74e3e5e6a11b61bcc4").expect("valid key")
            ],
            false).expect("all keys are valid and there’s not more than 20 of them"));
        /// The scriptPubkey used for the static test case.
        pub static ref SCRIPT_PUBKEY: script::PubKey = script::Component(pay_to_script_hash(&REDEEM_SCRIPT));
        /// The scriptSig used for the static test case.
        pub static ref SCRIPT_SIG: script::Sig = script::Component(vec![
            push_num(0),
            pv::push_value(&<[u8; 0x48]>::from_hex("3045022100d2ab3e6258fe244fa442cfb38f6cef9ac9a18c54e70b2f508e83fa87e20d040502200eead947521de943831d07a350e45af8e36c2166984a8636f0a8811ff03ed09401").expect("valid sig")).expect("fits into a PushValue"),
            pv::push_value(&<[u8; 0x47]>::from_hex("3044022013e15d865010c257eef133064ef69a780b4bc7ebe6eda367504e806614f940c3022062fdbc8c2d049f91db2042d6c9771de6f1ef0b3b1fea76c1ab5542e44ed29ed801").expect("valid sig")).expect("fits into a PushValue"),
            push_script(&REDEEM_SCRIPT).expect("fits into a PushValue"),
        ]);
        /// The combined script used for the static test case.
        pub static ref SCRIPT: script::Raw =
            script::Raw::from_raw_parts(SCRIPT_SIG.to_bytes(), SCRIPT_PUBKEY.to_bytes());
        /// The same script as `SCRIPT`, but using the “authoring” types.
        pub static ref AUTHORED_SCRIPT: Script =
            Script{ sig : SCRIPT_SIG.clone(), pub_key : SCRIPT_PUBKEY.clone() };
    }

    /// The correct sighash for the static test case.
    pub fn sighash(_script_code: &script::Code, _hash_type: &HashType) -> Option<[u8; 32]> {
        <[u8; 32]>::from_hex("e8c7bdac77f6bb1f3aba2eaa1fada551a9c8b3b5ecd1ef86e6e58a5f1aab952c")
            .ok()
    }

    /// An incorrect sighash for the static test case – for checking failure cases.
    pub fn invalid_sighash(_script_code: &script::Code, _hash_type: &HashType) -> Option<[u8; 32]> {
        <[u8; 32]>::from_hex("08c7bdac77f6bb1f3aba2eaa1fada551a9c8b3b5ecd1ef86e6e58a5f1aab952c")
            .ok()
    }

    /// A callback that returns no sighash at all – another failure case.
    pub fn missing_sighash(_script_code: &script::Code, _hash_type: &HashType) -> Option<[u8; 32]> {
        None
    }

    /// Run a single test case against some function.
    ///
    /// `try_normalized_error` indicates whether the results should be normalized before being
    /// compared. In particular, the C++ implementation doesn't carry much failure information, so
    /// the results need to be normalized to discard the corresponding information from the expected
    /// results.
    pub fn run_test_vector(
        tv: &TestVector,
        try_normalized_error: bool,
        interpreter_fn: &dyn Fn(&script::Raw, interpreter::Flags) -> Result<bool, AnnError>,
        sigop_count_fn: &dyn Fn(&script::Code) -> Result<u32, script::Error>,
    ) {
        match tv.run(&|script, flags| interpreter_fn(script, flags), &|pubkey| {
            sigop_count_fn(pubkey).unwrap_or_else(|e| panic!("something bad happened: {:?}", e))
        }) {
            Ok(()) => (),
            Err((actual_res, actual_count)) => {
                if !(try_normalized_error
                    && tv.result.clone().normalized()
                        == actual_res.clone().map_err(|(_, e)| e.normalize())
                    && tv.sigop_count == actual_count)
                {
                    panic!(
                        "Either {:?} didn’t match the result or {} didn’t match the sigop_count in

    {:?}
",
                        actual_res, actual_count, tv
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloc::string::ToString;

    use super::{
        test_vectors::test_vectors,
        testing::{missing_sighash, run_test_vector},
    };
    use crate::{
        interpreter::CallbackTransactionSignatureChecker,
        opcode::Operation,
        pv,
        script::{Asm, Code, Evaluable},
        testing::{AUTHORED_SCRIPT, SCRIPT, SCRIPT_PUBKEY},
        Opcode,
    };

    #[cfg(feature = "signature-validation")]
    #[test]
    fn test_test_vectors() {
        for tv in test_vectors() {
            run_test_vector(
                &tv,
                false,
                &|script, flags| {
                    script
                        .eval(
                            flags,
                            &CallbackTransactionSignatureChecker {
                                sighash: &missing_sighash,
                                lock_time: 0,
                                is_final: false,
                            },
                        )
                        .map_err(|(t, e)| (Some(t), e))
                },
                &|pubkey| Ok(pubkey.sig_op_count(false)),
            )
        }
    }

    #[test]
    fn to_asm_works() {
        assert_eq!(SCRIPT.to_asm(true), "0 3045022100d2ab3e6258fe244fa442cfb38f6cef9ac9a18c54e70b2f508e83fa87e20d040502200eead947521de943831d07a350e45af8e36c2166984a8636f0a8811ff03ed094[ALL] 3044022013e15d865010c257eef133064ef69a780b4bc7ebe6eda367504e806614f940c3022062fdbc8c2d049f91db2042d6c9771de6f1ef0b3b1fea76c1ab5542e44ed29ed8[ALL] 522103b2cc71d23eb30020a4893982a1e2d352da0d20ee657fa02901c432758909ed8f21029d1e9a9354c0d2aee9ffd0f0cea6c39bbf98c4066cf143115ba2279d0ba7dabe2103e32096b63fd57f3308149d238dcbb24d8d28aad95c0e4e74e3e5e6a11b61bcc453ae OP_HASH160 c117756dcbe144a12a7c33a77cfa81aa5aeeb381 OP_EQUAL");

        assert_eq!(AUTHORED_SCRIPT.to_asm(true), "0 3045022100d2ab3e6258fe244fa442cfb38f6cef9ac9a18c54e70b2f508e83fa87e20d040502200eead947521de943831d07a350e45af8e36c2166984a8636f0a8811ff03ed094[ALL] 3044022013e15d865010c257eef133064ef69a780b4bc7ebe6eda367504e806614f940c3022062fdbc8c2d049f91db2042d6c9771de6f1ef0b3b1fea76c1ab5542e44ed29ed8[ALL] 522103b2cc71d23eb30020a4893982a1e2d352da0d20ee657fa02901c432758909ed8f21029d1e9a9354c0d2aee9ffd0f0cea6c39bbf98c4066cf143115ba2279d0ba7dabe2103e32096b63fd57f3308149d238dcbb24d8d28aad95c0e4e74e3e5e6a11b61bcc453ae OP_HASH160 c117756dcbe144a12a7c33a77cfa81aa5aeeb381 OP_EQUAL");

        let decoded = Code(hex::decode("004830450221008549700e9f2be9a9986b690ef0a3df6b5a81cb71c6b1392d4136ec19fc09c414022034491aaeb00d455033377c53de8b6496348178c7fcb170bd494622436d7c393601473044022007f264890935969ff73a9dd56ecd00a4462ca39d5d3c7c8bb2c0c9284df68c3702202f24463448d23db8240b233ca3ef56abdbe7867801886af68a72cd2a2d27ad93014c69522103598e93d3702e45890545716c70a5d643dd6779f02fe1dd75b8b5b39c2d368b87210365045e8f10b3602ad02c7d75dce867533c7ba18cf80669a509bc240be272b36c2102714e7e89b7838881be418f577d4320b9280939dc9cd5028414989a739de631ee53ae").unwrap()).to_asm(true);
        let expected = "0 30450221008549700e9f2be9a9986b690ef0a3df6b5a81cb71c6b1392d4136ec19fc09c414022034491aaeb00d455033377c53de8b6496348178c7fcb170bd494622436d7c3936[ALL] 3044022007f264890935969ff73a9dd56ecd00a4462ca39d5d3c7c8bb2c0c9284df68c3702202f24463448d23db8240b233ca3ef56abdbe7867801886af68a72cd2a2d27ad93[ALL] 522103598e93d3702e45890545716c70a5d643dd6779f02fe1dd75b8b5b39c2d368b87210365045e8f10b3602ad02c7d75dce867533c7ba18cf80669a509bc240be272b36c2102714e7e89b7838881be418f577d4320b9280939dc9cd5028414989a739de631ee53ae";
        assert_eq!(decoded, expected);

        let decoded =
            Code(hex::decode("76a914d6791b1b29afe3997a5cdbc40f03d37baed379b988ac").unwrap())
                .to_string();
        let expected =
            "OP_DUP OP_HASH160 d6791b1b29afe3997a5cdbc40f03d37baed379b9 OP_EQUALVERIFY OP_CHECKSIG";
        assert_eq!(decoded, expected);

        assert_eq!(
            "OP_NOP2",
            format!("{}", Opcode::Operation(Operation::OP_CHECKLOCKTIMEVERIFY))
        );

        let der_sig = "304502207fa7a6d1e0ee81132a269ad84e68d695483745cde8b541e3bf630749894e342a022100c1f7ab20e13e22fb95281a870f3dcf38d782e53023ee313d741ad0cfbc0c5090";
        let pubkey = "03b0da749730dc9b4b1f4a14d6902877a92541f5368778853d9c4a0cb7802dcfb2";

        // Helper for signature suffix tests
        fn check_sig_display(der_sig: &str, pubkey: &str, suffix: &str, expected_suffix: &str) {
            let sig_bytes = hex::decode(der_sig.to_string() + suffix).expect("valid sig");
            let vch_pubkey = hex::decode(pubkey).unwrap();
            let expected = der_sig.to_string() + expected_suffix + " " + pubkey;
            let actual = format!(
                "{} {}",
                pv::push_value(&sig_bytes).unwrap().to_asm(true),
                pv::push_value(&vch_pubkey).unwrap().to_asm(false),
            );
            assert_eq!(expected, actual);
        }

        // Sighash suffixes and their expected asm string representations
        let sighash_cases = [
            ("00", "00"),
            ("80", "80"),
            ("01", "[ALL]"),
            ("02", "[NONE]"),
            ("03", "[SINGLE]"),
            ("81", "[ALL|ANYONECANPAY]"),
            ("82", "[NONE|ANYONECANPAY]"),
            ("83", "[SINGLE|ANYONECANPAY]"),
        ];

        for (suffix, expected_suffix) in sighash_cases.iter() {
            check_sig_display(der_sig, pubkey, suffix, expected_suffix);
        }
    }

    #[test]
    fn is_unspendable_works() {
        let code = Code(SCRIPT_PUBKEY.to_bytes());
        assert!(!code.is_unspendable());
        let code = Code(vec![0x6A]);
        assert!(code.is_unspendable());
    }
}
