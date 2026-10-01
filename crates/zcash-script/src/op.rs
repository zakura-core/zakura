//! Convenience definitions for all opcodes.

#![allow(missing_docs)]

use crate::{
    opcode::{Control::*, Operation::*},
    pv,
    Opcode::{self, Control, Operation, PushValue},
};

pub const _0: Opcode = PushValue(pv::_0);
pub const _1NEGATE: Opcode = PushValue(pv::_1NEGATE);
pub const _1: Opcode = PushValue(pv::_1);
pub const _2: Opcode = PushValue(pv::_2);
pub const _3: Opcode = PushValue(pv::_3);
pub const _4: Opcode = PushValue(pv::_4);
pub const _5: Opcode = PushValue(pv::_5);
pub const _6: Opcode = PushValue(pv::_6);
pub const _7: Opcode = PushValue(pv::_7);
pub const _8: Opcode = PushValue(pv::_8);
pub const _9: Opcode = PushValue(pv::_9);
pub const _10: Opcode = PushValue(pv::_10);
pub const _11: Opcode = PushValue(pv::_11);
pub const _12: Opcode = PushValue(pv::_12);
pub const _13: Opcode = PushValue(pv::_13);
pub const _14: Opcode = PushValue(pv::_14);
pub const _15: Opcode = PushValue(pv::_15);
pub const _16: Opcode = PushValue(pv::_16);

/// Produces a minimally-encoded data value. It fails if the slice is larger than
/// `opcode::push_value::LargeValue::MAX_SIZE`.
pub fn push_value(value: &[u8]) -> Option<Opcode> {
    pv::push_value(value).map(PushValue)
}

pub const NOP: Opcode = Operation(OP_NOP);
pub const IF: Opcode = Control(OP_IF);
pub const NOTIF: Opcode = Control(OP_NOTIF);
pub const ELSE: Opcode = Control(OP_ELSE);
pub const ENDIF: Opcode = Control(OP_ENDIF);
pub const VERIFY: Opcode = Operation(OP_VERIFY);
pub const RETURN: Opcode = Operation(OP_RETURN);
pub const TOALTSTACK: Opcode = Operation(OP_TOALTSTACK);
pub const FROMALTSTACK: Opcode = Operation(OP_FROMALTSTACK);
pub const _2DROP: Opcode = Operation(OP_2DROP);
pub const _2DUP: Opcode = Operation(OP_2DUP);
pub const _3DUP: Opcode = Operation(OP_3DUP);
pub const _2OVER: Opcode = Operation(OP_2OVER);
pub const _2ROT: Opcode = Operation(OP_2ROT);
pub const _2SWAP: Opcode = Operation(OP_2SWAP);
pub const IFDUP: Opcode = Operation(OP_IFDUP);
pub const DEPTH: Opcode = Operation(OP_DEPTH);
pub const DROP: Opcode = Operation(OP_DROP);
pub const DUP: Opcode = Operation(OP_DUP);
pub const NIP: Opcode = Operation(OP_NIP);
pub const OVER: Opcode = Operation(OP_OVER);
pub const PICK: Opcode = Operation(OP_PICK);
pub const ROLL: Opcode = Operation(OP_ROLL);
pub const ROT: Opcode = Operation(OP_ROT);
pub const SWAP: Opcode = Operation(OP_SWAP);
pub const TUCK: Opcode = Operation(OP_TUCK);
pub const SIZE: Opcode = Operation(OP_SIZE);
pub const EQUAL: Opcode = Operation(OP_EQUAL);
pub const EQUALVERIFY: Opcode = Operation(OP_EQUALVERIFY);
pub const _1ADD: Opcode = Operation(OP_1ADD);
pub const _1SUB: Opcode = Operation(OP_1SUB);
pub const NEGATE: Opcode = Operation(OP_NEGATE);
pub const ABS: Opcode = Operation(OP_ABS);
pub const NOT: Opcode = Operation(OP_NOT);
pub const _0NOTEQUAL: Opcode = Operation(OP_0NOTEQUAL);
pub const ADD: Opcode = Operation(OP_ADD);
pub const SUB: Opcode = Operation(OP_SUB);
pub const BOOLAND: Opcode = Operation(OP_BOOLAND);
pub const BOOLOR: Opcode = Operation(OP_BOOLOR);
pub const NUMEQUAL: Opcode = Operation(OP_NUMEQUAL);
pub const NUMEQUALVERIFY: Opcode = Operation(OP_NUMEQUALVERIFY);
pub const NUMNOTEQUAL: Opcode = Operation(OP_NUMNOTEQUAL);
pub const LESSTHAN: Opcode = Operation(OP_LESSTHAN);
pub const GREATERTHAN: Opcode = Operation(OP_GREATERTHAN);
pub const LESSTHANOREQUAL: Opcode = Operation(OP_LESSTHANOREQUAL);
pub const GREATERTHANOREQUAL: Opcode = Operation(OP_GREATERTHANOREQUAL);
pub const MIN: Opcode = Operation(OP_MIN);
pub const MAX: Opcode = Operation(OP_MAX);
pub const WITHIN: Opcode = Operation(OP_WITHIN);
pub const RIPEMD160: Opcode = Operation(OP_RIPEMD160);
pub const SHA1: Opcode = Operation(OP_SHA1);
pub const SHA256: Opcode = Operation(OP_SHA256);
pub const HASH160: Opcode = Operation(OP_HASH160);
pub const HASH256: Opcode = Operation(OP_HASH256);
pub const CHECKSIG: Opcode = Operation(OP_CHECKSIG);
pub const CHECKSIGVERIFY: Opcode = Operation(OP_CHECKSIGVERIFY);
pub const CHECKMULTISIG: Opcode = Operation(OP_CHECKMULTISIG);
pub const CHECKMULTISIGVERIFY: Opcode = Operation(OP_CHECKMULTISIGVERIFY);
pub const NOP1: Opcode = Operation(OP_NOP1);
pub const CHECKLOCKTIMEVERIFY: Opcode = Operation(OP_CHECKLOCKTIMEVERIFY);
pub const NOP3: Opcode = Operation(OP_NOP3);
pub const NOP4: Opcode = Operation(OP_NOP4);
pub const NOP5: Opcode = Operation(OP_NOP5);
pub const NOP6: Opcode = Operation(OP_NOP6);
pub const NOP7: Opcode = Operation(OP_NOP7);
pub const NOP8: Opcode = Operation(OP_NOP8);
pub const NOP9: Opcode = Operation(OP_NOP9);
pub const NOP10: Opcode = Operation(OP_NOP10);
