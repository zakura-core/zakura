//! Test helpers for transaction standardness. The rules themselves live in
//! `zakura-mempool`, which both pools call.

use zakura_chain::transparent;

/// Build a P2PKH lock script: OP_DUP OP_HASH160 <20-byte hash> OP_EQUALVERIFY OP_CHECKSIG
pub(super) fn p2pkh_lock_script(hash: &[u8; 20]) -> transparent::Script {
    let mut s = vec![0x76, 0xa9, 0x14];
    s.extend_from_slice(hash);
    s.push(0x88);
    s.push(0xac);
    transparent::Script::new(&s)
}
