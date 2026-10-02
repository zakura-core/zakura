//! zcashd's `CScript::GetSigOpCount` overloads, linked from the C++ in `libzcash_script`.
//!
//! `libzcash_script` exposes only the legacy count, so `cxx/count.cpp` calls the accurate and P2SH
//! overloads directly.
#![allow(unsafe_code)]

unsafe extern "C" {
    fn oracle_sigops(bytes: *const u8, size: usize, accurate: bool) -> u32;
    fn oracle_p2sh_sigops(key: *const u8, key_size: usize, sig: *const u8, sig_size: usize) -> u32;
}

/// Returns `CScript::GetSigOpCount(accurate)`.
pub fn sigops(script: &[u8], accurate: bool) -> u32 {
    // SAFETY: the C++ code copies the slice into a `CScript` and keeps no pointer.
    unsafe { oracle_sigops(script.as_ptr(), script.len(), accurate) }
}

/// Returns `CScript::GetSigOpCount(scriptSig)` for `script_pub_key`.
pub fn p2sh_sigops(script_pub_key: &[u8], script_sig: &[u8]) -> u32 {
    // SAFETY: the C++ code copies both slices into `CScript`s and keeps no pointer.
    unsafe {
        oracle_p2sh_sigops(
            script_pub_key.as_ptr(),
            script_pub_key.len(),
            script_sig.as_ptr(),
            script_sig.len(),
        )
    }
}
