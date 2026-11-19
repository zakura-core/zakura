//! A fixed stream of byte inputs, the same on every call, for seeds and
//! tests that need arbitrary Plan bytes without committing them.

/// `count` byte inputs of up to `max_len` bytes each, drawn from a SplitMix64
/// stream with a fixed start, so every call returns the same inputs.
pub fn byte_inputs(count: usize, max_len: usize) -> Vec<Vec<u8>> {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut next = move || {
        // SplitMix64.
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    (0..count)
        .map(|_| {
            // The remainder is at most max_len, which is a usize.
            let len = (next() % (max_len as u64 + 1)) as usize;
            // Keeping the low byte is the point.
            (0..len).map(|_| next() as u8).collect()
        })
        .collect()
}

/// Input `index` of [`byte_inputs`]`(count, max_len)`.
///
/// # Panics
///
/// If `index` isn't below `count`.
pub fn byte_input(count: usize, max_len: usize, index: usize) -> Vec<u8> {
    byte_inputs(count, max_len).swap_remove(index)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The stream is fixed: seeds built from it keep their bytes.
    #[test]
    fn the_stream_is_fixed() {
        let inputs = byte_inputs(2, 8);
        assert_eq!(inputs, byte_inputs(2, 8));
        assert_eq!(byte_input(2, 8, 1), inputs[1]);
        assert!(inputs.iter().all(|input| input.len() <= 8));
    }
}
