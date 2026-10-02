//! Sigop counts at push-encoding boundaries and over seeded random scripts.

use rand::{rngs::StdRng, Rng as _, SeedableRng as _};
use zakura_script_oracle::{check_p2sh_sigops, check_script_sigops};

/// Every push encoding at sizes around its limits and the 520-byte execution limit, complete and
/// truncated, between sigop opcodes.
#[test]
fn push_encoding_boundaries() {
    for (prefix, width, max) in [(0x4c, 1, 0xff), (0x4d, 2, 0xffff), (0x4e, 4, 0x1_0001)] {
        for size in [0u32, 1, 75, 76, 255, 256, 519, 520, 521, 65_535, 65_536] {
            if size > max {
                continue;
            }
            let mut script = vec![0xac, prefix];
            script.extend(&size.to_le_bytes()[..width]);
            script.resize(script.len() + size as usize, 0xae);
            script.extend([0x53, 0xae, 0xab, 0xac]);
            for end in [script.len(), script.len() - 5, 2 + width, 2] {
                check_script_sigops(&script[..end]);
                check_p2sh_sigops(&script[..end]);
            }
        }
    }
}

/// Random byte strings, which reach truncated pushes and every opcode byte.
#[test]
fn seeded_random_scripts() {
    let mut rng = StdRng::seed_from_u64(0x5167_0905);
    for _ in 0..200_000 {
        let len = rng.gen_range(0..=600);
        let script: Vec<u8> = (0..len).map(|_| rng.gen()).collect();
        check_script_sigops(&script);
        check_p2sh_sigops(&script);
    }
}
