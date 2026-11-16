//! SHA256d, a.k.a., double SHA2, a.k.a., 2 SHA 2 Furious

use std::{fmt, io::prelude::*};

use sha2::{Digest, Sha256};

/// An `io::Write` instance that produces a SHA256d output.
pub struct Writer {
    hash: ring::digest::Context,
}

impl Default for Writer {
    fn default() -> Self {
        Self {
            hash: ring::digest::Context::new(&ring::digest::SHA256),
        }
    }
}

impl Writer {
    /// Consume the Writer and produce the hash result.
    pub fn finish(self) -> [u8; 32] {
        let result1 = self.hash.finish();
        let result2 = ring::digest::digest(&ring::digest::SHA256, result1.as_ref());
        let mut buffer = [0u8; 32];
        buffer.copy_from_slice(result2.as_ref());
        buffer
    }
}

impl Write for Writer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hash.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A 4-byte checksum using truncated double-SHA256 (two rounds of SHA256).
#[derive(Copy, Clone, Eq, PartialEq)]
pub struct Checksum(pub [u8; 4]);

impl<'a> From<&'a [u8]> for Checksum {
    fn from(bytes: &'a [u8]) -> Self {
        let hash1 = Sha256::digest(bytes);
        let hash2 = Sha256::digest(hash1);
        let mut checksum = [0u8; 4];
        checksum[0..4].copy_from_slice(&hash2[0..4]);
        Self(checksum)
    }
}

impl fmt::Debug for Checksum {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("Sha256dChecksum")
            .field(&hex::encode(self.0))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference_hash(bytes: &[u8]) -> [u8; 32] {
        Sha256::digest(Sha256::digest(bytes)).into()
    }

    #[test]
    fn writer_known_answers() {
        for (input, expected) in [
            (
                b"".as_slice(),
                "5df6e0e2761359d30a8275058e299fcc0381534545f55cf43e41983f5d4c9456",
            ),
            (
                b"hello".as_slice(),
                "9595c9df90075148eb06860365df33584b75bff782a510c6cd4883a419833d50",
            ),
            (
                b"abc".as_slice(),
                "4f8b42c22dd3729b519ba6f68d2da7cc5b2d606d05daed5ad5128cc03e6c6358",
            ),
        ] {
            let mut writer = Writer::default();
            writer.write_all(input).unwrap();
            assert_eq!(hex::encode(writer.finish()), expected);
        }
    }

    #[test]
    fn changed_or_reordered_bytes_do_not_match_the_original_hash() {
        let expected = reference_hash(b"hello");
        for input in [b"Hello".as_slice(), b"olleh".as_slice()] {
            let mut writer = Writer::default();
            writer.write_all(input).unwrap();
            assert_ne!(writer.finish(), expected);
        }
    }

    #[test]
    fn writer_preserves_thread_and_unwind_safety() {
        fn assert_traits<
            T: Send + Sync + Unpin + std::panic::UnwindSafe + std::panic::RefUnwindSafe,
        >() {
        }
        assert_traits::<Writer>();
    }

    #[test]
    fn writer_matches_sha2_across_padding_and_write_boundaries() {
        let bytes: Vec<_> = (0..=255u8).cycle().take(2_000_000).collect();
        for length in (0..=256).chain([511, 512, 513, 1487, 50_000, 2_000_000]) {
            let input = &bytes[..length];
            let expected = reference_hash(input);
            for chunk_size in [1, 7, 32, 63, 64, 65, 128, 4096] {
                // Large payloads exercise streaming without millions of tiny writes.
                if length > 513 && chunk_size < 64 {
                    continue;
                }
                let mut writer = Writer::default();
                for chunk in input.chunks(chunk_size) {
                    writer.write_all(chunk).unwrap();
                    writer.flush().unwrap();
                    writer.write_all(&[]).unwrap();
                }
                assert_eq!(
                    writer.finish(),
                    expected,
                    "length {length}, chunks {chunk_size}"
                );
            }
        }
    }

    proptest::proptest! {
        #[test]
        fn writer_matches_sha2_for_arbitrary_bytes(
            bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..8192),
            chunk_size in 1usize..256,
        ) {
            let mut writer = Writer::default();
            for chunk in bytes.chunks(chunk_size) {
                writer.write_all(chunk).unwrap();
            }
            proptest::prop_assert_eq!(writer.finish(), reference_hash(&bytes));
        }
    }

    #[test]
    fn sha256d_checksum() {
        let _init_guard = zakura_test::init();

        // https://en.bitcoin.it/wiki/Protocol_documentation#Hashes
        let input = b"hello";
        let checksum = Checksum::from(&input[..]);
        let expected = Checksum([0x95, 0x95, 0xc9, 0xdf]);
        assert_eq!(checksum, expected);
    }

    #[test]
    fn sha256d_checksum_debug() {
        let _init_guard = zakura_test::init();

        let input = b"hello";
        let checksum = Checksum::from(&input[..]);

        assert_eq!(format!("{checksum:?}"), "Sha256dChecksum(\"9595c9df\")");
    }
}
