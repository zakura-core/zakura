# C++ oracle header provenance

The headers under `zcash/` come from the crates.io `libzcash_script` 0.1.0
package. Its Cargo checksum is
`3f8ce05b56f3cbc65ec7d0908adb308ed91281e022f61c8c3a0c9388b5380b17`.
The headers retain their original copyright notices and MIT license.
The wrapper in `count.cpp` calls the original CScript methods linked from
that package. It adds no opcode parsing or counting logic.

| Source path in `depend/zcash/src` | SHA256 |
| --- | --- |
| `compat/byteswap.h` | `b9323d2369e09bf7abd7af651deddfd5ebad5b311e36ec8baee621fa1d916e44` |
| `compat/endian.h` | `6b940aecc64f19e0d59e4eb26fc04996e2d27561dd351085d04062513658eeb1` |
| `crypto/common.h` | `effb3a50dfc9fd55b4fa8e191ca3542d02f352100d79999b86f2c6c6a21c1484` |
| `prevector.h` | `ec762a69fee4f7df42281e0a735a243c6cc8b8fb418f9da11ce008287bebd2a4` |
| `script/script.h` | `cedd08bda720b6721514d187132128704b47e2bbab3cf351613a0c05c328c18b` |
| `uint256.h` | `bb9f3785864963a9e36d98ce88112e2cc167af2057206f155e9d5d98cade5429` |
