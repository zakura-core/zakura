# WOTS-only regtest demo

This demo funds a P2SH output whose redeem script is
`<32-byte WOTS key commitment> CHECKWOTS`. There is no EC fallback or branch
selector. Ordinary EC outputs remain a separate, unchanged spending type.

From the Zakura checkout, run:

```sh
python3 tools/wots-regtest/run.py
```

The runner verifies the pinned Git dependencies for the node and harness,
builds both, starts an isolated ephemeral regtest node, and stops that node on
exit. It does not use a wallet database or an existing node. Python 3.11+ and
the repository's Rust toolchain are required. Git dependencies must be reachable
or already present in Cargo's cache.

The scenario mines an ordinary P2PKH EC spend before activation, a WOTS-only
spend after activation, and another ordinary P2PKH EC spend afterward. It checks
rejection before activation, altered WOTS signatures, malformed chunks, changed
signed outputs, and EC authorization supplied to a WOTS-only output. Transport
failures never count as successful rejection.

The WOTS signature is 2,144 bytes and the full verifying key is 2,176 bytes.
They are split into four 512-byte chunks and one tail each (96 and 128 bytes).
Together with the 34-byte redeem script and push encodings, the scriptSig is
4,383 bytes; the complete input is 4,426 bytes. This requires 30 transparent
input actions and a 150,000-zatoshi fee in the scenario.

Both the `experimental-wots` build feature and an explicit regtest activation
height are required. Public-network configuration is rejected. Verification
cost is charged per potential WOTS instruction, including unselected branches;
the experimental cost is not a measured production cost model.

The unchanged `p0mvn/wots-plus` implementation provides the cryptography.
Common authenticates the full public key using
`SHA256("Zakura/WOTS/key/v1" || full_public_key)` before verifying its signature
against the ordinary input-specific ZIP 244 SIGHASH_ALL digest. The threat model
is an untrusted spender supplying all authorization bytes; acceptance requires
the committed key and a valid signature for that transaction. This relies on
the library's one-time signature security and the hash commitment, and makes no
claim about production readiness or an independent cryptographic audit.

Every WOTS output uses fresh disposable key material. Signing consumes the key;
retrying sends the same signed bytes. Failure abandons the key and test funds.
Persistent key journals, seed restoration, rotation, multi-device use, sweeps,
shielded transfers, PIR, and UI are outside this prototype.
