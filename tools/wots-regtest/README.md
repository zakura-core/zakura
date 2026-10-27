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
public seeds and signed outputs, and EC authorization supplied to a WOTS-only
output. Transport failures never count as successful rejection.

The spend sends the 2,144-byte signature as four 512-byte chunks and one
96-byte tail, followed by a single 32-byte public seed and the 34-byte redeem
script. It never serializes the 67 public-key endpoints. Including push
encodings, the scriptSig is 2,226 bytes; the complete input is 2,269 bytes.
This requires 16 transparent input actions and an 80,000-zatoshi fee in the
scenario.

Both the `experimental-wots` build feature and an explicit regtest activation
height are required. Public-network configuration is rejected. Verification
cost is charged per potential WOTS instruction, including unselected branches;
the experimental cost is not a measured production cost model.

The `p0mvn/wots-plus` library provides signing and public-key reconstruction.
The verifier reconstructs all 67 endpoints from the signature, public seed,
and ordinary input-specific ZIP 244 SIGHASH_ALL digest, using RFC 8391
section 3.1.6. It then compares
`SHA256("Zakura/WOTS/key/v1" || public_seed || reconstructed_endpoints)`
with the output's authenticated commitment. Reconstruction alone is not
verification: arbitrary signature bytes also produce a candidate key.
Acceptance relies on WOTS+ unforgeability and the binding of the SHA-256
commitment. The public seed is included in the committed encoding.
The full public key is reconstructible by anyone, even though its endpoints
are absent from the transaction. The threat model is an untrusted spender
supplying all authorization bytes. Acceptance requires the committed key and
a valid signature for that transaction. This prototype makes no claim about
production readiness or an independent cryptographic audit.

Every WOTS output uses fresh disposable key material. Signing consumes the key;
retrying sends the same signed bytes. Failure abandons the key and test funds.
Persistent key journals, seed restoration, rotation, multi-device use, sweeps,
shielded transfers, PIR, and UI are outside this prototype.
