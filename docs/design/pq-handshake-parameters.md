# Post-quantum handshake parameters: category 5

The native v2 handshake (`p2p-v2/5`) uses ML-KEM-1024 in its hybrid key
exchange and ML-DSA-87 in its composite signature. Both are NIST security
category 5, the strongest parameter sets FIPS 203 and FIPS 204 define. Until
version 0.29 of the [`zakura-quic` spec](../specs/zakura-quic.md) the
handshake used ML-KEM-768 (category 3) and ML-DSA-44 (category 2).

This note records why we moved, what it costs, and how the costs were
measured. The spec (TLS-4, TLS-9, ID-13, §17aa) is authoritative for the
wire format.

## 1. Why

Both schemes rest on lattice problems. Their parameter sets are sized against
the best known attacks, with quantum attacks counted as at most a modest
gain. A new quantum algorithm with even a quadratic speedup against lattices
would eat into category 2 and 3 margins first. Category 5 keeps a wide margin
under that assumption, and only our own fleet spoke `p2p-v2/4`, so breaking
the handshake again cost little.

The classical halves stay as they were. X25519 and Ed25519 don't protect
against a quantum computer. They protect against a flaw in the young
post-quantum code, and 128-bit classical security is still out of reach for
that purpose.

| | `p2p-v2/4` | `p2p-v2/5` |
|---|---|---|
| Key exchange | X25519MLKEM768 (0x11EC) | X25519MLKEM1024 (private use, 0xFE24) |
| ML-KEM category | 3 | 5 |
| Signature | Ed25519 + ML-DSA-44 (0xFE44) | Ed25519 + ML-DSA-87 (0xFE87) |
| ML-DSA category | 2 | 5 |
| Credential tag | 2 | 3 |
| Node IDs | | all changed |
| Key files | | unchanged: the ML-DSA key still derives from the seed |

## 2. Sizes

| Item | ML-KEM-768 / ML-DSA-44 | ML-KEM-1024 / ML-DSA-87 |
|---|---|---|
| Dialer's key share | 1,216 B | 1,600 B |
| Listener's key share | 1,120 B | 1,600 B |
| ML-DSA public key in the credential | 1,312 B | 2,592 B |
| Composite signature | 2,484 B | 4,691 B |
| Dialer sends, whole handshake | 6,721 B in 7 datagrams | 11,501 B in 11 datagrams |
| Listener sends, whole handshake | ~5,760 B in 7 datagrams | ~9,915 B in 10 datagrams |

Each side sends its credential and a signature, because both sides
authenticate (TLS-5). A handshake moves about 21.4 KB in place of about
12.5 KB. Discovery records don't grow: they carry each key as a 32-byte
fingerprint (ID-8).

## 3. Round trips

Before the dialer's address is validated, QUIC lets the listener send three
times what it received. The dialer's hello is two 1,200-byte datagrams, so
the allowance is 7,200 bytes.

- On `p2p-v2/4` the listener's whole first flight, about 5.8 KB, fit. A dial
  completed after one round trip.
- On `p2p-v2/5` the flight is about 9.9 KB. quinn-proto stops at the
  allowance, finishing the datagram it started (7,248 bytes in practice), and
  sends the rest once the dialer's acknowledgements validate its address. A
  dial completes after two round trips.

Measured in the simulator with a fixed one-way delay:

| RTT | `p2p-v2/4` dial | `p2p-v2/5` dial |
|---|---|---|
| 2 ms | 2 ms | 4 ms |
| 50 ms | 50 ms | 100 ms |
| 124 ms (Mainnet fleet median) | 124 ms | 248 ms |
| 250 ms (Mainnet fleet p90) | 250 ms | 500 ms |

The cost is one added RTT per new v2 connection, about 124 ms at the fleet's
median. Peer connections last hours, so this changes how fast a node fills
its peer set after a restart and how fast it replaces a lost peer, not steady
state. A pending handshake also holds its admission slot (ADM-7) one RTT
longer.

The added round trip can't be avoided by trimming. Category 5 signatures
alone outgrow the allowance: ML-DSA-87 with ML-KEM-768 would still send
about 9.4 KB, from the measured flight less the 480-byte smaller ciphertext.
The two ways to remove it are both larger changes:

- pad the dialer's hello to three datagrams, which raises the allowance to
  10.8 KB but changes ADM-1's per-datagram admission accounting and adds
  1.2 KB to every dial;
- validate the dialer's address first with a Retry, which costs the same
  round trip.

The test `the_listeners_first_flight_waits_one_round_trip_for_validation`
pins the behaviour: one added round trip, and no more.

## 4. CPU

### 4.1 Primitives

`libcrux` 0.0.12 as `zakura-quic` uses it, and ring for the classical
operations. Criterion, one core (`taskset -c 6`) of a DigitalOcean premium
Intel host, mean time per operation. ML-DSA signing draws fresh hedging
randomness each time, so rejection sampling averages out.

| Operation | 768 / 44 | 1024 / 87 | Ratio |
|---|---|---|---|
| ML-KEM keygen (dialer) | 34.3 µs | 45.0 µs | 1.3× |
| ML-KEM key check + encaps (listener) | 32.0 µs | 44.4 µs | 1.4× |
| ML-KEM decaps (dialer) | 31.5 µs | 43.3 µs | 1.4× |
| ML-DSA keygen + sign, per handshake signature | 208.6 µs | 480.3 µs | 2.3× |
| ML-DSA verify | 60.8 µs | 163.1 µs | 2.7× |
| ML-DSA keygen alone (node key load) | 53.5 µs | 153.2 µs | 2.9× |

For scale: Ed25519 sign 34.8 µs, verify 67.2 µs, X25519 keygen + agree
88.2 µs. ML-DSA-65, the category 3 option we didn't take, measured 356.9 µs
for keygen + sign and 100.8 µs for verify. ring's P-384 keygen + agree,
the classical half of the standard category 5 hybrid, measured 1,180 µs.

`zakura-quic` regenerates the ML-DSA signing key from its seed for each
signature (ID-13), so that it holds no expanded secret key between
handshakes. That's why "keygen + sign" is the cost per signature.

### 4.2 Per handshake

The post-quantum and Ed25519 work on each side, summed from §4.1:

| Side | 768 / 44 | 1024 / 87 |
|---|---|---|
| Dialer | ~437 µs | ~834 µs |
| Listener | ~403 µs | ~790 µs |

Before the dialer proves anything, the listener spends ML-KEM encapsulation,
X25519 and its own composite signature on each accepted hello. That's about
275 µs on `p2p-v2/4` and about 560 µs on `p2p-v2/5`, roughly 1,800
unauthenticated hellos per core-second in place of 3,600. ADM-3's pending
limits and Retry threshold bound how many a source can start.

The committed bench `crates/zakura-quic/benches/handshake.rs` measures whole
handshakes over UDP loopback, both endpoints in one process on four cores
(`taskset -c 4-7`), through the public API only. The same file ran
unchanged on main:

| Bench | main (768 / 44) | this change (1024 / 87) | Change |
|---|---|---|---|
| `handshake/loopback` | 3.92–3.97 ms | 4.79–4.90 ms | +0.88 ms, +22% |
| `key/load` | 105 µs | 208 µs | once per node start |

The +0.88 ms matches the +0.78 ms the primitives predict for both sides
together. The rest is the extra datagrams. Loopback has no RTT, so the added
round trip doesn't appear here (§3).

## 5. Rejected options

- **SecP384r1MLKEM1024 (IANA 0x11ED).** This is the standard category 5
  hybrid. Its P-384 half costs about 1.2 ms per exchange in ring, about 13×
  X25519, and the listener pays it on every unauthenticated hello. ring
  won't take injected randomness for P-384 either, so the deterministic
  simulator would need a new P-384 dependency. Its gain, a 192-bit classical
  half, protects only against a classical break of ML-KEM. Only Zakura nodes
  speak the v2 handshake, so a standard code point buys no interoperability.
- **ML-DSA-65.** This is category 3. It saves about 120 µs per signature and
  about 2 KB per side (a 1,952-byte key and a 3,309-byte signature), but the
  listener's flight would still be about 7.9 KB alongside ML-KEM-1024, over
  the allowance, so it doesn't avoid the added round trip.
- **Pure ML-KEM-1024 (0x0202).** Dropping X25519 would lose the hedge
  against a flaw in the post-quantum code.

## 6. Reproducing

- Primitives: a throwaway criterion crate with `libcrux-ml-kem` 0.0.12 (all
  parameter sets), `libcrux-ml-dsa` 0.0.12 (all parameter sets) and ring
  0.17.14. It isn't in the repository, so as not to widen DEP-2.
- Handshakes: `cargo bench -p zakura-quic --bench handshake`, on this change
  and on a main checkout with the same file.
- Round trips and bytes: a simulator test over a link with a fixed delay,
  counting each side's datagrams until the dial completes. The committed
  test above checks the round trips.
- Fleet: after the Testnet deploy, compare the
  `zakura.quic.handshake.duration_seconds` histogram on the three Testnet
  nodes before and after.
