# Post-quantum handshake parameters

The native v2 handshake (`p2p-v2/5`) uses ML-KEM-1024 in its hybrid key
exchange, NIST security category 5, and ML-DSA-65 in its composite
signature, category 3. Until version 0.29 of the
[`zakura-quic` spec](../specs/zakura-quic.md) the handshake used ML-KEM-768
(category 3) and ML-DSA-44 (category 2). A draft of version 0.29 signed with
ML-DSA-87 (category 5). It never shipped.

This note records why we chose these sets, what they cost, and how the costs
were measured. The spec (TLS-4, TLS-9, ID-13, §17aa) is authoritative for
the wire format.

## 1. Why

Both schemes rest on lattice problems. Their parameter sets are sized against
the best known attacks, with quantum attacks counted as at most a modest
gain. A new attack, quantum or classical, would eat into the smaller sets'
margins first. The two halves of the handshake face that risk differently.

- **Key exchange: category 5.** It protects confidentiality. An attacker can
  record handshakes and traffic today and decrypt them once ML-KEM falls
  ("harvest now, decrypt later"). Nothing we ship later protects what was
  recorded before, so the key exchange takes the widest margin FIPS 203
  offers.
- **Signature: category 3.** It protects authentication, and only while the
  handshake runs. A later break of ML-DSA can't forge a handshake that has
  already completed, and it reveals nothing about the session keys. The
  signature needs to hold only until we can replace it.
- **If a scheme breaks,** we ship a new node-ID stack: a new signature, a new
  credential tag and a new ALPN, the way `p2p-v2/5` replaced `p2p-v2/4`.
  We don't carry a pre-committed backup key or other agility machinery,
  which would add a key to every credential and code paths that only run
  in an emergency.

The classical halves stay as they were. X25519 and Ed25519 don't protect
against a quantum computer. They protect against a flaw in the young
post-quantum code, and 128-bit classical security is still out of reach for
that purpose.

Only our own fleet spoke `p2p-v2/4`, so breaking the handshake again cost
little.

| | `p2p-v2/4` | `p2p-v2/5` |
|---|---|---|
| Key exchange | X25519MLKEM768 (0x11EC) | X25519MLKEM1024 (private use, 0xFE24) |
| ML-KEM category | 3 | 5 |
| Signature | Ed25519 + ML-DSA-44 (0xFE44) | Ed25519 + ML-DSA-65 (0xFE65) |
| ML-DSA category | 2 | 3 |
| Credential tag | 2 | 3 |
| Node IDs | | all changed |
| Key files | | unchanged: the ML-DSA key still derives from the seed |

## 2. Sizes

The last column is the ML-DSA-87 draft, for comparison (§5).

| Item | 768 / 44 | 1024 / 65 | 1024 / 87 |
|---|---|---|---|
| Dialer's key share | 1,216 B | 1,600 B | 1,600 B |
| Listener's key share | 1,120 B | 1,600 B | 1,600 B |
| ML-DSA public key in the credential | 1,312 B | 1,952 B | 2,592 B |
| Composite signature | 2,484 B | 3,373 B | 4,691 B |
| Listener's first flight | ~5.8 KB | ~7.9 KB | ~9.9 KB |
| Dialer sends, whole handshake | 6,721 B in 7 datagrams | 9,534 B in 11 datagrams | 11,501 B in 11 datagrams |
| Listener sends, whole handshake | ~5,760 B in 7 datagrams | ~7,930 B in 10 datagrams | ~9,915 B in 10 datagrams |

Each side sends its credential and a signature, because both sides
authenticate (TLS-5). A handshake moves about 17.5 KB, in place of about
12.5 KB on `p2p-v2/4` and 21.4 KB with ML-DSA-87. Discovery records don't
grow: they carry each key as a 32-byte fingerprint (ID-8).

The listener's figures vary by a few bytes between runs, with the size of
its acknowledgements: its first flight measured 7,861 to 7,873 bytes.

## 3. Round trips

Before the dialer's address is validated, QUIC lets the listener send three
times what it received. The dialer's hello is two 1,200-byte datagrams, so
the allowance is 7,200 bytes.

- On `p2p-v2/4` the listener's whole first flight, about 5.8 KB, fit. A dial
  completed after one round trip.
- On `p2p-v2/5` the flight is about 7.9 KB. The listener holds the first datagram
  that would exceed the allowance. It sends the rest once the dialer's
  Handshake acknowledgement validates its address. A dial completes after
  two round trips.

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

The flight overshoots the allowance by about 670 bytes. Only a category 2
signature would bring it back under (§5). The two ways to remove the round
trip are both larger changes:

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

| Operation | 768 / 44 | 1024 / 65 | 1024 / 87 |
|---|---|---|---|
| ML-KEM keygen (dialer) | 34.3 µs | 45.0 µs | 45.0 µs |
| ML-KEM key check + encaps (listener) | 32.0 µs | 44.4 µs | 44.4 µs |
| ML-KEM decaps (dialer) | 31.5 µs | 43.3 µs | 43.3 µs |
| ML-DSA keygen + sign, per handshake signature | 208.6 µs | 356.9 µs | 480.3 µs |
| ML-DSA verify | 60.8 µs | 100.8 µs | 163.1 µs |

For scale: Ed25519 sign 34.8 µs, verify 67.2 µs, X25519 keygen + agree
88.2 µs. ring's P-384 keygen + agree, the classical half of the standard
category 5 hybrid, measured 1,180 µs.

`zakura-quic` regenerates the ML-DSA signing key from its seed for each
signature (ID-13), so that it holds no expanded secret key between
handshakes. That's why "keygen + sign" is the cost per signature.

### 4.2 Per handshake

The post-quantum and Ed25519 work on each side, summed from §4.1:

| Side | 768 / 44 | 1024 / 65 | 1024 / 87 |
|---|---|---|---|
| Dialer | ~437 µs | ~648 µs | ~834 µs |
| Listener | ~403 µs | ~604 µs | ~790 µs |

Before the dialer proves anything, the listener spends ML-KEM encapsulation
and its own composite signature on each accepted hello, plus 88 µs of
X25519. Without the X25519, that's about 275 µs on `p2p-v2/4`, about 436 µs
on `p2p-v2/5`, and would have been about 560 µs with ML-DSA-87: roughly
3,600, 2,300 and 1,800 unauthenticated hellos per core-second. ADM-3's
pending limits and Retry threshold bound how many a source can start.

The committed bench `crates/zakura-quic/benches/handshake.rs` times the dialer's
`connect` call over UDP loopback, with both endpoints in one process on four cores
(`taskset -c 4-7`), through the public API only. The same file ran
unchanged on main and on the ML-DSA-87 draft:

| Bench | main (768 / 44) | this change (1024 / 65) | ML-DSA-87 draft (1024 / 87) |
|---|---|---|---|
| `handshake/loopback` | 3.92–3.97 ms | 4.45–4.59 ms | 4.79–4.90 ms |
| `key/load` | 105 µs | 146–150 µs | 208 µs |

The dialer returned about 0.58 ms (15%) later than on main in these runs.
The timer can stop before the listener finishes authenticating the dialer.
The simulated link in §3 measures the effect of network delay. `key/load`
runs once per node start. The ML-DSA-65 range spans three runs on 2026-11-25. In the same session the ML-DSA-87 draft measured
4.81–4.93 ms and 207 µs, which matches its earlier figures.

## 5. Rejected options

- **ML-DSA-87.** Category 5, as a draft of this change used. It buys margin
  the signature doesn't need (§1). Against ML-DSA-65 it adds a 2,592-byte
  key and a 4,627-byte signature, about 2 KB more from each side and 3.9 KB
  per handshake. It costs about 120 µs more per signature and 60 µs more
  per verification, so the listener's work before the dialer proves
  anything rises from 436 µs to 560 µs. Its 9.9 KB first flight also needs
  the extra round trip, and with a padded hello it would leave only about
  880 bytes of the 10.8 KB allowance, against about 2.9 KB with ML-DSA-65.
- **ML-DSA-44 with ML-KEM-1024.** Category 2 for the signature. Its first
  flight would be about 6.3 KB, from the measured flight less the 640-byte
  smaller key and the 889-byte smaller signature. That fits the unpadded
  allowance, so a dial would take one round trip, with about 850 bytes to
  spare. It would also save about 150 µs per signature and 40 µs per
  verification. But category 2 is the thinnest margin FIPS 204 offers, and
  the signature has to hold until a replacement stack reaches the whole
  fleet, which takes at least a release cycle. A padded hello recovers the
  round trip without giving up that margin.
- **SecP384r1MLKEM1024 (IANA 0x11ED).** This is the standard category 5
  hybrid. Its P-384 half costs about 1.2 ms per exchange in ring, about 13×
  X25519, and the listener pays it on every unauthenticated hello. ring
  won't take injected randomness for P-384 either, so the deterministic
  simulator would need a new P-384 dependency. Its gain, a 192-bit classical
  half, protects only against a classical break of ML-KEM. Only Zakura nodes
  speak the v2 handshake, so a standard code point buys no interoperability.
- **Pure ML-KEM-1024 (0x0202).** Dropping X25519 would lose the hedge
  against a flaw in the post-quantum code.

## 6. Reproducing

- Primitives: a throwaway criterion crate with `libcrux-ml-kem` 0.0.12 (all
  parameter sets), `libcrux-ml-dsa` 0.0.12 (all parameter sets) and ring
  0.17.14. It isn't in the repository, so as not to widen DEP-2.
- Handshakes: `cargo bench -p zakura-quic --bench handshake`, on this change
  and on a checkout of main or the ML-DSA-87 draft with the same file.
- Round trips and bytes: a simulator test over a link with a fixed delay,
  counting each side's datagrams until the dial completes and after the
  handshake settles. The committed test above checks the round trips.
- Fleet: after the Testnet deploy, compare the
  `zakura.quic.handshake.duration_seconds` histogram on the three Testnet
  nodes before and after.
