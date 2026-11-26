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
- **If a scheme breaks,** we add a key of a new scheme to each node's
  credential, under a new tag, and later retire the old key. Neither step
  breaks node IDs (spec 0.31 and 0.34, TLS-7, ID-12, DIAL-9, COMPAT-13):
  - A dial to the ID a node had before it added the key still reaches it,
    as long as the node signs with the keys that ID names. The dialer then
    follows the node to its new ID.
  - A retired key stays in the credential as its 32-byte fingerprint, so
    the ID doesn't change a second time.
  - The rules had to ship with `p2p-v2/5`. The nodes of this release are
    the old nodes of the first upgrade, and a release without the rules
    would refuse the longer credential on a dial to the earlier ID, and a
    fingerprint entry everywhere.
  - The room is 30 more keys per node ID. A key list holds 32 entries, two
    of them today's keys, and a retired key keeps its entry. A key over
    2,592 bytes doesn't fit an entry, and a credential over 10,385 bytes
    is refused, which leaves 8,392 bytes beside today's two keys: three
    more keys of the longest length in full, or one in full and the rest
    retired to 36-byte fingerprint entries. Past any of these limits an
    upgrade is a hard break again.
  - Each new key takes a tag above every tag in use, so a node's earlier
    key lists are prefixes of its current one. Every later release has to
    keep to that order.

  We still don't carry a pre-committed backup key: it would add a key to
  every credential today. The fingerprint entry leaves that open, since a
  node could commit to a backup key by its fingerprint for 36 bytes per
  handshake and nothing in a discovery record beyond one 34-byte entry.

  **Why a fingerprint entry is safe.** It commits to a key without showing
  it, as an entry under an unknown tag already does. It can't check a
  signature, so it can't help anyone answer as the node, and a verifier
  refuses a credential whose signing key is only a fingerprint.

  **What the earlier-ID rule costs.** The dialed ID must name a prefix of
  the node's key list, so a verifier hashes fewer than 32 key lists of at
  most 1,089 bytes on a dial whose ID doesn't match outright, one per
  entry. Spec 0.31 accepted any list of the entries in their order. That
  was 4 hashes with 4 entries and would be 2^30 with 32, on a credential
  the dialer has not authenticated yet. The dialed ID must name both
  signing keys, so adding a key to another node's public keys gains
  nothing: the signature is still checked with that node's keys.

  **What a long credential costs.** A credential travels whole in each
  handshake. The byte limit keeps the flight that carries it inside the
  16 KiB of handshake data `quinn-proto` buffers: about 14 KB at the
  limit, so the largest credential completes a handshake in any arrival
  order. Loss is a different matter. With the largest credential on the
  listener, losing the datagram that starts its ServerHello fails the dial
  at the handshake deadline: in a `quinn-proto` trace the loss shrinks the
  congestion window below the unacknowledged flight and the resend waits
  behind it. Today's 1,993-byte credential recovers. In one simulator
  measurement, which no test pins, 2,793 bytes still recovered and 3,193
  did not. The release that adds a key in full has to fix that or accept
  a redial on such a loss (COMPAT-13).

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
12.5 KB on `p2p-v2/4` and 21.4 KB with ML-DSA-87. The keys don't make a
discovery record grow: it carries each as a 32-byte fingerprint (ID-8). The
largest record a node accepts does grow, from 753 to 9,601 bytes: 7,896
bytes provide room for a post-quantum record signature and its key
(COMPAT-13), and 952 hold the 28 more key list entries a node may come to
have.
§7 gives the cost of signing records with ML-DSA-65 in that room.

The listener's figures vary by a few bytes between runs, with the size of
its acknowledgements: its first flight measured 7,861 to 7,873 bytes.

## 3. Round trips

Before the dialer's address is validated, QUIC lets the listener send three
times what it received. The dialer's hello is two 1,200-byte datagrams, so
the allowance is 7,200 bytes.

- On `p2p-v2/4` the listener's whole first flight, about 5.8 KB, fit. A dial
  completed after one round trip.
- On `p2p-v2/5` without padding the flight is about 7.9 KB. The listener holds the first datagram
  that would exceed the allowance. It sends the rest once the dialer's
  Handshake acknowledgement validates its address. A dial completes after
  two round trips.

The dialer therefore pads its first flight (spec DIAL-8). Right after the
hello it sends one more Initial datagram, a PING that quinn-proto pads to
1,200 bytes. The listener has then received 3,600 bytes, its allowance is
10,800, and its whole first flight fits. If the padding arrives late, the
listener stays within 7,200 bytes until it receives more bytes or validates
the dialer's address.

Measured in the simulator with a fixed one-way delay:

| RTT | `p2p-v2/4` dial | `p2p-v2/5`, unpadded | `p2p-v2/5`, padded |
|---|---|---|---|
| 2 ms | 2 ms | 4 ms | 2 ms |
| 50 ms | 50 ms | 100 ms | 50 ms |
| 124 ms (Mainnet fleet median) | 124 ms | 248 ms | 124 ms |
| 250 ms (Mainnet fleet p90) | 250 ms | 500 ms | 250 ms |

Without the padding the cost would be one added RTT per new v2 connection,
about 124 ms at the fleet's median.

The padding costs:

- **Upload:** a completed dial sends 48 bytes more, 9,582 bytes in 11
  datagrams in place of 9,534. The padding datagram stands in for the
  acknowledgement the dialer would have sent, and padded to 1,200 bytes,
  while the listener waited. A dial the listener refuses or never answers
  sends the full 1,200 bytes more.
- **Headroom:** about 2.9 KB. The flight is ~7,870 bytes against a 10,800
  byte allowance, so a later change that grows the listener's flight by
  more than that brings the second round trip back. With ML-DSA-87 the
  headroom would have been about 880 bytes.
  `the_listeners_first_flight_fits_the_padded_allowance` fails if it does.
- **Admission:** a refused or retried hybrid dial is now decided and
  answered three times, once per datagram, in place of twice (ADM-1).
- **Classical hellos:** a one-datagram hello isn't padded. A listener keeps
  no state for a hello it refuses at the first packet, so a padding datagram
  would start an attempt of its own with no hello in it.

The unpadded flight overshoots the allowance by about 670 bytes, and only a
category 2 signature would have brought it back under (§5). Validating the
address first with a Retry would cost the same round trip.

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

These measurements include regenerating the ML-DSA signing key for each
signature. The ML-DSA-65 implementation now generates the key once when
loading the identity and reuses it for handshakes (ID-13). The timings below
use the earlier per-signature key generation baseline.

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
- Record signatures: `cargo bench -p zakura-network --bench
  discovery_record`, whose `ed25519_only` rows are the work a record took
  before.
- Fleet: after the Testnet deploy, compare the
  `zakura.quic.handshake.duration_seconds` histogram on the three Testnet
  nodes before and after.

## 7. Record signatures

A prototype signs every discovery record with the node's ML-DSA-65 key as
well as its Ed25519 key, and requires both (ID-7). The record carries the
1,952-byte key and the 3,309-byte signature as two extensions, 5,269 bytes
with their headers. This section records what that costs. "Before" is a
record signed with Ed25519 alone.

### 7.1 Sizes

A default node's record lists its two keys, one IPv4 address and seven
services. The figures come from the encoders, in the test
`a_default_nodes_record_grows_by_the_key_and_signature`.

| | Before | After |
|---|---|---|
| A default node's record, and its `Hello` payload | 361 B | 5,630 B |
| A full sample of 32 such records | 11,554 B | 180,162 B |
| The `Peers` answer a node sends for it | 11,554 B, 32 records | 22,522 B, 4 records |
| The largest record | 9,601 B | 9,601 B |
| The largest `Peers` a node accepts, and sends | 307,234 B, 24,098 B | 307,234 B, 24,098 B |
| A full book file of 256 such records | 113,162 B | 1,462,026 B |
| The largest book file | 2,585,610 B | 2,585,610 B |
| The records of a full book of 10,000 | 3.6 MB | 56.3 MB |

No limit moves. The served `Peers` cap of 24,098 bytes was sized as 32
records without extensions, so it now holds 4 records.

### 7.2 CPU

`cargo bench -p zakura-network --bench discovery_record`, one core
(`taskset -c 7`) of the same DigitalOcean premium Intel host, release
build, mean time. Two other builds were running on the host, so the
figures can be high by a few percent. The ranges span two runs on
2026-11-25. The refused record has one flipped byte in its signature's
commitment hash, so the verifier does all its work, and it was measured in
the second run only.

| Operation | Before | After |
|---|---|---|
| Sign one record | 69–70 µs | 354–363 µs |
| Verify one record | 176–177 µs | 302–303 µs |
| Verify the largest record | n/a | 324–329 µs |
| Refuse a record with a valid Ed25519 and a wrong ML-DSA-65 signature | n/a | 298 µs |
| Decode and verify a `Peers` of 32 records | 6.17–6.18 ms | 10.2–10.7 ms |
| Decode a `Peers` of 32 records, no verification | n/a | 0.44 ms |

Ed25519 verification is the strict one of ID-6, which checks the key and
the signature's `R` for torsion, so it costs more than the 67 µs of §4.1,
and more than the ML-DSA-65 check it now runs before: about 126 µs with the
extension parse and the key fingerprint.
A node signs its record at startup and when the record nears expiry, about
once a day. The later signing runs on the async runtime under the
discovery state lock, as the Ed25519 signing did. A record identical to the
stored one skips verification. A record that verifies and is then
discarded, for its protocol range, expiry, address or a storage limit, is
not stored, so it is verified again each time it arrives: a peer that
answers every `GetPeers` with the same 32 such records costs a node the
`Peers` row above every 15 seconds on that connection, where it cost the
"before" column. Verification runs in a blocking task under a node-wide
slot, a quarter of the cores, for `Hello`, `Peers` and the book file.

### 7.3 Bandwidth

On a shared connection, peer and live-service queries continue every 15
seconds. The node sends its signed `Hello` on a new stream and when the
record changes. Each transmission costs 5,638 bytes including framing.
At 92 persistent connections, one publication costs 518,696 bytes in the
sending direction. With a stable address and service list, the record is
renewed roughly once a day. Reconnects cost one full `Hello` per stream.

For comparison, sending the unchanged record every 15 seconds on all 92
connections would cost 2.99 GB per day in each direction. The older 361-byte
record at that cadence cost 0.20 GB. These are application-frame estimates
for continuously occupied connections and exclude transport overhead.

With a 24,098-byte outgoing payload limit, a full reply of four default
records costs 22,530 bytes with framing. At one such reply every 15 seconds
on 92 connections, this is 138.2 kB/s in the sending direction. Sending one
full 32-record sample to each peer every ten minutes costs about 27.6 kB/s.

A requester gets four default records per answer. One peer's 32-record window
takes at least 8 answers, 105 seconds after the first: a requester excludes
only the records it stored, so one it discards can be served again. A
discovery-only connection, which makes one exchange, yields 4 records.

### 7.4 Cold discovery

The decision metric was time for an empty node to register four distinct peers
beyond its seed. Four is the default number of concurrent discovery dials.
The local Regtest comparison used native QUIC, one seed with 32 confirmed
records, and the ordinary discovery service and candidate dialer. Each record
had one IPv4 address and seven services. On the limited path the seed's UDP
downlink carried 1 Mbit/s with 40 ms of added delay. Candidate dials used
loopback directly. The per-IP allowance was raised to allow the nodes to
share one host.

| Outgoing payload cap | Default records in first reply | Median time to four, all reachable | Median discovery traffic to four, all reachable |
|---|---:|---:|---:|
| 24,098 B | 4 | 1.21 s | 46 kB |
| 48,000 B | 8 | 1.21 s | 58 kB |
| 96,000 B | 17 | 1.21 s | 108 kB |
| 192,000 B | 32 | 2.21 s | 193 kB |

Each row is five fresh requesters using a shared seed connection. Traffic
counts discovery frames read at either end involving the requester, in both
directions. It excludes handshake, packet, and other service traffic.

With 16 of the 32 advertised addresses stale, the requester sometimes had
enough reachable addresses in its book well before it registered four peers.
The dialer allows four concurrent attempts, and a live registration can stay
under observation for up to 60 seconds. For a second comparison, three fresh
requesters per cap had up to 75 seconds to register four non-seed peers.
All twelve reached four.

| Outgoing payload cap | Median time to four, half stale | Median discovery traffic to four, half stale |
|---|---:|---:|
| 24,098 B | 26.2 s | 96 kB |
| 48,000 B | 41.3 s | 193 kB |
| 96,000 B | 31.3 s | 238 kB |
| 192,000 B | 32.3 s | 238 kB |

The outgoing cap stays at 24,098 bytes. Larger replies delivered more
addresses on the first exchange, but used more bytes without a consistent
improvement in registration time. These small local samples cover one
topology. They do not estimate bootstrap latency on Mainnet.

Discovery-only seeds need separate qualification. At the existing cap, the
seed offered four records per full answer. With all candidates reachable,
both unrestricted controls decoded a four-record answer, while neither of
the two bandwidth-limited controls decoded a nonempty answer. None registered
a non-seed peer within 20 seconds. These observations do not establish where
delivery or import stopped. The shared-connection results above apply to a
seed that stays connected across exchanges.
