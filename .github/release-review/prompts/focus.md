# Review focus

## Consensus

Check invalid-block acceptance, valid-block rejection, checkpoint versus semantic
validation, upgrade-height boundaries, transaction balances, lock times, expiry,
serialization, state commitments, reorg handling, and consistent validation
across entry points. Trace effects across chain, consensus, script, and state.

## Cryptography

Check signature and proof verification, transcript and domain separation,
public-input binding, canonical encodings, malformed inputs, key/nonce handling,
and whether batching or concurrency skips a required check. Distinguish public
data from secret data before claiming a side channel. Require a concrete
violation; do not treat an unfamiliar construction as proof of insecurity.

## Mining

Check block templates, transaction dependencies, fee and coinbase calculations,
size and signature-operation limits, difficulty and time rules, template
freshness, long polling, proposal validation, and block submission. Identify how
a miner would lose work or construct a block peers reject.

## Block propagation

Check announcements, peer selection, block requests, validation scheduling,
queues, backpressure, cancellation, retries, timeouts, and resource accounting.
Look for lost wakeups, head-of-line stalls, starvation, repeated downloads,
or resource growth that materially delays valid blocks. Explain the triggering
sequence and mechanism; do not invent benchmark numbers.
