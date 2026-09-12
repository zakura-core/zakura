# Transport patch

These are copies of the exact crates.io versions in transport-provenance.json.
Their original licenses and package manifests are retained. The workspace keeps
registry version requirements and selects this family with a root patch table.

The local patch carries an application owner into QUIC connection construction.
The owner is the final field of the asynchronous connection State, after protocol
buffers, queued packets and send storage. Closing or cancelling a handshake does
not release it early. Callers must reserve before calling the owned constructor.
Chunks returned to an application can outlive State and need separate ownership.

The iroh endpoint builder forwards incoming queue limits to the endpoint's server
configuration. These limits apply before acceptance. Their byte counts exclude
each incoming connection's first datagram, which needs a separate allowance.

The endpoint also bounds datagrams waiting for one connection driver to 256.
Excess datagrams are discarded before the protocol acknowledges them. Local close
and endpoint control events retain their existing ordered path. A packet slot is
retained through protocol processing, where receive flow control takes over.
The packet-count bound needs an allocation-size allowance for receive batching.

Router's optional incoming admission callback transfers that owner before spawning
handshake work. The native node shares one transport slot pool across inbound and
outbound connections. Slots remain charged after the application handler exits
while any stream handle or transport driver retains connection state. The native
endpoint also caps its raw incoming queue before Router can accept an attempt.

Optional limits bound receive fragment records and locally initiated stream state.
A local stream retains its slot until both halves are freed, including stopped
receive halves awaiting their final offset. Fragment compaction frees retained
heap capacity on stop and pool reuse. The fragment limit does not bound unordered
read history or the separate crypto stream. Native message readers use ordered
reads. The new optional limits have not yet been selected for native defaults.

The send window counts retained payload, including acknowledged tails behind a
missing prefix. Reset releases abandoned send storage before refunding its space
and preserves the final offset required by RESET_STREAM. Rejected early data
returns both local stream slots and retained send capacity.

An optional packet-history limit bounds the packet-number span of sent and lost
records in each path and encryption space. The indexed buffers allocate for gaps
as well as occupied records, so a record count alone is insufficient. Exhausting
this local limit terminates the connection before inserting another record and
discards any unfinished transmit batch. Native defaults do not yet enable it.

This is work in progress. The node memory envelope, metadata and buffer allocation
allowances, production window policy and final qualification remain incomplete.
The patch is not evidence of production compliance.
Cargo patches are not inherited by downstream workspaces, so publishing packages
that use these new APIs requires publishing the transport changes and updating
the registry requirements first. This branch must not be called release-ready
while it relies on unpublished APIs.

The nested vendor workspace exists to run dependency tests with their development
dependencies. Its lockfile does not change root dependency resolution. Use the
same target directory as the parent workspace to avoid a duplicate build cache:

```sh
cargo nextest run --manifest-path vendor/Cargo.toml --locked -p noq-proto --lib
```
