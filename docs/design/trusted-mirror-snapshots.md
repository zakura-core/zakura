# Trusted mirror snapshots

The trusted indexer stream identifies the primary process with the
`x-zakura-receipt-session` response header. A secondary returns that value as
`receipt_session` on resubscription. If it differs, the server ignores known tips
and sends a complete snapshot. The secondary clears its local forks and
resubscribes with empty tips when the response session changes. Legacy servers
omit this identity, so reconnecting with existing forks also requires a reset.

Secondaries request `include_chain_snapshot` to receive a `chain_snapshot`
message after each batch of blocks. Its complete retained tip set covers changes
to already-known blocks, including invalidation and reconsideration. Snapshot
messages contain no block data. Missing tips cause a complete resubscription.
An empty tip set clears the mirror's non-finalized state.

With session-aware servers, blocks stay private until the snapshot marker
reconciles the full fork set. Only then does the mirror publish the new state and
tip. A disconnected batch is discarded and resubscription uses the last published
snapshot. Incomplete snapshots and block commit failures request all forks again
in empty private staging, so obsolete forks cannot consume the replacement's
fork capacity. The last published state stays visible until a complete
replacement is ready.

Legacy servers retain incremental publication. After the block syncer takes over
publication, a legacy reset immediately clears the published fork and refreshes
the finalized tip, even if the replacement stream sends no messages. An initial
empty snapshot leaves finalized-tip tracking active during checkpoint sync. The
first real block transfers that responsibility to the block syncer.

Snapshot clients can drain a full listener buffer and reconcile the completed
batch. Streams without snapshot support close when that buffer fills so legacy
clients reconnect after possible gaps in state updates. Servers send snapshot
messages only to clients that request them. Ordinary block encodings and the
P2P header protocol are unchanged.
