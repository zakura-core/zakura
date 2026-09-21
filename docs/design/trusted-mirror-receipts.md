# Trusted mirror receipt order

The trusted indexer block stream carries optional `receipt_order` metadata from
the primary verifier. A secondary retains that value rather than treating its
own delivery order as the primary's arrival order. This supports the
first-received mining policy added separately.

Receipt orders belong to one primary process. The `x-zakura-receipt-session`
response header identifies that process, and a secondary returns it as
`receipt_session` when reconnecting. The server ignores known tips when the
session is absent or different. If the response session changes, the secondary
clears its old orders and resubscribes with empty tips before accepting blocks.
The published state also clears, and finalized-tip tracking continues if the
replacement stream is empty.

Within the same session, reconnects retain existing orders and duplicate blocks
do not replace them. Older servers omit the session and receipt fields. The
secondary ignores orders from unidentified sessions and uses the hash fallback.
Older clients can decode the extended block messages but cannot reproduce the
first-received preference. Upgrade trusted secondaries alongside their primary.

These fields are additive on the wire. The Rust block structs and listener item
change public APIs. Ordinary block serialization is unchanged. This change
retains the existing incremental block stream and its full-buffer disconnect
behavior. Complete fork snapshots and general mirror recovery are separate work.
