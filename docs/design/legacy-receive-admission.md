# Legacy receive admission

Legacy streams use the same transport frame reader as native services. Their message types, reserved flags, and fixed or bounded payload lengths are now checked before payload allocation. Impossible legacy lengths reach the fatal oversize path before local capacity checks. These checks retain the existing wire limits and exact-budget EOF behavior.

## Ownership

Each connection's receive and output shares bound what one connection holds, and the node's peer memory budget counts those shares for every connection it admits. The limits here close the gaps those shares leave on the legacy streams.

Each phase of a legacy request's read, its first header byte, the rest of its header and its payload, has a local ten-second deadline, capped by any shorter negotiated idle timeout. Partial progress within a phase cannot renew it. A wait for room in the connection's raw-frame gate between header and payload doesn't count, because that wait is local rather than the peer's. Expiry closes the stalled peer's connection, so a peer holds a request's raw-frame grant for at most ten seconds.

Responder encoding stops at 8,192 frames per response. Chunk iteration allocates no per-chunk index, and each frame is charged before its payload allocation. Tiny peer-selected caps therefore cause a local resource reset instead of millions of small allocations.

Requesters allow as many frames as their responder's chunks need for the requested items, plus one, computed from the frame cap the requester advertised and capped at the same 8,192 frames. An empty request therefore allows a single frame. The byte allowance includes a chunk header for every admitted frame, still subject to the 16 MiB aggregate wire ceiling. With at most 8,192 frames, per-frame overhead stays near 1 MiB, so the frame ceiling and that aggregate bound a response's retained memory. A legal full block can therefore cross smaller negotiated frames even when its serialized size equals the negotiated message limit. Exhausting the framing allowance resets the exchange without marking conforming chunks as unsolicited.

Decoded gossip has eight node-wide permits acquired before decoding and retained through queueing and service processing. Gossip therefore occupies at most eight entries of the shared 256-entry service queue, leaving room for legacy requests. Each peer may hold at most two of those permits across all of its gossip sessions, so reopening the gossip stream adds none and one peer cannot keep every other peer's gossip out. Holding all eight takes at least four peer identities, each with its own connection and handshake.
