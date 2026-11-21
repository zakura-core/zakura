## Changed

- **Breaking for v2 P2P:** v2 handshakes are now signed with a composite of
  Ed25519 and the post-quantum ML-DSA-44, and a peer is accepted only when
  both signatures verify. A node's ML-DSA-44 key derives from its existing
  key file, so no file changes, but every node's ID changes once more; this
  ships in the same release as the node ID change, so deployed IDs change
  once ([#PENDING](https://github.com/zakura-core/zakura/pull/PENDING)).
