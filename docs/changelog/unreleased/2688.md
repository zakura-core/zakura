<!-- changelog: none -->

This PR adds fuzz targets for the post-quantum handshake's credential, key-list
and composite-signature parsing, with their drivers behind `zakura-quic`'s new
off-by-default `p2p-fuzz` feature. Release builds don't change.
