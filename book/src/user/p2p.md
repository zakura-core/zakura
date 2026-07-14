# Experimental Zakura P2P v2

The native Zakura P2P v2 stack is **experimental**. It is off by default on
Mainnet, where an unset `network.p2p_stack` resolves to the legacy TCP stack.
Testnet and Regtest currently default to dual-stack operation so the new stack
gets exercised while legacy peers remain available.

The experimental designation applies to the native Zakura stack used by the
`"zakura"` and `"dual"` modes. It does not apply to the legacy Zcash TCP stack.

## Current tradeoff

The stack has bounded framing, admission, connection, and queue mechanisms, but
it intentionally does not yet impose universal limits on a node's total usable
bandwidth or total number of requests. Avoiding those blunt global limits lets
different message types use available capacity and supports high-throughput
sync and gossip. The tradeoff is that there are known denial-of-service vectors
in which a peer can consume disproportionate bandwidth or request-processing
capacity.

Treat `p2p_stack = "zakura"` and `p2p_stack = "dual"` as opt-in testing modes,
especially on Mainnet. Operators should monitor resource use and should not
rely on the native stack as a hardened trust boundary yet.

## Removing the designation

Zakura P2P v2 will remain experimental until it:

- defines and enforces explicit resource and behavior expectations for every
  message type; and
- prioritizes latency-sensitive, consensus-critical streams over bulk or
  background traffic such as block sync and mempool traffic.

These controls must bound abusive work without imposing a universal ceiling
that prevents a well-behaved node from using its available bandwidth.
