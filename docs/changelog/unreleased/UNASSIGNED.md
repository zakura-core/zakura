<!-- PLACEHOLDER: rename this file to <PR-number>.md and replace the two
     pull/UNASSIGNED links below with the real number once the draft PR is
     opened. Fragments are numbered by PR — see README.md in this directory. -->

## Changed

- `zakurad` now shares a native discovery record with other peers only after it
  confirms that record's owner directly, either through a first-party discovery
  exchange with the owner or a successful dial. Records learned second-hand from
  another peer still serve as local dial hints, so bootstrap reach is unchanged
  ([#UNASSIGNED](https://github.com/zakura-core/zakura/pull/UNASSIGNED)).
- Native discovery answers a peer's `GetPeers` request from a bounded per-peer
  record set that is stable for a ten-minute window. Repeated requests from one
  peer, however they vary their limits, service filters, or exclusion lists, no
  longer walk further through the local discovery book
  ([#UNASSIGNED](https://github.com/zakura-core/zakura/pull/UNASSIGNED)).
