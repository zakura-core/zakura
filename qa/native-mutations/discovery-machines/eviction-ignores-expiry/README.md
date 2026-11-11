# Book eviction ignores expiry

`ZakuraDiscoveryBook::next_eviction_candidate` skips its first rule: an
expired entry goes first. A full book evicts by failures and confirmation
instead, so expired records outlive fresh unconfirmed ones. The retained
planted patch `wrong-eviction-victim` flips the confirmation order. This
patch drops the expiry order.

The witness's history fills the book after two disclosure epochs, when the
first records have expired (step 12 or step 16). The model evicts an expired
record, and the patched book evicts relay-supplied author 5 or another
unconfirmed record.

## Witness

`qa/planted/discovery/record-book/skip-book-eviction.toml`, the work list's witness for `discovery/record-book`. It holds on `main`, and
its committed input reaches the patched code.

## Reproduce

The loop applies the patch in its own worktree. Run it over this
property's cases:

```sh
cargo xtask planted loop --only discovery/record-book --out <report dir>
```

Required failure: the run ends at `discovery/record-book`.
