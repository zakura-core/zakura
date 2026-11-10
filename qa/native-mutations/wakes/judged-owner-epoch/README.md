# Reactor reports the judged owner's epoch

`judged-owner-epoch.patch` is a mutation-loop patch for the `wakes` work-list
entry. Fault class: stale-result.

## What it breaks

The reactor's `body_completion_authority` returns the epoch of the
owner the gate judged as the completion's `committed_epoch`, instead of the
committed view's body-work epoch. A stale completion has no judged owner,
so it carries no epoch. The sequencer's stale branch then never finds that
the completion arrived under the epoch it holds, and a stale reply that
frees room wakes no waiting work.

## Witness

The witness is `qa/planted/block-sync/capacity-release-wakes/stale-completion-no-wake.toml`, which runs
`crates/zakura-sim/tests/fixtures/block-sync-capacity-frontier-first.bin`.

The input fills the 401-submission window, then resets to a fork from
height 1, which detaches it. Storage commits height 1, and block sync
handles the tip before the reply (#1523's order). The reply is stale and
has no `applying` entry at its height, so only `holds_committed_epoch`
lets it wake the waiting fork body. With no epoch, the body stays
unsubmitted.

## Remaining guards

The sequencer's stale branch still wakes work when the completion has a
semantic owner; the input's stale reply has none.

## Reproduce

```sh
git apply qa/native-mutations/wakes/judged-owner-epoch/judged-owner-epoch.patch
cargo xtask planted loop --out "$TMPDIR/loop" --only block-sync/capacity-release-wakes
```

Required failure: the run ends at `block-sync/capacity-release-wakes` with a violation.
A compiler error, timeout, panic, or another rule's violation is not
detection.
