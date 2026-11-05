# Handoff floor availability

`missing-availability.patch` prevents the synchronous receipt handoff from
marking its peer unavailable to a concurrent ranker.
The existing simulator witness is
`block_resource_properties::an_unmeasured_routine_asks_for_the_floor_while_the_others_are_paused`.
It requires an unmeasured routine to request the withheld floor before reset.

```sh
RUSTFLAGS='--cfg zakura_sim' cargo test --locked -p zakura-sim \
  --test block_resource_properties -- \
  an_unmeasured_routine_asks_for_the_floor_while_the_others_are_paused --exact
```

Run from unchanged source, then apply the patch, then restore it and rerun.
The intended failure is the `asks_for_the_withheld_floor_before_the_reset`
assertion. A compiler error, timeout, or unrelated failure is not detection.
This case has no `qa/planted` input sidecar or registered property ID.
Its test mapping does not establish current-commit sensitivity.
