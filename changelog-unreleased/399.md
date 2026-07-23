<!-- changelog: none -->

This PR only refactors CI tooling: the mempool load-test harness now delegates
per-node config localization and chain seeding to `kresko localize-fleet` /
`kresko seed-local` instead of doing the TOML rewriting and block submission in
Python. The rendered per-node configs are byte-identical to before. It has no
operator- or crate-consumer-visible effect on zakurad or the published crates.
