//! Zakura is a Zcash full node written in Rust. Follow the [introductory
//! page](https://github.com/zakura-core/zakura#documentation) in the Zakura documentation to learn more.
//!
//! ## About Zcash
//!
//! Zcash is a cryptocurrency designed to preserve the user's privacy. Like most
//! cryptocurrencies, it works by a collection of software nodes run by members of
//! the Zcash community or any other interested parties. The nodes talk to each
//! other in peer-to-peer fashion in order to maintain the state of the Zcash
//! blockchain. They also communicate with miners who create new blocks. When a
//! Zcash user sends Zcash, their wallet broadcasts transactions to these nodes
//! which will eventually reach miners, and the mined transaction will then go
//! through Zcash nodes until they reach the recipient's wallet which will report
//! the received Zcash to the recipient.
//!
//! ## Alternative Implementations
//!
//! The original Zcash node is named `zcashd` and is developed by the Electric Coin
//! Company as a fork of the original Bitcoin node. Zakura, on the other hand, is
//! an independent Zcash node implementation developed from scratch. Since they
//! implement the same protocol, `zcashd` and Zakura nodes can communicate with each
//! other and maintain the Zcash network together.
//!
//! ## Zakura Advantages
//!
//! These are some of the advantages or benefits of Zakura:
//!
//! - Better performance: since it was implemented from scratch in an async, parallelized way, Zakura
//!   is currently faster than `zcashd`.
//! - Better security: since it is developed in a memory-safe language (Rust), Zakura
//!   is less likely to be affected by memory-safety and correctness security bugs that
//!   could compromise the environment where it is run.
//! - Better governance: with a new node deployment, there will be more developers
//!   who can implement different features for the Zcash network.
//! - Dev accessibility: supports more developers, which gives new developers
//!   options for contributing to Zcash protocol development.
//! - Runtime safety: with an independent implementation, the detection of consensus bugs
//!   can happen quicker, reducing the risk of consensus splits.
//! - Spec safety: with several node implementations, it is much easier to notice
//!   bugs and ambiguity in protocol specification.
//! - User options: different nodes present different features and tradeoffs for
//!   users to decide on their preferred options.
//! - Additional contexts: wider target deployments for people to use a consensus
//!   node in more contexts e.g. mobile, wasm, etc.
//!
//! ## Configuration
//!
//! The command below places the generated `zakura.toml` config file in the default preferences directory of Linux:
//!
//! ```console
//! zakurad generate -o ~/.config/zakura.toml
//! ```
//!
//! See [`config::ZakuradConfig`] for other OSes default locations or more information about how to configure Zakura.
//!
//! ## Zakura Feature Flags
//!
//! The following [Cargo
//! features](https://doc.rust-lang.org/cargo/reference/features.html#command-line-feature-options)
//! are available at compile time:
//!
//! ### Metrics
//!
//! * configuring a `tracing.progress_bar`: shows key metrics in the terminal using progress bars,
//!   and automatically configures Zakura to send logs to a file.
//!   (The `progress-bar` feature is activated by default.)
//! * `prometheus`: export metrics to prometheus.
//!
//! ### Tracing
//!
//! Sending traces to different subscribers:
//! * configuring a `tracing.log_file`: appends traces to a file on disk.
//! * `journald`: send tracing spans and events to `systemd-journald`.
//! * `sentry`: send crash and panic events to sentry.io.
//! * `flamegraph`: generate a flamegraph of tracing spans.
//!
//! Changing the traces that are collected:
//! * `filter-reload`: dynamically reload tracing filters at runtime.
//! * `error-debug`: enable extra debugging in release builds.
//! * `tokio-console`: enable tokio's `console-subscriber` (needs [specific compiler flags])
//! * A set of features that [skip verbose tracing].
//!   The default features ignore `debug` and `trace` logs in release builds.
//!
//! [skip verbose tracing]: https://docs.rs/tracing/0.1.35/tracing/level_filters/index.html#compile-time-filters
//! [specific compiler flags]: https://docs.rs/console-subscriber/latest/console_subscriber/#enabling-tokio-instrumentation
//!
//! ### Testing
//!
//! * `proptest-impl`: enable randomised test data generation.
//! * `lightwalletd-grpc-tests`: enable Zakura JSON-RPC tests that query `lightwalletd` using gRPC.
//!
//! ### Experimental
//!
//! * `elasticsearch`: save block data into an Elasticsearch database.
//! * `internal-miner`: enable experimental support for mining inside Zakura, without an external
//!   mining pool. This feature is only supported on testnet. Use a GPU or ASIC on mainnet for
//!   efficient mining.
//!
//! ## Zakura crates
//!
//! [The Zakura monorepo](https://github.com/zakura-core/zakura) is a collection of the following
//! crates:
//!
//! - [zakura-tower-batch-control](https://docs.rs/zakura-tower-batch-control/latest/tower_batch_control/)
//! - [zakura-tower-fallback](https://docs.rs/zakura-tower-fallback/latest/tower_fallback/)
//! - [zakura-chain](https://docs.rs/zakura-chain/latest/zakura_chain/)
//! - [zakura-consensus](https://docs.rs/zakura-consensus/latest/zakura_consensus/)
//! - [zakura-network](https://docs.rs/zakura-network/latest/zakura_network/)
//! - [zakura-node-services](https://docs.rs/zakura-node-services/latest/zakura_node_services/)
//! - [zakura-rpc](https://docs.rs/zakura-rpc/latest/zakura_rpc/)
//! - [zakura-script](https://docs.rs/zakura-script/latest/zakura_script/)
//! - [zakura-state](https://docs.rs/zakura-state/latest/zakura_state/)
//! - [zakura-test](https://docs.rs/zakura-test/latest/zakura_test/)
//! - [zakura-utils](https://docs.rs/zakura-utils/latest/zakura_utils/)
//! - [zakura](https://docs.rs/zakura/latest/zakurad/)
//!
//! The links in the list above point to the documentation of the public APIs of the crates. For
//! the documentation of the internal APIs, follow <https://zakura-core.github.io/zakura/internal> that lists
//! all Zakura crates as well in the left sidebar.

#![doc(html_root_url = "https://docs.rs/zakura")]
// Tracing causes false positives on this lint:
// https://github.com/tokio-rs/tracing/issues/553
#![allow(clippy::cognitive_complexity)]

#[macro_use]
extern crate tracing;

/// Error type alias to make working with tower traits easier.
///
/// Note: the 'static lifetime bound means that the *type* cannot have any
/// non-'static lifetimes, (e.g., when a type contains a borrow and is
/// parameterized by 'a), *not* that the object itself has 'static lifetime.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub mod application;
pub mod commands;
pub mod components;
pub mod config;
pub mod prelude;

#[cfg(feature = "sentry")]
pub(crate) mod sentry;
