//! `cargo xtask check-rt-imports`: the v2 P2P code takes runtime names from
//! `zakura-rt`.
//!
//! The check parses every production module under `zakura-network`'s
//! `zakura` module and in `zakura-quic`, and flags paths to tokio, the std mutex and atomics, std's
//! hashed collections, and the thread RNG. It also flags the std blocking
//! primitives and channels, `futures::select!` and `tokio_util::time`, which
//! `zakura-rt` has no names for: under the simulator a blocking wait hangs the
//! run's only thread, and `futures::select!` shuffles its branches with a
//! thread RNG. Code gated by a test-only `cfg` is skipped. Clippy's `disallowed_types` can't do this job while `zakura-rt`
//! only re-exports, because a re-export resolves to the same item.
//!
//! The suggested replacements come from the `pub use` items in `zakura-rt`'s
//! `lib.rs`, so the crate stays the one list of runtime names.
//!
//! Time and threads follow the same rule. The check flags std's `Instant`,
//! the wall-clock reads `SystemTime::now`, `Utc::now` and `Local::now`, std's
//! thread spawns, sleeps and parks, and whole-module imports of `std::time` and
//! `std::thread`. `Duration`, `UNIX_EPOCH`, the `SystemTime` type and
//! `std::thread::panicking` stay allowed.
//!
//! Scopes aren't tracked. A first pass records every path each name is
//! bound to by a production `use`, or by a non-generic `type` alias of a plain
//! path, anywhere in the file, in any module, block, `cfg` alternative or
//! macro argument. A path is flagged if its written
//! spelling, or any expansion of it, hits a rule: expansion drops `self`,
//! `super`, `crate` and the file's inline module names from the front, and
//! replaces a bound first segment with each path the file binds it to,
//! renames and local re-exports included. Expansion can only add findings, so
//! the bare spellings `SystemTime::now`, `Utc::now`, `Local::now` and
//! `Arc::downgrade` stay flagged however they're imported. Qualified calls
//! such as `<Instant>::now()` are checked as the plain path.
//! `UNIX_EPOCH.elapsed()`, under any parentheses or `&`, and
//! `SystemTime::elapsed` read the wall clock, so they're flagged. In a file
//! allowed to name std's `Instant`, every `.elapsed()` call is flagged, since
//! the receiver's type isn't known. Macro arguments that parse as expressions
//! or statements, as `dbg!`, `assert!`, `assert_eq!` and `println!` take, are
//! visited as code under the same rules.
//!
//! The trade-off is false positives on purpose. A file that binds one name to
//! both a std clock and a zakura_rt one is flagged wherever it uses the name,
//! even where Rust would pick the zakura_rt one, and renaming one import fixes
//! it. Because `crate::` is dropped, a local shim module named like a runtime
//! crate, such as `crate::tokio`, would be flagged too. Nothing in the tree
//! trips either today.
//!
//! Limits: macros with syntax of their own, such as `tokio::select!` or a
//! DSL, can't be parsed generically and only get a token scan. It sees
//! written-out paths like `std::time::Instant::now`, expanded through the
//! file's names, but not imports made inside the macro. Only simple path
//! aliases are followed: a generic `type` alias, or one whose target isn't a
//! plain path, isn't. `.elapsed()` on a
//! `SystemTime` value other than the epoch isn't flagged outside the files
//! that may name std's `Instant`, since the check doesn't know a value's type.
//! The timer adapters in `tokio_stream` and
//! `std::thread::available_parallelism`, a host property discovery reads,
//! aren't covered yet.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, fs,
    path::{Path, PathBuf},
};

use proc_macro2::{Spacing, Span, TokenStream, TokenTree};
use syn::{
    punctuated::Punctuated,
    spanned::Spanned,
    visit::{self, Visit},
    Attribute, Item, Meta, Token, UseTree,
};

use crate::BoxError;

/// The root modules the check walks, each with the child modules under it
/// that are not v2 production code.
const ROOTS: [(&str, &[&str]); 2] = [
    (
        "crates/zakura-network/src/zakura.rs",
        &["legacy_gossip", "testkit"],
    ),
    ("crates/zakura-quic/src/lib.rs", &[]),
];

const RT_LIB: &str = "crates/zakura-rt/src/lib.rs";

/// Other spellings of the paths `zakura-rt` re-exports, rewritten in order.
const ALIASES: [(&str, &str); 3] = [
    ("core", "std"),
    ("tokio::spawn", "tokio::task::spawn"),
    (
        "std::hash::RandomState",
        "std::collections::hash_map::RandomState",
    ),
];

/// `rand` functions that draw from the thread RNG.
const THREAD_RNG_FUNCTIONS: [&str; 7] = [
    "random",
    "random_range",
    "random_bool",
    "random_ratio",
    "random_iter",
    "fill",
    "make_rng",
];

/// `std::thread` items that start, block or yield an OS thread, which a
/// simulated run can't schedule.
const THREAD_ITEMS: [&str; 7] = [
    "spawn",
    "sleep",
    "park",
    "park_timeout",
    "scope",
    "Builder",
    "yield_now",
];

/// Direct runtime paths the v2 code may keep, with the reason for each.
///
/// Each entry allows one exact path, so allowing a type doesn't allow its
/// associated items, such as `Instant::now`.
///
/// For `zakura-quic` this is the bypass inventory M3.7 asks for. Two more
/// pieces of state outlive a simulated run without a runtime path to flag:
/// the Linux GSO probe's `OnceLock`, which only the production socket
/// reaches, and `ConnShared`'s connection counter, a trace ID that numbers
/// connections process-wide and never reaches the wire or a decision. The
/// interface watcher reads the host's interfaces through `getifaddrs`, but it
/// starts only for a wildcard socket, and `bind_with` refuses one inside a
/// simulated run.
const ALLOWED: [(&str, &str, &str); 24] = [
    (
        "crates/zakura-network/src/zakura/handler.rs",
        "tokio::sync::oneshot::Sender",
        "legacy gossip's exchange-start signal, and legacy code stays on tokio",
    ),
    (
        "crates/zakura-network/src/zakura.rs",
        "tokio::sync::mpsc::Sender",
        "the legacy address book's updater, and legacy code stays on tokio",
    ),
    (
        "crates/zakura-quic/src/socket.rs",
        "tokio::net::UdpSocket",
        PRODUCTION_SOCKET,
    ),
    (
        "crates/zakura-quic/src/socket.rs",
        "tokio::net::UdpSocket::from_std",
        PRODUCTION_SOCKET,
    ),
    (
        "crates/zakura-quic/src/socket.rs",
        "tokio::io::Interest::READABLE",
        SOCKET_READINESS,
    ),
    (
        "crates/zakura-quic/src/socket.rs",
        "tokio::io::Interest::WRITABLE",
        SOCKET_READINESS,
    ),
    (
        "crates/zakura-quic/src/sim/net.rs",
        "std::sync::Mutex",
        SIM_HARNESS,
    ),
    (
        "crates/zakura-quic/src/sim/net.rs",
        "std::sync::MutexGuard",
        SIM_HARNESS,
    ),
    (
        "crates/zakura-quic/src/sim/net.rs",
        "std::sync::PoisonError",
        SIM_HARNESS,
    ),
    (
        "crates/zakura-quic/src/sim/net.rs",
        "std::sync::Mutex::new",
        SIM_HARNESS,
    ),
    (
        "crates/zakura-quic/src/sim/net.rs",
        "std::sync::PoisonError::into_inner",
        SIM_HARNESS,
    ),
    (
        "crates/zakura-quic/src/sim/seeded.rs",
        "std::sync::Mutex",
        SIM_HARNESS,
    ),
    (
        "crates/zakura-quic/src/sim/seeded.rs",
        "std::sync::MutexGuard",
        SIM_HARNESS,
    ),
    (
        "crates/zakura-quic/src/sim/seeded.rs",
        "std::sync::PoisonError",
        SIM_HARNESS,
    ),
    (
        "crates/zakura-quic/src/sim/seeded.rs",
        "std::sync::Weak",
        SIM_HARNESS,
    ),
    (
        "crates/zakura-quic/src/sim/seeded.rs",
        "std::sync::Arc::downgrade",
        SIM_HARNESS,
    ),
    (
        "crates/zakura-quic/src/sim/seeded.rs",
        "Arc::downgrade",
        SIM_HARNESS,
    ),
    (
        "crates/zakura-quic/src/sim/seeded.rs",
        "std::sync::Mutex::new",
        SIM_HARNESS,
    ),
    (
        "crates/zakura-quic/src/sim/seeded.rs",
        "std::sync::Mutex::default",
        SIM_HARNESS,
    ),
    (
        "crates/zakura-quic/src/sim/seeded.rs",
        "std::sync::PoisonError::into_inner",
        SIM_HARNESS,
    ),
    (
        "crates/zakura-quic/src/congestion.rs",
        "std::time::Instant",
        QUINN_INSTANT,
    ),
    (
        "crates/zakura-quic/src/driver/connection.rs",
        "std::time::Instant",
        QUINN_INSTANT,
    ),
    (
        "crates/zakura-quic/src/driver/endpoint.rs",
        "std::time::Instant",
        QUINN_INSTANT,
    ),
    (
        "crates/zakura-quic/src/sim/net.rs",
        "std::time::Instant",
        SIM_HARNESS,
    ),
];

/// Why `zakura-quic` may name tokio's socket.
const PRODUCTION_SOCKET: &str = "the production socket and its Linux fast path, which a \
     simulated run replaces with an injected socket through the socket factory";

/// Why `zakura-quic` may name tokio's readiness flags.
const SOCKET_READINESS: &str =
    "readiness for the production socket, which a simulated run never binds";

/// Why `zakura-quic`'s driver may name std's `Instant`.
const QUINN_INSTANT: &str = "quinn-proto's API takes std's Instant, and the driver \
     reads time only through driver::connection::now, which reads zakura_rt's \
     clock and so follows a simulated run's virtual time";

/// Why `zakura-quic`'s `sim` module may use std's runtime names. It compiles
/// only under `cfg(any(test, zakura_sim))`, which the check doesn't treat as
/// test-only, and it's harness infrastructure, not code under test.
const SIM_HARNESS: &str = "harness infrastructure for simulated runs: the fake \
     network and the process-wide seed pool, not code under test";

const MISSING_NAME: &str = "a zakura_rt name (add one if it's missing)";

const MODULE_IMPORT: &str = "zakura_rt names, imported by item";

pub fn check(repo_root: &Path) -> Result<(), BoxError> {
    let rules = Rules::load(repo_root)?;
    let mut findings = Findings::default();
    for (root, excluded) in ROOTS {
        check_file(
            repo_root,
            &rules,
            &repo_root.join(root),
            excluded,
            &mut findings,
        )?;
    }

    for (used, (file, prefix, _)) in findings.allowances_used.iter().zip(ALLOWED) {
        if !used {
            findings.violations.push(Violation {
                location: file.to_owned(),
                path: prefix.to_owned(),
                replacement: "nothing: no code uses this allowlist entry, so remove it".to_owned(),
            });
        }
    }

    if findings.violations.is_empty() {
        println!(
            "{} v2 P2P files import runtime names through zakura-rt",
            findings.files
        );
        return Ok(());
    }

    for violation in &findings.violations {
        eprintln!("{violation}");
    }
    Err(format!(
        "{} runtime paths bypass zakura-rt; import them from zakura_rt, \
         or add an allowlist entry with a reason in crates/xtask/src/rt_imports.rs",
        findings.violations.len()
    )
    .into())
}

#[derive(Default)]
struct Findings {
    files: usize,
    violations: Vec<Violation>,
    allowances_used: [bool; ALLOWED.len()],
}

fn parse_file(repo_root: &Path, path: &Path) -> Result<syn::File, BoxError> {
    let relative = relative_path(repo_root, path);
    let source = fs::read_to_string(path).map_err(|e| format!("{relative}: {e}"))?;
    syn::parse_file(&source).map_err(|e| {
        let start = e.span().start();
        format!("{relative}:{}:{}: {e}", start.line, start.column + 1).into()
    })
}

fn relative_path(repo_root: &Path, path: &Path) -> String {
    path.strip_prefix(repo_root)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned()
}

/// Checks the module in `path` and its child modules, skipping the children
/// named in `excluded`.
fn check_file(
    repo_root: &Path,
    rules: &Rules,
    path: &Path,
    excluded: &[&str],
    findings: &mut Findings,
) -> Result<(), BoxError> {
    let file = parse_file(repo_root, path)?;
    if is_test_only(&file.attrs) {
        return Ok(());
    }
    findings.files += 1;

    let relative = relative_path(repo_root, path);
    PathVisitor::new(&relative, rules, findings).visit_file_items(&file.items);

    // `foo.rs` and `foo/mod.rs` both keep their child modules in `foo/`, and
    // a crate's `lib.rs` keeps them beside it.
    let child_dir = if path
        .file_name()
        .is_some_and(|name| name == "mod.rs" || name == "lib.rs")
    {
        path.parent()
            .ok_or_else(|| format!("{relative} has no parent directory"))?
            .to_path_buf()
    } else {
        path.with_extension("")
    };
    check_child_modules(
        repo_root,
        rules,
        &child_dir,
        &file.items,
        excluded,
        findings,
    )
}

/// Checks the child modules declared in `items`, whose files are in `dir`,
/// skipping the ones named in `excluded`.
fn check_child_modules(
    repo_root: &Path,
    rules: &Rules,
    dir: &Path,
    items: &[Item],
    excluded: &[&str],
    findings: &mut Findings,
) -> Result<(), BoxError> {
    for item in items {
        let Item::Mod(module) = item else { continue };
        let name = module.ident.to_string();
        if is_test_only(&module.attrs) || excluded.contains(&name.as_str()) {
            continue;
        }
        if module.attrs.iter().any(|attr| attr.path().is_ident("path")) {
            return Err(format!(
                "module `{name}` in {} sets #[path], which the check can't follow",
                relative_path(repo_root, dir)
            )
            .into());
        }
        match &module.content {
            // The visitor already walked the inline module's items.
            Some((_, items)) => {
                check_child_modules(repo_root, rules, &dir.join(&name), items, &[], findings)?
            }
            None => {
                let path = module_file(repo_root, dir, &name)?;
                check_file(repo_root, rules, &path, &[], findings)?;
            }
        }
    }
    Ok(())
}

fn module_file(repo_root: &Path, dir: &Path, name: &str) -> Result<PathBuf, BoxError> {
    [
        dir.join(format!("{name}.rs")),
        dir.join(name).join("mod.rs"),
    ]
    .into_iter()
    .find(|path| path.is_file())
    .ok_or_else(|| {
        format!(
            "no file for module `{name}` in {}",
            relative_path(repo_root, dir)
        )
        .into()
    })
}

/// Whether a `cfg` attribute compiles the item only in tests.
fn is_test_only(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("cfg")
            && attr
                .parse_args::<Meta>()
                .is_ok_and(|predicate| implies_test(&predicate))
    })
}

/// Whether the `cfg` predicate holds only when `test` holds.
fn implies_test(predicate: &Meta) -> bool {
    match predicate {
        Meta::Path(path) => path.is_ident("test"),
        Meta::List(list) if list.path.is_ident("all") => list
            .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            .is_ok_and(|predicates| predicates.iter().any(implies_test)),
        _ => false,
    }
}

/// The runtime paths `zakura-rt` re-exports, and the names it gives them.
struct Rules {
    reexports: Vec<Reexport>,
}

struct Reexport {
    source: Vec<String>,
    name: Vec<String>,
}

impl Rules {
    fn load(repo_root: &Path) -> Result<Self, BoxError> {
        let file = parse_file(repo_root, &repo_root.join(RT_LIB))?;
        let mut reexports = Vec::new();
        collect_reexports(&file.items, &mut Vec::new(), &mut reexports);
        Ok(Self { reexports })
    }

    /// The `zakura_rt` name to use instead of `segments`, if `segments` is a
    /// runtime path the v2 code must not name directly.
    fn replacement(&self, segments: &[String]) -> Option<String> {
        let path = resolve_aliases(segments);
        if let Some(reexport) = self
            .reexports
            .iter()
            .find(|reexport| path.starts_with(&reexport.source))
        {
            let name = reexport
                .name
                .iter()
                .chain(&path[reexport.source.len()..])
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join("::");
            return Some(format!("zakura_rt::{name}"));
        }
        // A module import would let the code name re-exported items through it.
        if self.reexports.iter().any(|reexport| {
            reexport.source.len() > path.len() && reexport.source.starts_with(&path)
        }) {
            return Some(MODULE_IMPORT.to_owned());
        }
        let path: Vec<&str> = path.iter().map(String::as_str).collect();
        match path.as_slice() {
            ["rand_10", name] if THREAD_RNG_FUNCTIONS.contains(name) => {
                Some("zakura_rt::rand::rng".to_owned())
            }
            ["rand_10", "rngs", "ThreadRng" | "SysRng", ..]
            | ["tokio", ..]
            | ["tokio_util", "sync" | "task" | "time", ..]
            | ["std", "sync", "RwLock" | "RwLockReadGuard" | "RwLockWriteGuard" | "Condvar" | "Barrier"
            | "BarrierWaitResult" | "mpsc", ..]
            | ["futures" | "futures_util", "select"] => Some(MISSING_NAME.to_owned()),
            ["std", "thread", name, ..] if THREAD_ITEMS.contains(name) => {
                Some(MISSING_NAME.to_owned())
            }
            // `Arc::downgrade` makes std's `Weak`, whose reads have no
            // preemption points.
            ["Arc", "downgrade"] | ["std" | "alloc", "sync", "Arc", "downgrade"] => {
                Some("zakura_rt::sync::downgrade".to_owned())
            }
            ["std", "time", "Instant", rest @ ..] => Some(
                ["zakura_rt::time::Instant"]
                    .into_iter()
                    .chain(rest.iter().copied())
                    .collect::<Vec<_>>()
                    .join("::"),
            ),
            // The wall clock. `UNIX_EPOCH.elapsed()` reads it too, and the
            // visitor checks that call as the path `UNIX_EPOCH::elapsed`.
            ["SystemTime", "now" | "elapsed"]
            | ["std", "time", "SystemTime", "now" | "elapsed"]
            | ["UNIX_EPOCH", "elapsed"]
            | ["std", "time", "UNIX_EPOCH", "elapsed"]
            | ["SystemTime", "UNIX_EPOCH", "elapsed"]
            | ["std", "time", "SystemTime", "UNIX_EPOCH", "elapsed"]
            | ["Utc" | "Local", "now"]
            | ["chrono", "Utc" | "Local", "now"]
            | ["chrono", "offset", "Utc" | "Local", "now"] => {
                Some("zakura_rt::time::utc_now".to_owned())
            }
            // A module import would let the code name the items above through it.
            ["std", "time" | "thread"] => Some(MODULE_IMPORT.to_owned()),
            _ => None,
        }
    }
}

fn collect_reexports(items: &[Item], module: &mut Vec<String>, reexports: &mut Vec<Reexport>) {
    for item in items {
        match item {
            Item::Use(item) if matches!(item.vis, syn::Visibility::Public(_)) => {
                for leaf in use_leaves(&item.tree) {
                    if let Some(name) = leaf.name {
                        reexports.push(Reexport {
                            source: leaf.path,
                            name: module.iter().cloned().chain([name]).collect(),
                        });
                    }
                }
            }
            Item::Mod(item) => {
                if let Some((_, items)) = &item.content {
                    module.push(item.ident.to_string());
                    collect_reexports(items, module, reexports);
                    module.pop();
                }
            }
            _ => {}
        }
    }
}

fn resolve_aliases(segments: &[String]) -> Vec<String> {
    let mut path = segments.to_vec();
    for (alias, target) in ALIASES {
        if starts_with(&path, alias) {
            path.splice(
                ..alias.split("::").count(),
                target.split("::").map(str::to_owned),
            );
        }
    }
    path
}

/// Whether `path` starts with the whole segments of `prefix`, like `a::b`.
fn starts_with(path: &[String], prefix: &str) -> bool {
    let prefix: Vec<&str> = prefix.split("::").collect();
    path.len() >= prefix.len() && path.iter().zip(prefix).all(|(a, b)| a == b)
}

/// One leaf of a `use` tree.
struct UseLeaf {
    /// The full path the leaf imports.
    path: Vec<String>,
    /// The name the leaf binds, or `None` for a glob.
    name: Option<String>,
    span: Span,
}

fn use_leaves(tree: &UseTree) -> Vec<UseLeaf> {
    fn walk(tree: &UseTree, prefix: &mut Vec<String>, leaves: &mut Vec<UseLeaf>) {
        let mut leaf = |path: Vec<String>, name: Option<String>, span| {
            leaves.push(UseLeaf { path, name, span });
        };
        match tree {
            UseTree::Path(path) => {
                prefix.push(path.ident.to_string());
                walk(&path.tree, prefix, leaves);
                prefix.pop();
            }
            UseTree::Name(name) if name.ident == "self" => {
                leaf(prefix.clone(), prefix.last().cloned(), name.span());
            }
            UseTree::Name(name) => {
                let path = prefix.iter().cloned().chain([name.ident.to_string()]);
                leaf(path.collect(), Some(name.ident.to_string()), name.span());
            }
            UseTree::Rename(rename) => {
                let mut path = prefix.clone();
                if rename.ident != "self" {
                    path.push(rename.ident.to_string());
                }
                leaf(path, Some(rename.rename.to_string()), rename.span());
            }
            UseTree::Glob(glob) => leaf(prefix.clone(), None, glob.span()),
            UseTree::Group(group) => {
                for tree in &group.items {
                    walk(tree, prefix, leaves);
                }
            }
        }
    }
    let mut leaves = Vec::new();
    walk(tree, &mut Vec::new(), &mut leaves);
    leaves
}

/// How many `use` hops expansion follows from one path before it stops.
const MAX_ALIAS_HOPS: usize = 8;

/// What a file that may name std's `Instant` uses instead of `.elapsed()`.
const ELAPSED_IN_STD_INSTANT_FILE: &str = "`saturating_duration_since` against zakura_rt's \
     clock, such as the driver's `now()`: this file may name std's Instant, so `.elapsed()` \
     can read the host clock";

struct PathVisitor<'a> {
    file: &'a str,
    rules: &'a Rules,
    findings: &'a mut Findings,
    /// Every path each name is bound to by a production `use` anywhere in
    /// the file: any module, block, `cfg` alternative or macro argument.
    names: BTreeMap<String, BTreeSet<Vec<String>>>,
    /// The names of the file's inline modules.
    modules: BTreeSet<String>,
    /// True on the first pass, which only records `names` and `modules`.
    collecting: bool,
}

impl<'a> PathVisitor<'a> {
    /// A visitor for the file at `file`, a path from the repository root.
    fn new(file: &'a str, rules: &'a Rules, findings: &'a mut Findings) -> Self {
        PathVisitor {
            file,
            rules,
            findings,
            names: BTreeMap::new(),
            modules: BTreeSet::new(),
            collecting: false,
        }
    }

    /// Visits a file's items twice: first to record every name its `use`
    /// items bind and its inline modules, then to check its paths.
    fn visit_file_items(&mut self, items: &[Item]) {
        for collecting in [true, false] {
            self.collecting = collecting;
            for item in items {
                self.visit_item(item);
            }
        }
    }

    /// `segments` and every path it can expand to, the most expanded first.
    /// `self`, `super`, `crate` and the file's inline module names drop off
    /// the front, and a bound first segment becomes each path the file
    /// binds that name to, anywhere, so `SystemTime::now` after
    /// `use clocks::SystemTime` also reads as std's when `clocks` re-exports
    /// it. Only binding replacements count toward [`MAX_ALIAS_HOPS`].
    /// Scopes aren't tracked, so expansion can only add findings.
    fn expansions(&self, segments: &[String]) -> Vec<Vec<String>> {
        let mut seen = BTreeSet::new();
        let mut layers = vec![self.without_prefixes(segments.to_vec(), &mut seen)];
        for _ in 0..MAX_ALIAS_HOPS {
            let mut next = Vec::new();
            for path in layers.last().into_iter().flatten() {
                for target in self.names.get(&path[0]).into_iter().flatten() {
                    let target = target.iter().chain(&path[1..]).cloned().collect();
                    next.extend(self.without_prefixes(target, &mut seen));
                }
            }
            if next.is_empty() {
                break;
            }
            layers.push(next);
        }
        layers.into_iter().rev().flatten().collect()
    }

    /// `path` and each shorter path left by dropping `self`, `super`,
    /// `crate` or an inline module name off its front, skipping any in
    /// `seen` and adding the rest to it.
    fn without_prefixes(
        &self,
        mut path: Vec<String>,
        seen: &mut BTreeSet<Vec<String>>,
    ) -> Vec<Vec<String>> {
        let mut paths = Vec::new();
        loop {
            if seen.insert(path.clone()) {
                paths.push(path.clone());
            }
            let first = path[0].as_str();
            if path.len() == 1
                || !(matches!(first, "self" | "super" | "crate") || self.modules.contains(first))
            {
                return paths;
            }
            path.remove(0);
        }
    }

    /// Checks the path written as `segments` and each of its expansions, and
    /// reports one violation if any of them is forbidden here.
    fn check_written(&mut self, segments: &[String], span: Span) {
        for path in self.expansions(segments) {
            if self.check(&path, span) {
                return;
            }
        }
    }

    /// Whether the allowlist lets this file name std's `Instant`.
    fn names_std_instant(&self) -> bool {
        ALLOWED
            .iter()
            .any(|(file, path, _)| *file == self.file && *path == "std::time::Instant")
    }

    /// Checks a path written as `segments`: as written if it starts with `::`,
    /// and otherwise through [`Self::check_written`].
    fn check_path(&mut self, segments: &[String], absolute: bool, span: Span) {
        if absolute {
            self.check(segments, span);
        } else {
            self.check_written(segments, span);
        }
    }

    /// Reports `segments` if the rules forbid it and the allowlist doesn't
    /// allow it here. Returns whether it reported.
    fn check(&mut self, segments: &[String], span: Span) -> bool {
        if self.collecting {
            return false;
        }
        let Some(replacement) = self.rules.replacement(segments) else {
            return false;
        };
        let allowance = ALLOWED
            .iter()
            .position(|(file, path, _)| *file == self.file && segments.join("::") == *path);
        if let Some(allowance) = allowance {
            self.findings.allowances_used[allowance] = true;
            return false;
        }
        self.report(segments.join("::"), replacement, span);
        true
    }

    /// Records a violation of `path` at `span`, suggesting `replacement`.
    fn report(&mut self, path: String, replacement: String, span: Span) {
        let start = span.start();
        self.findings.violations.push(Violation {
            location: format!("{}:{}:{}", self.file, start.line, start.column + 1),
            path,
            replacement,
        });
    }

    /// Checks written-out `a::b::c` paths inside macro arguments that don't
    /// parse as code, expanded through the file's names. Imports inside such
    /// arguments aren't seen.
    fn check_tokens(&mut self, tokens: TokenStream) {
        let tokens: Vec<TokenTree> = tokens.into_iter().collect();
        let mut i = 0;
        while i < tokens.len() {
            match &tokens[i] {
                TokenTree::Group(group) => {
                    self.check_tokens(group.stream());
                    i += 1;
                }
                TokenTree::Ident(ident) if !continues_path(&tokens, i) => {
                    let mut segments = vec![ident.to_string()];
                    let mut next = i + 1;
                    while let (Some(TokenTree::Ident(segment)), true) =
                        (tokens.get(next + 2), is_path_separator(&tokens, next))
                    {
                        segments.push(segment.to_string());
                        next += 3;
                    }
                    if segments.len() > 1 {
                        self.check_written(&segments, ident.span());
                    }
                    i = next;
                }
                _ => i += 1,
            }
        }
    }
}

impl<'ast> Visit<'ast> for PathVisitor<'_> {
    fn visit_item(&mut self, item: &'ast Item) {
        if !item_attrs(item).is_some_and(is_test_only) {
            visit::visit_item(self, item);
        }
    }

    fn visit_impl_item(&mut self, item: &'ast syn::ImplItem) {
        let attrs = match item {
            syn::ImplItem::Const(item) => &item.attrs,
            syn::ImplItem::Fn(item) => &item.attrs,
            syn::ImplItem::Type(item) => &item.attrs,
            syn::ImplItem::Macro(item) => &item.attrs,
            _ => return visit::visit_impl_item(self, item),
        };
        if !is_test_only(attrs) {
            visit::visit_impl_item(self, item);
        }
    }

    fn visit_trait_item(&mut self, item: &'ast syn::TraitItem) {
        let attrs = match item {
            syn::TraitItem::Const(item) => &item.attrs,
            syn::TraitItem::Fn(item) => &item.attrs,
            syn::TraitItem::Type(item) => &item.attrs,
            syn::TraitItem::Macro(item) => &item.attrs,
            _ => return visit::visit_trait_item(self, item),
        };
        if !is_test_only(attrs) {
            visit::visit_trait_item(self, item);
        }
    }

    fn visit_field(&mut self, field: &'ast syn::Field) {
        if !is_test_only(&field.attrs) {
            visit::visit_field(self, field);
        }
    }

    fn visit_field_value(&mut self, field: &'ast syn::FieldValue) {
        if !is_test_only(&field.attrs) {
            visit::visit_field_value(self, field);
        }
    }

    fn visit_variant(&mut self, variant: &'ast syn::Variant) {
        if !is_test_only(&variant.attrs) {
            visit::visit_variant(self, variant);
        }
    }

    fn visit_arm(&mut self, arm: &'ast syn::Arm) {
        if !is_test_only(&arm.attrs) {
            visit::visit_arm(self, arm);
        }
    }

    fn visit_local(&mut self, local: &'ast syn::Local) {
        if !is_test_only(&local.attrs) {
            visit::visit_local(self, local);
        }
    }

    fn visit_stmt_macro(&mut self, stmt: &'ast syn::StmtMacro) {
        if !is_test_only(&stmt.attrs) {
            visit::visit_stmt_macro(self, stmt);
        }
    }

    fn visit_expr(&mut self, expr: &'ast syn::Expr) {
        if !expr_attrs(expr).is_some_and(is_test_only) {
            visit::visit_expr(self, expr);
        }
    }

    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        if self.collecting && item.content.is_some() {
            self.modules.insert(item.ident.to_string());
        }
        visit::visit_item_mod(self, item);
    }

    /// Checks `receiver.elapsed()` as the path `receiver::elapsed` when the
    /// receiver is a path, under any parentheses or `&`, so
    /// `(&UNIX_EPOCH).elapsed()` meets the wall-clock rule. In a file that
    /// may name std's `Instant`, every `.elapsed()` is flagged, since the
    /// receiver's type isn't known.
    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        if call.method == "elapsed" && !self.collecting && self.names_std_instant() {
            let replacement = ELAPSED_IN_STD_INSTANT_FILE.to_owned();
            self.report(".elapsed()".to_owned(), replacement, call.method.span());
            return visit::visit_expr_method_call(self, call);
        }
        let mut receiver = &*call.receiver;
        while let syn::Expr::Paren(syn::ExprParen { expr, .. })
        | syn::Expr::Reference(syn::ExprReference { expr, .. })
        | syn::Expr::Group(syn::ExprGroup { expr, .. }) = receiver
        {
            receiver = expr;
        }
        if let (syn::Expr::Path(receiver), true) = (receiver, call.method == "elapsed") {
            if let Some((mut segments, absolute)) = expr_path_segments(receiver) {
                segments.push(call.method.to_string());
                self.check_path(&segments, absolute, call.method.span());
            }
        }
        visit::visit_expr_method_call(self, call);
    }

    /// Checks `<Type>::item` and `<Type as Trait>::item` as the path
    /// `Type::item`, since the type and the item are visited apart.
    fn visit_expr_path(&mut self, expr: &'ast syn::ExprPath) {
        if expr.qself.is_some() {
            if let Some((segments, absolute)) = expr_path_segments(expr) {
                self.check_path(&segments, absolute, expr.span());
            }
        }
        visit::visit_expr_path(self, expr);
    }

    /// Records a non-generic `type X = path;` as a binding of `X`, like a
    /// `use`, then checks the aliased path.
    fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
        let mut ty = &*item.ty;
        while let syn::Type::Paren(syn::TypeParen { elem, .. })
        | syn::Type::Group(syn::TypeGroup { elem, .. }) = ty
        {
            ty = elem;
        }
        if let (true, syn::Type::Path(syn::TypePath { qself: None, path })) =
            (self.collecting && item.generics.params.is_empty(), ty)
        {
            let target = path
                .segments
                .iter()
                .map(|segment| segment.ident.to_string());
            self.names
                .entry(item.ident.to_string())
                .or_default()
                .insert(target.collect());
        }
        visit::visit_item_type(self, item);
    }

    fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
        for leaf in use_leaves(&item.tree) {
            if !self.collecting {
                self.check(&leaf.path, leaf.span);
            } else if let Some(name) = leaf.name.filter(|name| name != "_") {
                self.names.entry(name).or_default().insert(leaf.path);
            }
        }
    }

    fn visit_item_extern_crate(&mut self, item: &'ast syn::ItemExternCrate) {
        if item.rename.is_some() {
            self.check(&[item.ident.to_string()], item.span());
        }
    }

    fn visit_path(&mut self, path: &'ast syn::Path) {
        let segments: Vec<String> = path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect();
        // A one-segment path names a local item or binding, like a `core`
        // variable, not a crate.
        if path.leading_colon.is_some() {
            self.check(&segments, path.span());
        } else if segments.len() > 1 {
            self.check_written(&segments, path.span());
        }
        visit::visit_path(self, path);
    }

    /// Visits a macro's arguments as code when they parse as
    /// comma-separated expressions, like `dbg!` and `assert_eq!`, or as
    /// statements, so imports and calls inside them meet the same rules.
    /// Arguments with syntax of their own, like `select!`'s, get the token
    /// scan.
    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        self.visit_path(&mac.path);
        if let Ok(args) = mac.parse_body_with(Punctuated::<syn::Expr, Token![,]>::parse_terminated)
        {
            for arg in &args {
                Visit::visit_expr(self, arg);
            }
        } else if let Ok(stmts) = mac.parse_body_with(syn::Block::parse_within) {
            for stmt in &stmts {
                Visit::visit_stmt(self, stmt);
            }
        } else {
            self.check_tokens(mac.tokens.clone());
        }
    }
}

/// The segments of an expression path, with a qualified type joined to its
/// associated item so `<Type as Trait>::item` reads as `Type::item`, and
/// whether the path starts with `::`. `None` for a qualified type that isn't
/// a plain path.
fn expr_path_segments(expr: &syn::ExprPath) -> Option<(Vec<String>, bool)> {
    let idents = |segments: &mut dyn Iterator<Item = &syn::PathSegment>| {
        segments.map(|segment| segment.ident.to_string()).collect()
    };
    let Some(qself) = &expr.qself else {
        let segments = idents(&mut expr.path.segments.iter());
        return Some((segments, expr.path.leading_colon.is_some()));
    };
    let mut ty = &*qself.ty;
    while let syn::Type::Paren(syn::TypeParen { elem, .. })
    | syn::Type::Group(syn::TypeGroup { elem, .. }) = ty
    {
        ty = elem;
    }
    let syn::Type::Path(syn::TypePath { qself: None, path }) = ty else {
        return None;
    };
    let mut segments = path
        .segments
        .iter()
        .chain(expr.path.segments.iter().skip(qself.position));
    Some((idents(&mut segments), path.leading_colon.is_some()))
}

fn item_attrs(item: &Item) -> Option<&[Attribute]> {
    Some(match item {
        Item::Const(item) => &item.attrs,
        Item::Enum(item) => &item.attrs,
        Item::ExternCrate(item) => &item.attrs,
        Item::Fn(item) => &item.attrs,
        Item::ForeignMod(item) => &item.attrs,
        Item::Impl(item) => &item.attrs,
        Item::Macro(item) => &item.attrs,
        Item::Mod(item) => &item.attrs,
        Item::Static(item) => &item.attrs,
        Item::Struct(item) => &item.attrs,
        Item::Trait(item) => &item.attrs,
        Item::TraitAlias(item) => &item.attrs,
        Item::Type(item) => &item.attrs,
        Item::Union(item) => &item.attrs,
        Item::Use(item) => &item.attrs,
        _ => return None,
    })
}

/// The attributes of expressions that can stand as a statement.
fn expr_attrs(expr: &syn::Expr) -> Option<&[Attribute]> {
    Some(match expr {
        syn::Expr::Assign(expr) => &expr.attrs,
        syn::Expr::Async(expr) => &expr.attrs,
        syn::Expr::Await(expr) => &expr.attrs,
        syn::Expr::Block(expr) => &expr.attrs,
        syn::Expr::Call(expr) => &expr.attrs,
        syn::Expr::ForLoop(expr) => &expr.attrs,
        syn::Expr::If(expr) => &expr.attrs,
        syn::Expr::Loop(expr) => &expr.attrs,
        syn::Expr::Macro(expr) => &expr.attrs,
        syn::Expr::Match(expr) => &expr.attrs,
        syn::Expr::MethodCall(expr) => &expr.attrs,
        syn::Expr::Try(expr) => &expr.attrs,
        syn::Expr::Unsafe(expr) => &expr.attrs,
        syn::Expr::While(expr) => &expr.attrs,
        _ => return None,
    })
}

fn is_path_separator(tokens: &[TokenTree], i: usize) -> bool {
    matches!(
        (tokens.get(i), tokens.get(i + 1)),
        (Some(TokenTree::Punct(first)), Some(TokenTree::Punct(second)))
            if first.as_char() == ':' && first.spacing() == Spacing::Joint && second.as_char() == ':'
    )
}

/// Whether the ident at `i` continues a path, as `b` does in `a::b`. In
/// `::b` or `, ::b`, it starts one.
fn continues_path(tokens: &[TokenTree], i: usize) -> bool {
    i >= 3
        && is_path_separator(tokens, i - 2)
        && match &tokens[i - 3] {
            TokenTree::Ident(_) => true,
            TokenTree::Punct(punct) => punct.as_char() == '>',
            _ => false,
        }
}

struct Violation {
    location: String,
    path: String,
    replacement: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: `{}` bypasses zakura-rt; use {}",
            self.location, self.path, self.replacement
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each violation in `source` as `path -> replacement`.
    fn violations(source: &str) -> Vec<String> {
        violations_in("fixture.rs", source)
    }

    #[test]
    fn suggestions_name_the_zakura_rt_reexport() {
        let source = "
            use tokio::sync::{mpsc, Mutex, MutexGuard};
            use core::sync::atomic::Ordering;
            fn f() {
                tokio::spawn(async {});
                let _ = std::hash::RandomState::new();
                let _ = rand_10::random::<u8>();
            }
        ";
        assert_eq!(
            violations(source),
            [
                "tokio::sync::mpsc -> zakura_rt::sync::mpsc",
                "tokio::sync::Mutex -> zakura_rt::sync::AsyncMutex",
                "tokio::sync::MutexGuard -> a zakura_rt name (add one if it's missing)",
                "core::sync::atomic::Ordering -> zakura_rt::sync::atomic::Ordering",
                "tokio::spawn -> zakura_rt::task::spawn",
                "std::hash::RandomState::new -> zakura_rt::collections::hash_map::RandomState::new",
                "rand_10::random -> zakura_rt::rand::rng",
            ]
        );
    }

    #[test]
    fn module_imports_renames_and_globs_are_flagged() {
        let source = "
            use std::sync;
            use std::{self as s};
            use rand_10::*;
            extern crate tokio as tk;
            use std::{collections::BTreeMap, sync::Arc};
            use tokio_util::codec::Framed;
            fn f(core: Core) -> u8 { core.0 }
        ";
        assert_eq!(
            violations(source),
            [
                "std::sync -> zakura_rt names, imported by item",
                "std -> zakura_rt names, imported by item",
                "rand_10 -> zakura_rt names, imported by item",
                "tokio -> zakura_rt names, imported by item",
            ]
        );
    }

    #[test]
    fn macro_arguments_are_checked() {
        let source = "
            async fn f(d: Duration) {
                tokio::select! { _ = ::tokio::time::sleep(d) => {} }
                info!(\"{:?}\", crate::tokio::spawn);
            }
        ";
        // `crate::tokio` may be a local module, but expansion drops `crate`
        // and can only add findings.
        assert_eq!(
            violations(source),
            [
                "tokio::select -> zakura_rt::select",
                "tokio::time::sleep -> zakura_rt::time::sleep",
                "tokio::spawn -> zakura_rt::task::spawn",
            ]
        );
    }

    #[test]
    fn blocking_primitives_and_shuffled_selects_are_flagged() {
        let source = "
            use std::sync::{Condvar, RwLock, mpsc::channel};
            use futures::select;
            use tokio_util::time::DelayQueue;
            async fn f(barrier: &core::sync::Barrier) {
                futures::select! { _ = a => {} }
                futures::select_biased! { _ = a => {} }
                let _ = futures::future::select(a, b);
                let _ = std::sync::Arc::new(());
            }
        ";
        let missing = MISSING_NAME;
        assert_eq!(
            violations(source),
            [
                format!("std::sync::Condvar -> {missing}"),
                format!("std::sync::RwLock -> {missing}"),
                format!("std::sync::mpsc::channel -> {missing}"),
                format!("futures::select -> {missing}"),
                format!("tokio_util::time::DelayQueue -> {missing}"),
                format!("core::sync::Barrier -> {missing}"),
                format!("futures::select -> {missing}"),
            ]
        );
    }

    #[test]
    fn std_downgrades_are_flagged() {
        let source = "
            fn f(a: &Arc<u8>, r: &Rc<u8>) {
                let _ = Arc::downgrade(a);
                let _ = std::sync::Arc::downgrade(a);
                let _ = core::sync::Arc::downgrade(a);
                let _ = Rc::downgrade(r);
                let _ = zakura_rt::sync::downgrade(a);
            }
        ";
        assert_eq!(
            violations(source),
            [
                "Arc::downgrade -> zakura_rt::sync::downgrade",
                "std::sync::Arc::downgrade -> zakura_rt::sync::downgrade",
                "core::sync::Arc::downgrade -> zakura_rt::sync::downgrade",
            ]
        );
    }

    #[test]
    fn std_clocks_and_threads_are_flagged() {
        let source = "
            use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
            use std::thread::{panicking, Builder};
            fn f() {
                let _ = std::time::Instant::now();
                let _ = SystemTime::now();
                let _ = std::time::SystemTime::now();
                let _ = std::time::SystemTime::UNIX_EPOCH;
                let _ = Utc::now();
                let _ = Local::now();
                let _ = chrono::Utc::now();
                let _ = chrono::Local::now();
                let _ = chrono::offset::Utc::now();
                let _ = chrono::offset::Local::now();
                let _ = chrono::Utc::now;
                std::thread::spawn(|| {});
                std::thread::sleep(Duration::ZERO);
                std::thread::park();
                std::thread::park_timeout(Duration::ZERO);
                std::thread::scope(|_| {});
                std::thread::yield_now();
                let _ = std::thread::panicking();
            }
        ";
        let (missing, utc_now) = (MISSING_NAME, "zakura_rt::time::utc_now");
        assert_eq!(
            violations(source),
            [
                "std::time::Instant -> zakura_rt::time::Instant".to_owned(),
                format!("std::thread::Builder -> {missing}"),
                "std::time::Instant::now -> zakura_rt::time::Instant::now".to_owned(),
                format!("std::time::SystemTime::now -> {utc_now}"),
                format!("std::time::SystemTime::now -> {utc_now}"),
                format!("Utc::now -> {utc_now}"),
                format!("Local::now -> {utc_now}"),
                format!("chrono::Utc::now -> {utc_now}"),
                format!("chrono::Local::now -> {utc_now}"),
                format!("chrono::offset::Utc::now -> {utc_now}"),
                format!("chrono::offset::Local::now -> {utc_now}"),
                format!("chrono::Utc::now -> {utc_now}"),
                format!("std::thread::spawn -> {missing}"),
                format!("std::thread::sleep -> {missing}"),
                format!("std::thread::park -> {missing}"),
                format!("std::thread::park_timeout -> {missing}"),
                format!("std::thread::scope -> {missing}"),
                format!("std::thread::yield_now -> {missing}"),
            ]
        );
    }

    #[test]
    fn std_time_and_thread_module_imports_are_flagged() {
        let source = "
            use std::time::*;
            use std::time;
            use std::time as t;
            use std::thread;
            use std::thread as threads;
            use std::{thread::{self}, time::{Duration, UNIX_EPOCH}};
            use core::time::Duration as CoreDuration;
        ";
        assert_eq!(
            violations(source),
            [
                "std::time -> zakura_rt names, imported by item",
                "std::time -> zakura_rt names, imported by item",
                "std::time -> zakura_rt names, imported by item",
                "std::thread -> zakura_rt names, imported by item",
                "std::thread -> zakura_rt names, imported by item",
                "std::thread -> zakura_rt names, imported by item",
            ]
        );
    }

    /// Each violation in `source` as `path -> replacement`, checked as the
    /// repository file `file`, so its allowlist entries apply.
    fn violations_in(file: &str, source: &str) -> Vec<String> {
        let repo_root = crate::repo_root().expect("xtask lives in the workspace");
        let rules = Rules::load(&repo_root).expect("zakura-rt's lib.rs parses");
        let parsed = syn::parse_file(source).expect("the fixture parses");
        let mut findings = Findings::default();
        PathVisitor::new(file, &rules, &mut findings).visit_file_items(&parsed.items);
        findings
            .violations
            .iter()
            .map(|violation| format!("{} -> {}", violation.path, violation.replacement))
            .collect()
    }

    /// The driver may name std's `Instant` for quinn-proto, but reading the
    /// clock through it bypasses `driver::connection::now`.
    #[test]
    fn an_allowed_instant_type_does_not_allow_reading_the_clock() {
        let source = "
            use std::time::{Duration, Instant};
            fn f(deadline: Option<Instant>) -> Instant {
                let _ = Instant::now();
                std::time::Instant::now()
            }
        ";
        assert_eq!(
            violations_in("crates/zakura-quic/src/driver/connection.rs", source),
            [
                "std::time::Instant::now -> zakura_rt::time::Instant::now",
                "std::time::Instant::now -> zakura_rt::time::Instant::now",
            ]
        );
    }

    /// Renamed imports resolve, in the module, in a block, and in a child
    /// module, since the check doesn't track scopes.
    #[test]
    fn renamed_imports_are_flagged() {
        let source = "
            use std::time::SystemTime as Clock;
            use chrono::Utc as U;
            use chrono::offset::Local as L;
            use std::sync::Arc as Shared;
            fn f(a: &Shared<u8>) {
                let _ = Clock::now();
                let _ = U::now();
                let _ = L::now();
                let _ = Shared::downgrade(a);
                let _ = Clock::UNIX_EPOCH;
            }
            fn g() {
                use std::time::Instant as Tick;
                let _ = Tick::now();
            }
            mod inner {
                fn h() { let _ = Clock::now(); }
            }
        ";
        assert_eq!(
            violations(source),
            [
                "std::time::SystemTime::now -> zakura_rt::time::utc_now",
                "chrono::Utc::now -> zakura_rt::time::utc_now",
                "chrono::offset::Local::now -> zakura_rt::time::utc_now",
                "std::sync::Arc::downgrade -> zakura_rt::sync::downgrade",
                "std::time::Instant -> zakura_rt::time::Instant",
                "std::time::Instant::now -> zakura_rt::time::Instant::now",
                "std::time::SystemTime::now -> zakura_rt::time::utc_now",
            ]
        );
    }

    /// `elapsed` on `UNIX_EPOCH` is `SystemTime::now` underneath, however
    /// the epoch is named.
    #[test]
    fn unix_epoch_elapsed_is_flagged() {
        let source = "
            use std::time::{SystemTime, UNIX_EPOCH};
            fn f() {
                let _ = UNIX_EPOCH.elapsed();
                let _ = std::time::UNIX_EPOCH.elapsed();
                let _ = SystemTime::UNIX_EPOCH.elapsed();
                let _ = SystemTime::elapsed(&UNIX_EPOCH);
                let _ = UNIX_EPOCH.duration_since(UNIX_EPOCH);
                let _ = Instant::elapsed(&start);
            }
        ";
        let utc_now = "zakura_rt::time::utc_now";
        assert_eq!(
            violations(source),
            [
                format!("std::time::UNIX_EPOCH::elapsed -> {utc_now}"),
                format!("std::time::UNIX_EPOCH::elapsed -> {utc_now}"),
                format!("std::time::SystemTime::UNIX_EPOCH::elapsed -> {utc_now}"),
                format!("std::time::SystemTime::elapsed -> {utc_now}"),
            ]
        );
    }

    /// Local re-exports, `self` and `super` paths and chained renames resolve
    /// to std's clocks, and a path that leaves the file keeps its written
    /// spelling's check. A zakura_rt rename spelled `SystemTime` is flagged
    /// too: the file also binds that name to std's clock, and the written
    /// spelling is always checked.
    #[test]
    fn local_reexports_and_chained_aliases_resolve() {
        let source = "
            mod clocks {
                pub use std::time::SystemTime;
                pub mod inner {
                    pub use super::SystemTime as Wall;
                }
            }
            use clocks::SystemTime;
            use self::clocks::inner::Wall;
            use chrono::Utc as U1;
            use U1 as U2;
            use crate::elsewhere::Utc;
            fn f() {
                let _ = SystemTime::now();
                let _ = Wall::now();
                let _ = U2::now();
                let _ = Utc::now();
            }
            fn g() {
                use zakura_rt::time::Instant as SystemTime;
                let _ = SystemTime::now();
            }
        ";
        let utc_now = "zakura_rt::time::utc_now";
        assert_eq!(
            violations(source),
            [
                format!("std::time::SystemTime::now -> {utc_now}"),
                format!("std::time::SystemTime::now -> {utc_now}"),
                format!("chrono::Utc::now -> {utc_now}"),
                format!("Utc::now -> {utc_now}"),
                format!("std::time::SystemTime::now -> {utc_now}"),
            ]
        );

        let source = "
            mod clocks {
                pub use std::time::Instant;
            }
            use clocks::Instant;
            fn f() -> Instant {
                Instant::now()
            }
        ";
        assert_eq!(
            violations_in("crates/zakura-quic/src/driver/connection.rs", source),
            ["std::time::Instant::now -> zakura_rt::time::Instant::now"]
        );
    }

    /// `cfg` alternatives that bind one name are all checked, whichever
    /// comes last.
    #[test]
    fn every_conditional_binding_is_checked() {
        let sim = "#[cfg(zakura_sim)] use std::time::Instant;";
        let production = "#[cfg(not(zakura_sim))] use zakura_rt::time::Instant;";
        for (first, second) in [(sim, production), (production, sim)] {
            let source = format!("{first} {second} fn f() -> Instant {{ Instant::now() }}");
            assert_eq!(
                violations_in("crates/zakura-quic/src/driver/connection.rs", &source),
                ["std::time::Instant::now -> zakura_rt::time::Instant::now"],
                "{first} {second}"
            );
        }

        let source = "
            #[cfg(zakura_sim)]
            use std::time::SystemTime;
            #[cfg(not(zakura_sim))]
            use zakura_rt::time::Instant as SystemTime;
            fn f() { let _ = SystemTime::now(); }
        ";
        assert_eq!(
            violations(source),
            ["std::time::SystemTime::now -> zakura_rt::time::utc_now"]
        );
    }

    /// A qualified call names the same associated item as the plain path.
    #[test]
    fn qualified_clock_reads_are_flagged() {
        let source = "
            use std::time::{Instant, SystemTime};
            fn f() {
                let _ = <std::time::Instant>::now();
                let _ = <Instant>::now();
                let _ = <std::time::SystemTime>::now();
                let _ = <(SystemTime)>::now();
                let _ = <SystemTime as Wall>::now();
            }
        ";
        let instant = "std::time::Instant::now -> zakura_rt::time::Instant::now";
        let system = "std::time::SystemTime::now -> zakura_rt::time::utc_now";
        assert_eq!(
            violations_in("crates/zakura-quic/src/driver/connection.rs", source),
            [instant, instant, system, system, system]
        );
    }

    /// The epoch's `elapsed` is flagged through parentheses and references.
    /// The `zakura_rt` epoch in `g` is flagged too, since the file also binds
    /// `UNIX_EPOCH` to std's and the spelling is always checked.
    #[test]
    fn wrapped_epoch_receivers_are_flagged() {
        let source = "
            use std::time::{SystemTime, UNIX_EPOCH};
            fn f() {
                let _ = (std::time::UNIX_EPOCH).elapsed();
                let _ = (&UNIX_EPOCH).elapsed();
                let _ = (&(SystemTime::UNIX_EPOCH)).elapsed();
                let _ = <SystemTime>::UNIX_EPOCH.elapsed();
            }
            fn g() {
                use zakura_rt::time::UNIX_EPOCH;
                let _ = (&UNIX_EPOCH).elapsed();
            }
        ";
        let utc_now = "zakura_rt::time::utc_now";
        assert_eq!(
            violations(source),
            [
                format!("std::time::UNIX_EPOCH::elapsed -> {utc_now}"),
                format!("std::time::UNIX_EPOCH::elapsed -> {utc_now}"),
                format!("std::time::SystemTime::UNIX_EPOCH::elapsed -> {utc_now}"),
                format!("std::time::SystemTime::UNIX_EPOCH::elapsed -> {utc_now}"),
                format!("std::time::UNIX_EPOCH::elapsed -> {utc_now}"),
            ]
        );
    }

    /// Arguments that parse as expressions or statements are visited as
    /// code, with block-local imports and the alias map.
    #[test]
    fn macro_arguments_are_visited_as_code() {
        let source = "
            use std::time::{Instant, UNIX_EPOCH};
            fn f(t: Instant) {
                dbg!({ use std::time::Instant as Tick; Tick::now() });
                dbg!(std::time::UNIX_EPOCH.elapsed());
                assert!(Instant::now() > t, \"{:?}\", t);
                assert_eq!(<std::time::Instant>::now(), t);
                println!(\"{:?}\", (&UNIX_EPOCH).elapsed());
                let _ = vec![Instant::now(); 2];
            }
        ";
        let instant = "std::time::Instant::now -> zakura_rt::time::Instant::now".to_owned();
        // The driver file may name std's Instant, so any `.elapsed()` is flagged.
        let elapsed = format!(".elapsed() -> {ELAPSED_IN_STD_INSTANT_FILE}");
        assert_eq!(
            violations_in("crates/zakura-quic/src/driver/connection.rs", source),
            [
                instant.clone(),
                elapsed.clone(),
                instant.clone(),
                instant.clone(),
                elapsed,
                instant
            ]
        );
    }

    /// Same-named `cfg` modules can't hide each other's bindings: every
    /// binding in the file counts.
    #[test]
    fn conditional_modules_cannot_hide_a_clock() {
        let source = "
            #[cfg(zakura_sim)]
            mod clock {
                use std::time::SystemTime as Clock;
                pub fn read() { let _ = Clock::now(); }
            }
            #[cfg(not(zakura_sim))]
            mod clock {
                use zakura_rt::time::Instant as Clock;
                pub fn read() { let _ = Clock::now(); }
            }
        ";
        let system = "std::time::SystemTime::now -> zakura_rt::time::utc_now";
        // The union rule flags both bodies, since either `Clock` may be std's.
        assert_eq!(violations(source), [system, system]);
    }

    /// A `self::` path is checked against every binding of its name, so
    /// shadowing can't hide a std read. The union rule also flags the
    /// runtime-only order, which renaming one import fixes.
    #[test]
    fn self_paths_check_every_binding() {
        let std_module = "use std::time::Instant; fn f() { use zakura_rt::time::Instant; let _ = self::Instant::now(); }";
        let rt_module = "use zakura_rt::time::Instant; fn f() { use std::time::Instant; let _ = self::Instant::now(); }";
        let instant = "std::time::Instant::now -> zakura_rt::time::Instant::now";
        for source in [std_module, rt_module] {
            assert_eq!(
                violations_in("crates/zakura-quic/src/driver/connection.rs", source),
                [instant],
                "{source}"
            );
        }
    }

    /// A file that may name std's `Instant` can't call `.elapsed()`, since
    /// the check can't tell a std `Instant` receiver from another.
    #[test]
    fn std_instant_files_cannot_call_elapsed() {
        let source = "fn age(start: std::time::Instant) { let _ = start.elapsed(); }";
        assert_eq!(
            violations_in("crates/zakura-quic/src/driver/connection.rs", source),
            [format!(".elapsed() -> {ELAPSED_IN_STD_INSTANT_FILE}")]
        );
        let elsewhere = "fn age(start: Start) { let _ = start.elapsed(); }";
        assert_eq!(violations(elsewhere), Vec::<String>::new());
    }

    /// A plain `type` alias binds its name like a `use` does.
    #[test]
    fn simple_type_aliases_are_followed() {
        let source = "type Clock = std::time::SystemTime; fn read() -> Clock { Clock::now() }";
        assert_eq!(
            violations(source),
            ["std::time::SystemTime::now -> zakura_rt::time::utc_now"]
        );
        let source = "type Clock = std::time::Instant; fn read() -> Clock { Clock::now() }";
        assert_eq!(
            violations_in("crates/zakura-quic/src/driver/connection.rs", source),
            ["std::time::Instant::now -> zakura_rt::time::Instant::now"]
        );
    }

    /// Dropping module prefixes doesn't use up the hop budget, so a chain of
    /// re-exports through sibling modules still reaches std's clock.
    #[test]
    fn module_prefixes_do_not_count_as_hops() {
        let source = "
            mod first { pub use std::time::SystemTime as Raw; }
            mod second { pub use super::first::Raw as Middle; }
            mod third { pub use super::second::Middle as Wall; }
            use third::Wall as Clock;
            fn read() -> Clock { Clock::now() }
        ";
        assert_eq!(
            violations(source),
            ["std::time::SystemTime::now -> zakura_rt::time::utc_now"]
        );
    }

    /// M3.7's bypass inventory: every runtime path left in `zakura-network`'s
    /// v2 code and in `zakura-quic` has an allowlist entry with its reason,
    /// and every `zakura-quic` entry says why a simulated run never reaches
    /// that path or why it's harness code.
    #[test]
    fn the_repository_inventories_every_runtime_bypass() {
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("xtask lives at crates/xtask");
        check(repo_root).expect("every runtime path is on the allowlist");
        for (file, path, reason) in ALLOWED {
            if file.starts_with("crates/zakura-quic/") {
                assert!(reason.contains("simulated run"), "{file} {path}: {reason}");
            }
        }
    }

    /// A `test` or `zakura_sim` gate compiles the code in simulated runs, so
    /// the check covers it. Harness modules under that gate need allowlist
    /// entries.
    #[test]
    fn simulator_gated_code_is_checked() {
        let source = "
            #[cfg(any(test, zakura_sim))]
            mod sim {
                use std::sync::Mutex;
            }
            #[cfg(zakura_sim)]
            fn f() { tokio::task::yield_now(); }
        ";
        assert_eq!(
            violations(source),
            [
                "std::sync::Mutex -> zakura_rt::sync::Mutex",
                "tokio::task::yield_now -> zakura_rt::task::yield_now",
            ]
        );
    }

    #[test]
    fn only_test_only_code_is_skipped() {
        let source = "
            #[cfg(test)]
            fn a() { tokio::spawn(x); }
            #[cfg(all(feature = \"x\", test))]
            fn b() { tokio::spawn(x); }
            #[cfg(any(test, feature = \"x\"))]
            fn c() { tokio::task::spawn(x); }
            #[cfg(not(test))]
            fn d() { tokio::task::yield_now(); }
            async fn e() {
                #[cfg(test)]
                tokio::task::yield_now().await;
            }
        ";
        assert_eq!(
            violations(source),
            [
                "tokio::task::spawn -> zakura_rt::task::spawn",
                "tokio::task::yield_now -> zakura_rt::task::yield_now",
            ]
        );
    }
}
