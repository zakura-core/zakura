//! `cargo xtask check-rt-imports`: the v2 P2P code takes runtime names from
//! `zakura-rt`.
//!
//! The check parses every production module under `zakura-network`'s
//! `zakura` module and flags paths to tokio, the std mutex and atomics, std's
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
//! The check does not yet cover `std::time::{Instant, SystemTime}`,
//! `std::thread`, or the timer adapters in `tokio_stream`.

use std::{
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

const ROOT_MODULE: &str = "crates/zakura-network/src/zakura.rs";

const RT_LIB: &str = "crates/zakura-rt/src/lib.rs";

/// Modules under `zakura` that are not v2 production code.
const EXCLUDED_MODULES: [&str; 2] = ["legacy_gossip", "testkit"];

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

/// Direct runtime paths the v2 code may keep, with the reason for each.
const ALLOWED: [(&str, &str, &str); 2] = [
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
];

const MISSING_NAME: &str = "a zakura_rt name (add one if it's missing)";

const MODULE_IMPORT: &str = "zakura_rt names, imported by item";

pub fn check(repo_root: &Path) -> Result<(), BoxError> {
    let rules = Rules::load(repo_root)?;
    let mut findings = Findings::default();
    check_file(
        repo_root,
        &rules,
        &repo_root.join(ROOT_MODULE),
        true,
        &mut findings,
    )?;

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

fn check_file(
    repo_root: &Path,
    rules: &Rules,
    path: &Path,
    is_root: bool,
    findings: &mut Findings,
) -> Result<(), BoxError> {
    let file = parse_file(repo_root, path)?;
    if is_test_only(&file.attrs) {
        return Ok(());
    }
    findings.files += 1;

    let relative = relative_path(repo_root, path);
    let mut visitor = PathVisitor {
        file: &relative,
        rules,
        findings,
    };
    for item in &file.items {
        visitor.visit_item(item);
    }

    // `foo.rs` and `foo/mod.rs` both keep their child modules in `foo/`.
    let child_dir = if path.file_name().is_some_and(|name| name == "mod.rs") {
        path.parent()
            .ok_or_else(|| format!("{relative} has no parent directory"))?
            .to_path_buf()
    } else {
        path.with_extension("")
    };
    check_child_modules(repo_root, rules, &child_dir, &file.items, is_root, findings)
}

fn check_child_modules(
    repo_root: &Path,
    rules: &Rules,
    dir: &Path,
    items: &[Item],
    is_root: bool,
    findings: &mut Findings,
) -> Result<(), BoxError> {
    for item in items {
        let Item::Mod(module) = item else { continue };
        let name = module.ident.to_string();
        if is_test_only(&module.attrs) || (is_root && EXCLUDED_MODULES.contains(&name.as_str())) {
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
                check_child_modules(repo_root, rules, &dir.join(&name), items, false, findings)?
            }
            None => {
                let path = module_file(repo_root, dir, &name)?;
                check_file(repo_root, rules, &path, false, findings)?;
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

struct PathVisitor<'a> {
    file: &'a str,
    rules: &'a Rules,
    findings: &'a mut Findings,
}

impl PathVisitor<'_> {
    fn check(&mut self, segments: &[String], span: Span) {
        let Some(replacement) = self.rules.replacement(segments) else {
            return;
        };
        let allowance = ALLOWED
            .iter()
            .position(|(file, prefix, _)| *file == self.file && starts_with(segments, prefix));
        if let Some(allowance) = allowance {
            self.findings.allowances_used[allowance] = true;
            return;
        }
        let start = span.start();
        self.findings.violations.push(Violation {
            location: format!("{}:{}:{}", self.file, start.line, start.column + 1),
            path: segments.join("::"),
            replacement,
        });
    }

    /// Checks `a::b::c` paths inside macro arguments, which syn leaves as tokens.
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
                        self.check(&segments, ident.span());
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

    fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
        for leaf in use_leaves(&item.tree) {
            self.check(&leaf.path, leaf.span);
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
        if segments.len() > 1 || path.leading_colon.is_some() {
            self.check(&segments, path.span());
        }
        visit::visit_path(self, path);
    }

    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        self.visit_path(&mac.path);
        self.check_tokens(mac.tokens.clone());
    }
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
        let repo_root = crate::repo_root().expect("xtask lives in the workspace");
        let rules = Rules::load(&repo_root).expect("zakura-rt's lib.rs parses");
        let file = syn::parse_file(source).expect("the fixture parses");
        let mut findings = Findings::default();
        let mut visitor = PathVisitor {
            file: "fixture.rs",
            rules: &rules,
            findings: &mut findings,
        };
        for item in &file.items {
            visitor.visit_item(item);
        }
        findings
            .violations
            .iter()
            .map(|violation| format!("{} -> {}", violation.path, violation.replacement))
            .collect()
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
        assert_eq!(
            violations(source),
            [
                "tokio::select -> zakura_rt::select",
                "tokio::time::sleep -> zakura_rt::time::sleep",
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
