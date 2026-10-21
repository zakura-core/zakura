//! `cargo xtask check-rt-imports`: the v2 P2P code takes runtime names from
//! `zakura-rt`.
//!
//! The check parses every production module under `zakura-network`'s
//! `zakura` module and flags paths to tokio, the std mutex and atomics, std's
//! hashed collections, and the thread RNG. Code gated by a test-only `cfg` is
//! skipped. Clippy's `disallowed_types` can't do this job while `zakura-rt`
//! only re-exports, because a re-export resolves to the same item.

use std::{
    fmt, fs,
    path::{Path, PathBuf},
};

use proc_macro2::{Spacing, TokenStream, TokenTree};
use syn::{
    spanned::Spanned,
    visit::{self, Visit},
    Attribute, Item, UseTree,
};

use crate::BoxError;

const ROOT_MODULE: &str = "crates/zakura-network/src/zakura.rs";

/// Modules under `zakura` that are not v2 production code.
const EXCLUDED_MODULES: [&str; 2] = ["legacy_gossip", "testkit"];

/// `rand` functions that draw from the thread RNG.
const THREAD_RNG_FUNCTIONS: [&str; 7] = [
    "rng",
    "random",
    "random_range",
    "random_bool",
    "random_ratio",
    "random_iter",
    "fill",
];

/// Direct runtime paths the v2 code may keep, with the reason for each.
const ALLOWED: [(&str, &str, &str); 1] = [(
    "crates/zakura-network/src/zakura/handler.rs",
    "tokio::runtime::Handle",
    "runtime introspection, which the simulator build will define",
)];

pub fn check(repo_root: &Path) -> Result<(), BoxError> {
    let mut violations = Vec::new();
    let mut files = 0;
    check_file(
        repo_root,
        &repo_root.join(ROOT_MODULE),
        true,
        &mut files,
        &mut violations,
    )?;

    if violations.is_empty() {
        println!("{files} v2 P2P files import runtime names through zakura-rt");
        return Ok(());
    }

    for violation in &violations {
        eprintln!("{violation}");
    }
    Err(format!(
        "{} runtime paths bypass zakura-rt; import them from zakura_rt, \
         or add an allowlist entry with a reason in crates/xtask/src/rt_imports.rs",
        violations.len()
    )
    .into())
}

fn check_file(
    repo_root: &Path,
    path: &Path,
    is_root: bool,
    files: &mut usize,
    violations: &mut Vec<Violation>,
) -> Result<(), BoxError> {
    let source = fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let file = syn::parse_file(&source).map_err(|e| format!("{}: {e}", path.display()))?;
    *files += 1;

    let relative = path
        .strip_prefix(repo_root)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned();
    let mut visitor = PathVisitor {
        file: &relative,
        violations,
    };
    for item in &file.items {
        visitor.visit_item(item);
    }

    // `foo.rs` and `foo/mod.rs` both keep their child modules in `foo/`.
    let child_dir = if path.file_name().is_some_and(|name| name == "mod.rs") {
        path.parent().map(Path::to_path_buf)
    } else {
        Some(path.with_extension(""))
    }
    .ok_or_else(|| format!("{} has no parent directory", path.display()))?;
    check_child_modules(
        repo_root,
        &child_dir,
        &file.items,
        is_root,
        files,
        violations,
    )
}

fn check_child_modules(
    repo_root: &Path,
    dir: &Path,
    items: &[Item],
    is_root: bool,
    files: &mut usize,
    violations: &mut Vec<Violation>,
) -> Result<(), BoxError> {
    for item in items {
        let Item::Mod(module) = item else { continue };
        let name = module.ident.to_string();
        if is_test_only(&module.attrs) || (is_root && EXCLUDED_MODULES.contains(&name.as_str())) {
            continue;
        }
        match &module.content {
            // The visitor already walked the inline module's items.
            Some((_, items)) => {
                check_child_modules(repo_root, &dir.join(&name), items, false, files, violations)?
            }
            None => {
                let path = module_file(dir, &name)?;
                check_file(repo_root, &path, false, files, violations)?;
            }
        }
    }
    Ok(())
}

fn module_file(dir: &Path, name: &str) -> Result<PathBuf, BoxError> {
    [
        dir.join(format!("{name}.rs")),
        dir.join(name).join("mod.rs"),
    ]
    .into_iter()
    .find(|path| path.is_file())
    .ok_or_else(|| format!("no file for module `{name}` in {}", dir.display()).into())
}

/// Whether the attributes include `#[cfg(test)]` or `#[cfg(all(test, ..))]`.
fn is_test_only(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        if !attr.path().is_ident("cfg") {
            return false;
        }
        let Ok(tokens) = attr.meta.require_list().map(|list| list.tokens.to_string()) else {
            return false;
        };
        let tokens = tokens.replace(' ', "");
        tokens == "test" || tokens.starts_with("all(test,")
    })
}

struct PathVisitor<'a> {
    file: &'a str,
    violations: &'a mut Vec<Violation>,
}

impl PathVisitor<'_> {
    fn check(&mut self, segments: &[String], span: proc_macro2::Span) {
        let Some(replacement) = replacement(segments) else {
            return;
        };
        let path = segments.join("::");
        let allowed = ALLOWED
            .iter()
            .any(|(file, prefix, _)| *file == self.file && path.starts_with(prefix));
        if !allowed {
            let start = span.start();
            self.violations.push(Violation {
                location: format!("{}:{}:{}", self.file, start.line, start.column + 1),
                path,
                replacement,
            });
        }
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
                TokenTree::Ident(ident) if !follows_path_separator(&tokens, i) => {
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
        let test_only = match expr {
            syn::Expr::Block(expr) => is_test_only(&expr.attrs),
            syn::Expr::Call(expr) => is_test_only(&expr.attrs),
            syn::Expr::MethodCall(expr) => is_test_only(&expr.attrs),
            syn::Expr::Macro(expr) => is_test_only(&expr.attrs),
            _ => false,
        };
        if !test_only {
            visit::visit_expr(self, expr);
        }
    }

    fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
        let mut prefix = Vec::new();
        self.visit_use_leaves(&item.tree, &mut prefix);
    }

    fn visit_path(&mut self, path: &'ast syn::Path) {
        let segments: Vec<String> = path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect();
        self.check(&segments, path.span());
        visit::visit_path(self, path);
    }

    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        self.visit_path(&mac.path);
        self.check_tokens(mac.tokens.clone());
    }
}

impl PathVisitor<'_> {
    fn visit_use_leaves(&mut self, tree: &UseTree, prefix: &mut Vec<String>) {
        match tree {
            UseTree::Path(path) => {
                prefix.push(path.ident.to_string());
                self.visit_use_leaves(&path.tree, prefix);
                prefix.pop();
            }
            UseTree::Name(name) if name.ident == "self" => self.check(prefix, name.span()),
            UseTree::Name(name) => {
                prefix.push(name.ident.to_string());
                self.check(prefix, name.span());
                prefix.pop();
            }
            UseTree::Rename(rename) => {
                prefix.push(rename.ident.to_string());
                self.check(prefix, rename.span());
                prefix.pop();
            }
            UseTree::Glob(glob) => self.check(prefix, glob.span()),
            UseTree::Group(group) => {
                for tree in &group.items {
                    self.visit_use_leaves(tree, prefix);
                }
            }
        }
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

fn is_path_separator(tokens: &[TokenTree], i: usize) -> bool {
    matches!(
        (tokens.get(i), tokens.get(i + 1)),
        (Some(TokenTree::Punct(first)), Some(TokenTree::Punct(second)))
            if first.as_char() == ':' && first.spacing() == Spacing::Joint && second.as_char() == ':'
    )
}

fn follows_path_separator(tokens: &[TokenTree], i: usize) -> bool {
    i >= 2 && is_path_separator(tokens, i - 2)
}

/// The `zakura_rt` name to use instead of `segments`, if `segments` is a
/// runtime path the v2 code must not name directly.
fn replacement(segments: &[String]) -> Option<String> {
    let segments: Vec<&str> = segments.iter().map(String::as_str).collect();
    let rt = |path: &str| Some(format!("zakura_rt::{path}"));
    match segments.as_slice() {
        ["tokio", "sync", "Mutex", ..] => rt("sync::AsyncMutex"),
        ["tokio", "spawn", ..] => rt("task::spawn"),
        ["tokio", "sync" | "time" | "task", ..] => rt(&segments[1..].join("::")),
        ["tokio", name @ ("select" | "pin")] => rt(name),
        ["tokio", ..] => Some("a zakura_rt name (add one if it's missing)".to_owned()),
        ["tokio_util", "sync" | "task", ..] => rt(&segments[1..].join("::")),
        ["tokio_stream", "StreamMap", ..] => rt("stream::StreamMap"),
        ["rand_10", name] if THREAD_RNG_FUNCTIONS.contains(name) => rt("rand::rng"),
        ["std" | "core", "sync", "atomic", ..] => rt(&segments[1..].join("::")),
        ["std", "sync", "Mutex" | "MutexGuard" | "PoisonError" | "TryLockError", ..] => {
            rt(&segments[1..].join("::"))
        }
        ["std", "collections", "HashMap" | "HashSet" | "hash_map" | "hash_set", ..] => {
            rt(&segments[1..].join("::"))
        }
        // A module import would let the code name the items above through it.
        ["std" | "core", "sync"] | ["std", "collections"] => {
            Some("zakura_rt names, imported by item".to_owned())
        }
        _ => None,
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
