//! `cargo xtask check-bounded-decode`: peer and RPC code decodes blocks and
//! transactions with the size-checked decoders.
//!
//! The block and transaction parsers in `zakura-chain` don't enforce the
//! consensus limit on a block's encoded size. `Block::zcash_deserialize_bounded`
//! and `Transaction::zcash_deserialize_bounded` do, so bytes from a peer or an
//! RPC client have to go through them. This check parses every production
//! module of `zakura-network` and `zakura-rpc` and flags the other ways to
//! decode a block or a transaction:
//!
//! - `Block::` or `Transaction::` followed by `zcash_deserialize`,
//!   `zcash_deserialize_from` or `zcash_deserialize_from_slice`, including the
//!   `<Block as ZcashDeserialize>::` spelling;
//! - the same functions called on the trait, as in
//!   `ZcashDeserialize::zcash_deserialize_from_slice`, where the decoded type
//!   is inferred;
//! - `zcash_deserialize_into` and `read_value` with a turbofish that names
//!   `Block` or `Transaction`;
//! - `zcash_deserialize_into` without a turbofish, unless it initializes a
//!   `let` whose annotation names some other type.
//!
//! Code gated by a test-only `cfg` is skipped. A site that decodes bytes the
//! node produced itself goes in [`ALLOWED`] with the reason.
//!
//! Limits: types are matched by name, so a renamed import or a type alias of
//! `Block` or `Transaction` isn't seen, and neither is `read_value` without a
//! turbofish or a generic helper called with one of the two types. Macro
//! arguments only get a token scan with the same rules.

use std::{fmt, path::Path};

use proc_macro2::{Delimiter, TokenStream, TokenTree};
use quote::ToTokens;
use syn::{
    visit::{self, Visit},
    Item,
};

use crate::{
    rt_imports::{is_test_only, item_attrs, module_file, parse_file, relative_path},
    BoxError,
};

/// The crates whose production code reads bytes from peers and RPC clients.
const ROOTS: [&str; 2] = [
    "crates/zakura-network/src/lib.rs",
    "crates/zakura-rpc/src/lib.rs",
];

/// The types whose parsers leave the block size limit to the caller.
const UNBOUNDED_TYPES: [&str; 2] = ["Block", "Transaction"];

/// The `ZcashDeserialize` functions that don't check the block size limit.
const UNBOUNDED_FUNCTIONS: [&str; 3] = [
    "zcash_deserialize",
    "zcash_deserialize_from",
    "zcash_deserialize_from_slice",
];

/// Unbounded decodes that production code may keep: the file, the call as the
/// check reports it, and why its bytes don't need the size check.
///
/// Each entry allows every such call in its file.
const ALLOWED: [(&str, &str, &str); 2] = [
    (
        "crates/zakura-rpc/src/methods/types/get_block_template/proposal.rs",
        "ZcashDeserialize::zcash_deserialize_from_slice",
        "the transactions of a block template this node built",
    ),
    (
        "crates/zakura-network/src/zakura/block_sync/sequencer_task/state/tests.rs",
        "zcash_deserialize_into",
        "a checked-in block vector, in a module built only for tests and fuzzing",
    ),
];

/// Checks that the peer and RPC crates decode blocks and transactions with the
/// size-checked decoders.
pub fn check(repo_root: &Path) -> Result<(), BoxError> {
    let mut findings = Findings::default();
    for root in ROOTS {
        check_file(repo_root, &repo_root.join(root), &mut findings)?;
    }

    for (used, (file, call, _)) in findings.allowances_used.iter().zip(ALLOWED) {
        if !used {
            findings.violations.push(Violation {
                location: file.to_owned(),
                call: call.to_owned(),
                advice: "no code uses this allowlist entry, so remove it",
            });
        }
    }

    if findings.violations.is_empty() {
        println!(
            "{} peer and RPC files decode blocks and transactions with the size check",
            findings.files
        );
        return Ok(());
    }

    for violation in &findings.violations {
        eprintln!("{violation}");
    }
    Err(format!(
        "{} block or transaction decodes skip the block size limit; use \
         `zcash_deserialize_bounded`, or add an allowlist entry with a reason in \
         crates/xtask/src/bounded_decode.rs",
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

struct Violation {
    location: String,
    call: String,
    advice: &'static str,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: `{}`: {}", self.location, self.call, self.advice)
    }
}

const NAMED_TYPE: &str =
    "decodes a block or transaction without the size limit; use `zcash_deserialize_bounded`";
const INFERRED_TYPE: &str = "decodes an inferred type without the size limit; name the type, \
                             and use `zcash_deserialize_bounded` for a block or transaction";

/// Checks the module in `path` and its child modules.
fn check_file(repo_root: &Path, path: &Path, findings: &mut Findings) -> Result<(), BoxError> {
    let file = parse_file(repo_root, path)?;
    if is_test_only(&file.attrs) {
        return Ok(());
    }
    findings.files += 1;

    let relative = relative_path(repo_root, path);
    let mut visitor = DecodeVisitor {
        file: &relative,
        findings,
    };
    for item in &file.items {
        visitor.visit_item(item);
    }

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
    check_child_modules(repo_root, &child_dir, &file.items, findings)
}

/// Checks the child modules declared in `items`, whose files are in `dir`.
fn check_child_modules(
    repo_root: &Path,
    dir: &Path,
    items: &[Item],
    findings: &mut Findings,
) -> Result<(), BoxError> {
    for item in items {
        let Item::Mod(module) = item else { continue };
        if is_test_only(&module.attrs) {
            continue;
        }
        let name = module.ident.to_string();
        if module.attrs.iter().any(|attr| attr.path().is_ident("path")) {
            return Err(format!(
                "module `{name}` in {} sets #[path], which the check can't follow",
                relative_path(repo_root, dir)
            )
            .into());
        }
        match &module.content {
            // The visitor already walked the inline module's items.
            Some((_, items)) => check_child_modules(repo_root, &dir.join(&name), items, findings)?,
            None => check_file(repo_root, &module_file(repo_root, dir, &name)?, findings)?,
        }
    }
    Ok(())
}

struct DecodeVisitor<'a> {
    file: &'a str,
    findings: &'a mut Findings,
}

impl DecodeVisitor<'_> {
    fn report(&mut self, line: usize, call: String, advice: &'static str) {
        let allowance = ALLOWED
            .iter()
            .position(|(file, allowed, _)| *file == self.file && *allowed == call);
        if let Some(index) = allowance {
            self.findings.allowances_used[index] = true;
            return;
        }
        self.findings.violations.push(Violation {
            location: format!("{}:{line}", self.file),
            call,
            advice,
        });
    }

    /// Reports the unbounded decodes written in `tokens`, which are an
    /// expression path, a method call's name and turbofish, or a macro's
    /// arguments.
    fn scan(&mut self, tokens: TokenStream) {
        let tokens: Vec<TokenTree> = tokens.into_iter().collect();
        for (i, token) in tokens.iter().enumerate() {
            match token {
                TokenTree::Group(group) => self.scan(group.stream()),
                TokenTree::Ident(ident) => {
                    let name = ident.to_string();
                    let line = ident.span().start().line;
                    let turbofish = turbofish_after(&tokens, i);
                    let names_unbounded_type =
                        turbofish.as_deref().is_some_and(names_unbounded_type);
                    if UNBOUNDED_FUNCTIONS.contains(&name.as_str()) {
                        match owner_before(&tokens, i).as_deref() {
                            Some(owner) if UNBOUNDED_TYPES.contains(&owner) => {
                                self.report(line, format!("{owner}::{name}"), NAMED_TYPE)
                            }
                            Some("ZcashDeserialize") => self.report(
                                line,
                                format!("ZcashDeserialize::{name}"),
                                INFERRED_TYPE,
                            ),
                            _ => {}
                        }
                    } else if name == "zcash_deserialize_into" {
                        match turbofish {
                            Some(_) if names_unbounded_type => self.report(
                                line,
                                format!("{name}::<Block or Transaction>"),
                                NAMED_TYPE,
                            ),
                            Some(_) => {}
                            None => self.report(line, name, INFERRED_TYPE),
                        }
                    } else if name == "read_value" && names_unbounded_type {
                        self.report(line, format!("{name}::<Block or Transaction>"), NAMED_TYPE);
                    }
                }
                _ => {}
            }
        }
    }
}

/// The name that owns the associated function at `tokens[i]`: `Block` in both
/// `block::Block::f` and `<Block as ZcashDeserialize>::f`.
fn owner_before(tokens: &[TokenTree], i: usize) -> Option<String> {
    if !is_path_separator(tokens, i.checked_sub(2)?) {
        return None;
    }
    match tokens.get(i.checked_sub(3)?)? {
        TokenTree::Ident(owner) => Some(owner.to_string()),
        // `<Type as Trait>::f`: the type is the first path before `as`.
        TokenTree::Punct(close) if close.as_char() == '>' => {
            let open = tokens[..i - 3].iter().rposition(
                |token| matches!(token, TokenTree::Punct(punct) if punct.as_char() == '<'),
            )?;
            let qualified = &tokens[open + 1..i - 3];
            let type_end = qualified
                .iter()
                .position(|token| matches!(token, TokenTree::Ident(ident) if ident == "as"))
                .unwrap_or(qualified.len());
            match qualified[..type_end].last()? {
                TokenTree::Ident(owner) => Some(owner.to_string()),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Whether `tokens[i]` and `tokens[i + 1]` spell `::`.
fn is_path_separator(tokens: &[TokenTree], i: usize) -> bool {
    let colon = |token: Option<&TokenTree>| matches!(token, Some(TokenTree::Punct(punct)) if punct.as_char() == ':');
    colon(tokens.get(i)) && colon(tokens.get(i + 1))
}

/// The tokens inside the `::<…>` that follows `tokens[i]`, if there is one.
fn turbofish_after(tokens: &[TokenTree], i: usize) -> Option<Vec<TokenTree>> {
    if !is_path_separator(tokens, i + 1) {
        return None;
    }
    let is_punct = |token: &TokenTree, expected: char| matches!(token, TokenTree::Punct(punct) if punct.as_char() == expected);
    if !is_punct(tokens.get(i + 3)?, '<') {
        return None;
    }
    let mut depth = 1_usize;
    let mut arguments = Vec::new();
    for token in &tokens[i + 4..] {
        if is_punct(token, '<') {
            depth += 1;
        } else if is_punct(token, '>') {
            depth -= 1;
            if depth == 0 {
                return Some(arguments);
            }
        }
        arguments.push(token.clone());
    }
    None
}

/// Whether the type arguments name `Block` or `Transaction` anywhere, as in
/// `Arc<block::Block>`.
fn names_unbounded_type(arguments: &[TokenTree]) -> bool {
    arguments.iter().any(|token| match token {
        TokenTree::Ident(ident) => UNBOUNDED_TYPES.contains(&ident.to_string().as_str()),
        TokenTree::Group(group) if group.delimiter() != Delimiter::Brace => {
            names_unbounded_type(&group.stream().into_iter().collect::<Vec<_>>())
        }
        _ => false,
    })
}

/// The `zcash_deserialize_into()` call without a turbofish that `expr` is,
/// under any `?` and parentheses.
fn inferred_decode(expr: &syn::Expr) -> Option<&syn::ExprMethodCall> {
    match expr {
        syn::Expr::Try(inner) => inferred_decode(&inner.expr),
        syn::Expr::Paren(inner) => inferred_decode(&inner.expr),
        syn::Expr::MethodCall(call)
            if call.method == "zcash_deserialize_into" && call.turbofish.is_none() =>
        {
            Some(call)
        }
        _ => None,
    }
}

impl<'ast> Visit<'ast> for DecodeVisitor<'_> {
    fn visit_item(&mut self, item: &'ast Item) {
        if item_attrs(item).is_some_and(is_test_only) {
            return;
        }
        visit::visit_item(self, item);
    }

    fn visit_impl_item_fn(&mut self, function: &'ast syn::ImplItemFn) {
        if !is_test_only(&function.attrs) {
            visit::visit_impl_item_fn(self, function);
        }
    }

    fn visit_local(&mut self, local: &'ast syn::Local) {
        // `let value: Type = bytes.zcash_deserialize_into()?;` names its type
        // in the annotation.
        if let (syn::Pat::Type(annotated), Some(init)) = (&local.pat, &local.init) {
            if let Some(call) = inferred_decode(&init.expr) {
                let annotation: Vec<TokenTree> =
                    annotated.ty.to_token_stream().into_iter().collect();
                if names_unbounded_type(&annotation) {
                    self.report(
                        call.method.span().start().line,
                        "zcash_deserialize_into::<Block or Transaction>".to_owned(),
                        NAMED_TYPE,
                    );
                }
                self.visit_expr(&call.receiver);
                if let Some((_, diverge)) = &init.diverge {
                    self.visit_expr(diverge);
                }
                return;
            }
        }
        visit::visit_local(self, local);
    }

    fn visit_expr_path(&mut self, path: &'ast syn::ExprPath) {
        self.scan(path.to_token_stream());
    }

    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        let mut tokens = call.method.to_token_stream();
        if let Some(turbofish) = &call.turbofish {
            turbofish.to_tokens(&mut tokens);
        }
        self.scan(tokens);
        visit::visit_expr_method_call(self, call);
    }

    fn visit_macro(&mut self, invocation: &'ast syn::Macro) {
        self.scan(invocation.tokens.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each finding in `source`, read as the file `path`, as `line: call`.
    fn findings_in(path: &str, source: &str) -> Vec<String> {
        let file = syn::parse_file(source).expect("the fixture parses");
        let mut findings = Findings::default();
        let mut visitor = DecodeVisitor {
            file: path,
            findings: &mut findings,
        };
        for item in &file.items {
            visitor.visit_item(item);
        }
        findings
            .violations
            .iter()
            .map(|violation| {
                let line = violation.location.rsplit(':').next().expect("a line");
                format!("{line}: {}", violation.call)
            })
            .collect()
    }

    fn findings(source: &str) -> Vec<String> {
        findings_in("fixture.rs", source)
    }

    #[test]
    fn unbounded_decodes_of_blocks_and_transactions_are_flagged() {
        let source = "
fn f(mut bytes: &[u8]) {
    let _ = Block::zcash_deserialize_from_slice(&mut bytes);
    let _ = block::Block::zcash_deserialize(bytes);
    let _ = Transaction::zcash_deserialize_from(&mut reader);
    let _ = <Block as ZcashDeserialize>::zcash_deserialize(bytes);
    let _ = bytes.zcash_deserialize_into::<Arc<Block>>();
    let _ = reader.read_value::<Vec<Transaction>>();
    let block: Block = bytes.zcash_deserialize_into()?;
}";
        assert_eq!(
            findings(source),
            [
                "3: Block::zcash_deserialize_from_slice",
                "4: Block::zcash_deserialize",
                "5: Transaction::zcash_deserialize_from",
                "6: Block::zcash_deserialize",
                "7: zcash_deserialize_into::<Block or Transaction>",
                "8: read_value::<Block or Transaction>",
                "9: zcash_deserialize_into::<Block or Transaction>",
            ]
        );
    }

    #[test]
    fn decodes_of_an_inferred_type_are_flagged() {
        let source = "
fn f(mut bytes: &[u8]) -> Result<Block, Error> {
    let block = ZcashDeserialize::zcash_deserialize_from_slice(&mut bytes)?;
    consume(bytes.zcash_deserialize_into()?);
    Ok(block)
}";
        assert_eq!(
            findings(source),
            [
                "3: ZcashDeserialize::zcash_deserialize_from_slice",
                "4: zcash_deserialize_into",
            ]
        );
    }

    #[test]
    fn bounded_decodes_and_other_types_pass() {
        let source = "
fn f(mut bytes: &[u8]) {
    let _ = Block::zcash_deserialize_bounded(&mut bytes);
    let _ = Transaction::zcash_deserialize_bounded(&mut bytes);
    let _ = Header::zcash_deserialize(bytes);
    let _ = block::Hash::zcash_deserialize_from_slice(&mut bytes);
    let _ = bytes.zcash_deserialize_into::<CountedHeader>();
    let _ = reader.read_value::<Vec<Hash>>();
    let count: CompactSizeMessage = (&mut reader).zcash_deserialize_into()?;
}";
        assert_eq!(findings(source), [] as [&str; 0]);
    }

    #[test]
    fn macro_arguments_are_scanned() {
        let source = "
fn f(bytes: &[u8]) {
    assert!(Block::zcash_deserialize(bytes).is_ok());
}";
        assert_eq!(findings(source), ["3: Block::zcash_deserialize"]);
    }

    #[test]
    fn test_only_code_is_skipped() {
        let source = "
#[cfg(test)]
fn fixture(bytes: &[u8]) -> Block {
    Block::zcash_deserialize(bytes).unwrap()
}
#[cfg(test)]
mod tests {
    fn fixture(bytes: &[u8]) -> Block {
        Block::zcash_deserialize(bytes).unwrap()
    }
}
impl Codec {
    #[cfg(test)]
    fn fixture(bytes: &[u8]) -> Block {
        Block::zcash_deserialize(bytes).unwrap()
    }
}";
        assert_eq!(findings(source), [] as [&str; 0]);
    }

    #[test]
    fn an_allowlist_entry_covers_only_its_file_and_call() {
        let (file, call, _) = ALLOWED[0];
        let source = format!("fn f(mut bytes: &[u8]) {{ let _ = {call}(&mut bytes); }}");
        assert_eq!(findings_in(file, &source), [] as [&str; 0]);
        assert_eq!(findings(&source), [format!("1: {call}")]);
    }

    #[test]
    fn the_tree_passes() {
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        check(&repo_root).expect("peer and RPC code uses the size-checked decoders");
    }
}
