//! The header nodes' evidence-store inventory (C14, C17), checked from the
//! source.
//!
//! A static scan of the header harness's sources finds every struct and
//! enum variant field that holds shared state: a `Cell`, `RefCell`, `Mutex`, `RwLock` or
//! atomic, in an `Rc` or `Arc` or not, with type aliases followed. Each is either a
//! `Registered<T>`, which only a node's `EvidenceStores` builds, so it
//! freezes with the run's log and reaches its snapshots, or on
//! [`ALLOWED`], a live resource or harness mechanism that isn't evidence,
//! with its reason. A new field that's neither fails here.
//!
//! Each registered store is on [`REGISTERED`], which names the checkpoint
//! that delivers its facts and the producer test that checks them against
//! the owner state or message they come from. The scan fails if that test
//! doesn't exist. A store with no producer test would say why, as owed to
//! hf-7, and [`owed_to_hf7`] checks that none is.
#![cfg(zakura_sim)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use syn::{Attribute, Fields, Item, Type};

/// The checkpoint whose evidence check delivers a store's facts with the
/// run's log.
#[derive(Clone, Copy, Debug)]
enum Checkpoint {
    /// The node's reactor tap (`EvidenceKind::Reactor`).
    HeaderTap,
    /// The node's header storage source (`EvidenceKind::Storage`).
    HeaderStorage,
    /// The node's transport tap (`EvidenceKind::Transport`).
    TransportNode,
}

/// Where a registered store's producer test is.
#[derive(Clone, Copy, Debug)]
enum Producer {
    /// The test `name` in `tests/<file>.rs`.
    Test {
        /// The test file's stem.
        file: &'static str,
        /// The test's function name.
        name: &'static str,
    },
    /// None yet: hf-7 owes one, for this reason.
    OwedToHf7(&'static str),
}

/// One registered store: the field that holds its `Registered<T>`.
struct Store {
    /// The source file, from the crate root.
    file: &'static str,
    /// The struct that holds it.
    owner: &'static str,
    /// The field, or `0` for a tuple struct's.
    field: &'static str,
    /// The checkpoints that deliver its facts.
    checkpoints: &'static [Checkpoint],
    /// Its producer test.
    producer: Producer,
}

use Checkpoint::{HeaderStorage, HeaderTap, TransportNode};

/// `owner`'s `field` in `file`, delivered by `checkpoints`, checked by the
/// test `name` in `tests/<test>.rs`.
const fn store(
    file: &'static str,
    owner: &'static str,
    field: &'static str,
    checkpoints: &'static [Checkpoint],
    (test, name): (&'static str, &'static str),
) -> Store {
    Store {
        file,
        owner,
        field,
        checkpoints,
        producer: Producer::Test { file: test, name },
    }
}

/// Every registered store the scan finds.
const REGISTERED: &[Store] = &[
    store(
        "src/peer/evidence.rs",
        "SentPages",
        "0",
        &[TransportNode],
        (
            "header_despite_close",
            "the_staging_companion_needs_the_rejected_page_staged",
        ),
    ),
    store(
        "src/harness/header_node/ends.rs",
        "EndEvidence",
        "0",
        &[HeaderTap],
        (
            "header_node_ends",
            "close_and_relink_steps_record_the_connections_they_end",
        ),
    ),
    store(
        "src/harness/header_node/adoption.rs",
        "AdoptionEvidence",
        "0",
        &[HeaderTap],
        (
            "header_node_adoption",
            "new_work_carries_the_adopted_generation",
        ),
    ),
    store(
        "src/harness/header_node/session.rs",
        "SessionEvidence",
        "0",
        &[HeaderTap, TransportNode],
        (
            "header_node_sessions",
            "a_close_then_a_relink_ends_or_replaces_the_first_session",
        ),
    ),
    store(
        "src/harness/header_node/fault.rs",
        "CooldownEvidence",
        "0",
        &[HeaderTap],
        (
            "header_wire_cooldown",
            "a_late_fault_cools_down_the_peer_and_refuses_its_redial",
        ),
    ),
    store(
        "src/harness/header_node/publication.rs",
        "PublicationEvidence",
        "0",
        &[HeaderTap],
        (
            "header_publication",
            "each_served_witness_serves_what_it_names",
        ),
    ),
    store(
        "src/harness/header_node/claim.rs",
        "SettlementEvidence",
        "0",
        &[HeaderTap],
        ("header_teardown", "teardown_waits_for_held_lease"),
    ),
    store(
        "src/harness/header_node/claim.rs",
        "ClaimEvidence",
        "0",
        &[HeaderTap],
        (
            "header_budget_claims",
            "an_over_budget_reservation_violates_chunk_budget_bound",
        ),
    ),
    store(
        "src/harness/header_node/capacity.rs",
        "CapacityEvidence",
        "0",
        &[HeaderTap],
        (
            "header_node_capacity",
            "the_probe_matches_the_accounting_through_close_relink_and_shutdown",
        ),
    ),
    store(
        "src/harness/header_node/reservation.rs",
        "ReservationEvidence",
        "0",
        &[HeaderTap],
        (
            "header_node_reservations",
            "a_round_retires_with_headers_still_reserved",
        ),
    ),
    store(
        "src/harness/header_node/stale.rs",
        "StaleEvidence",
        "0",
        &[HeaderTap],
        (
            "header_stale",
            "the_stale_prepare_input_holds_stale_completion_inert",
        ),
    ),
    store(
        "src/harness/header_node/round_life.rs",
        "RoundLifeEvidence",
        "0",
        &[HeaderTap],
        (
            "header_reset_invalidates",
            "every_witness_holds_and_applies",
        ),
    ),
    store(
        "src/harness/header_node/retirement.rs",
        "RetirementEvidence",
        "0",
        &[HeaderTap],
        ("header_peer_retirement", "every_departure_fixture_holds"),
    ),
    store(
        "src/harness/header_node/outstanding.rs",
        "OutstandingEvidence",
        "0",
        &[HeaderTap],
        (
            "header_outstanding",
            "the_limit_fixtures_reach_and_hold_their_bounds",
        ),
    ),
    store(
        "src/harness/header_node/hold_wake.rs",
        "HoldWakeEvidence",
        "0",
        &[HeaderStorage],
        (
            "header_hold_release",
            "every_fixture_holds_and_takes_its_path",
        ),
    ),
    store(
        "src/harness/header_node/fork_choice.rs",
        "ForkChoiceEvidence",
        "0",
        &[HeaderTap],
        ("header_chain_selected", "every_witness_holds_and_applies"),
    ),
    store(
        "src/harness/header_node/chain_effect.rs",
        "ChainEffectEvidence",
        "0",
        &[HeaderTap],
        ("header_stale_chain", "every_witness_holds_and_applies"),
    ),
    store(
        "src/harness/header_node/authority.rs",
        "AuthorityEvidence",
        "0",
        &[HeaderTap],
        ("header_authority", "every_witness_holds_and_applies"),
    ),
    store(
        "src/harness/header_node/wake.rs",
        "WakeEvidence",
        "0",
        &[HeaderTap],
        (
            "header_release_wake",
            "the_fixtures_settle_after_their_measured_boundary_and_not_before",
        ),
    ),
    store(
        "src/harness/header_node/recovery.rs",
        "RecoveryEvidence",
        "recovery",
        &[HeaderTap, HeaderStorage],
        ("header_hostile_prefix", "every_prefix_class_recovers"),
    ),
    store(
        "src/harness/header_node/recovery.rs",
        "Prefix",
        "0",
        &[HeaderStorage],
        (
            "header_recovery_prefix",
            "a_typed_prefix_stops_at_the_bound",
        ),
    ),
    store(
        "src/storage/header.rs",
        "FaultAttributions",
        "0",
        &[HeaderStorage],
        (
            "header_fault_attribution",
            "an_attributed_prepare_failure_owes_its_source_a_penalty",
        ),
    ),
    store(
        "src/harness/header_node.rs",
        "Effects",
        "ended",
        &[HeaderTap],
        (
            "header_node_session_ends",
            "each_recorded_end_is_its_links_cancellation",
        ),
    ),
    // A second, non-owning handle on `Effects::ended`, which the
    // fork-choice check reads for the mirrors' link ends, so the same
    // producer test.
    store(
        "src/harness/header_node/fork_choice.rs",
        "LinkEnds",
        "ended",
        &[HeaderTap],
        (
            "header_node_session_ends",
            "each_recorded_end_is_its_links_cancellation",
        ),
    ),
];

/// The shared-state fields that aren't evidence, by file, struct and
/// field, each with why.
const ALLOWED: &[(&str, &str, &str, &str)] = &[
    // The reactor tap's own state: what it hasn't delivered to the log yet,
    // and the checks whose facts go to registered stores.
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "buffer",
        "events not yet delivered, which the tap's checkpoint delivers to the log",
    ),
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "work",
        "the ownership ledger, a check whose breaks fail the run or go to the claims store",
    ),
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "probe",
        "the reactor's live resource probe handle",
    ),
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "off_reactor",
        "a task ID the check compares against, not a fact",
    ),
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "deferred",
        "a harness failure held back to the run's end hook, which reports it",
    ),
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "teardown_claims",
        "which teardown claims were already recorded in the claims store",
    ),
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "stale",
        "the stale check, whose facts go to the stale-call store",
    ),
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "wake",
        "the wake check, whose facts go to the wake store",
    ),
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "round_life",
        "the reset check, whose facts go to the round-life store",
    ),
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "chain_effect",
        "the stale-chain check, whose facts go to the chain-effect store",
    ),
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "outstanding",
        "the outstanding-work check, whose facts go to the outstanding store",
    ),
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "fork_choice",
        "the fork-choice check, whose facts go to the fork-choice store",
    ),
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "publication",
        "the publication check, whose facts go to the publication store",
    ),
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "authority",
        "the authority check, whose facts go to the authority store",
    ),
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "retirement",
        "the departed-work check, whose facts go to the retirement store",
    ),
    // The tap's flags, each a live state or a guard, whose facts go to the
    // stores or fail the run.
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "refused",
        "an event recorded outside the run, which fails the run",
    ),
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "port_gone",
        "the node's port dropped mid-run, which the tap's health fails the run on",
    ),
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "unwound",
        "whether the reactor panicked, which the tap's health fails the run on",
    ),
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "shutdown",
        "whether the harness began stopping the reactor, which each drain copies into the \
         settlement store's row",
    ),
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "exited",
        "a guard that records the reactor's own exit in the ends store once",
    ),
    (
        "src/harness/header_node/tap.rs",
        "TapState",
        "settled",
        "whether the stopped reactor's teardown is done, which each drain copies into the \
         settlement store's row",
    ),
    // The node's stamp counter.
    (
        "src/harness/header_node/stamp.rs",
        "Stamps",
        "0",
        "the next fact stamp, which a registered store takes under its own lock as it keeps a \
         fact, so the stamps are in its snapshot and a frozen store takes none",
    ),
    // The fault property's own state.
    (
        "src/harness/header_node/fault.rs",
        "FaultEvidence",
        "settled",
        "the run's end, copied from `RunEnd` by `HasView::ended` before the log freezes",
    ),
    (
        "src/harness/header_node/fault.rs",
        "FaultEvidence",
        "judge",
        "the property's judge, which reads the log and the cooldown store once each",
    ),
    // The topology: its scenario inputs, live links, and the counts its
    // policy source turns into trigger offers during the run.
    (
        "src/harness/header_node.rs",
        "Effects",
        "names",
        "each node's name, a scenario input set before the run",
    ),
    (
        "src/harness/header_node.rs",
        "Effects",
        "links",
        "the live links, whose tokens and streams cleanup closes",
    ),
    (
        "src/harness/header_node.rs",
        "Effects",
        "grown",
        "a trigger count the policy source reads at each step, not a fact a check reads",
    ),
    (
        "src/harness/header_node.rs",
        "Effects",
        "reset",
        "a trigger count the policy source reads at each step, not a fact a check reads",
    ),
    (
        "src/harness/header_node.rs",
        "Effects",
        "released",
        "a trigger count the policy source reads at each step, not a fact a check reads",
    ),
    (
        "src/harness/header_node.rs",
        "Effects",
        "owned",
        "the holds the scenario owns, a scenario input",
    ),
    (
        "src/harness/header_node.rs",
        "Effects",
        "failure",
        "a control failure the policy source turns into a harness failure",
    ),
    (
        "src/harness/header_node.rs",
        "PolicySource",
        "predicates",
        "the policy's trigger state, which decides offers",
    ),
    (
        "src/harness/header_node.rs",
        "Topology",
        "last_id",
        "the next connection and session ID",
    ),
    (
        "src/harness/header_node.rs",
        "Topology",
        "stopping",
        "whether a shutdown was picked, which stops churn",
    ),
    (
        "src/harness/header_node.rs",
        "Control",
        "predicates",
        "the policy's trigger state, shared with the policy source",
    ),
    (
        "src/harness/header_node.rs",
        "Control",
        "queued",
        "how many control steps the control task hasn't run yet",
    ),
    (
        "src/harness/header_sync.rs",
        "Storage",
        "dispatch",
        "the dispatch guard's flag, which the view reports live",
    ),
    (
        "src/harness/header_sync.rs",
        "HeaderWire",
        "dispatch",
        "the dispatch guard's flag, which the view reports live",
    ),
    (
        "src/harness/header_sync.rs",
        "WirePorts",
        "startup",
        "what the reactor started with, set once at build",
    ),
    (
        "src/harness/header_sync.rs",
        "WirePorts",
        "adoption",
        "a slot for the tap's registered adoption store, filled once at build",
    ),
    // The scripted peers' state, C11's: a peer-evidence checkpoint owes
    // these their freeze and their producer tests.
    (
        "src/peer/evidence.rs",
        "PeerFeed",
        "records",
        "C11: the peers' records, owed a peer-evidence checkpoint",
    ),
    (
        "src/peer/evidence.rs",
        "PeerFeed",
        "taken",
        "C11: how many records the feed took, owed a peer-evidence checkpoint",
    ),
    (
        "src/peer/evidence.rs",
        "PeerFeed",
        "withheld",
        "C11: how many records the feed withheld, owed a peer-evidence checkpoint",
    ),
    (
        "src/peer/evidence.rs",
        "WireEvidence",
        "shared_ids",
        "C11: the peers' shared IDs, owed a peer-evidence checkpoint",
    ),
    (
        "src/peer/mod.rs",
        "PeerFailure",
        "0",
        "a peer's harness failure, which fails the run",
    ),
    (
        "src/peer/mod.rs",
        "Readiness",
        "0",
        "a peer task's live phase, not a fact",
    ),
    (
        "src/peer/mod.rs",
        "Forward",
        "live",
        "a scripted peer's live connection count, which gates its next accept, not a fact",
    ),
    (
        "src/peer/mod.rs",
        "Live",
        "0",
        "one live connection's share of its peer's count, not a fact",
    ),
    (
        "src/peer/mod.rs",
        "PeerControl",
        "records",
        "C11: the peers' records, owed a peer-evidence checkpoint",
    ),
    (
        "src/peer/mod.rs",
        "PeerTask",
        "records",
        "C11: the peers' records, owed a peer-evidence checkpoint",
    ),
    (
        "src/peer/mod.rs",
        "PeerTask",
        "closed",
        "C11: the peers that closed their own connection, owed a peer-evidence checkpoint",
    ),
    (
        "src/harness/header_sync.rs",
        "WirePorts",
        "records",
        "C11: the scripted peers' records, owed a peer-evidence checkpoint",
    ),
    (
        "src/harness/header_sync.rs",
        "WirePorts",
        "closed",
        "C11: the peers that closed their own connection, owed a peer-evidence checkpoint",
    ),
    (
        "src/harness/header_sync.rs",
        "HeaderWire",
        "closed",
        "C11: the peers that closed their own connection, owed a peer-evidence checkpoint",
    ),
];

/// Every source file the scan reads, from the crate root: the header node,
/// the header harnesses, header storage, and the scripted peers, whose
/// fields are C11's.
fn scanned(root: &Path) -> Vec<PathBuf> {
    let mut files = vec![
        root.join("src/harness/header_node.rs"),
        root.join("src/harness/header_sync.rs"),
        root.join("src/storage/header.rs"),
    ];
    for dir in [
        "src/harness/header_node",
        "src/harness/header_sync",
        "src/peer",
    ] {
        walk(&root.join(dir), &mut files);
    }
    files.sort();
    files
}

/// Adds every `.rs` file under `dir` to `files`, but test modules' own
/// files, which no run builds.
fn walk(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap_or_else(|error| panic!("{}: {error}", dir.display())) {
        let path = entry.expect("a readable directory entry").path();
        if path.is_dir() {
            walk(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "rs")
            && path.file_name().is_some_and(|name| name != "tests.rs")
        {
            files.push(path);
        }
    }
}

/// Whether `attrs` hold `#[cfg(test)]`.
fn test_only(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("cfg")
            && attr
                .parse_args::<syn::Ident>()
                .is_ok_and(|ident| ident == "test")
    })
}

/// `ty`'s source text in `lines`, its file's, with no whitespace.
fn text(ty: &Type, lines: &[&str]) -> String {
    use syn::spanned::Spanned;
    let (start, end) = (ty.span().start(), ty.span().end());
    let mut text = String::new();
    for line in start.line..=end.line {
        let chars: Vec<char> = lines.get(line - 1).unwrap_or(&"").chars().collect();
        let from = if line == start.line { start.column } else { 0 };
        let to = if line == end.line {
            end.column
        } else {
            chars.len()
        };
        text.extend(&chars[from.min(chars.len())..to.min(chars.len())]);
    }
    text.chars().filter(|c| !c.is_whitespace()).collect()
}

/// One struct field the scan found: its file, struct, field and type.
#[derive(Clone, Debug)]
struct Field {
    file: String,
    owner: String,
    field: String,
    ty: String,
}

/// Each of `declared`'s fields, by name or tuple index, as `owner`'s,
/// from `file` whose source is `lines`, into `fields`.
fn push_fields(
    (file, lines): (&str, &[&str]),
    owner: &str,
    declared: &Fields,
    fields: &mut Vec<Field>,
) {
    let named: Vec<(String, &Type)> = match declared {
        Fields::Named(named) => named
            .named
            .iter()
            .map(|field| {
                let name = field.ident.as_ref().expect("a named field has a name");
                (name.to_string(), &field.ty)
            })
            .collect(),
        Fields::Unnamed(unnamed) => unnamed
            .unnamed
            .iter()
            .enumerate()
            .map(|(index, field)| (index.to_string(), &field.ty))
            .collect(),
        Fields::Unit => Vec::new(),
    };
    fields.extend(named.into_iter().map(|(field, ty)| Field {
        file: file.to_owned(),
        owner: owner.to_owned(),
        field,
        ty: text(ty, lines),
    }));
}

/// Every non-test struct field, enum variant field and type alias in
/// `items`, from `file` whose source is `lines`, into `fields` and
/// `aliases`. A variant's fields are owned by `Enum::Variant`.
fn collect(
    (file, lines): (&str, &[&str]),
    items: &[Item],
    fields: &mut Vec<Field>,
    aliases: &mut BTreeMap<String, Vec<String>>,
) {
    for item in items {
        match item {
            Item::Struct(item) if !test_only(&item.attrs) => {
                push_fields((file, lines), &item.ident.to_string(), &item.fields, fields);
            }
            Item::Enum(item) if !test_only(&item.attrs) => {
                for variant in &item.variants {
                    push_fields(
                        (file, lines),
                        &format!("{}::{}", item.ident, variant.ident),
                        &variant.fields,
                        fields,
                    );
                }
            }
            Item::Type(item) if !test_only(&item.attrs) => {
                aliases
                    .entry(item.ident.to_string())
                    .or_default()
                    .push(text(&item.ty, lines));
            }
            Item::Mod(item) if !test_only(&item.attrs) => {
                if let Some((_, items)) = &item.content {
                    collect((file, lines), items, fields, aliases);
                }
            }
            _ => {}
        }
    }
}

/// Whether `ty` names `word` as a whole path segment followed by `<`.
fn names_generic(ty: &str, word: &str) -> bool {
    ty.match_indices(word).any(|(at, _)| {
        let before = ty[..at].chars().next_back();
        let after = ty[at + word.len()..].chars().next();
        before.is_none_or(|c| !(c.is_alphanumeric() || c == '_')) && after == Some('<')
    })
}

/// Whether `ty` names `word` as a whole identifier.
fn names(ty: &str, word: &str) -> bool {
    ty.match_indices(word).any(|(at, _)| {
        let before = ty[..at].chars().next_back();
        let after = ty[at + word.len()..].chars().next();
        let boundary = |c: Option<char>| c.is_none_or(|c| !(c.is_alphanumeric() || c == '_'));
        boundary(before) && boundary(after)
    })
}

/// Whether `ty` names an atomic, such as `AtomicU64` or
/// `zakura_rt::sync::atomic::AtomicBool`: an identifier `Atomic` followed
/// by its kind, on any path.
fn names_atomic(ty: &str) -> bool {
    ty.split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .any(|ident| {
            ident
                .strip_prefix("Atomic")
                .and_then(|kind| kind.chars().next())
                .is_some_and(|c| c.is_ascii_uppercase())
        })
}

/// Whether `ty` holds shared state directly: a cell, a lock or an atomic.
fn shared(ty: &str) -> bool {
    ["Cell", "RefCell", "Mutex", "RwLock"]
        .iter()
        .any(|word| names_generic(ty, word))
        || names_atomic(ty)
}

/// Whether `ty`, with every alias in `aliases` it names followed, holds
/// shared state.
fn shared_through(ty: &str, aliases: &BTreeMap<String, Vec<String>>) -> bool {
    let mut seen = BTreeSet::new();
    let mut open = vec![ty.to_owned()];
    while let Some(ty) = open.pop() {
        if shared(&ty) {
            return true;
        }
        for (alias, targets) in aliases {
            if names(&ty, alias) && seen.insert(alias.clone()) {
                open.extend(targets.iter().cloned());
            }
        }
    }
    false
}

/// Whether `ty` is a registered store's cell, or a non-owning handle on
/// one, which a store holds to read another of its node's stores.
fn registered(ty: &str) -> bool {
    ["Registered<", "WeakRegistered<"].iter().any(|cell| {
        ty.starts_with(cell)
            || ty
                .strip_prefix("crate::stores::")
                .is_some_and(|ty| ty.starts_with(cell))
    })
}

/// Every field the scan reads, and every alias, from the crate's sources.
fn scan() -> (Vec<Field>, BTreeMap<String, Vec<String>>) {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut fields = Vec::new();
    let mut aliases = BTreeMap::new();
    for path in scanned(&root) {
        let source = fs::read_to_string(&path).expect("a scanned file reads");
        let parsed = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("{} parses: {error}", path.display()));
        let file = path
            .strip_prefix(&root)
            .expect("a scanned file is under the crate")
            .to_string_lossy()
            .replace('\\', "/");
        let lines: Vec<&str> = source.lines().collect();
        collect((&file, &lines), &parsed.items, &mut fields, &mut aliases);
    }
    (fields, aliases)
}

/// Every field holding shared state is registered or allowed, and the
/// inventory and the allowlist name only fields the scan finds.
#[test]
fn every_shared_field_is_a_registered_store_or_allowed() {
    let (fields, aliases) = scan();
    let mut unlisted = Vec::new();
    let mut found_registered = BTreeSet::new();
    let mut found_allowed = BTreeSet::new();
    for field in &fields {
        let key = (
            field.file.as_str(),
            field.owner.as_str(),
            field.field.as_str(),
        );
        if registered(&field.ty) {
            found_registered.insert(key);
            if !REGISTERED
                .iter()
                .any(|store| (store.file, store.owner, store.field) == key)
            {
                unlisted.push(format!("registered but not in REGISTERED: {field:?}"));
            }
        } else if shared_through(&field.ty, &aliases) {
            found_allowed.insert(key);
            if !ALLOWED
                .iter()
                .any(|(file, owner, name, _)| (*file, *owner, *name) == key)
            {
                unlisted.push(format!(
                    "shared state neither registered nor allowed: {field:?}"
                ));
            }
        }
    }
    for store in REGISTERED {
        if !found_registered.contains(&(store.file, store.owner, store.field)) {
            unlisted.push(format!(
                "REGISTERED names {}::{}.{}, which the scan doesn't find registered",
                store.file, store.owner, store.field
            ));
        }
    }
    for (file, owner, field, _) in ALLOWED {
        if !found_allowed.contains(&(*file, *owner, *field)) {
            unlisted.push(format!(
                "ALLOWED names {file}::{owner}.{field}, which holds no shared state"
            ));
        }
    }
    assert!(unlisted.is_empty(), "{unlisted:#?}");
}

/// The scan sees through a type alias, such as the peers' `PeerRecords`,
/// a field holding a store's handle isn't shared state itself, and a new
/// store outside a handle is.
#[test]
fn the_scan_follows_aliases() {
    let (fields, aliases) = scan();
    let records = fields
        .iter()
        .find(|field| field.owner == "WirePorts" && field.field == "records")
        .expect("the wire ports keep the peers' records");
    assert_eq!(records.ty, "PeerRecords");
    assert!(!shared(&records.ty));
    assert!(shared_through(&records.ty, &aliases));
    let ends = fields
        .iter()
        .find(|field| field.owner == "TapState" && field.field == "ends")
        .expect("the tap keeps the node's end facts");
    assert!(!shared_through(&ends.ty, &aliases));
    assert!(names_generic("Rc<std::cell::Cell<bool>>", "Cell"));
    assert!(!names_generic("Rc<RefCell<u8>>", "Cell"));
    // The control: a new store kept outside a handle is shared state,
    // through its alias too, so the inventory check would flag it.
    let source = "type Kept = Arc<Mutex<Vec<u8>>>;\nstruct New {\n    kept: Kept,\n}\n";
    let parsed = syn::parse_file(source).expect("the control parses");
    let lines: Vec<&str> = source.lines().collect();
    let (mut new, mut new_aliases) = (Vec::new(), BTreeMap::new());
    collect(
        ("new.rs", &lines),
        &parsed.items,
        &mut new,
        &mut new_aliases,
    );
    assert_eq!(new[0].ty, "Kept");
    assert!(!registered(&new[0].ty));
    assert!(shared_through(&new[0].ty, &new_aliases));
}

/// An atomic field, on any of its paths and through an alias, is shared
/// state the scan finds, so an unlisted one fails the inventory, as the
/// control's fields show. A type merely named like one isn't.
#[test]
fn the_scan_finds_atomics() {
    let source = "type Count = std::sync::atomic::AtomicUsize;\n\
                  struct New {\n    \
                  counter: Arc<AtomicU64>,\n    \
                  flag: zakura_rt::sync::atomic::AtomicBool,\n    \
                  count: Count,\n    \
                  plain: Atomicity,\n\
                  }\n";
    let parsed = syn::parse_file(source).expect("the control parses");
    let lines: Vec<&str> = source.lines().collect();
    let (mut new, mut aliases) = (Vec::new(), BTreeMap::new());
    collect(("new.rs", &lines), &parsed.items, &mut new, &mut aliases);
    let found: Vec<(&str, bool)> = new
        .iter()
        .map(|field| {
            let flagged = shared_through(&field.ty, &aliases)
                && !registered(&field.ty)
                && !ALLOWED.iter().any(|(file, owner, name, _)| {
                    (*file, *owner, *name) == ("new.rs", "New", &field.field)
                });
            (field.field.as_str(), flagged)
        })
        .collect();
    assert_eq!(
        found,
        [
            ("counter", true),
            ("flag", true),
            ("count", true),
            ("plain", false)
        ]
    );
    let (fields, _) = scan();
    let stamps = fields
        .iter()
        .find(|field| field.owner == "Stamps")
        .expect("the stamp counter is scanned");
    assert!(shared(&stamps.ty), "{stamps:?}");
}

/// An enum variant's fields, tuple or named, are scanned as a struct's
/// are, so shared state an unlisted variant holds fails the inventory, as
/// the control's variants show, and the scan reads the tree's real
/// variants.
#[test]
fn the_scan_reads_enum_variant_fields() {
    let source = "enum E {\n    \
                  A(Arc<AtomicU64>),\n    \
                  B { kept: Mutex<u8>, plain: u8 },\n    \
                  C,\n\
                  }\n";
    let parsed = syn::parse_file(source).expect("the control parses");
    let lines: Vec<&str> = source.lines().collect();
    let (mut new, mut aliases) = (Vec::new(), BTreeMap::new());
    collect(("new.rs", &lines), &parsed.items, &mut new, &mut aliases);
    let found: Vec<(&str, &str, bool)> = new
        .iter()
        .map(|field| {
            let flagged = shared_through(&field.ty, &aliases)
                && !registered(&field.ty)
                && !ALLOWED.iter().any(|(file, owner, name, _)| {
                    (*file, *owner, *name) == ("new.rs", field.owner.as_str(), &field.field)
                });
            (field.owner.as_str(), field.field.as_str(), flagged)
        })
        .collect();
    assert_eq!(
        found,
        [
            ("E::A", "0", true),
            ("E::B", "kept", true),
            ("E::B", "plain", false)
        ]
    );
    let (fields, _) = scan();
    assert!(
        fields
            .iter()
            .any(|field| field.owner == "ProbeSlot::Held" && field.field == "0"),
        "the scan reads the tap's probe slot's variant"
    );
}

/// Whether `tests/<file>.rs` defines the test `name`.
fn test_exists(file: &str, name: &str) -> bool {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join(format!("{file}.rs"));
    let source = fs::read_to_string(path).unwrap_or_default();
    syn::parse_file(&source).is_ok_and(|parsed| {
        parsed.items.iter().any(|item| {
            matches!(item, Item::Fn(function)
                if function.sig.ident == name
                    && function.attrs.iter().any(|attr| attr.path().is_ident("test")))
        })
    })
}

/// Every registered store names at least one checkpoint, and a producer
/// test that exists, or why hf-7 owes one. A name no test has fails, as
/// the control shows.
#[test]
fn every_registered_store_names_its_checkpoint_and_producer() {
    assert!(
        !test_exists("header_node_ends", "no_such_producer_test"),
        "the control: a missing test is missing"
    );
    let mut missing = Vec::new();
    for store in REGISTERED {
        assert!(
            !store.checkpoints.is_empty(),
            "{} names no checkpoint",
            store.owner
        );
        match store.producer {
            Producer::Test { file, name } => {
                if !test_exists(file, name) {
                    missing.push(format!(
                        "{}: no test `{name}` in tests/{file}.rs",
                        store.owner
                    ));
                }
            }
            Producer::OwedToHf7(why) => assert!(!why.is_empty(), "{} says why", store.owner),
        }
    }
    assert!(missing.is_empty(), "{missing:#?}");
}

/// The stores of `stores` marked [`Producer::OwedToHf7`], as
/// `owner.field`.
fn owed(stores: &[Store]) -> Vec<String> {
    stores
        .iter()
        .filter(|store| matches!(store.producer, Producer::OwedToHf7(_)))
        .map(|store| format!("{}.{}", store.owner, store.field))
        .collect()
}

/// No store is owed a producer test: hf-7 gave the last two theirs, so a
/// new store marked [`Producer::OwedToHf7`] fails here instead of skipping
/// its producer silently, as the control shows.
#[test]
fn owed_to_hf7() {
    let control = Store {
        file: "new.rs",
        owner: "New",
        field: "kept",
        checkpoints: &[HeaderTap],
        producer: Producer::OwedToHf7("the control"),
    };
    assert_eq!(owed(&[control]), ["New.kept"]);
    let owed = owed(REGISTERED);
    assert!(owed.is_empty(), "{owed:?}");
}

/// C14's proving tests outside this file, which acceptance requires as
/// `Passes`, are exactly [`REGISTERED`]'s producer tests, so a store added
/// here is required there too. A control with one more producer differs.
#[test]
fn c14_is_proved_by_every_registered_store_s_producer() {
    use zakura_sim::harness::header_acceptance::{contract_table, ContractId, ContractState};
    let c14 = contract_table()
        .into_iter()
        .find(|contract| contract.id == ContractId::C14)
        .expect("C14 has a row");
    assert_eq!(c14.state, ContractState::Satisfied);
    let proving: BTreeSet<(String, String)> = c14
        .proving
        .iter()
        .filter(|test| test.package == "zakura-sim" && test.file != "tests/evidence_inventory.rs")
        .map(|test| (test.file.to_owned(), test.test.to_owned()))
        .collect();
    let mut producers: BTreeSet<(String, String)> = REGISTERED
        .iter()
        .filter_map(|store| match store.producer {
            Producer::Test { file, name } => Some((format!("tests/{file}.rs"), name.to_owned())),
            Producer::OwedToHf7(_) => None,
        })
        .collect();
    assert_eq!(proving, producers);
    producers.insert((
        "tests/header_new.rs".to_owned(),
        "a_new_store_keeps_its_facts".to_owned(),
    ));
    assert_ne!(proving, producers);
}
