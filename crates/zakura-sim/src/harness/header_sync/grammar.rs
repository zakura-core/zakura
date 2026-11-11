//! `header-sync/storage`'s scenario prefix (G1): a [`Layout`] of a 24-byte
//! header and 32 ten-byte action slots, 344 bytes in all.
//!
//! [`decode`] turns any prefix of that length into a [`StorageScenario`],
//! except one whose version byte names another generator. Every value is
//! reduced into its range, so a mutated byte always picks a defined value.
//! [`encode`] writes a scenario's canonical prefix, the one encoding that
//! [`decode`] turns back into the same scenario.
//!
//! Header:
//!
//! | Bytes | Meaning |
//! |-------|---------|
//! | 0 | Version: [`GENERATOR`] |
//! | 1 | Action count, modulo 33 |
//! | 2 | Bit 0: base outcomes, 0 `success-only`, 1 `any`. Bits 1–2 and 3–4: branch 1's and branch 2's weight, 0 to 3 |
//! | 3 | Base bound: 0 none, else n × 100 ms |
//! | 4–6 | The node's chain |
//! | 7 | Mirror count: 1 + n mod 4 |
//! | 8–23 | Four mirror records of 4 bytes: chain, then bit 0 of the dialer, 0 `mirror`, 1 `node` |
//!
//! A chain is 3 bytes: main headers (n mod 17), fork branch (1 + n mod 2),
//! fork headers (n mod 17, 0 for no fork). So a scenario uses at most three
//! branches of 16 headers each. Records past the mirror count are padding.
//!
//! Slot `i` starts at byte 24 + 10 × i. Byte 0 is the opcode, bytes 1–4 its
//! operands, and bytes 5–9 its trigger:
//!
//! | Opcode | Action | Operands |
//! |--------|--------|----------|
//! | `0x01` | `Close` control step | 1: mirror, n mod mirror count |
//! | `0x02` | `Relink` control step | 1: mirror |
//! | `0x03` | `Grow` control step | 1: mirror; 2: headers, 1 + n mod 16 |
//! | `0x04` | `Shutdown`, naming the node | |
//! | `0x05` | `Reset`, naming the node | 1: bit 0, 0 invalidate, 1 reconsider; 2: an invalidation's depth, n mod 17 |
//! | `0x06` | `Release`, naming the node | 1: hold, n mod the hold count |
//! | `0x07` | `Recover` | 1: the source mirror |
//! | `0x08` | `StopStorage`, naming the node | |
//! | `0x10` | Hold | 1–3: selection; 4: offered outcomes, a bit mask |
//! | `0x11` | Reply hold | 1–3: selection, of applies only, so its kind is ignored |
//!
//! The outcome mask's bits 0–4 offer `engine`, `timeout`, `unavailable`,
//! `local-resource` and `rejected`, in that order. A selection is 3 bytes:
//! bit 7 for `held`, then the low 7 bits mod 9 pick no kind or one of the 8
//! [`OpKind`]s in declaration order; `nth` (n mod 65, 0 for none); and the
//! session (n mod 37, 0 for none). Only a hold's or reply hold's trigger
//! reads the `held` bit. Elsewhere `held` matches nothing, so the bit is
//! ignored there. A run has at most 36 sessions: one per mirror at setup,
//! and one per relink.
//!
//! A trigger is 5 bytes: its kind, a count, then operands.
//!
//! | Kind | Trigger | Operands |
//! |------|---------|----------|
//! | 0 | None | |
//! | 1–5 | `submitted`, `caller-dropped`, `completed`, `reply-taken`, `handled` | 2–4: selection |
//! | 6–8 | `closed`, `linked`, `grown` | 2: mirror |
//! | 9 | `virtual-time` | 2–3: n × 100 ms, little-endian |
//! | 10–11 | `reservation-opened`, `session-admitted` | 2: mirror |
//! | 12 | `reset`, of the node | |
//! | 13 | `released` | |
//! | 14 | `committed` | 2–4: selection |
//! | 15 | `storage-stopped`, of the node | |
//!
//! A nonzero count wraps an event trigger, every kind but 9 and 13, as
//! `count(n, …)`. `released` holds only in a hold's own trigger and ignores
//! the count byte, so a slot that gives it anywhere else is dropped. A
//! control step without a trigger is always offered. A hold or reply hold
//! without one opens at once, as `virtual-time(0)`.
//!
//! Decoding also drops a control slot whose position makes it invalid
//! whatever the run does: any step after a `Shutdown` but one
//! `StopStorage`, a `StopStorage` that follows no `Shutdown`, and any step
//! after a `Recover`. A `Release` slot in a scenario with no hold is
//! dropped too. So every decoded scenario prepares, and [`encode`] refuses
//! a scenario that breaks the order.
//!
//! Every opcode and trigger kind not listed is inert: [`decode`] drops its
//! slot, so the scenario never claims that action. Opcodes `0x09`–`0x0f`
//! are reserved for more control steps, and `0x12`–`0x1f` for more storage
//! actions. The grammar has no `reply-queued` or `prepared` trigger, since
//! a header node never records them, and no hold deadline, since a deadline
//! before the hold opens stops time with nothing to offer. It has no fail or
//! succeed opcode either, since a hold with a one-outcome mask is one, and a
//! second spelling would give a scenario two encodings. Bytes no field
//! reads are ignored, and the canonical prefix sets them to 0.
//!
//! Changing any of this changes what an input's bytes mean, so it bumps
//! [`GENERATOR`]. [`v2`] keeps generator 2's decoder, which reads the
//! subset generator 2 had, so [`migrate`] can carry a generator-2 input to
//! this generator with its meaning and its Plan bytes.

use super::{Dialer, MirrorSpec, StorageScenario, Weights};
use crate::harness::{
    header_node::{
        policy::{Allow, Hold, OpKind, ReplyHold, Select, Trigger},
        Base, Chain, ControlAction, ControlStep, Fork, Policy, Reset,
    },
    scenario::{Layout, Rejection, Unencodable},
};

/// `header-sync/storage`'s generator version, and its prefix's version
/// byte. Generator 1 read no scenario bytes, and generator 2 had no
/// weights, node steps, recover or reply hold.
pub const GENERATOR: u32 = 3;

const _: () = assert!(GENERATOR <= u8::MAX as u32);

/// The most mirrors a scenario has.
pub const MAX_MIRRORS: usize = 4;

/// The most actions a scenario has: holds, reply holds and control steps
/// together.
pub const MAX_ACTIONS: usize = 32;

/// The most headers a chain has on each of its branches.
pub const MAX_HEADERS: u32 = 16;

/// The fork branches a chain may use. Branch 0 is the main branch.
pub const FORK_BRANCHES: u8 = 2;

/// The most headers one growth adds.
pub const MAX_GROWTH: u32 = 16;

/// The deepest invalidation a reset names.
pub const MAX_DEPTH: u32 = 16;

/// The largest `nth` a selection names.
pub const MAX_NTH: u32 = 64;

/// The largest session a selection names: one per mirror's setup link, and
/// one per relink.
pub const MAX_SESSION: u64 = (MAX_MIRRORS + MAX_ACTIONS) as u64;

// Each encodes in one byte.
const _: () = assert!(
    MAX_HEADERS <= u8::MAX as u32
        && MAX_GROWTH <= u8::MAX as u32
        && MAX_DEPTH <= u8::MAX as u32
        && MAX_NTH <= u8::MAX as u32
        && MAX_SESSION <= u8::MAX as u64
);

/// The base bound's unit.
pub const BOUND_UNIT_MS: u64 = 100;

/// A `virtual-time` trigger's unit.
pub const TIME_UNIT_MS: u64 = 100;

/// The prefix's layout.
pub const LAYOUT: Layout = Layout {
    // The assertion above checks that the generator fits a byte.
    version: GENERATOR as u8,
    header: 24,
    slot: 10,
    slots: MAX_ACTIONS,
};

/// Header offsets.
const BASE: usize = 2;
const BOUND: usize = 3;
const NODE: usize = 4;
const MIRROR_COUNT: usize = 7;
const MIRRORS: usize = 8;
/// A mirror record's length: its chain, then its dialer.
const MIRROR: usize = 4;

/// Where branch 1's and branch 2's weights sit in the base byte, two bits
/// each.
const WEIGHT_SHIFTS: [u8; 2] = [1, 3];

/// Opcodes.
const CLOSE: u8 = 0x01;
const RELINK: u8 = 0x02;
const GROW: u8 = 0x03;
const SHUTDOWN: u8 = 0x04;
const RESET: u8 = 0x05;
const RELEASE: u8 = 0x06;
const RECOVER: u8 = 0x07;
const STOP_STORAGE: u8 = 0x08;
const HOLD: u8 = 0x10;
const REPLY_HOLD: u8 = 0x11;

/// Where a slot's trigger starts.
const TRIGGER: usize = 5;

/// Trigger kinds.
const NONE: u8 = 0;
const SUBMITTED: u8 = 1;
const CALLER_DROPPED: u8 = 2;
const COMPLETED: u8 = 3;
const REPLY_TAKEN: u8 = 4;
const HANDLED: u8 = 5;
const CLOSED: u8 = 6;
const LINKED: u8 = 7;
const GROWN: u8 = 8;
const VIRTUAL_TIME: u8 = 9;
const RESERVATION_OPENED: u8 = 10;
const SESSION_ADMITTED: u8 = 11;
const RESET_DONE: u8 = 12;
const RELEASED: u8 = 13;
const COMMITTED: u8 = 14;
const STORAGE_STOPPED: u8 = 15;

/// A selection's `held` bit.
const HELD: u8 = 0x80;

/// The node under test's name, which node steps and node triggers carry.
const NODE_NAME: &str = "node";

/// The operation kinds a selection's kind byte picks, from 1.
const OP_KINDS: [OpKind; 8] = [
    OpKind::Locator,
    OpKind::VctContext,
    OpKind::Acquire,
    OpKind::Read,
    OpKind::Release,
    OpKind::Prepare,
    OpKind::Apply,
    OpKind::Membership,
];

/// The outcomes a hold's mask offers, by bit.
const ALLOWS: [Allow; 5] = [
    Allow::Engine,
    Allow::Timeout,
    Allow::Unavailable,
    Allow::LocalResource,
    Allow::Rejected,
];

/// Which generator's meanings a decode reads.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Generator {
    /// Generator 2: no weights, no opcode past `0x03` but `0x10`, no trigger
    /// kind past 9.
    Two,
    /// Generator 3, [`GENERATOR`].
    Three,
}

/// Where a trigger is, which decides what it may say.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Site {
    /// A control step's trigger.
    Control,
    /// A hold's own trigger: `held` and `released` count.
    Hold,
    /// A reply hold's own trigger: `held` counts.
    ReplyHold,
}

impl Site {
    /// Whether a selection's `held` bit can match here.
    fn held(self) -> bool {
        self != Site::Control
    }
}

/// The scenario `prefix` encodes. It's rejected only if it isn't
/// [`LAYOUT`]'s length, or its version byte isn't [`GENERATOR`].
pub fn decode(prefix: &[u8]) -> Result<StorageScenario, Rejection> {
    decode_as(LAYOUT, Generator::Three, prefix)
}

/// Generator 2's decoder, kept for [`migrate`].
pub mod v2 {
    use super::{decode_as, Generator, Layout, Rejection, StorageScenario, LAYOUT};

    /// Generator 2's layout: generator 3's, under version byte 2.
    pub const LAYOUT_V2: Layout = Layout {
        version: 2,
        ..LAYOUT
    };

    /// The scenario a generator-2 `prefix` encodes, with generator 2's
    /// meanings: weight bits, opcodes and trigger kinds generator 3 added
    /// are ignored or inert. It's rejected only if it isn't the layout's
    /// length, or its version byte isn't 2.
    pub fn decode(prefix: &[u8]) -> Result<StorageScenario, Rejection> {
        decode_as(LAYOUT_V2, Generator::Two, prefix)
    }
}

/// Carries generator-2 fuzz `input` to this generator: decodes its prefix
/// as generator 2 did, encodes the same scenario's canonical prefix, and
/// keeps its Plan bytes, which start at the same offset since both
/// prefixes are 344 bytes. An input shorter than a prefix has its missing
/// bytes read as 0, as the harness reads it, and no Plan bytes.
///
/// # Errors
///
/// If the input's prefix isn't generator 2's.
pub fn migrate(input: &[u8]) -> Result<Vec<u8>, String> {
    let at = LAYOUT.prefix_len().min(input.len());
    let (prefix, plan) = input.split_at(at);
    let mut prefix = prefix.to_vec();
    prefix.resize(LAYOUT.prefix_len(), 0);
    let scenario = v2::decode(&prefix).map_err(|rejection| rejection.to_string())?;
    let mut migrated = encode(&scenario).map_err(|error| {
        format!(
            "generator 3 can't encode a generator-2 scenario: {}",
            error.0
        )
    })?;
    migrated.extend_from_slice(plan);
    Ok(migrated)
}

/// Decodes `prefix` under `layout` with `generator`'s meanings.
fn decode_as(
    layout: Layout,
    generator: Generator,
    prefix: &[u8],
) -> Result<StorageScenario, Rejection> {
    let three = generator == Generator::Three;
    let read = layout.read(prefix)?;
    let header = read.header;
    let mirrors = 1 + usize::from(header[MIRROR_COUNT]) % MAX_MIRRORS;
    let weight = |branch: usize| match generator {
        Generator::Two => 0,
        Generator::Three => header[BASE] >> WEIGHT_SHIFTS[branch] & 3,
    };
    let mut scenario = StorageScenario {
        node: chain(&header[NODE..NODE + 3]),
        mirrors: header[MIRRORS..]
            .as_chunks::<MIRROR>()
            .0
            .iter()
            .take(mirrors)
            .map(|record| MirrorSpec {
                chain: chain(&record[..3]),
                dialer: if record[3] & 1 == 0 {
                    Dialer::Mirror
                } else {
                    Dialer::Node
                },
            })
            .collect(),
        policy: Policy {
            base: if header[BASE] & 1 == 0 {
                Base::SuccessOnly
            } else {
                Base::Any
            },
            bound_ms: (header[BOUND] != 0).then(|| u64::from(header[BOUND]) * BOUND_UNIT_MS),
            holds: Vec::new(),
            reply_holds: Vec::new(),
        },
        controls: Vec::new(),
        weights: Weights {
            branch_1: weight(0),
            branch_2: weight(1),
        },
    };
    let decode_trigger = |trigger: &[u8], site| decode_trigger(trigger, mirrors, site, generator);
    // Holds first, so a release knows how many there are.
    for slot in &read.slots {
        let trigger = &slot[TRIGGER..];
        let until = |site| match trigger[0] {
            NONE => Some(Trigger::VirtualTime(0)),
            _ => decode_trigger(trigger, site),
        };
        match slot[0] {
            HOLD => {
                if let Some(until) = until(Site::Hold) {
                    scenario.policy.holds.push(Hold {
                        select: decode_select(&slot[1..4], false),
                        until,
                        then: (0..ALLOWS.len())
                            .filter(|bit| slot[4] >> bit & 1 == 1)
                            .map(|bit| ALLOWS[bit])
                            .collect(),
                        deadline_ms: None,
                    });
                }
            }
            REPLY_HOLD if three => {
                if let Some(until) = until(Site::ReplyHold) {
                    scenario.policy.reply_holds.push(ReplyHold {
                        select: Select {
                            kind: Some(OpKind::Apply),
                            ..decode_select(&slot[1..4], false)
                        },
                        until,
                    });
                }
            }
            _ => {}
        }
    }
    let holds = scenario.policy.holds.len();
    let mut order = Order::default();
    for slot in &read.slots {
        let trigger = &slot[TRIGGER..];
        let node = || NODE_NAME.to_owned();
        let (peer, action) = match slot[0] {
            CLOSE => (mirror(slot[1], mirrors), ControlAction::Close),
            RELINK => (mirror(slot[1], mirrors), ControlAction::Relink),
            GROW => (
                mirror(slot[1], mirrors),
                ControlAction::Grow {
                    len: 1 + u32::from(slot[2]) % MAX_GROWTH,
                },
            ),
            SHUTDOWN if three => (node(), ControlAction::Shutdown),
            RESET if three => (
                node(),
                ControlAction::Reset(if slot[1] & 1 == 0 {
                    Reset::Invalidate {
                        depth: u32::from(slot[2]) % (MAX_DEPTH + 1),
                    }
                } else {
                    Reset::Reconsider
                }),
            ),
            RELEASE if three && holds > 0 => (
                node(),
                ControlAction::Release {
                    hold: usize::from(slot[1]) % holds,
                },
            ),
            RECOVER if three => (mirror(slot[1], mirrors), ControlAction::Recover),
            STOP_STORAGE if three => (node(), ControlAction::StopStorage),
            _ => continue,
        };
        let when = match trigger[0] {
            NONE => None,
            _ => match decode_trigger(trigger, Site::Control) {
                Some(when) => Some(when),
                None => continue,
            },
        };
        if order.admit(action) {
            scenario.controls.push(ControlStep { peer, action, when });
        }
    }
    Ok(scenario)
}

/// Where a scenario's control steps have got to, for the static order.
#[derive(Default)]
struct Order {
    shut_down: bool,
    stopped: bool,
    recovered: bool,
}

impl Order {
    /// Whether `action` may come next, and if so, takes it. Nothing follows
    /// a recover. After a shutdown only one storage stop may come, and a
    /// storage stop needs a shutdown before it.
    fn admit(&mut self, action: ControlAction) -> bool {
        let next = match action {
            _ if self.recovered || self.stopped => false,
            ControlAction::StopStorage => self.shut_down,
            _ => !self.shut_down,
        };
        if next {
            match action {
                ControlAction::Shutdown => self.shut_down = true,
                ControlAction::StopStorage => self.stopped = true,
                ControlAction::Recover => self.recovered = true,
                _ => {}
            }
        }
        next
    }
}

/// `scenario`'s canonical prefix: its holds' slots in order, then its
/// reply holds', then its control steps'. Fails if the grammar can't
/// express some part of it.
pub fn encode(scenario: &StorageScenario) -> Result<Vec<u8>, Unencodable> {
    let mirrors = scenario.mirrors.len();
    if !(1..=MAX_MIRRORS).contains(&mirrors) {
        return Err(Unencodable(format!(
            "it has {mirrors} mirrors, not 1 to {MAX_MIRRORS}"
        )));
    }
    let mut writer = LAYOUT.writer();
    let header = writer.header();
    header[BASE] = match scenario.policy.base {
        Base::SuccessOnly => 0,
        Base::Any => 1,
    };
    for (weight, shift) in [scenario.weights.branch_1, scenario.weights.branch_2]
        .into_iter()
        .zip(WEIGHT_SHIFTS)
    {
        if weight > 3 {
            return Err(Unencodable(format!("a weight is {weight}, not 0 to 3")));
        }
        header[BASE] |= weight << shift;
    }
    header[BOUND] = match scenario.policy.bound_ms {
        None => 0,
        Some(ms) => units(ms, BOUND_UNIT_MS, 1, u8::MAX, "the base bound")?,
    };
    encode_chain(&scenario.node, &mut header[NODE..NODE + 3])?;
    // At most MAX_MIRRORS, checked above.
    header[MIRROR_COUNT] = (mirrors - 1) as u8;
    for (spec, record) in scenario
        .mirrors
        .iter()
        .zip(header[MIRRORS..].as_chunks_mut::<MIRROR>().0)
    {
        encode_chain(&spec.chain, &mut record[..3])?;
        record[3] = match spec.dialer {
            Dialer::Mirror => 0,
            Dialer::Node => 1,
        };
    }
    for hold in &scenario.policy.holds {
        let slot = writer.slot()?;
        if hold.deadline_ms.is_some() {
            return Err(Unencodable("a hold has a deadline".to_owned()));
        }
        slot[0] = HOLD;
        encode_select(&hold.select, &mut slot[1..4], false)?;
        let mut mask = 0u8;
        let mut last = None;
        for allow in &hold.then {
            let bit = ALLOWS
                .iter()
                .position(|known| known == allow)
                .expect("ALLOWS lists every outcome");
            if last.is_some_and(|last| bit <= last) {
                return Err(Unencodable(format!(
                    "a hold offers {:?}, not distinct outcomes in mask order",
                    hold.then
                )));
            }
            last = Some(bit);
            mask |= 1 << bit;
        }
        slot[4] = mask;
        encode_trigger(&hold.until, mirrors, Site::Hold, &mut slot[TRIGGER..])?;
    }
    for hold in &scenario.policy.reply_holds {
        let slot = writer.slot()?;
        if hold.select.kind != Some(OpKind::Apply) {
            return Err(Unencodable(format!(
                "a reply hold selects {:?}, and the grammar's select applies",
                hold.select.kind
            )));
        }
        slot[0] = REPLY_HOLD;
        encode_select(
            &Select {
                kind: None,
                ..hold.select
            },
            &mut slot[1..4],
            false,
        )?;
        encode_trigger(&hold.until, mirrors, Site::ReplyHold, &mut slot[TRIGGER..])?;
    }
    let holds = scenario.policy.holds.len();
    let mut order = Order::default();
    for step in &scenario.controls {
        if !order.admit(step.action) {
            return Err(Unencodable(format!(
                "{:?} is out of order: nothing follows a recover, only one storage stop \
                 follows a shutdown, and a storage stop needs one",
                step.action
            )));
        }
        let slot = writer.slot()?;
        let on_node = || {
            if step.peer == NODE_NAME {
                Ok(())
            } else {
                Err(Unencodable(format!(
                    "a {:?} names the node, not `{}`",
                    step.action, step.peer
                )))
            }
        };
        slot[0] = match step.action {
            ControlAction::Close => CLOSE,
            ControlAction::Relink => RELINK,
            ControlAction::Grow { len } => {
                // The limit fits a byte, as the assertion on it checks.
                slot[2] = units::<u8>(len.into(), 1, 1, MAX_GROWTH as u8, "a growth")? - 1;
                GROW
            }
            ControlAction::Recover => RECOVER,
            ControlAction::Shutdown => {
                on_node()?;
                SHUTDOWN
            }
            ControlAction::StopStorage => {
                on_node()?;
                STOP_STORAGE
            }
            ControlAction::Reset(reset) => {
                on_node()?;
                match reset {
                    Reset::Invalidate { depth } => {
                        // The limit fits a byte, as the assertion on it checks.
                        slot[2] = units(depth.into(), 1, 0, MAX_DEPTH as u8, "a reset's depth")?;
                    }
                    Reset::Reconsider => slot[1] = 1,
                }
                RESET
            }
            ControlAction::Release { hold } => {
                on_node()?;
                if hold >= holds {
                    return Err(Unencodable(format!(
                        "a release names hold {hold} of {holds}"
                    )));
                }
                // Below the hold count, which the action limit keeps under
                // a byte.
                slot[1] = hold as u8;
                RELEASE
            }
            action @ (ControlAction::Commit { .. }
            | ControlAction::Flood
            | ControlAction::Churn
            | ControlAction::RepairStatus { .. }) => {
                return Err(Unencodable(format!(
                    "the grammar has no {action:?} control step"
                )))
            }
        };
        if matches!(
            step.action,
            ControlAction::Close
                | ControlAction::Relink
                | ControlAction::Grow { .. }
                | ControlAction::Recover
        ) {
            slot[1] = mirror_index(&step.peer, mirrors)?;
        }
        if let Some(when) = &step.when {
            encode_trigger(when, mirrors, Site::Control, &mut slot[TRIGGER..])?;
        }
    }
    Ok(writer.finish())
}

/// A full fuzz input for `scenario`: its canonical prefix, then `plan`,
/// the Plan bytes. A seed tool writes typed scenarios this way.
pub fn input(scenario: &StorageScenario, plan: &[u8]) -> Result<Vec<u8>, Unencodable> {
    let mut input = encode(scenario)?;
    input.extend_from_slice(plan);
    Ok(input)
}

/// The chain 3 bytes encode.
fn chain(bytes: &[u8]) -> Chain {
    let len = u32::from(bytes[2]) % (MAX_HEADERS + 1);
    Chain {
        main: u32::from(bytes[0]) % (MAX_HEADERS + 1),
        fork: (len > 0).then(|| Fork {
            branch: 1 + bytes[1] % FORK_BRANCHES,
            len,
        }),
    }
}

/// Writes `chain` to `out`'s 3 bytes.
fn encode_chain(chain: &Chain, out: &mut [u8]) -> Result<(), Unencodable> {
    // The limit fits a byte, as the assertion on it checks.
    let headers = |len: u32| units(len.into(), 1, 0, MAX_HEADERS as u8, "a branch");
    out[0] = headers(chain.main)?;
    if let Some(fork) = chain.fork {
        if fork.len == 0 {
            return Err(Unencodable("a fork has no headers".to_owned()));
        }
        if !(1..=FORK_BRANCHES).contains(&fork.branch) {
            return Err(Unencodable(format!(
                "a fork is on branch {}, not 1 to {FORK_BRANCHES}",
                fork.branch
            )));
        }
        out[1] = fork.branch - 1;
        out[2] = headers(fork.len)?;
    }
    Ok(())
}

/// `mirror-<n mod mirrors>`.
fn mirror(byte: u8, mirrors: usize) -> String {
    format!("mirror-{}", usize::from(byte) % mirrors)
}

/// The index of the mirror named `peer`, among `mirrors`.
fn mirror_index(peer: &str, mirrors: usize) -> Result<u8, Unencodable> {
    (0..mirrors)
        .find(|index| peer == format!("mirror-{index}"))
        // At most MAX_MIRRORS.
        .map(|index| index as u8)
        .ok_or_else(|| Unencodable(format!("`{peer}` isn't one of its {mirrors} mirrors")))
}

/// The selection 3 bytes encode. Its `held` bit counts only where `held`
/// can match: in a hold's or reply hold's trigger.
fn decode_select(bytes: &[u8], held: bool) -> Select {
    let kind = usize::from(bytes[0] & !HELD) % (OP_KINDS.len() + 1);
    let nth = u32::from(bytes[1]) % (MAX_NTH + 1);
    let session = u64::from(bytes[2]) % (MAX_SESSION + 1);
    Select {
        kind: kind.checked_sub(1).map(|kind| OP_KINDS[kind]),
        nth: (nth > 0).then_some(nth),
        session: (session > 0).then_some(session),
        held: held && bytes[0] & HELD != 0,
    }
}

/// Writes `select` to `out`'s 3 bytes. Fails for a `held` selection
/// unless `held` says it can match.
fn encode_select(select: &Select, out: &mut [u8], held: bool) -> Result<(), Unencodable> {
    if select.held && !held {
        return Err(Unencodable(
            "a `held` selection outside a hold's or reply hold's trigger matches nothing"
                .to_owned(),
        ));
    }
    let kind = select.kind.map_or(0, |kind| {
        1 + OP_KINDS
            .iter()
            .position(|known| *known == kind)
            .expect("OP_KINDS lists every kind")
    });
    // At most 8, the number of kinds.
    out[0] = kind as u8 | if select.held { HELD } else { 0 };
    // The limits fit a byte, as the assertion on them checks.
    if let Some(nth) = select.nth {
        out[1] = units(nth.into(), 1, 1, MAX_NTH as u8, "a selection's nth")?;
    }
    if let Some(session) = select.session {
        out[2] = units(session, 1, 1, MAX_SESSION as u8, "a selection's session")?;
    }
    Ok(())
}

/// The trigger `bytes` encode at `site` under `generator`, or `None` for a
/// kind that generator doesn't list, or `released` outside a hold's own
/// trigger.
fn decode_trigger(
    bytes: &[u8],
    mirrors: usize,
    site: Site,
    generator: Generator,
) -> Option<Trigger> {
    let select = || decode_select(&bytes[2..5], site.held());
    let three = generator == Generator::Three;
    let event = match bytes[0] {
        SUBMITTED => Trigger::Submitted(select()),
        CALLER_DROPPED => Trigger::CallerDropped(select()),
        COMPLETED => Trigger::Completed(select()),
        REPLY_TAKEN => Trigger::ReplyTaken(select()),
        HANDLED => Trigger::Handled(select()),
        CLOSED => Trigger::Closed(mirror(bytes[2], mirrors)),
        LINKED => Trigger::Linked(mirror(bytes[2], mirrors)),
        GROWN => Trigger::Grown(mirror(bytes[2], mirrors)),
        VIRTUAL_TIME => {
            let units = u16::from_le_bytes([bytes[2], bytes[3]]);
            return Some(Trigger::VirtualTime(u64::from(units) * TIME_UNIT_MS));
        }
        RESERVATION_OPENED if three => Trigger::ReservationOpened(mirror(bytes[2], mirrors)),
        SESSION_ADMITTED if three => Trigger::SessionAdmitted(mirror(bytes[2], mirrors)),
        RESET_DONE if three => Trigger::Reset(NODE_NAME.to_owned()),
        RELEASED if three => return (site == Site::Hold).then_some(Trigger::Released),
        COMMITTED if three => Trigger::Committed(select()),
        STORAGE_STOPPED if three => Trigger::StorageStopped(NODE_NAME.to_owned()),
        _ => return None,
    };
    Some(match bytes[1] {
        0 => event,
        count => Trigger::Count(count.into(), Box::new(event)),
    })
}

/// Writes `trigger` at `site` to `out`'s 5 bytes.
fn encode_trigger(
    trigger: &Trigger,
    mirrors: usize,
    site: Site,
    out: &mut [u8],
) -> Result<(), Unencodable> {
    let event = match trigger {
        Trigger::VirtualTime(ms) => {
            let units = units(*ms, TIME_UNIT_MS, 0, u16::MAX, "a virtual time")?;
            out[0] = VIRTUAL_TIME;
            out[2..4].copy_from_slice(&units.to_le_bytes());
            return Ok(());
        }
        Trigger::Released if site == Site::Hold => {
            out[0] = RELEASED;
            return Ok(());
        }
        Trigger::Count(count, inner)
            if !matches!(
                inner.as_ref(),
                Trigger::VirtualTime(_) | Trigger::Released | Trigger::Count(..) | Trigger::All(_)
            ) =>
        {
            out[1] = units((*count).into(), 1, 1, u8::MAX, "a trigger's count")?;
            inner.as_ref()
        }
        event => event,
    };
    let (kind, select) = match event {
        Trigger::Submitted(select) => (SUBMITTED, select),
        Trigger::CallerDropped(select) => (CALLER_DROPPED, select),
        Trigger::Completed(select) => (COMPLETED, select),
        Trigger::ReplyTaken(select) => (REPLY_TAKEN, select),
        Trigger::Handled(select) => (HANDLED, select),
        Trigger::Committed(select) => (COMMITTED, select),
        Trigger::Closed(peer)
        | Trigger::Linked(peer)
        | Trigger::Grown(peer)
        | Trigger::ReservationOpened(peer)
        | Trigger::SessionAdmitted(peer) => {
            out[0] = match event {
                Trigger::Closed(_) => CLOSED,
                Trigger::Linked(_) => LINKED,
                Trigger::Grown(_) => GROWN,
                Trigger::ReservationOpened(_) => RESERVATION_OPENED,
                _ => SESSION_ADMITTED,
            };
            out[2] = mirror_index(peer, mirrors)?;
            return Ok(());
        }
        Trigger::Reset(peer) | Trigger::StorageStopped(peer) if peer == NODE_NAME => {
            out[0] = match event {
                Trigger::Reset(_) => RESET_DONE,
                _ => STORAGE_STOPPED,
            };
            return Ok(());
        }
        other => {
            return Err(Unencodable(format!(
                "the grammar has no trigger {other:?} here"
            )))
        }
    };
    out[0] = kind;
    encode_select(select, &mut out[2..5], site.held())
}

/// `value` in `unit`s, checked to be a whole number from `min` to `max`.
fn units<T>(value: u64, unit: u64, min: T, max: T, what: &str) -> Result<T, Unencodable>
where
    T: Copy + Into<u64> + TryFrom<u64>,
{
    let (low, high) = (min.into(), max.into());
    let units = value / unit;
    value
        .is_multiple_of(unit)
        .then_some(units)
        .filter(|units| (low..=high).contains(units))
        .and_then(|units| T::try_from(units).ok())
        .ok_or_else(|| {
            Unencodable(format!(
                "{what} is {value}, not {low} to {high} times {unit}"
            ))
        })
}
