//! Generated properties (properties.md, Generated properties): each tabled
//! stream's rows become receiver properties, with no check written per row.
//!
//! A generated property's ID names everything its check needs:
//! `<family>/s<kind>v<version>[/<row>]/<claim>`. The check parses its own ID
//! back, so the ID is the one record of the stream, row and template a
//! property covers.
//!
//! The checks read four observations: [`Observation::PeerFrame`], which a
//! peer's role models label at send time; [`Observation::FrameJudged`], the
//! node's reader reaching the bytes that decide the label;
//! [`Observation::ConnectionClosed`]; and [`Observation::Misbehavior`].
//! Each judged frame needs exactly one earlier `PeerFrame` with its
//! connection, stream and sequence number, and a judged fault needs a
//! registered `<label>-closes` property for its rule. A missing or repeated
//! label, or an unclaimed fault, fails the run as a harness failure, so it
//! can't hide an obligation.

use std::fmt;

use zakura_network::zakura::{MessageRole, MessageRule, Stream};

use crate::{
    model::{consequence, is_fault_cause, Consequence, Label},
    observe::{Entry, StreamId},
    property::{
        Cardinality, CheckCtx, HasView, Kind, LivenessBound, ObligationKey, Property, PropertyId,
        Source, Status, Subject, Verdict,
    },
    Observation,
};

/// What a generated property claims.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Template {
    /// Per stream: a peer whose judged frames on every stream of the
    /// connection are all `Conformant` is never punished.
    ConformantNeverCloses,
    /// A judged frame with this label gets its consequence before
    /// quiescence: a fault close, or for `BadContent` the family's content
    /// consequence. `UndeclaredType` is per stream; the others are per row.
    Closes(Label),
    /// Per row with a cadence: emptying the row's bucket never gets a
    /// conformant peer punished.
    CadenceNeverCloses,
}

impl Template {
    /// The ID's last segment, such as `reserved-flags-closes`.
    pub fn claim(self) -> String {
        match self {
            Template::ConformantNeverCloses => "conformant-never-closes".to_owned(),
            Template::Closes(label) => format!("{}-closes", label.name()),
            Template::CadenceNeverCloses => "cadence-never-closes".to_owned(),
        }
    }

    /// The template whose [`Template::claim`] is `claim`.
    pub fn parse(claim: &str) -> Option<Template> {
        match claim {
            "conformant-never-closes" => Some(Template::ConformantNeverCloses),
            "cadence-never-closes" => Some(Template::CadenceNeverCloses),
            _ => {
                let name = claim.strip_suffix("-closes")?;
                let label = Label::ALL.into_iter().find(|l| l.name() == name)?;
                (consequence(label) != Consequence::NoPunishment).then_some(Template::Closes(label))
            }
        }
    }

    /// The claim, in one sentence.
    fn sentence(self) -> &'static str {
        match self {
            Template::ConformantNeverCloses => "a conformant peer is never punished",
            Template::Closes(Label::BadContent) => {
                "a frame with bad content gets a Misbehavior or a fault close"
            }
            Template::Closes(_) => "a frame with this label gets a fault close",
            Template::CadenceNeverCloses => "exhausting a cadence bucket is never punished",
        }
    }

    /// The sources the check reads: frames and closes, and storage for the
    /// templates that read `Misbehavior`.
    fn needs(self) -> &'static [Source] {
        match self {
            Template::Closes(label) if consequence(label) != Consequence::FamilyContent => {
                &[Source::Frames, Source::Closes]
            }
            _ => &[Source::Frames, Source::Closes, Source::Storage],
        }
    }

    /// The property's kind: a fault close is an obligation, and the rest
    /// hold at every step.
    fn kind(self) -> Kind {
        match self {
            Template::Closes(_) => Kind::Liveness {
                bound: LivenessBound::BeforeQuiescence,
            },
            Template::ConformantNeverCloses | Template::CadenceNeverCloses => Kind::Safety,
        }
    }
}

/// A generated property's ID, in parts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GeneratedId<'a> {
    /// The family, such as `header-sync`.
    pub family: &'a str,
    /// The stream.
    pub stream: StreamId,
    /// The row's name, or `None` for a per-stream template.
    pub row: Option<&'a str>,
    /// The template.
    pub template: Template,
}

impl<'a> GeneratedId<'a> {
    /// The parts of `id`, if it has a generated property's shape.
    pub fn parse(id: &'a str) -> Option<Self> {
        let parts: Vec<&str> = id.split('/').collect();
        let (family, stream, row, claim) = match parts[..] {
            [family, stream, claim] => (family, stream, None, claim),
            [family, stream, row, claim] => (family, stream, Some(row), claim),
            _ => return None,
        };
        let (kind, version) = stream.strip_prefix('s')?.split_once('v')?;
        Some(GeneratedId {
            family,
            stream: StreamId {
                kind: kind.parse().ok()?,
                version: version.parse().ok()?,
            },
            row,
            template: Template::parse(claim)?,
        })
    }
}

impl fmt::Display for GeneratedId<'_> {
    /// Writes the ID.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/", self.family, self.stream)?;
        if let Some(row) = self.row {
            write!(f, "{row}/")?;
        }
        f.write_str(&self.template.claim())
    }
}

/// The receiver properties of `family`'s tabled `streams`: per stream,
/// `conformant-never-closes` and `undeclared-type-closes`; per row, the
/// `<label>-closes` of each label the row's role can commit, including
/// `bad-content-closes`, and `cadence-never-closes` for a row with a
/// cadence. A stream with no table yields none.
///
/// # Panics
///
/// If `family` breaks the ID grammar. Row names follow it, because
/// `Stream::check_layout` refuses one that doesn't.
pub fn table_properties<S: HasView>(family: &'static str, streams: &[Stream]) -> Vec<Property<S>> {
    let mut properties = Vec::new();
    for stream in streams {
        let Some(rules) = stream.messages else {
            continue;
        };
        let id = |row, template| GeneratedId {
            family,
            stream: StreamId {
                kind: stream.kind,
                version: stream.version,
            },
            row,
            template,
        };
        properties.push(generated(id(None, Template::ConformantNeverCloses)));
        properties.push(generated(id(None, Template::Closes(Label::UndeclaredType))));
        for rule in rules {
            for template in row_templates(rule) {
                properties.push(generated(id(Some(rule.name), template)));
            }
        }
    }
    properties
}

/// The per-row templates of `rule`, in label order.
fn row_templates(rule: &MessageRule) -> Vec<Template> {
    let mut labels = vec![
        Label::ReservedFlags,
        Label::ShortPayload,
        Label::LongPayload,
    ];
    let cadence = match rule.role {
        MessageRole::Announcement { .. } => true,
        MessageRole::Request { cadence, .. } => {
            labels.push(Label::OverMargin);
            cadence.is_some()
        }
        MessageRole::Response { .. } => {
            labels.extend([Label::Unsolicited, Label::OverOwed]);
            false
        }
        MessageRole::Subscription { cadence, .. } => {
            labels.extend([
                Label::OverLive,
                Label::OverCredit,
                Label::AfterClose,
                Label::BadCursor,
            ]);
            cadence.is_some()
        }
    };
    labels.push(Label::BadContent);
    let mut templates: Vec<Template> = labels.into_iter().map(Template::Closes).collect();
    if cadence {
        templates.push(Template::CadenceNeverCloses);
    }
    templates
}

/// The property `id` names.
fn generated<S: HasView>(id: GeneratedId<'_>) -> Property<S> {
    Property {
        id: PropertyId::generated(id.to_string())
            .unwrap_or_else(|error| panic!("a generated ID breaks the grammar: {error:?}")),
        claim: id.template.sentence(),
        kind: id.template.kind(),
        assumes: &[],
        needs: id.template.needs(),
        status: Status::Active,
        check: check_generated::<S>,
        witness: None,
    }
}

/// The stream and row whose rule `label` enforces on a frame of
/// `frame_row` on `frame_stream`, for [`Observation::PeerFrame`]'s
/// `rule_stream` and `row`: the frame's own, except a page's `OverCredit`,
/// which its subscription row enforces. That row may sit on another stream
/// of `layout`.
pub fn rule_row<'a>(
    layout: &'a [Stream],
    frame_stream: &'a Stream,
    frame_row: &'a MessageRule,
    label: Label,
) -> (&'a Stream, &'a MessageRule) {
    if let MessageRole::Response { request, .. } = frame_row.role {
        if label == Label::OverCredit {
            let subscription = std::iter::once(frame_stream)
                .chain(layout)
                .find_map(|stream| Some((stream, MessageRule::find(stream.messages?, request)?)));
            if let Some(found) = subscription {
                return found;
            }
        }
    }
    (frame_stream, frame_row)
}

/// Checks a generated property, from the template its ID names.
fn check_generated<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    let id = cx.id().clone();
    let id = GeneratedId::parse(id.as_str()).expect("table_properties wrote this ID");
    match id.template {
        Template::Closes(label) => check_closes(cx, &id, label),
        Template::ConformantNeverCloses => {
            if let Some(unclaimed) = unclaimed_fault(cx, &id) {
                cx.missing_evidence(unclaimed);
                return Verdict::NotApplicable;
            }
            check_never_closes(cx, &id, |_| true)
        }
        Template::CadenceNeverCloses => {
            check_never_closes(cx, &id, |frame| frame.exhausted && frame.row == id.row)
        }
    }
}

/// One frame a peer sent.
#[derive(Clone, Copy, Debug)]
struct Sent<'a> {
    stream: StreamId,
    row: Option<&'a str>,
    rule_stream: StreamId,
    label: Label,
    exhausted: bool,
}

/// The frame `seq` the peer sent on `connection` and `stream`, from
/// `entries`. Every judged frame needs exactly one label there, so a missing
/// or repeated `PeerFrame` is missing evidence, described in the error.
fn sent(
    entries: &[Entry],
    connection: u64,
    stream: StreamId,
    seq: u64,
) -> Result<Sent<'_>, String> {
    let mut labels = entries.iter().filter_map(|entry| match &entry.obs {
        Observation::PeerFrame {
            connection: c,
            stream: s,
            seq: q,
            row,
            rule_stream,
            label,
            exhausted,
        } if (*c, *s, *q) == (connection, stream, seq) => Some(Sent {
            stream,
            row: row.as_deref(),
            rule_stream: *rule_stream,
            label: *label,
            exhausted: *exhausted,
        }),
        _ => None,
    });
    let frame = format!("connection {connection} judged frame {seq} on {stream}");
    match (labels.next(), labels.next()) {
        (Some(sent), None) => Ok(sent),
        (None, _) => Err(format!("{frame} has no PeerFrame label before it")),
        (Some(_), Some(_)) => Err(format!("{frame} has more than one PeerFrame label")),
    }
}

/// Every frame the node judged on `connection` in `entries`, in order, or
/// the first one's missing evidence.
fn judged(entries: &[Entry], connection: u64) -> Result<Vec<Sent<'_>>, String> {
    entries
        .iter()
        .enumerate()
        .filter_map(|(at, entry)| match entry.obs {
            Observation::FrameJudged {
                connection: c,
                stream,
                seq,
            } if c == connection => Some(sent(&entries[..at], connection, stream, seq)),
            _ => None,
        })
        .collect()
}

/// True if `entries` hold a close of `connection`, with any cause.
fn closed(entries: &[Entry], connection: u64) -> bool {
    entries.iter().any(|entry| {
        matches!(entry.obs, Observation::ConnectionClosed { connection: c, .. } if c == connection)
    })
}

/// The subject of an obligation on `connection`.
fn session(connection: u64) -> Subject {
    Subject::Session(connection.to_string())
}

/// True if `obs` closes the obligation's connection with a fault cause.
fn fault_close(key: &ObligationKey, obs: &Observation) -> bool {
    matches!(obs, Observation::ConnectionClosed { connection, cause }
        if is_fault_cause(cause) && key.subject == session(*connection))
}

/// True if `obs` closes the obligation's connection with a fault cause, or
/// is a `Misbehavior` against it: either is a family's content consequence.
/// Which one a family owes needs its adapter (properties.md, Consequence
/// table), so this accepts both.
fn content_consequence(key: &ObligationKey, obs: &Observation) -> bool {
    fault_close(key, obs)
        || matches!(obs, Observation::Misbehavior { connection }
            if key.subject == session(*connection))
}

/// The first fault judged on the ID's stream this step that no registered
/// `<label>-closes` property claims, described. A label on a row whose role
/// can't commit it, or a missing row, would otherwise open no obligation.
fn unclaimed_fault<S: HasView>(cx: &CheckCtx<'_, S>, id: &GeneratedId<'_>) -> Option<String> {
    let entries = cx.log.entries();
    let start = entries.partition_point(|e| e.step < cx.step);
    entries[start..].iter().enumerate().find_map(|(at, entry)| {
        let Observation::FrameJudged {
            connection,
            stream,
            seq,
        } = entry.obs
        else {
            return None;
        };
        if stream != id.stream {
            return None;
        }
        let frame = sent(&entries[..start + at], connection, stream, seq).ok()?;
        if consequence(frame.label) == Consequence::NoPunishment {
            return None;
        }
        let claim = GeneratedId {
            family: id.family,
            stream: frame.rule_stream,
            row: frame.row.filter(|_| frame.label != Label::UndeclaredType),
            template: Template::Closes(frame.label),
        };
        (!cx.is_registered(&claim.to_string())).then(|| {
            format!("connection {connection} judged frame {seq} on {stream} has no {claim}")
        })
    })
}

/// `<label>-closes`: each frame with `label` whose rule is the ID's stream
/// and row (any row for `UndeclaredType`) that the node judges this step, on a connection still open, opens an
/// obligation that the label's consequence discharges: a fault close of the
/// connection, or [`content_consequence`] for `BadContent`. One discharge
/// covers every obligation on its connection.
fn check_closes<S: HasView>(
    cx: &mut CheckCtx<'_, S>,
    id: &GeneratedId<'_>,
    label: Label,
) -> Verdict {
    let entries = cx.log.entries();
    let start = entries.partition_point(|e| e.step < cx.step);
    let mut obliged = Vec::new();
    for (at, entry) in entries[start..].iter().enumerate() {
        let Observation::FrameJudged {
            connection,
            stream,
            seq,
        } = entry.obs
        else {
            continue;
        };
        let before = &entries[..start + at];
        let frame = match sent(before, connection, stream, seq) {
            Ok(frame) => frame,
            Err(missing) => {
                cx.missing_evidence(missing);
                return Verdict::NotApplicable;
            }
        };
        // `UndeclaredType` is per stream, whatever row the frame's type has
        // elsewhere.
        let row_matches = label == Label::UndeclaredType || frame.row == id.row;
        let matches = frame.rule_stream == id.stream && frame.label == label && row_matches;
        if matches && !closed(before, connection) {
            obliged.push((at, connection));
        }
    }
    if obliged.is_empty() {
        return Verdict::NotApplicable;
    }
    let discharged_by: fn(&ObligationKey, &Observation) -> bool = match consequence(label) {
        Consequence::FamilyContent => content_consequence,
        Consequence::FaultClose | Consequence::NoPunishment => fault_close,
    };
    for (at, connection) in obliged {
        let mut obligation = cx.obligation(Some(at), session(connection), discharged_by);
        obligation.cardinality = Cardinality::Every;
        cx.oblige(obligation);
    }
    Verdict::Holds
}

/// `conformant-never-closes` and `cadence-never-closes`. The property
/// applies at each step where the node judges a frame on the ID's stream
/// for which `applies` holds. At each fault close or `Misbehavior` on a
/// connection with such a frame, if every frame the node judged on that
/// connection before the punishment, on any stream, is `Conformant`, the
/// punishment is a violation. A fault on a sibling stream justifies it, but
/// only one judged before it. For `conformant-never-closes`, a punishment
/// before the node judged any frame on the connection is a violation too.
/// Every unjustified punishment at the step is a violation of its own.
fn check_never_closes<S: HasView>(
    cx: &mut CheckCtx<'_, S>,
    id: &GeneratedId<'_>,
    applies: impl Fn(&Sent<'_>) -> bool,
) -> Verdict {
    let entries = cx.log.entries();
    let start = entries.partition_point(|e| e.step < cx.step);
    let mut verdict = Verdict::NotApplicable;
    let mut unjustified = Vec::new();
    for (at, entry) in entries[start..].iter().enumerate() {
        let before = &entries[..start + at];
        let (connection, punishment) = match &entry.obs {
            Observation::FrameJudged {
                connection,
                stream,
                seq,
            } => {
                match sent(before, *connection, *stream, *seq) {
                    Ok(frame) if *stream == id.stream && applies(&frame) => {
                        verdict = Verdict::Holds;
                    }
                    Ok(_) => {}
                    Err(missing) => {
                        cx.missing_evidence(missing);
                        return Verdict::NotApplicable;
                    }
                }
                continue;
            }
            Observation::ConnectionClosed { connection, cause } if is_fault_cause(cause) => {
                (*connection, format!("a close with cause {cause}"))
            }
            Observation::Misbehavior { connection } => (*connection, "a Misbehavior".to_owned()),
            _ => continue,
        };
        let judged = match judged(before, connection) {
            Ok(judged) => judged,
            Err(missing) => {
                cx.missing_evidence(missing);
                return Verdict::NotApplicable;
            }
        };
        // A punishment before the node judged anything on the connection is
        // unjustified too: the peer had sent nothing wrong.
        let nothing_judged = judged.is_empty() && id.template == Template::ConformantNeverCloses;
        let covered = nothing_judged || judged.iter().any(|f| f.stream == id.stream && applies(f));
        if covered && judged.iter().all(|f| f.label == Label::Conformant) {
            unjustified.push((connection, punishment));
        }
    }
    let expected = |connection| {
        format!("connection {connection}'s judged frames are all conformant, so no punishment")
    };
    let mut unjustified = unjustified.into_iter();
    let Some((connection, punishment)) = unjustified.next() else {
        return verdict;
    };
    for (connection, punishment) in unjustified {
        cx.record_violation(expected(connection), punishment);
    }
    cx.violated(expected(connection), punishment)
}
