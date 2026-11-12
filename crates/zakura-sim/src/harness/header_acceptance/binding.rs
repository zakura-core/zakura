//! Rule 25's one binding check (adopted 2026-11-12).
//!
//! Every artifact the acceptance tool reads names the execution it's about:
//! the digest of the exact trace it judged, the build that wrote or judged
//! it, a rebuilt checkout's base and patch, a run's `Draft` override, the
//! run and task that wrote a task directory, a harness and an input.
//! [`check`] is the only place any of those is compared with what its
//! reader expects. Each artifact [`Kind`] declares which [`Field`]s it
//! carries, where, and which of them every reader must match. A reader
//! builds an [`Expect`] from what it knows, most often the trace beside the
//! artifact ([`Expect::of_trace`]), and never compares one of those fields
//! itself.

use std::{collections::BTreeMap, fmt::Write as _, path::Path};

use serde::Serialize;
use serde_json::Value;

use super::tool::NOT_EMBEDDED;

/// One identity field an artifact can carry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Field {
    /// The SHA-256 of the `trace.jsonl` it judged
    /// ([`Trace::id`](crate::trace::Trace::id)).
    Trace,
    /// The SHA-256 of the baseline `trace.jsonl` a planted verdict judged.
    BaselineTrace,
    /// The SHA-256 of the patched `trace.jsonl` a planted verdict judged.
    PatchedTrace,
    /// How many steps a run took.
    Steps,
    /// The digest of those steps ([`Ran`](super::history::Ran)).
    StepsDigest,
    /// The source digest of the build that wrote or judged it. Both sides
    /// must be [`known`].
    Source,
    /// The commit a checkout was made from.
    Base,
    /// The SHA-256 of the patch a checkout applied.
    PatchSha256,
    /// The run's `Draft` override.
    Draft,
    /// The acceptance run's ID, as its `run.json` names it.
    RunId,
    /// A task's index in its plan.
    TaskIndex,
    /// The task itself.
    Task,
    /// The harness a run ran on.
    Harness,
    /// The SHA-256 of the input a run ran.
    InputSha256,
    /// That input's length.
    InputLen,
    /// A planted case's property directory.
    Property,
    /// A planted case's name.
    Case,
    /// A fuzz target.
    Target,
    /// The scenario a run ran, as its harness describes it.
    Scenario,
    /// The Plan a run took its choices from, other than a fuzz input.
    Plan,
    /// Where in its task's directory the run an artifact is about sits:
    /// the run's subdirectory, such as `patched` or `draft`, or empty for
    /// the task's own.
    Role,
    /// The SHA-256 of the executable a run ran.
    Executable,
}

impl Field {
    /// What an artifact says when it carries this field, before its value.
    fn says(self) -> &'static str {
        match self {
            Field::Trace => "judged the trace",
            Field::BaselineTrace => "judged the baseline trace",
            Field::PatchedTrace => "judged the patched trace",
            Field::Steps => "took steps",
            Field::StepsDigest => "took the steps",
            Field::Source => "was built from",
            Field::Base => "was made from",
            Field::PatchSha256 => "applied the patch",
            Field::Draft => "ran the Draft override",
            Field::RunId => "names the run",
            Field::TaskIndex => "names task",
            Field::Task => "names the task",
            Field::Harness => "ran",
            Field::InputSha256 => "ran the input",
            Field::InputLen => "ran an input of length",
            Field::Property => "judges the property",
            Field::Case => "judges the case",
            Field::Target => "names the target",
            Field::Scenario => "ran the scenario",
            Field::Plan => "ran the Plan",
            Field::Role => "names the role",
            Field::Executable => "ran the executable",
        }
    }

    /// What an artifact that leaves this field out carries: no override
    /// for [`Field::Draft`], which a trace's meta omits when it's empty,
    /// and `null` for any other.
    fn absent(self) -> Value {
        match self {
            Field::Draft => Value::Array(Vec::new()),
            _ => Value::Null,
        }
    }
}

/// An artifact kind: where it carries each identity field, which of those
/// every reader must match, and whether it may hold anything else.
#[derive(Clone, Copy, Debug)]
pub struct Kind {
    /// The artifact's name in refusals, such as `witness.json`.
    pub name: &'static str,
    /// Each field it carries, by its JSON pointer.
    pub carries: &'static [(Field, &'static str)],
    /// The carried fields every reader must expect. A reader may also
    /// expect any other carried field, and expectations of fields the kind
    /// doesn't carry are ignored.
    pub required: &'static [Field],
    /// Whether the artifact is an object holding only the carried fields'
    /// top-level keys.
    pub closed: bool,
}

/// `record --witness`'s `witness.json`.
pub const WITNESS: Kind = Kind {
    name: "witness.json",
    carries: &[
        (Field::Trace, "/trace"),
        (Field::Source, "/source"),
        (Field::Draft, "/draft"),
    ],
    required: &[Field::Trace, Field::Source, Field::Draft],
    closed: false,
};

/// A `Draft` run's `companions.json`
/// ([`CompanionsRun`](super::tool::CompanionsRun)).
pub const COMPANIONS: Kind = Kind {
    name: "companions.json",
    carries: &[(Field::Trace, "/trace"), (Field::Source, "/source")],
    required: &[Field::Trace, Field::Source],
    closed: false,
};

/// A test's `test.json` ([`TestRun`](super::tool::TestRun)), which the
/// runner writes for the run, task and role it ran, with the executable it
/// ran.
pub const TEST: Kind = Kind {
    name: "test.json",
    carries: &[
        (Field::RunId, "/run_id"),
        (Field::TaskIndex, "/index"),
        (Field::Role, "/role"),
        (Field::Executable, "/executable_sha256"),
    ],
    required: &[Field::RunId, Field::TaskIndex, Field::Role],
    closed: false,
};

/// A unit witness run's `build.json` ([`UnitBuild`](super::tool::UnitBuild)),
/// which the runner writes for the run, task and role it measured. Its
/// `source` and executable are measurements, which its reader holds its
/// test to.
pub const UNIT_BUILD: Kind = Kind {
    name: "build.json",
    carries: &[
        (Field::RunId, "/run_id"),
        (Field::TaskIndex, "/index"),
        (Field::Role, "/role"),
        (Field::Base, "/base"),
        (Field::PatchSha256, "/patch_sha256"),
        (Field::Source, "/source"),
        (Field::Executable, "/executable_sha256"),
    ],
    required: &[
        Field::RunId,
        Field::TaskIndex,
        Field::Role,
        Field::Base,
        Field::PatchSha256,
    ],
    closed: false,
};

/// A fresh replay's `replay.json` ([`ReplayRun`](super::tool::ReplayRun)),
/// which the runner writes for the run, task and role whose recording it
/// replayed: that recording's trace, and the source of the build that
/// replayed it, as that build's own report names it.
pub const REPLAY: Kind = Kind {
    name: "replay.json",
    carries: &[
        (Field::RunId, "/run_id"),
        (Field::TaskIndex, "/index"),
        (Field::Role, "/role"),
        (Field::Trace, "/trace"),
        (Field::Source, "/source"),
    ],
    required: &[
        Field::RunId,
        Field::TaskIndex,
        Field::Role,
        Field::Trace,
        Field::Source,
    ],
    closed: false,
};

/// A recording's or a fresh replay's `full-report.json` (C8).
pub const FULL_REPORT: Kind = Kind {
    name: "full-report.json",
    carries: &[(Field::Trace, "/trace"), (Field::Source, "/source")],
    required: &[Field::Trace, Field::Source],
    closed: false,
};

/// A fresh replay's log, as [`replay_claims`] reads each line that says
/// the replay matched.
pub const REPLAY_LOG: Kind = Kind {
    name: "the replay's log",
    carries: &[(Field::Trace, "/trace")],
    required: &[Field::Trace],
    closed: true,
};

/// A declaration's `ran.json` ([`Ran`](super::history::Ran)).
pub const RAN: Kind = Kind {
    name: "ran.json",
    carries: &[(Field::Steps, "/steps"), (Field::StepsDigest, "/digest")],
    required: &[Field::Steps, Field::StepsDigest],
    closed: true,
};

/// A planted case's `verdict.json` ([`VerdictCheck`](super::tool::VerdictCheck)).
pub const VERDICT: Kind = Kind {
    name: "verdict.json",
    carries: &[
        (Field::Property, "/property"),
        (Field::Case, "/name"),
        (Field::BaselineTrace, "/baseline"),
        (Field::PatchedTrace, "/patched"),
    ],
    required: &[
        Field::Property,
        Field::Case,
        Field::BaselineTrace,
        Field::PatchedTrace,
    ],
    closed: false,
};

/// A planted case's `target.json` ([`TargetCheck`](super::tool::TargetCheck)).
/// Its `trace` and `input_sha256` are `null` with no target.
pub const TARGET: Kind = Kind {
    name: "target.json",
    carries: &[
        (Field::Property, "/property"),
        (Field::Case, "/name"),
        (Field::Target, "/target"),
        (Field::Source, "/source"),
        (Field::InputSha256, "/input_sha256"),
        (Field::Trace, "/trace"),
    ],
    required: &[
        Field::Property,
        Field::Case,
        Field::Target,
        Field::Source,
        Field::InputSha256,
        Field::Trace,
    ],
    closed: false,
};

/// An integrity mutation's `patched/build.json`
/// ([`PatchedBuild`](super::tool::PatchedBuild)). Its `source` is the
/// measurement other artifacts must match, so no reader needs to expect it.
pub const PATCHED_BUILD: Kind = Kind {
    name: "build.json",
    carries: &[
        (Field::Base, "/base"),
        (Field::PatchSha256, "/patch_sha256"),
        (Field::Source, "/source"),
    ],
    required: &[Field::Base, Field::PatchSha256],
    closed: false,
};

/// A planted case's `provenance.json`
/// ([`ProvenanceCheck`](super::tool::ProvenanceCheck)), whose `source` is a
/// measurement as [`PATCHED_BUILD`]'s is.
pub const PROVENANCE: Kind = Kind {
    name: "provenance.json",
    carries: &[
        (Field::Base, "/base"),
        (Field::PatchSha256, "/patch_sha256"),
        (Field::Source, "/source"),
    ],
    required: &[Field::Base, Field::PatchSha256],
    closed: false,
};

/// A task directory's `task.json` ([`task_binding`](super::tool::task_binding)).
pub const TASK: Kind = Kind {
    name: "task.json",
    carries: &[
        (Field::RunId, "/run_id"),
        (Field::TaskIndex, "/index"),
        (Field::Task, "/task"),
    ],
    required: &[Field::RunId, Field::TaskIndex, Field::Task],
    closed: true,
};

/// A trace's own `meta` line, which names the build, harness, override,
/// scenario and input or Plan its run used. Readers expect what they know of the run.
pub const TRACE_META: Kind = Kind {
    name: "its trace",
    carries: &[
        (Field::Source, "/build/source"),
        (Field::Harness, "/sim"),
        (Field::Draft, "/draft"),
        (Field::InputSha256, "/input/sha256"),
        (Field::InputLen, "/input/len"),
        (Field::Scenario, "/scenario"),
        (Field::Plan, "/plan"),
    ],
    required: &[],
    closed: false,
};

/// What a reader expects an artifact to carry, by field. Expecting two
/// values for one field, or one the reader couldn't read, refuses every
/// artifact that carries it.
#[derive(Clone, Debug, Default)]
pub struct Expect {
    fields: BTreeMap<Field, Result<Value, String>>,
}

impl Expect {
    /// An expectation of nothing.
    pub fn new() -> Expect {
        Expect::default()
    }

    /// This expectation, also expecting `field` to be `value`. If it already
    /// expects another value there, no value satisfies it.
    pub fn with(mut self, field: Field, value: impl Serialize) -> Expect {
        let value = serde_json::to_value(value).expect("an expected value is plain data");
        let entry = match self.fields.remove(&field) {
            Some(Ok(old)) if old != value => Err(format!(
                "its reader expects both {} and {} there",
                shown(Some(&old)),
                shown(Some(&value))
            )),
            Some(old) => old,
            None => Ok(value),
        };
        self.fields.insert(field, entry);
        self
    }

    /// This expectation, refusing any `field` because the reader couldn't
    /// learn what it must be, for `why`.
    pub fn unknown(mut self, field: Field, why: impl Into<String>) -> Expect {
        self.fields.insert(field, Err(why.into()));
        self
    }

    /// `with(field, value)` if `value` is `Some`, else [`Expect::unknown`]
    /// for `why`.
    pub fn with_read(self, field: Field, value: Option<impl Serialize>, why: &str) -> Expect {
        match value {
            Some(value) => self.with(field, value),
            None => self.unknown(field, why),
        }
    }

    /// What an artifact judging the recorded run in `dir` must carry: its
    /// trace's digest, the build its meta records, and its harness and
    /// `Draft` override, read by the simulator's own trace reader, which
    /// checks the trace whole.
    pub fn of_trace(dir: &Path) -> Result<Expect, String> {
        let trace = crate::trace::TraceReader::open(dir)
            .map_err(|error| format!("{}: {error}", dir.display()))?;
        Ok(Expect::new()
            .with(Field::Trace, &trace.id)
            .with(Field::Source, &trace.meta.build.source)
            .with(Field::Harness, &trace.meta.sim)
            .with(Field::Draft, &trace.meta.draft))
    }
}

/// Whether `source` is a digest at all: not missing, empty, `unknown` or
/// [`NOT_EMBEDDED`], none of which names a source, so two of them never
/// match.
pub fn known(source: Option<&str>) -> bool {
    source.is_some_and(|source| !source.is_empty() && source != "unknown" && source != NOT_EMBEDDED)
}

/// `value` as a refusal shows it: a string as itself, anything else as
/// JSON, and a missing one as `nothing`.
fn shown(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(text)) => text.clone(),
        Some(other) => other.to_string(),
        None => "nothing".to_owned(),
    }
}

/// Why `artifact`, of `kind`, isn't bound to what `expect` names, or `Ok`
/// if it is: the one comparison of every identity field any artifact
/// carries. Each field `kind` requires must be expected, and each carried
/// field that's expected must equal the expectation, a [`Field::Source`]
/// on both sides a [`known`] one. A field the artifact leaves out is
/// [`Field::absent`]'s value. A closed kind holds nothing else.
pub fn check(kind: &Kind, artifact: &Value, expect: &Expect) -> Result<(), String> {
    if kind.closed {
        let carried: Vec<&str> = kind
            .carries
            .iter()
            .filter_map(|(_, pointer)| pointer.split('/').nth(1))
            .collect();
        let held = artifact
            .as_object()
            .ok_or_else(|| format!("{} isn't an object", kind.name))?;
        if let Some(other) = held.keys().find(|key| !carried.contains(&key.as_str())) {
            return Err(format!(
                "{} holds {other}, which it doesn't carry",
                kind.name
            ));
        }
    }
    for &(field, pointer) in kind.carries {
        let want = match expect.fields.get(&field) {
            None if kind.required.contains(&field) => {
                return Err(format!(
                    "{}'s reader expects no {field:?}, which it must match",
                    kind.name
                ))
            }
            None => continue,
            Some(Err(why)) => return Err(format!("{}: {why}", kind.name)),
            Some(Ok(want)) => want,
        };
        let absent = field.absent();
        let found = artifact.pointer(pointer).unwrap_or(&absent);
        let mut why = String::new();
        if field == Field::Source {
            if !known(want.as_str()) {
                let _ = write!(why, " (its reader knows no source)");
            } else if !known(found.as_str()) {
                let _ = write!(why, " (which names no source)");
            }
        }
        if found != want || !why.is_empty() {
            return Err(format!(
                "{} {} {}, not {}{why}",
                kind.name,
                field.says(),
                shown(Some(found)),
                shown(Some(want))
            ));
        }
    }
    Ok(())
}

/// The JSON artifact of `kind` at `path`, if it's readable and bound to
/// what `expect` names ([`check`]), or why not.
pub fn read(kind: &Kind, path: &Path, expect: &Expect) -> Result<Value, String> {
    let artifact: Value = std::fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .ok_or_else(|| format!("no readable {}", kind.name))?;
    check(kind, &artifact, expect)?;
    Ok(artifact)
}

/// The `meta` line of the trace in `dir`, read without checking the rest
/// of the trace, so a reader can bind a run whose trace it doesn't judge
/// whole, or why it can't be read.
pub fn trace_meta(dir: &Path) -> Result<Value, String> {
    let path = dir.join("trace.jsonl");
    let text =
        std::fs::read_to_string(&path).map_err(|error| format!("{}: {error}", path.display()))?;
    text.lines()
        .next()
        .and_then(|line| serde_json::from_str::<Value>(line).ok())
        .and_then(|first| first.get("meta").cloned())
        .ok_or_else(|| format!("{}: no meta line", path.display()))
}

/// [`check`] of [`TRACE_META`] for the trace in `dir`'s `meta` line
/// ([`trace_meta`]).
pub fn check_trace(dir: &Path, expect: &Expect) -> Result<(), String> {
    check(&TRACE_META, &trace_meta(dir)?, expect)
}

/// The claim each line of a replay's `log` that says it matched makes, as
/// [`REPLAY_LOG`] carries it: `replayed trace <trace>: ...; every pick,
/// draw and observation matched` names `<trace>`.
pub fn replay_claims(log: &str) -> Vec<Value> {
    log.lines()
        .filter(|line| line.ends_with("every pick, draw and observation matched"))
        .filter_map(|line| line.strip_prefix("replayed trace "))
        .filter_map(|rest| rest.split_once(": "))
        .map(|(trace, _)| serde_json::json!({ "trace": trace }))
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// A witness file binds only to the trace, build and override its
    /// reader expects: every required field present and equal, a source a
    /// known digest. Each field off, missing or unexpected refuses it, and
    /// so do two expectations of one field.
    #[test]
    fn an_artifact_binds_only_to_its_expected_identity() {
        let expect = Expect::new()
            .with(Field::Trace, "t")
            .with(Field::Source, "s")
            .with(Field::Draft, ["d"])
            .with(Field::Harness, "h");
        let witness = json!({ "trace": "t", "source": "s", "draft": ["d"], "passed": true });
        assert_eq!(check(&WITNESS, &witness, &expect), Ok(()));
        for (key, value) in [
            ("trace", json!("u")),
            ("source", json!("r")),
            ("draft", json!([])),
            ("source", json!("unknown")),
            ("source", json!(NOT_EMBEDDED)),
        ] {
            let mut off = witness.clone();
            off[key] = value;
            assert!(check(&WITNESS, &off, &expect).is_err(), "{off}");
        }
        for key in ["trace", "source", "draft"] {
            let mut missing = witness.clone();
            missing.as_object_mut().unwrap().remove(key);
            assert!(check(&WITNESS, &missing, &expect).is_err(), "{key}");
        }
        let unknown = Expect::new()
            .with(Field::Trace, "unknown")
            .with(Field::Source, "unknown")
            .with(Field::Draft, ["d"]);
        let both_unknown = json!({ "trace": "unknown", "source": "unknown", "draft": ["d"] });
        assert!(check(&WITNESS, &both_unknown, &unknown).is_err());
        assert!(check(&WITNESS, &witness, &Expect::new().with(Field::Trace, "t")).is_err());
        let twice = expect.clone().with(Field::Source, "r");
        assert!(check(&WITNESS, &witness, &twice).is_err());
        let unread = expect.unknown(Field::Trace, "its trace can't be read");
        assert!(check(&WITNESS, &witness, &unread).is_err());
    }

    /// A closed kind holds nothing but what it carries, so a task binding
    /// with an extra key is refused.
    #[test]
    fn a_closed_artifact_holds_only_its_fields() {
        let expect = Expect::new()
            .with(Field::RunId, "r")
            .with(Field::TaskIndex, 3)
            .with(Field::Task, json!({ "test": {} }));
        let task = json!({ "run_id": "r", "index": 3, "task": { "test": {} } });
        assert_eq!(check(&TASK, &task, &expect), Ok(()));
        let mut extra = task.clone();
        extra["also"] = json!(1);
        assert!(check(&TASK, &extra, &expect).is_err());
        assert!(check(&TASK, &json!([]), &expect).is_err());
    }

    /// Each line of a replay's log that says it matched claims its trace,
    /// and no other line does.
    #[test]
    fn a_replay_log_claims_the_traces_it_matched() {
        let log =
            "replayed trace ab: idle after 3 steps; every pick, draw and observation matched\n\
                   replayed trace cd b: idle; 1 records differed\n\
                   replayed trace ef with --prefix: idle; everything matched, but a";
        assert_eq!(replay_claims(log), [json!({ "trace": "ab" })]);
    }
}
