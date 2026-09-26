//! Conservative joins between sampled CPU and disjoint, explicitly entered contexts.
use anyhow::{ensure, Result};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::collections::BTreeMap;

const MAX_INTERVALS: usize = 65_536;

#[derive(Debug)]
struct Interval {
    start: u64,
    end: u64,
    span: u64,
}

fn matching_span(intervals: &[Interval], at: u64, error: u64) -> Option<u64> {
    let i = intervals
        .partition_point(|entry| entry.start <= at)
        .checked_sub(1)?;
    let entry = &intervals[i];
    (at.checked_sub(error)? >= entry.start && at.checked_add(error)? < entry.end)
        .then_some(entry.span)
}

fn path(spans: &BTreeMap<u64, &Value>, mut id: u64) -> Option<Vec<u64>> {
    let mut result = Vec::new();
    while id != 0 {
        if result.len() >= 64 {
            return None;
        }
        result.push(id);
        let parent = spans.get(&id)?["parent"].as_u64()?;
        if parent >= id {
            return None;
        }
        id = parent;
    }
    result.push(0);
    result.reverse();
    Some(result)
}

pub(crate) fn associate(
    db: &Connection,
    run: &str,
    attempt: u64,
    detail: &Value,
    cpu: &mut Value,
) -> Result<()> {
    let mut stmt = db.prepare("SELECT thread,start_us,end_us,span FROM executions WHERE run=? AND attempt=? ORDER BY thread,start_us LIMIT 65537")?;
    let rows = stmt
        .query_map(params![run, i64::try_from(attempt)?], |r| {
            Ok((
                u64::try_from(r.get::<_, i64>(0)?).map_err(|_| rusqlite::Error::InvalidQuery)?,
                Interval {
                    start: u64::try_from(r.get::<_, i64>(1)?)
                        .map_err(|_| rusqlite::Error::InvalidQuery)?,
                    end: u64::try_from(r.get::<_, i64>(2)?)
                        .map_err(|_| rusqlite::Error::InvalidQuery)?,
                    span: u64::try_from(r.get::<_, i64>(3)?)
                        .map_err(|_| rusqlite::Error::InvalidQuery)?,
                },
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let limited = rows.len() > MAX_INTERVALS;
    let available = !rows.is_empty();
    let mut threads = BTreeMap::<u64, Vec<Interval>>::new();
    if !limited {
        for (tid, entry) in rows {
            threads.entry(tid).or_default().push(entry);
        }
    }
    // Unexpected overlap is not resolved by guessing which context owns the thread.
    threads.retain(|_, list| !list.windows(2).any(|pair| pair[0].end > pair[1].start));
    let spans: BTreeMap<_, _> = detail["spans"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|s| s["span"].as_u64().map(|id| (id, s)))
        .collect();
    let paths: BTreeMap<_, _> = spans
        .keys()
        .copied()
        .chain([0])
        .filter_map(|id| path(&spans, id).map(|p| (id, p)))
        .collect();
    let start = detail["summary"]["start_us"].as_u64().unwrap_or(0);
    let error = cpu["coverage"]["clock_error_us"]
        .as_u64()
        .unwrap_or(u64::MAX);
    let mut associated = 0;
    if let Some(samples) = cpu["samples"].as_array_mut() {
        for sample in samples {
            let matched = sample["tid"]
                .as_u64()
                .zip(sample["at_us"].as_u64())
                .and_then(|(tid, offset)| {
                    matching_span(threads.get(&tid)?, start.checked_add(offset)?, error)
                })
                .and_then(|id| paths.get(&id));
            sample["context"] = matched.map_or(Value::Null, |p| json!(p));
            associated += usize::from(matched.is_some());
        }
    }
    let total = cpu["samples"].as_array().map_or(0, Vec::len);
    let mut contexts = vec![
        json!({"span":0,"label":format!("Block {} work",detail["summary"]["height"]),"parent":null}),
    ];
    for (id, span) in &spans {
        let stage = span["stage"].as_str().unwrap_or("stage");
        // Membership records describe shared work, not an entered CPU context.
        if stage.starts_with("verification_") {
            continue;
        }
        let label = if stage == "transaction" {
            span["transaction_index"]
                .as_u64()
                .map(|i| format!("Transaction {}", i + 1))
                .unwrap_or_else(|| "Transaction".into())
        } else {
            stage.replace('_', " ")
        };
        contexts.push(json!({"span":id,"parent":span["parent"],"label":label}));
    }
    cpu["attribution"] = json!({"available":available,"associated_samples":associated,"unassigned_samples":total-associated,"query_limited":limited,"contexts":contexts,
        "reason":"Recorded active context at the sample timestamp. Unassigned samples may include uninstrumented or shared work, other tasks, lost detail, and boundary uncertainty. CPU weights remain estimates."});
    Ok(())
}

pub(crate) fn select(cpu: &mut Value, view: &str, span: Option<u64>) -> Result<()> {
    ensure!(
        matches!(view, "context" | "raw" | "unassigned"),
        "invalid CPU view"
    );
    if let Some(span) = span {
        ensure!(span <= 65_536, "invalid CPU span");
    }
    cpu["view"] = json!(view);
    cpu["selected_span"] = json!(span);
    if let Some(samples) = cpu["samples"].as_array_mut() {
        samples.retain(|sample| {
            let context = sample["context"].as_array();
            (view != "unassigned" || context.is_none())
                && span.is_none_or(|id| {
                    context.is_some_and(|p| p.iter().any(|v| v.as_u64() == Some(id)))
                })
        });
        let count = samples.len();
        let total = samples.iter().try_fold(0u64, |sum, sample| {
            sum.checked_add(sample["cpu_period_ns"].as_u64()?)
        });
        cpu["selected_samples"] = json!(count);
        cpu["window_weight"] = cpu["weight"].clone();
        if cpu["weight"].is_object() {
            cpu["weight"]["estimated_cpu_ns"] = json!(total);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn gaps_and_boundaries_never_inherit_the_previous_context() {
        let entries = [
            Interval {
                start: 10,
                end: 20,
                span: 1,
            },
            Interval {
                start: 30,
                end: 50,
                span: 2,
            },
        ];
        assert_eq!(matching_span(&entries, 15, 2), Some(1));
        for at in [0, 10, 19, 20, 25, 30, 49, 50] {
            assert_eq!(matching_span(&entries, at, 2), None);
        }
        assert_eq!(matching_span(&entries, 35, 2), Some(2));
    }
    #[test]
    fn incomplete_ancestry_is_not_invented() {
        let root = json!({"parent":0});
        let child = json!({"parent":1});
        let spans = BTreeMap::from([(1, &root), (2, &child)]);
        assert_eq!(path(&spans, 2), Some(vec![0, 1, 2]));
        assert_eq!(path(&spans, 3), None);
    }
    #[test]
    fn filtering_keeps_descendants_once_and_unassigned_separate() {
        let original = json!({"samples":[{"context":[0,1,2]},{"context":[0,3]},{"context":null}]});
        let mut data = original.clone();
        select(&mut data, "context", Some(1)).unwrap();
        assert_eq!(data["samples"].as_array().unwrap().len(), 1);
        let mut data = original;
        select(&mut data, "unassigned", None).unwrap();
        assert_eq!(data["samples"], json!([{"context":null}]));
    }
    #[test]
    fn persisted_contexts_are_exclusive_and_do_not_attribute_gaps_or_other_threads() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch(include_str!("schema.sql"))?;
        db.execute(
            "INSERT INTO runs(id,metadata,utc_ms,seen_ms) VALUES('run','{}',0,0)",
            [],
        )?;
        db.execute("INSERT INTO attempts(run,attempt) VALUES('run',1)", [])?;
        for (span, start, end) in [(1, 110, 150), (2, 160, 200), (1, 220, 260)] {
            db.execute("INSERT INTO executions(run,attempt,span,thread,start_us,end_us) VALUES('run',1,?,7,?,?)",params![span,start,end])?;
        }
        let detail = json!({"summary":{"start_us":100,"height":1},"spans":[{"span":1,"parent":0,"stage":"finalization"},{"span":2,"parent":1,"stage":"finalize_forks"}]});
        let mut cpu = json!({"coverage":{"clock_error_us":2},"samples":[
            {"tid":7,"at_us":20},{"tid":7,"at_us":50},{"tid":7,"at_us":70},
            {"tid":7,"at_us":110},{"tid":8,"at_us":70},{"tid":7,"at_us":61}
        ]});
        associate(&db, "run", 1, &detail, &mut cpu)?;
        assert_eq!(cpu["attribution"]["associated_samples"], 2);
        assert_eq!(cpu["samples"][0]["context"], json!([0, 1]));
        assert_eq!(cpu["samples"][2]["context"], json!([0, 1, 2]));
        assert_eq!(cpu["attribution"]["unassigned_samples"], 4);
        let mut selected = cpu.clone();
        select(&mut selected, "context", Some(1))?;
        assert_eq!(selected["selected_samples"], 2);
        // Missing ancestry does not get replaced with a plausible stage name.
        let missing = json!({"summary":{"start_us":100},"spans":[]});
        associate(&db, "run", 1, &missing, &mut cpu)?;
        assert_eq!(cpu["attribution"]["associated_samples"], 0);
        // Unexpected overlapping intervals fail closed for the affected thread.
        db.execute("INSERT INTO executions(run,attempt,span,thread,start_us,end_us) VALUES('run',1,1,7,120,240)",[])?;
        associate(&db, "run", 1, &detail, &mut cpu)?;
        assert_eq!(cpu["attribution"]["associated_samples"], 0);
        db.execute("DELETE FROM attempts WHERE run='run'", [])?;
        assert_eq!(
            db.query_row("SELECT count(*) FROM executions", [], |r| r
                .get::<_, i64>(0))?,
            0
        );
        Ok(())
    }
}
