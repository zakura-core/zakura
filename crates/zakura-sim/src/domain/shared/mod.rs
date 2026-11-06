//! Properties shared across reactors (properties.md, Properties shared
//! across reactors). Each file defines one idea once. A reactor registers
//! an instance of it under its own property ID.
//!
//! `Property::check` is a `fn` pointer, so a check can't capture which
//! instance it serves. Each idea is a trait plus one generic constructor,
//! and each instance is a marker type that implements the trait for its
//! harness's view. The shared check reports through `cx.id()`, so one check
//! serves every instance's ID.

pub mod bounds;

use crate::property::{CheckCtx, HasView, Verdict};

/// A violation a model found, for one property to report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Finding {
    /// The step whose observations showed it.
    pub step: u64,
    /// What the property expected.
    pub expected: String,
    /// What the run did instead.
    pub actual: String,
}

/// Reports `findings`: the first as the verdict, the rest recorded.
pub fn report<S: HasView>(cx: &mut CheckCtx<'_, S>, findings: Vec<Finding>) -> Option<Verdict> {
    let mut findings = findings.into_iter();
    let first = findings.next()?;
    for finding in findings {
        cx.record_violation(finding.expected, finding.actual);
    }
    Some(cx.violated(first.expected, first.actual))
}
