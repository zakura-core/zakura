//! Rendering metrics in the Prometheus text format.
//!
//! The sanitizing rules are the same as `metrics-exporter-prometheus` 0.17, so metric names,
//! labels, and descriptions don't change.

use std::{collections::BTreeMap, fmt::Display, sync::atomic::Ordering, time::Instant};

use metrics::Key;

use super::{
    recorder::{read, Metrics},
    summary::QUANTILES,
};

/// Renders `metrics`, with histogram quantiles as of `now`.
///
/// Counters come first, then gauges, then histograms, which are rendered as summaries. Each
/// metric name gets one `# TYPE` line, and a `# HELP` line if it has a description. Metrics
/// whose keys sanitize to the same name and labels are rendered once.
pub(super) fn render(metrics: &Metrics, now: Instant) -> String {
    let descriptions = read(&metrics.descriptions);
    let mut output = String::new();

    let counters = by_name(
        read(&metrics.counters)
            .iter()
            .map(|(key, counter)| (key, counter.load(Ordering::Acquire))),
    );
    for (name, series) in counters {
        write_header(&mut output, &name, "counter", descriptions.get(&name));
        for (labels, value) in series {
            write_line(&mut output, &name, "", &labels, None, value);
        }
        output.push('\n');
    }

    let gauges = by_name(
        read(&metrics.gauges)
            .iter()
            .map(|(key, gauge)| (key, f64::from_bits(gauge.load(Ordering::Acquire)))),
    );
    for (name, series) in gauges {
        write_header(&mut output, &name, "gauge", descriptions.get(&name));
        for (labels, value) in series {
            write_line(&mut output, &name, "", &labels, None, value);
        }
        output.push('\n');
    }

    let histograms = by_name(read(&metrics.histograms).iter().map(|(key, samples)| {
        let summary =
            samples.with_summary(|summary| (summary.recent(now), summary.sum(), summary.count()));
        (key, summary)
    }));
    for (name, series) in histograms {
        write_header(&mut output, &name, "summary", descriptions.get(&name));
        for (labels, (recent, sum, count)) in series {
            for quantile in QUANTILES {
                let value = recent.quantile(quantile).unwrap_or(0.0);
                write_line(&mut output, &name, "", &labels, Some(quantile), value);
            }
            write_line(&mut output, &name, "_sum", &labels, None, sum);
            write_line(&mut output, &name, "_count", &labels, None, count);
        }
        output.push('\n');
    }

    output
}

/// Groups `metrics` by sanitized name, then by rendered labels.
fn by_name<'a, T>(
    metrics: impl Iterator<Item = (&'a Key, T)>,
) -> BTreeMap<String, BTreeMap<String, T>> {
    let mut by_name: BTreeMap<String, BTreeMap<String, T>> = BTreeMap::new();
    for (key, value) in metrics {
        by_name
            .entry(sanitize_metric_name(key.name()))
            .or_default()
            .insert(render_labels(key), value);
    }
    by_name
}

/// Returns the labels of `key` as comma-separated `name="value"` pairs.
///
/// If a label name appears more than once, the last value is used, at the first position.
fn render_labels(key: &Key) -> String {
    let mut labels: Vec<(&str, &str)> = Vec::new();
    for label in key.labels() {
        match labels.iter_mut().find(|(name, _)| *name == label.key()) {
            Some((_, value)) => *value = label.value(),
            None => labels.push((label.key(), label.value())),
        }
    }

    labels
        .iter()
        .map(|(name, value)| {
            format!(
                "{}=\"{}\"",
                sanitize_label_key(name),
                sanitize_label_value(value)
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Writes the `# HELP` line, if there is a `description`, and the `# TYPE` line for `name`.
fn write_header(
    output: &mut String,
    name: &str,
    kind: &str,
    description: Option<&impl AsRef<str>>,
) {
    if let Some(description) = description {
        output.push_str("# HELP ");
        output.push_str(name);
        output.push(' ');
        output.push_str(&sanitize_description(description.as_ref()));
        output.push('\n');
    }
    output.push_str("# TYPE ");
    output.push_str(name);
    output.push(' ');
    output.push_str(kind);
    output.push('\n');
}

/// Writes one sample of `name` with `suffix`, its `labels`, and a `quantile` label if it is set.
fn write_line(
    output: &mut String,
    name: &str,
    suffix: &str,
    labels: &str,
    quantile: Option<f64>,
    value: impl Display,
) {
    output.push_str(name);
    output.push_str(suffix);
    if !labels.is_empty() || quantile.is_some() {
        output.push('{');
        output.push_str(labels);
        if let Some(quantile) = quantile {
            if !labels.is_empty() {
                output.push(',');
            }
            output.push_str(&format!("quantile=\"{quantile}\""));
        }
        output.push('}');
    }
    output.push(' ');
    output.push_str(&value.to_string());
    output.push('\n');
}

/// Replaces characters that metric names can't contain with `_`.
///
/// The first character must match `[a-zA-Z_:]`, and the others `[a-zA-Z0-9_:]`.
pub(super) fn sanitize_metric_name(name: &str) -> String {
    sanitize_name(
        name,
        |c| c.is_ascii_alphabetic() || c == '_' || c == ':',
        |c| c.is_ascii_alphanumeric() || c == '_' || c == ':',
    )
}

/// Replaces characters that label names can't contain with `_`.
///
/// The first character must match `[a-zA-Z_]`, and the others `[a-zA-Z0-9_]`.
pub(super) fn sanitize_label_key(key: &str) -> String {
    sanitize_name(
        key,
        |c| c.is_ascii_alphabetic() || c == '_',
        |c| c.is_ascii_alphanumeric() || c == '_',
    )
}

/// Replaces each character of `name` that is invalid with `_`, using `valid_first` for the first
/// character, and `valid` for the others.
fn sanitize_name(name: &str, valid_first: fn(char) -> bool, valid: fn(char) -> bool) -> String {
    name.chars()
        .enumerate()
        .map(|(i, c)| {
            let is_valid = if i == 0 { valid_first(c) } else { valid(c) };
            if is_valid {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Escapes backslashes, double quotes, and line feeds in a label value.
pub(super) fn sanitize_label_value(value: &str) -> String {
    escape(value, true)
}

/// Escapes backslashes and line feeds in a description.
pub(super) fn sanitize_description(description: &str) -> String {
    escape(description, false)
}

/// Escapes backslashes, line feeds, and double quotes if `escape_quotes` is set.
///
/// A backslash that is already escaped is kept as it is.
fn escape(value: &str, escape_quotes: bool) -> String {
    let mut escaped = String::with_capacity(value.len());
    let mut previous_backslash = false;
    for c in value.chars() {
        match c {
            '\n' => escaped.push_str("\\n"),
            '"' if escape_quotes => {
                previous_backslash = false;
                escaped.push_str("\\\"");
            }
            '\\' => {
                // A second backslash completes an escaped backslash. A first one is held until
                // the next character shows whether it was already escaped.
                if previous_backslash {
                    escaped.push_str("\\\\");
                }
                previous_backslash = !previous_backslash;
            }
            c => {
                if previous_backslash {
                    previous_backslash = false;
                    escaped.push_str("\\\\");
                }
                escaped.push(c);
            }
        }
    }
    if previous_backslash {
        escaped.push_str("\\\\");
    }
    escaped
}
