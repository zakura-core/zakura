//! Indicatif-backed progress reporting.

use std::{collections::HashMap, time::Duration};

use howudoin::{
    report::{Message, Report, State},
    Consume, Controller, Id,
};
use indicatif::{HumanDuration, MultiProgress, ProgressBar, ProgressStyle};

/// A terminal progress consumer.
//
// Adapted from howudoin 0.1.2's MIT-licensed TermLine consumer so Zakura can
// use the maintained Indicatif release independently of howudoin's UI feature.
pub struct TermLine {
    debounce: Duration,
    bars: HashMap<Id, ProgressBar>,
    progress: MultiProgress,
}

impl TermLine {
    /// Creates a terminal consumer with the specified debounce duration.
    pub fn with_debounce(debounce: Duration) -> Self {
        Self {
            debounce,
            bars: HashMap::new(),
            progress: MultiProgress::new(),
        }
    }

    fn add_bar(&mut self, id: Id, parent: Option<Id>) -> ProgressBar {
        let bar = match parent.and_then(|parent| self.bars.get(&parent)) {
            Some(parent) => self.progress.insert_after(parent, progress_bar()),
            None => self.progress.add(progress_bar()),
        };

        self.bars.insert(id, bar.clone());
        bar
    }
}

impl Consume for TermLine {
    fn debounce(&self) -> Duration {
        self.debounce
    }

    fn rpt(&mut self, report: &Report, id: Id, parent: Option<Id>, _: &Controller) {
        let bar = self
            .bars
            .get(&id)
            .cloned()
            .unwrap_or_else(|| self.add_bar(id, parent));
        update_bar(&bar, report);
    }

    fn closed(&mut self, id: Id) {
        if let Some(bar) = self.bars.remove(&id) {
            bar.finish_and_clear();
            self.progress.remove(&bar);
        }
    }
}

fn update_bar(bar: &ProgressBar, report: &Report) {
    let Report {
        label,
        desc,
        state,
        accums,
    } = report;

    bar.set_prefix(label.clone());
    bar.set_message(desc.clone());

    match state {
        State::InProgress {
            len,
            pos,
            bytes,
            remaining: _,
        } => {
            bar.set_length(len.unwrap_or(u64::MAX));
            bar.set_position(*pos);
            if len.is_some() {
                bar.set_style(bar_style(*bytes));
            } else {
                bar.set_style(spinner_style(*bytes));
            }
        }
        State::Completed { duration } => {
            bar.finish_with_message(format!(
                "finished in {}",
                HumanDuration(Duration::try_from_secs_f32(*duration).unwrap_or_default())
            ));
        }
        State::Cancelled => bar.abandon_with_message("cancelled"),
    }

    for Message { severity, msg } in accums {
        bar.println(format!("{severity}: {msg}"));
    }
}

fn progress_bar() -> ProgressBar {
    let bar = ProgressBar::hidden().with_style(spinner_style(false));
    bar.enable_steady_tick(Duration::from_millis(250));
    bar
}

fn spinner_style(format_bytes: bool) -> ProgressStyle {
    let template = if format_bytes {
        format!(" {SPINNER} {PREFIX}: {BYTES} {BYTES_PER_SEC} {MSG}")
    } else {
        format!(" {SPINNER} {PREFIX}: {POS} {MSG}")
    };

    ProgressStyle::default_bar()
        .template(&template)
        .expect("progress template is a fixed valid string")
        .progress_chars("=> ")
        .tick_chars(r#"|/-\|"#)
}

fn bar_style(format_bytes: bool) -> ProgressStyle {
    let template = if format_bytes {
        format!(
            " {SPINNER} {PREFIX} {BYTES_PER_SEC} {ETA}\n {BAR} {PCT} ({BYTES}/{BYTES_TOTAL}) {MSG}"
        )
    } else {
        format!(" {SPINNER} {PREFIX} {ETA}\n {BAR} {PCT} ({POS}/{LEN}) {MSG}")
    };

    ProgressStyle::default_bar()
        .template(&template)
        .expect("progress template is a fixed valid string")
        .progress_chars("=> ")
        .tick_chars(r#"|/-\|"#)
}

const SPINNER: &str = "{spinner:.red.bold}";
const PREFIX: &str = "{prefix:.cyan.bold}";
const BYTES: &str = "{bytes}";
const BYTES_TOTAL: &str = "{total_bytes}";
const BYTES_PER_SEC: &str = "<{binary_bytes_per_sec:.yellow.bold}>";
const POS: &str = "{pos}";
const LEN: &str = "{len}";
const ETA: &str = "({eta:.green.bold.italic})";
const BAR: &str = "[{bar:30}]";
const PCT: &str = "{percent:>03}%";
const MSG: &str = "{wide_msg:.cyan}";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_consumer_tracks_bar_lifecycle() {
        let debounce = Duration::from_secs(2);
        let mut consumer = TermLine::with_debounce(debounce);

        let parent = consumer.add_bar(1, None);
        update_bar(&parent, &Report::default());
        consumer.add_bar(2, Some(1));

        assert_eq!(consumer.debounce(), debounce);
        assert_eq!(consumer.bars.len(), 2);

        consumer.closed(2);
        consumer.closed(1);
        assert!(consumer.bars.is_empty());
    }
}
