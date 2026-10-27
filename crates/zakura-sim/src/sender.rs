//! Sender generated checks (properties.md, Generated properties, per row,
//! sender side).
//!
//! These check the node's own frames as a peer's role models labelled them
//! on arrival ([`Observation::NodeFrame`]), under the sender's rules. Each
//! template reads the frames whose rule is its row: a page's `OverCredit`
//! belongs to its subscription row, as [`crate::table::rule_row`] picks it.

use crate::{
    downstream::{Failure, Found},
    model::Label,
    observe::RowAt,
    table::Template,
    Observation,
};

/// Sender template `template`'s verdict on `obs`: `None` unless `obs` is a
/// frame the node sent whose rule is `row`.
pub(crate) fn frame(template: Template, obs: &Observation, row: &RowAt) -> Found {
    let Observation::NodeFrame {
        connection,
        seq,
        row: Some(rule_row),
        rule_stream,
        label,
        exhausted,
        ..
    } = obs
    else {
        return None;
    };
    if *rule_stream != row.stream || *rule_row != row.row {
        return None;
    }
    let broken = match template {
        Template::SenderCadence => *exhausted,
        template => Some(*label) == broken_by(template),
    };
    if !broken {
        return Some(Ok(()));
    }
    let what = format!("connection {connection}'s {row} frame {seq} from the node");
    let actual = match template {
        Template::SenderCadence => "the peer charged it to an empty cadence bucket".to_owned(),
        _ => format!("the peer's models labelled it {label}"),
    };
    Some(Err(Failure::new(
        format!("{what} is conformant under {}", template.claim()),
        actual,
    )))
}

/// The label that breaks sender template `template`, if it reads one.
fn broken_by(template: Template) -> Option<Label> {
    match template {
        Template::SenderWithinLimit => Some(Label::OverMargin),
        Template::SenderAnswersOpen | Template::SenderEndsOnce => Some(Label::Unsolicited),
        Template::SenderWithinLive => Some(Label::OverLive),
        Template::SenderWithinCredit => Some(Label::OverCredit),
        _ => None,
    }
}
