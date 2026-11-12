//! Rule 25's one negative-control template for joins: a join that holds
//! for a ref must fail for each copy of it with exactly one key moved, an
//! unrelated event matching every key but one.
//!
//! A ref's keys are the leaves of its serialized form: each number,
//! string and boolean, at any depth, so a nested owner's every field is a
//! key. A `null` names no key. The acceptance library's unit tests include
//! this file by path, so both kinds of test share one template.

use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

/// Every leaf of `value` that's a key, as its JSON pointer.
fn keys(value: &Value, at: String, out: &mut Vec<String>) {
    match value {
        Value::Object(fields) => {
            for (name, field) in fields {
                let name = name.replace('~', "~0").replace('/', "~1");
                keys(field, format!("{at}/{name}"), out);
            }
        }
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                keys(item, format!("{at}/{index}"), out);
            }
        }
        Value::Null => {}
        _ => out.push(at),
    }
}

/// The values `leaf` can move to that no real event shares with it, most
/// distant first: a number far off, then nearer for a narrow type, a
/// boolean flipped, and a string with its last character changed within
/// its class, so a hex digest stays a hex digest of its length.
fn moves(leaf: &Value) -> Vec<Value> {
    match leaf {
        Value::Bool(flag) => vec![Value::Bool(!flag)],
        Value::Number(number) => match number.as_u64() {
            Some(n) => [1_000_003, 20_011, 101, 1]
                .into_iter()
                .filter_map(|by| n.checked_add(by).or_else(|| n.checked_sub(by)))
                .chain(n.checked_sub(101))
                .map(Value::from)
                .collect(),
            None => vec![Value::from(number.as_f64().unwrap_or(0.0) + 1_000_003.0)],
        },
        Value::String(text) => {
            let mut text = text.clone();
            let last = text.pop();
            let replaced = match last {
                Some('0') => "1".to_owned(),
                Some(c) if c.is_ascii_digit() => "0".to_owned(),
                Some('a') => "b".to_owned(),
                Some(c) if c.is_ascii_hexdigit() => "a".to_owned(),
                Some('z') => "y".to_owned(),
                Some(c) if c.is_ascii_alphabetic() => "z".to_owned(),
                Some(c) => format!("{c}x"),
                None => "x".to_owned(),
            };
            text.push_str(&replaced);
            vec![Value::String(text)]
        }
        _ => Vec::new(),
    }
}

/// Each copy of `positive` with exactly one key moved, the first of its
/// [`moves`] that reads back as `R`, with that key's pointer. A key with no
/// such move panics, so no key is left without its control.
pub fn one_key_off<R: Serialize + DeserializeOwned>(positive: &R) -> Vec<(String, R)> {
    let value = serde_json::to_value(positive).expect("a ref serializes");
    let mut found = Vec::new();
    keys(&value, String::new(), &mut found);
    found
        .into_iter()
        .map(|pointer| {
            let leaf = value.pointer(&pointer).expect("the key is there");
            let off = moves(leaf)
                .into_iter()
                .find_map(|moved| {
                    let mut off = value.clone();
                    *off.pointer_mut(&pointer).expect("the key is there") = moved;
                    serde_json::from_value(off).ok()
                })
                .unwrap_or_else(|| panic!("no move of {pointer} reads back"));
            (pointer, off)
        })
        .collect()
}

/// Checks `joins` holds for `positive` and fails for each of its
/// [`one_key_off`] copies, and returns how many keys it moved, which is at
/// least one.
pub fn assert_one_key_off<R: Serialize + DeserializeOwned + std::fmt::Debug>(
    positive: &R,
    joins: impl Fn(&R) -> bool,
) -> usize {
    assert!(joins(positive), "the positive join fails: {positive:?}");
    let controls = one_key_off(positive);
    assert!(!controls.is_empty(), "{positive:?} has no key");
    for (pointer, off) in &controls {
        assert!(!joins(off), "{pointer} moved still joins: {off:?}");
    }
    controls.len()
}
