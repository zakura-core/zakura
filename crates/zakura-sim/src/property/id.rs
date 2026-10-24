//! Property IDs and their grammar (properties.md, ID grammar).

use std::{borrow::Cow, fmt};

/// A property's stable ID, such as `header-sync/s5v9/conformant-never-closes`.
///
/// An ID is two or more `/`-separated segments, `<family>/<scope>/<claim>`
/// with the scope left out for domain properties. Each segment is lowercase
/// ASCII letters and digits in words joined by single hyphens, so `:` never
/// appears.
///
/// A domain ID is checked when it's built, and a bad one fails const
/// evaluation:
///
/// ```
/// use zakura_sim::property::PropertyId;
///
/// const LIVELOCK: PropertyId = PropertyId::new("sim/no-livelock");
/// assert_eq!(LIVELOCK.as_str(), "sim/no-livelock");
/// ```
///
/// ```compile_fail,E0080
/// use zakura_sim::property::PropertyId;
///
/// const BAD: PropertyId = PropertyId::new("Sim:no-livelock");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PropertyId(Cow<'static, str>);

impl PropertyId {
    /// A domain ID.
    ///
    /// # Panics
    ///
    /// If `id` breaks the grammar. In a `const`, that fails the build.
    pub const fn new(id: &'static str) -> Self {
        assert!(valid(id.as_bytes()), "a property ID breaks the grammar");
        PropertyId(Cow::Borrowed(id))
    }

    /// An ID built at run time, such as one generated from a message table.
    pub fn generated(id: String) -> Result<Self, BadId> {
        if valid(id.as_bytes()) {
            Ok(PropertyId(Cow::Owned(id)))
        } else {
            Err(BadId { id })
        }
    }

    /// The ID as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PropertyId {
    /// Writes the ID.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A generated ID that breaks the grammar.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BadId {
    /// The rejected ID.
    pub id: String,
}

impl fmt::Display for BadId {
    /// Names the rejected ID.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "`{}` is not a property ID", self.id)
    }
}

impl std::error::Error for BadId {}

/// True if `id` follows the grammar on [`PropertyId`].
const fn valid(id: &[u8]) -> bool {
    let mut segments = 1;
    // Starts as if after a separator, so a leading `-` or `/` fails.
    let mut prev = b'/';
    let mut i = 0;
    while i < id.len() {
        let b = id[i];
        match b {
            b'a'..=b'z' | b'0'..=b'9' => {}
            b'-' | b'/' => {
                // No empty word or segment.
                if prev == b'-' || prev == b'/' {
                    return false;
                }
                if b == b'/' {
                    segments += 1;
                }
            }
            _ => return false,
        }
        prev = b;
        i += 1;
    }
    segments >= 2 && prev != b'-' && prev != b'/'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grammar() {
        for good in [
            "sim/no-livelock",
            "block-sync/retained-byte-bound",
            "header-sync/s5v9/conformant-never-closes",
            "block-sync/s6v3/block/unsolicited-closes",
        ] {
            assert!(valid(good.as_bytes()), "{good}");
        }
        for bad in [
            "",
            "sim",
            "Sim/no-livelock",
            "sim:x/no-livelock",
            "sim//no-livelock",
            "/sim/no-livelock",
            "sim/no-livelock/",
            "sim/-no-livelock",
            "sim/no--livelock",
            "sim/no-livelock-",
            "sim/no_livelock",
            "sim/no livelock",
        ] {
            assert!(!valid(bad.as_bytes()), "{bad}");
        }
    }

    #[test]
    fn generated_checks_the_grammar() {
        let id = PropertyId::generated("block-sync/s6v3/block/unsolicited-closes".to_owned());
        assert_eq!(
            id.map(|id| id.to_string()).as_deref(),
            Ok("block-sync/s6v3/block/unsolicited-closes")
        );
        assert_eq!(
            PropertyId::generated("block-sync/s6v3/Block".to_owned()),
            Err(BadId {
                id: "block-sync/s6v3/Block".to_owned()
            })
        );
    }

    #[test]
    #[should_panic(expected = "breaks the grammar")]
    fn new_panics_at_run_time() {
        let id = std::hint::black_box("sim:no-livelock");
        PropertyId::new(id);
    }
}
