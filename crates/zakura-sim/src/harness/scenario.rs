//! The fixed-width action grammar a harness's scenario prefix can use (G1).
//!
//! A [`Layout`] splits the prefix into a header and fixed-width action
//! slots. Header byte 0 holds the grammar version, which is the harness's
//! `GENERATOR`, and byte 1 the action count. The harness owns the rest of
//! the header and every slot's meaning. The slots after the count are
//! padding, so an inserted, deleted or changed action never moves the Plan
//! bytes that follow the prefix.
//!
//! [`Layout::read`] rejects a prefix written for another grammar version, so
//! a later generator never reinterprets an input written under this one. An
//! input from before the harness had a grammar has no version byte, so its
//! first byte may still match. Each harness maps a version rejection to one
//! documented scenario. Every other prefix decodes.

use std::fmt;

/// The largest fuzz input a grammar-built harness is designed for: its
/// prefix plus the Plan bytes (G1 plan, Initial finite limits).
pub const MAX_INPUT_BYTES: usize = 4_096;

/// Where a grammar's header and slots sit in the scenario prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Layout {
    /// The version byte, header byte 0: the harness's `GENERATOR`.
    pub version: u8,
    /// The header's length, version and count bytes included.
    pub header: usize,
    /// Each action slot's length.
    pub slot: usize,
    /// How many slots follow the header.
    pub slots: usize,
}

/// Why a [`Layout`] doesn't decode a prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rejection {
    /// The prefix isn't [`Layout::prefix_len`] bytes long.
    Length {
        /// Its length.
        found: usize,
    },
    /// The version byte names another generator.
    Version {
        /// The version byte.
        found: u8,
    },
}

impl fmt::Display for Rejection {
    /// Names the rejection.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Rejection::Length { found } => write!(f, "the prefix is {found} bytes long"),
            Rejection::Version { found } => {
                write!(f, "the prefix was written for generator {found}")
            }
        }
    }
}

impl std::error::Error for Rejection {}

/// Why a scenario has no canonical encoding: it uses something the
/// grammar can't express.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unencodable(pub String);

impl fmt::Display for Unencodable {
    /// The reason.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the scenario has no encoding: {}", self.0)
    }
}

impl std::error::Error for Unencodable {}

impl Layout {
    /// The scenario prefix's length: the header, then every slot.
    pub const fn prefix_len(&self) -> usize {
        self.header + self.slot * self.slots
    }

    /// The header and the first `count` slots of `prefix`, where `count` is
    /// header byte 1 reduced modulo `slots + 1`. A layout has at most 255
    /// slots, so every count fits that byte.
    pub fn read<'a>(&self, prefix: &'a [u8]) -> Result<Read<'a>, Rejection> {
        if prefix.len() != self.prefix_len() {
            return Err(Rejection::Length {
                found: prefix.len(),
            });
        }
        if prefix[0] != self.version {
            return Err(Rejection::Version { found: prefix[0] });
        }
        let count = usize::from(prefix[1]) % (self.slots + 1);
        let (header, slots) = prefix.split_at(self.header);
        Ok(Read {
            header,
            slots: slots.chunks_exact(self.slot).take(count).collect(),
        })
    }

    /// A zeroed prefix to encode into.
    pub fn writer(&self) -> Writer {
        Writer {
            layout: *self,
            bytes: vec![0; self.prefix_len()],
            count: 0,
        }
    }
}

/// A prefix that [`Layout::read`] accepted.
#[derive(Debug)]
pub struct Read<'a> {
    /// The whole header.
    pub header: &'a [u8],
    /// The counted slots, in order.
    pub slots: Vec<&'a [u8]>,
}

/// A prefix being encoded: the header, then slots in order.
#[derive(Debug)]
pub struct Writer {
    layout: Layout,
    bytes: Vec<u8>,
    count: usize,
}

impl Writer {
    /// The whole header. [`Writer::finish`] writes bytes 0 and 1, the
    /// version and the count, so the harness writes only the rest.
    pub fn header(&mut self) -> &mut [u8] {
        &mut self.bytes[..self.layout.header]
    }

    /// The next slot, zeroed, or an error once every slot is used.
    pub fn slot(&mut self) -> Result<&mut [u8], Unencodable> {
        if self.count == self.layout.slots {
            return Err(Unencodable(format!(
                "it has more than {} actions",
                self.layout.slots
            )));
        }
        let start = self.layout.header + self.count * self.layout.slot;
        self.count += 1;
        Ok(&mut self.bytes[start..start + self.layout.slot])
    }

    /// The prefix, with the version and the count of slots written.
    pub fn finish(mut self) -> Vec<u8> {
        self.bytes[0] = self.layout.version;
        self.bytes[1] = u8::try_from(self.count).expect("a layout has at most 255 slots");
        self.bytes
    }
}
