//! `[memory]` configuration types and pure resolved budget passed from
//! `zakurad` into `zakura-network` (`02 §2`, `03 §3`, decision-2).

use std::{fmt, str::FromStr};

use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use thiserror::Error;

use super::draft::{
    derive_p2p_budget, BudgetDerivation, DRAFT_NON_P2P_BASE_BYTES, DRAFT_RHO_DENOMINATOR,
    DRAFT_RHO_NUMERATOR,
};
use crate::zakura::peer_memory::DEFAULT_ZAKURA_PEER_MEMORY_BUDGET_BYTES;

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const GIB: u64 = 1024 * MIB;
const TIB: u64 = 1024 * GIB;

const KB: u64 = 1_000;
const MB: u64 = 1_000 * KB;
const GB: u64 = 1_000 * MB;
const TB: u64 = 1_000 * GB;

/// Most decimal digits a [`ByteSize`] may carry after its decimal point.
const MAX_FRACTION_DIGITS: usize = 18;

/// Error parsing a human-readable [`ByteSize`] string.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid byte size {input:?}: {reason}")]
pub struct ByteSizeParseError {
    /// Original input string.
    pub input: String,
    /// Reason parsing failed.
    pub reason: &'static str,
}

/// A byte count that parses from human-readable strings (`"16 GiB"`, `"1.5 GiB"`,
/// `"256m"`, `"4096"`) or non-negative integers in TOML and env overrides.
///
/// Single-letter units (`k`, `m`, `g`, `t`) and `KiB`-style units are binary.
/// `KB`-style units are decimal. A fraction of a byte rounds down.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct ByteSize(pub u64);

impl ByteSize {
    /// Byte count as a raw `u64`.
    #[inline]
    pub const fn bytes(self) -> u64 {
        self.0
    }
}

impl FromStr for ByteSize {
    type Err = ByteSizeParseError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let err = |reason| ByteSizeParseError {
            input: raw.to_owned(),
            reason,
        };
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(err(
                "expected a byte count such as \"16 GiB\", \"256 MiB\", or 4096",
            ));
        }
        if trimmed.starts_with('-') {
            return Err(err("byte size cannot be negative"));
        }

        let number_end = trimmed
            .find(|c: char| !(c.is_ascii_digit() || c == '_' || c == '.'))
            .unwrap_or(trimmed.len());
        let (whole_part, fraction_part) = match trimmed[..number_end].split_once('.') {
            Some((whole, fraction)) => (whole, fraction),
            None => (&trimmed[..number_end], ""),
        };
        let suffix = trimmed[number_end..].trim();

        let whole: String = whole_part.chars().filter(|&c| c != '_').collect();
        let fraction: String = fraction_part.chars().filter(|&c| c != '_').collect();
        if whole.is_empty() && fraction.is_empty() {
            return Err(err("missing numeric value"));
        }
        if fraction.contains('.') || fraction.len() > MAX_FRACTION_DIGITS {
            return Err(err(
                "expected at most one decimal point and 18 decimal digits",
            ));
        }

        // Single-letter units are binary, like the repo's other sizes and
        // systemd's `MemoryMax=`: "m" is MiB. Two-letter units without "i"
        // are decimal: "MB" is 10^6 bytes.
        let multiplier = match suffix.to_ascii_lowercase().as_str() {
            "" | "b" => 1u64,
            "kib" | "ki" | "k" => KIB,
            "mib" | "mi" | "m" => MIB,
            "gib" | "gi" | "g" => GIB,
            "tib" | "ti" | "t" => TIB,
            "kb" => KB,
            "mb" => MB,
            "gb" => GB,
            "tb" => TB,
            _ => {
                return Err(err(
                    "unknown unit (use B, KiB, MiB, GiB, TiB, KB, MB, GB, or TB)",
                ));
            }
        };

        let overflow = || err("byte size overflows u64");
        let parse = |digits: &str| -> Result<u128, ByteSizeParseError> {
            if digits.is_empty() {
                Ok(0)
            } else {
                digits.parse().map_err(|_| overflow())
            }
        };
        let whole = parse(&whole)?;
        // `fraction` has at most 18 digits, so it and its scale fit in u128.
        let fraction_scale = 10u128.pow(fraction.len() as u32);
        let fraction = parse(&fraction)?;
        // A fraction of a byte rounds down.
        let bytes = whole
            .checked_mul(u128::from(multiplier))
            .and_then(|bytes| bytes.checked_add(fraction * u128::from(multiplier) / fraction_scale))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(overflow)?;

        Ok(Self(bytes))
    }
}

impl fmt::Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.0;
        if bytes == 0 {
            return f.write_str("0 B");
        }
        for (unit, scale) in [("TiB", TIB), ("GiB", GIB), ("MiB", MIB), ("KiB", KIB)] {
            if bytes.is_multiple_of(scale) {
                return write!(f, "{} {unit}", bytes / scale);
            }
        }
        write!(f, "{bytes} B")
    }
}

impl Serialize for ByteSize {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_size(
            deserializer,
            "a byte size string (e.g. \"16 GiB\") or non-negative integer",
            "byte size cannot be negative",
            ByteSize,
            ByteSize::from_str,
        )
    }
}

/// Deserializes a size from a non-negative integer, which `from_bytes`
/// wraps, or a string, which `parse` reads. A negative integer fails with
/// `negative`.
fn deserialize_size<'de, D: Deserializer<'de>, T>(
    deserializer: D,
    expecting: &'static str,
    negative: &'static str,
    from_bytes: fn(u64) -> T,
    parse: fn(&str) -> Result<T, ByteSizeParseError>,
) -> Result<T, D::Error> {
    struct SizeVisitor<T> {
        expecting: &'static str,
        negative: &'static str,
        from_bytes: fn(u64) -> T,
        parse: fn(&str) -> Result<T, ByteSizeParseError>,
    }

    impl<T> Visitor<'_> for SizeVisitor<T> {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.expecting)
        }

        fn visit_u64<E: de::Error>(self, value: u64) -> Result<T, E> {
            Ok((self.from_bytes)(value))
        }

        fn visit_i64<E: de::Error>(self, value: i64) -> Result<T, E> {
            u64::try_from(value)
                .map(self.from_bytes)
                .map_err(|_| E::custom(self.negative))
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<T, E> {
            (self.parse)(value).map_err(E::custom)
        }
    }

    deserializer.deserialize_any(SizeVisitor {
        expecting,
        negative,
        from_bytes,
        parse,
    })
}

/// Value for `[memory] budget`: `"auto"` (detect host/cgroup memory) or an
/// explicit byte size such as `"16 GiB"` (`02 §2`, `03 §3`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MemoryBudget {
    /// Probe the host and Linux cgroup limits at startup.
    #[default]
    Auto,
    /// Explicit total process memory budget in bytes.
    Fixed(ByteSize),
}

impl fmt::Display for MemoryBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Auto => f.write_str("auto"),
            Self::Fixed(size) => size.fmt(f),
        }
    }
}

impl Serialize for MemoryBudget {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Auto => serializer.serialize_str("auto"),
            Self::Fixed(size) => size.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for MemoryBudget {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize_size(
            deserializer,
            "\"auto\", a byte size string (e.g. \"16 GiB\"), or bytes",
            "[memory] budget cannot be negative",
            |bytes| MemoryBudget::Fixed(ByteSize(bytes)),
            |value| {
                if value.trim().eq_ignore_ascii_case("auto") {
                    Ok(MemoryBudget::Auto)
                } else {
                    ByteSize::from_str(value).map(MemoryBudget::Fixed)
                }
            },
        )
    }
}

/// Top-level `[memory]` section of `zakura.toml` (`02 §2`, `03 §3`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MemoryConfig {
    /// `"auto"` (detect tightest host or cgroup limit) or an explicit byte size
    /// such as `"16 GiB"`.
    pub budget: MemoryBudget,
}

impl MemoryConfig {
    /// Validate `[memory]` settings before resolving the budget.
    pub fn validate(&self) -> Result<(), String> {
        if let MemoryBudget::Fixed(size) = self.budget {
            if size.bytes() == 0 {
                return Err(
                    "invalid [memory] budget: must be greater than 0 (or \"auto\")".to_owned(),
                );
            }
        }
        Ok(())
    }

    /// Resolve a pure [`ResolvedMemoryBudget`] from a probed or configured host
    /// budget (`02 §2`, decision-2).
    pub fn resolve(
        &self,
        host_budget_bytes: u64,
        source: &'static str,
        managed_zcashd: bool,
    ) -> ResolvedMemoryBudget {
        ResolvedMemoryBudget::with_derivation(
            derive_p2p_budget(host_budget_bytes, managed_zcashd),
            source,
        )
    }
}

/// Pure, pre-resolved memory budget settings passed from `zakurad` into
/// `zakura-network` (`decision-2`: `zakura-network` never reads `/proc` or `/sys`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedMemoryBudget {
    /// Breakdown of `B = (budget - base - sidecar) / ρ`.
    pub derivation: BudgetDerivation,
    /// Source label for `zakura.memory.budget_bytes{source}` (`"auto"` or `"config"`).
    pub source: &'static str,
    /// Mempool pool capacity (`[mempool] tx_cost_limit`), counted as a fixed
    /// term and as the default cap for the `Tx` consumer (`02 §4`).
    pub mempool_tx_cost_limit: u64,
    /// `zakura-state`'s read parser allowance, counted in the state reads fixed
    /// term (`02 §4`).
    pub state_read_parser_bytes: u64,
    /// An operator-set `[state] read_result_byte_limit`, or `None` for the
    /// plan's derived limit (`02 §4`, decision-10).
    pub read_result_byte_limit: Option<u64>,
}

impl ResolvedMemoryBudget {
    /// A resolved budget from `derivation`, with the default mempool, state
    /// read and read limit terms, which `zakurad` sets from its own config.
    const fn with_derivation(derivation: BudgetDerivation, source: &'static str) -> Self {
        Self {
            derivation,
            source,
            mempool_tx_cost_limit: super::plan::DEFAULT_MEMPOOL_TX_COST_LIMIT_BYTES,
            state_read_parser_bytes: super::plan::DEFAULT_STATE_READ_PARSER_BYTES,
            read_result_byte_limit: None,
        }
    }

    /// Construct a resolved budget directly from a pure logical P2P budget `B`
    /// (for tests, simulator harnesses, and standalone `zakura-network` callers).
    pub const fn from_p2p_budget_b(p2p_budget_b: u64) -> Self {
        Self::with_derivation(
            BudgetDerivation {
                host_budget_bytes: p2p_budget_b,
                base_bytes: 0,
                sidecar_bytes: 0,
                rho_numerator: 1,
                rho_denominator: 1,
                p2p_budget_b,
            },
            "config",
        )
    }

    /// Logical-byte P2P budget `B`.
    #[inline]
    pub const fn p2p_budget_b(&self) -> u64 {
        self.derivation.p2p_budget_b
    }
}

impl Default for ResolvedMemoryBudget {
    fn default() -> Self {
        // Standalone `zakura-network` default used when `zakurad` has not
        // probed a host budget (e.g. unit tests constructing `Config::default()`).
        let p2p_budget_b = DEFAULT_ZAKURA_PEER_MEMORY_BUDGET_BYTES;
        let host_budget_bytes = DRAFT_NON_P2P_BASE_BYTES.saturating_add(
            u64::try_from(
                u128::from(p2p_budget_b).saturating_mul(u128::from(DRAFT_RHO_NUMERATOR))
                    / u128::from(DRAFT_RHO_DENOMINATOR),
            )
            .unwrap_or(u64::MAX),
        );
        Self::with_derivation(
            BudgetDerivation {
                host_budget_bytes,
                base_bytes: DRAFT_NON_P2P_BASE_BYTES,
                sidecar_bytes: 0,
                rho_numerator: DRAFT_RHO_NUMERATOR,
                rho_denominator: DRAFT_RHO_DENOMINATOR,
                p2p_budget_b,
            },
            "auto",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_size_parses_and_round_trips() {
        for (input, expected) in [
            ("0", 0),
            ("4096", 4096),
            ("1_024", 1024),
            ("64 KiB", 64 * KIB),
            ("256 MiB", 256 * MIB),
            ("16 GiB", 16 * GIB),
            ("1 TiB", TIB),
            ("16gib", 16 * GIB),
            ("  32   MiB  ", 32 * MIB),
            ("100 MB", 100 * MB),
            ("2 GB", 2 * GB),
            ("512 B", 512),
            ("1.5 GiB", GIB + GIB / 2),
            ("0.25 MiB", MIB / 4),
            ("1.5 B", 1),
            ("256m", 256 * MIB),
            ("4 G", 4 * GIB),
            ("64k", 64 * KIB),
            ("2.5 GB", 2_500_000_000),
        ] {
            let parsed: ByteSize = input.parse().unwrap_or_else(|err| panic!("{input}: {err}"));
            assert_eq!(parsed.bytes(), expected, "{input}");
            let reparsed: ByteSize = parsed.to_string().parse().expect("display round-trips");
            assert_eq!(reparsed, parsed);
        }

        for bad in [
            "",
            "   ",
            "-1 GiB",
            "GiB",
            "16 XB",
            "9999999999999999999999 GiB",
            "16777216 TiB",
            "1.2.3 GiB",
            ".",
        ] {
            assert!(bad.parse::<ByteSize>().is_err(), "should reject {bad:?}");
        }
    }

    #[test]
    fn memory_config_deserializes_from_toml() {
        let cfg: MemoryConfig = toml::from_str(
            r#"
            budget = "16 GiB"
            "#,
        )
        .expect("valid [memory] section");

        assert_eq!(cfg.budget, MemoryBudget::Fixed(ByteSize(16 * GIB)));
        assert!(cfg.validate().is_ok());

        let auto: MemoryConfig = toml::from_str(r#"budget = "auto""#).unwrap();
        assert_eq!(auto.budget, MemoryBudget::Auto);

        let int_budget: MemoryConfig = toml::from_str("budget = 17179869184").unwrap();
        assert_eq!(int_budget.budget, MemoryBudget::Fixed(ByteSize(16 * GIB)));

        assert!(toml::from_str::<MemoryConfig>("[weight]\ntx = 1\n").is_err());
    }
}
