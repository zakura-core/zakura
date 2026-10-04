//! ZEC amount formatting.
//!
//! The `f64` values returned by this type should not be used in consensus-critical code.
//! The values themselves are accurate, but any calculations using them could be lossy.

use std::{
    fmt,
    hash::{Hash, Hasher},
    ops,
    str::FromStr,
};

use zakura_chain::amount::{self, Amount, Constraint, COIN};

use zakura_node_services::BoxError;

// Doc links only
#[allow(unused_imports)]
use zakura_chain::amount::MAX_MONEY;

/// The maximum precision of a zatoshi in ZEC.
/// Also used as the default decimal precision for ZEC formatting.
///
/// This is the same as the `getblocksubsidy` RPC in `zcashd`:
/// <https://github.com/zcash/zcash/blob/f6a4f68115ea4c58d55c8538579d0877ba9c8f79/src/rpc/server.cpp#L134>
pub const MAX_ZEC_FORMAT_PRECISION: usize = 8;

/// A wrapper type that formats [`Amount`]s as ZEC, using double-precision floating point.
///
/// This formatting is accurate to the nearest zatoshi, as long as the number of floating-point
/// calculations is very small. This is because [`MAX_MONEY`] uses 51 bits, but [`f64`] has
/// [53 bits of precision](f64::MANTISSA_DIGITS).
///
/// Rust uses [`roundTiesToEven`](f32), which can lose one bit of precision per calculation
/// in the worst case. (Assuming the platform implements it correctly.)
///
/// Unlike `zcashd`, Zebra doesn't have control over its JSON number precision,
/// because it uses `serde_json`'s formatter. But `zcashd` uses a fixed-point calculation:
/// <https://github.com/zcash/zcash/blob/f6a4f68115ea4c58d55c8538579d0877ba9c8f79/src/rpc/server.cpp#L134>
#[derive(Clone, Copy, serde::Serialize, serde::Deserialize, Default)]
#[serde(try_from = "f64")]
#[serde(into = "f64")]
#[serde(bound = "C: Constraint + Clone")]
pub struct Zec<C: Constraint>(Amount<C>);

impl<C: Constraint> Zec<C> {
    /// Returns the `f64` ZEC value for the inner amount.
    ///
    /// The returned value should not be used for consensus-critical calculations,
    /// because it is lossy.
    pub fn lossy_zec(&self) -> f64 {
        let zats = self.zatoshis();
        // These conversions are exact, because f64 has 53 bits of precision,
        // MAX_MONEY has <51, and COIN has <27, so we have 2 extra bits of precision.
        let zats = zats as f64;
        let coin = COIN as f64;

        // After this calculation, we might have lost one bit of precision,
        // leaving us with only 1 extra bit.
        zats / coin
    }

    /// Converts a `f64` ZEC value to a [`Zec`] amount.
    ///
    /// This is the exact inverse of [`Zec::lossy_zec`]: every amount that method can produce
    /// is accepted here and maps back to the amount it came from.
    ///
    /// This method should not be used for consensus-critical calculations, because it is lossy.
    pub fn from_lossy_zec(lossy_zec: f64) -> Result<Self, BoxError> {
        // This conversion is exact, because f64 has 53 bits of precision, but COIN has <27
        let coin = COIN as f64;

        // Scaling by COIN is inexact in both directions, because COIN is not a power of two:
        // `lossy_zec` rounds when it divides, and this multiplication rounds again. Demanding
        // an integral product here would reject values `lossy_zec` itself produced -- a
        // 14_903_462_499_999 zatoshi balance renders as 149034.62499999, which scales back to
        // 14903462499998.998.
        let zats = lossy_zec * coin;

        // So round to the nearest zatoshi, then accept only if that amount re-encodes to
        // exactly the input. Anything else was not the ZEC form of a whole number of
        // zatoshis, and is still rejected.
        let zats = zats.round();

        if !zats.is_finite() || zats / coin != lossy_zec {
            return Err(
                "loss of precision parsing ZEC value: floating point had fractional zatoshis"
                    .into(),
            );
        }

        // We know this conversion is exact, because we just checked.
        let zats = zats as i64;
        let zats = Amount::try_from(zats)?;

        Ok(Self(zats))
    }
}

// These conversions are lossy, so they should not be used in consensus-critical code
impl<C: Constraint> From<Zec<C>> for f64 {
    fn from(zec: Zec<C>) -> f64 {
        zec.lossy_zec()
    }
}

impl<C: Constraint> TryFrom<f64> for Zec<C> {
    type Error = BoxError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Self::from_lossy_zec(value)
    }
}

// This formatter should not be used for consensus-critical outputs.
impl<C: Constraint> fmt::Display for Zec<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let zec = self.lossy_zec();

        // Try to format like `zcashd` by default
        let decimals = f.precision().unwrap_or(MAX_ZEC_FORMAT_PRECISION);
        let string = format!("{zec:.decimals$}");
        f.pad_integral(zec >= 0.0, "", &string)
    }
}

// This parser should not be used for consensus-critical inputs.
impl<C: Constraint> FromStr for Zec<C> {
    type Err = BoxError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lossy_zec: f64 = s.parse()?;

        Self::from_lossy_zec(lossy_zec)
    }
}

impl<C: Constraint> std::fmt::Debug for Zec<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(&format!("Zec<{}>", std::any::type_name::<C>()))
            .field("ZEC", &self.to_string())
            .field("zat", &self.0)
            .finish()
    }
}

impl<C: Constraint> ops::Deref for Zec<C> {
    type Target = Amount<C>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<C: Constraint> ops::DerefMut for Zec<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<C: Constraint> From<Amount<C>> for Zec<C> {
    fn from(amount: Amount<C>) -> Self {
        Self(amount)
    }
}

impl<C: Constraint> From<Zec<C>> for Amount<C> {
    fn from(zec: Zec<C>) -> Amount<C> {
        zec.0
    }
}

impl<C: Constraint> From<Zec<C>> for i64 {
    fn from(zec: Zec<C>) -> i64 {
        zec.0.into()
    }
}

impl<C: Constraint> TryFrom<i64> for Zec<C> {
    type Error = amount::Error;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Ok(Self(Amount::try_from(value)?))
    }
}

impl<C: Constraint> Hash for Zec<C> {
    /// Zecs with the same value are equal, even if they have different constraints
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl<C1: Constraint, C2: Constraint> PartialEq<Zec<C2>> for Zec<C1> {
    fn eq(&self, other: &Zec<C2>) -> bool {
        self.0.eq(&other.0)
    }
}

impl<C: Constraint> PartialEq<i64> for Zec<C> {
    fn eq(&self, other: &i64) -> bool {
        self.0.eq(other)
    }
}

impl<C: Constraint> PartialEq<Zec<C>> for i64 {
    fn eq(&self, other: &Zec<C>) -> bool {
        self.eq(&other.0)
    }
}

impl<C1: Constraint, C2: Constraint> PartialEq<Amount<C2>> for Zec<C1> {
    fn eq(&self, other: &Amount<C2>) -> bool {
        self.0.eq(other)
    }
}

impl<C1: Constraint, C2: Constraint> PartialEq<Zec<C2>> for Amount<C1> {
    fn eq(&self, other: &Zec<C2>) -> bool {
        self.eq(&other.0)
    }
}

impl<C: Constraint> Eq for Zec<C> {}

#[cfg(test)]
mod tests {
    use super::*;

    use zakura_chain::amount::{NonNegative, MAX_MONEY};

    /// The lockbox balance that made `getblockchaininfo` unparsable on a NU7 chain.
    ///
    /// Before NU7 the lockbox accrues 18_750_000 zatoshi per block, which is 0.1875 ZEC and
    /// needs only four decimal places, so it always round-tripped. NU7 divides the subsidy by
    /// three (ZIP 218), making the stream 6_249_999 zatoshi -- 0.06249999 ZEC, eight decimal
    /// places -- and roughly a third of the resulting balances have no exact `f64` form.
    const NU7_LOCKBOX_BALANCE: i64 = 14_903_462_499_999;

    #[test]
    fn lossy_zec_round_trip_accepts_the_value_it_emitted() {
        let amount = Amount::<NonNegative>::try_from(NU7_LOCKBOX_BALANCE).expect("valid amount");
        let zec = Zec(amount);

        let encoded = zec.lossy_zec();
        assert_eq!(encoded, 149_034.624_999_99);

        let decoded = Zec::<NonNegative>::from_lossy_zec(encoded)
            .expect("a value emitted by lossy_zec must parse back");
        assert_eq!(decoded.zatoshis(), NU7_LOCKBOX_BALANCE);
    }

    #[test]
    fn lossy_zec_round_trips_across_the_money_range() {
        // Strides chosen to be coprime with powers of ten, so the sweep lands on amounts with
        // all eight decimal places populated rather than repeatedly hitting round numbers.
        for stride in [1, 7, 6_249_999, 999_999_937] {
            for step in 0..10_000 {
                let zats = (MAX_MONEY / 2).saturating_sub(step * stride);
                let amount = Amount::<NonNegative>::try_from(zats).expect("valid amount");

                let round_tripped = Zec::<NonNegative>::from_lossy_zec(Zec(amount).lossy_zec())
                    .expect("every emitted value must parse back")
                    .zatoshis();

                assert_eq!(round_tripped, zats, "round trip changed {zats} zatoshi");
            }
        }
    }

    #[test]
    fn lossy_zec_round_trips_at_the_boundaries() {
        for zats in [0, 1, 2, COIN - 1, COIN, MAX_MONEY - 1, MAX_MONEY] {
            let amount = Amount::<NonNegative>::try_from(zats).expect("valid amount");

            let round_tripped = Zec::<NonNegative>::from_lossy_zec(Zec(amount).lossy_zec())
                .expect("every emitted value must parse back")
                .zatoshis();

            assert_eq!(round_tripped, zats);
        }
    }

    #[test]
    fn rejects_values_that_are_not_whole_zatoshis() {
        // Half a zatoshi, and a value with more precision than a zatoshi can carry.
        for lossy in [1.000_000_005_f64, 0.000_000_000_5, 123.456_789_012_3] {
            assert!(
                Zec::<NonNegative>::from_lossy_zec(lossy).is_err(),
                "{lossy} is not a whole number of zatoshis and must be rejected",
            );
        }
    }

    /// Values carrying more precision than a zatoshi are rejected, even when scaling them
    /// by `COIN` happens to land on an integer.
    ///
    /// The previous implementation accepted these, because "the product is integral" is an
    /// artifact rather than a definition. `13253377.913625069` scales to exactly
    /// `1325337791362507`, but the canonical rendering of that amount is
    /// `13253377.91362507`, a different `f64`, so this input was never a whole number of
    /// zatoshis in the first place.
    #[test]
    fn rejects_more_precision_than_a_zatoshi_can_carry() {
        let over_precise = 13_253_377.913_625_069_f64;

        assert_eq!(over_precise * COIN as f64, 1_325_337_791_362_507.0);
        assert!(Zec::<NonNegative>::from_lossy_zec(over_precise).is_err());

        // The canonical rendering of that amount is a different f64, and is accepted.
        let canonical = Zec::<NonNegative>::try_from(1_325_337_791_362_507_i64)
            .expect("valid amount")
            .lossy_zec();
        assert_ne!(canonical, over_precise);
        assert_eq!(
            Zec::<NonNegative>::from_lossy_zec(canonical)
                .expect("canonical renderings parse")
                .zatoshis(),
            1_325_337_791_362_507,
        );
    }

    #[test]
    fn rejects_non_finite_values() {
        for lossy in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(Zec::<NonNegative>::from_lossy_zec(lossy).is_err());
        }
    }

    #[test]
    fn rejects_amounts_outside_the_money_range() {
        let too_much = (MAX_MONEY as f64 / COIN as f64) * 2.0;
        assert!(Zec::<NonNegative>::from_lossy_zec(too_much).is_err());
    }
}
