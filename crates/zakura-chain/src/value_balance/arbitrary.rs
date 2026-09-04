use crate::{amount::*, value_balance::*};
use proptest::prelude::*;

#[cfg(zcash_unstable = "nutachyon")]
impl Arbitrary for ValueBalance<NegativeAllowed> {
    type Parameters = ();

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        (
            any::<Amount<NegativeAllowed>>(),
            any::<Amount<NegativeAllowed>>(),
            any::<Amount<NegativeAllowed>>(),
            any::<Amount<NegativeAllowed>>(),
            any::<Amount<NegativeAllowed>>(),
            any::<Amount<NegativeAllowed>>(),
            any::<Amount<NegativeAllowed>>(),
        )
            .prop_map(
                |(transparent, sprout, sapling, orchard, deferred, ironwood, tachyon)| Self {
                    transparent,
                    sprout,
                    sapling,
                    orchard,
                    deferred,
                    ironwood,
                    tachyon,
                },
            )
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

#[cfg(not(zcash_unstable = "nutachyon"))]
impl Arbitrary for ValueBalance<NegativeAllowed> {
    type Parameters = ();

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        (
            any::<Amount<NegativeAllowed>>(),
            any::<Amount<NegativeAllowed>>(),
            any::<Amount<NegativeAllowed>>(),
            any::<Amount<NegativeAllowed>>(),
            any::<Amount<NegativeAllowed>>(),
            any::<Amount<NegativeAllowed>>(),
        )
            .prop_map(
                |(transparent, sprout, sapling, orchard, deferred, ironwood)| Self {
                    transparent,
                    sprout,
                    sapling,
                    orchard,
                    deferred,
                    ironwood,
                },
            )
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

#[cfg(zcash_unstable = "nutachyon")]
impl Arbitrary for ValueBalance<NonNegative> {
    type Parameters = ();

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        (
            any::<Amount<NonNegative>>(),
            any::<Amount<NonNegative>>(),
            any::<Amount<NonNegative>>(),
            any::<Amount<NonNegative>>(),
            any::<Amount<NonNegative>>(),
            any::<Amount<NonNegative>>(),
            any::<Amount<NonNegative>>(),
        )
            .prop_map(
                |(transparent, sprout, sapling, orchard, deferred, ironwood, tachyon)| Self {
                    transparent,
                    sprout,
                    sapling,
                    orchard,
                    deferred,
                    ironwood,
                    tachyon,
                },
            )
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}

#[cfg(not(zcash_unstable = "nutachyon"))]
impl Arbitrary for ValueBalance<NonNegative> {
    type Parameters = ();

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        (
            any::<Amount<NonNegative>>(),
            any::<Amount<NonNegative>>(),
            any::<Amount<NonNegative>>(),
            any::<Amount<NonNegative>>(),
            any::<Amount<NonNegative>>(),
            any::<Amount<NonNegative>>(),
        )
            .prop_map(
                |(transparent, sprout, sapling, orchard, deferred, ironwood)| Self {
                    transparent,
                    sprout,
                    sapling,
                    orchard,
                    deferred,
                    ironwood,
                },
            )
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}
