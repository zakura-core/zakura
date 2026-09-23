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
            any::<Amount<NegativeAllowed>>(),
        )
            .prop_map(
                |(
                    transparent,
                    sprout,
                    sapling,
                    orchard,
                    deferred,
                    ironwood,
                    nsm_value_balance,
                    tachyon,
                )| Self {
                    transparent,
                    sprout,
                    sapling,
                    orchard,
                    deferred,
                    ironwood,
                    nsm_value_balance,
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
            any::<Amount<NegativeAllowed>>(),
        )
            .prop_map(
                |(transparent, sprout, sapling, orchard, deferred, ironwood, nsm_value_balance)| {
                    Self {
                        transparent,
                        sprout,
                        sapling,
                        orchard,
                        deferred,
                        ironwood,
                        nsm_value_balance,
                    }
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
            any::<Amount<NegativeAllowed>>(),
            any::<Amount<NonNegative>>(),
        )
            .prop_map(
                |(
                    transparent,
                    sprout,
                    sapling,
                    orchard,
                    deferred,
                    ironwood,
                    nsm_value_balance,
                    tachyon,
                )| Self {
                    transparent,
                    sprout,
                    sapling,
                    orchard,
                    deferred,
                    ironwood,
                    nsm_value_balance,
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
            any::<Amount<NegativeAllowed>>(),
        )
            .prop_map(
                |(transparent, sprout, sapling, orchard, deferred, ironwood, nsm_value_balance)| {
                    Self {
                        transparent,
                        sprout,
                        sapling,
                        orchard,
                        deferred,
                        ironwood,
                        nsm_value_balance,
                    }
                },
            )
            .boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}
