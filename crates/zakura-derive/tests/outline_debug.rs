//! `OutlineDebug` must format exactly as `#[derive(Debug)]` does.

/// Defines each type once with `#[derive(Debug)]` and once with
/// `#[derive(OutlineDebug)]`, then compares the compact and pretty output of
/// the same values.
macro_rules! assert_same_debug {
    ($test:ident { $({ $($item:tt)* })* } [$($value:expr),+ $(,)?]) => {
        #[test]
        fn $test() {
            mod derived {
                #![allow(dead_code, unused_imports, nonstandard_style)]
                use super::*;
                $(#[derive(Debug)] $($item)*)*
                pub fn render() -> Vec<String> {
                    vec![$(format!("{:?}\n{:#?}", $value, $value)),+]
                }
            }
            mod outlined {
                #![allow(dead_code, unused_imports, nonstandard_style)]
                use super::*;
                $(#[derive(zakura_derive::OutlineDebug)] $($item)*)*
                pub fn render() -> Vec<String> {
                    vec![$(format!("{:?}\n{:#?}", $value, $value)),+]
                }
            }
            assert_eq!(outlined::render(), derived::render());
        }
    };
}

/// A trait whose associated type a generic field projects.
pub trait Proof {
    /// The projected field type.
    type Bytes;
}

/// A `Proof` implementation for the projection tests.
#[derive(Debug)]
pub struct Groth;

impl Proof for Groth {
    type Bytes = [u8; 2];
}

assert_same_debug!(structs {
    { struct Named { height: u32, label: &'static str } }
    { struct Tuple(u8, Option<i64>); }
    { struct EmptyBraces {} }
} [
    Named { height: 7, label: "tip" },
    Tuple(1, Some(-2)),
    EmptyBraces {},
]);

#[test]
fn unit_and_empty_tuple_structs() {
    mod derived {
        #[derive(Debug)]
        pub struct Unit;
        #[derive(Debug)]
        pub struct EmptyTuple();
    }
    mod outlined {
        #[derive(zakura_derive::OutlineDebug)]
        pub struct Unit;
        #[derive(zakura_derive::OutlineDebug)]
        pub struct EmptyTuple();
    }
    assert_eq!(
        format!("{:?} {:#?}", outlined::Unit, outlined::Unit),
        format!("{:?} {:#?}", derived::Unit, derived::Unit),
    );
    assert_eq!(
        format!("{:?} {:#?}", outlined::EmptyTuple(), outlined::EmptyTuple()),
        format!("{:?} {:#?}", derived::EmptyTuple(), derived::EmptyTuple()),
    );
}

assert_same_debug!(enums {
    {
        enum Error {
            Unit,
            Tuple(u32, String),
            Named { expected: u64, actual: u64 },
            Nested(Box<Error>),
            EmptyBraces {},
            EmptyTuple(),
        }
    }
} [
    Error::Unit,
    Error::Tuple(3, "three".to_string()),
    Error::Named { expected: 1, actual: 2 },
    Error::Nested(Box::new(Error::Named { expected: 4, actual: 5 })),
    Error::EmptyBraces {},
    Error::EmptyTuple(),
]);

// Fields named like the formatter or with raw identifiers keep their names.
assert_same_debug!(awkward_names {
    { struct Awkward { f: u8, r#type: u8, __self_0: u8 } }
    { enum r#Raw { r#match { r#in: u8 }, f(u8) } }
} [
    Awkward { f: 1, r#type: 2, __self_0: 3 },
    r#Raw::r#match { r#in: 4 },
    r#Raw::f(5),
]);

assert_same_debug!(generics {
    { struct Generic<T> { value: T, values: Vec<T> } }
    { struct Projected<P: Proof> { proof: P::Bytes, qualified: <P as Proof>::Bytes } }
    { struct Borrowed<'a, T: ?Sized> { slice: &'a T } }
    { struct ConstSized<const N: usize> { bytes: [u8; N] } }
    { struct Bounded<T> where T: Copy { value: T } }
} [
    Generic { value: 1u8, values: vec![2, 3] },
    Projected::<Groth> { proof: [1, 2], qualified: [3, 4] },
    Borrowed { slice: "str" },
    ConstSized { bytes: [9u8; 3] },
    Bounded { value: 'x' },
]);
