//! Compatibility checks for the formats used before the adapter dependencies
//! were removed.

use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct ByteArray<const N: usize>(#[serde(with = "super::bytes")] [u8; N]);

fn check_byte_array<const N: usize>() {
    let bytes = std::array::from_fn(|index| {
        u8::try_from(index % 256).expect("the remainder fits in a byte")
    });
    let value = ByteArray(bytes);
    let json = serde_json::to_string(&bytes.as_slice()).unwrap();
    assert_eq!(serde_json::to_string(&value).unwrap(), json);
    assert_eq!(serde_json::from_str::<ByteArray<N>>(&json).unwrap(), value);

    // Fixed tuples have no length prefix: the stored bytes are exactly the
    // array contents, including for the legacy history-tree entry width.
    assert_eq!(bincode::serialize(&value).unwrap(), bytes);
    assert_eq!(bincode::deserialize::<ByteArray<N>>(&bytes).unwrap(), value);
}

#[test]
fn byte_array_json_and_binary_formats_are_unchanged() {
    check_byte_array::<0>();
    check_byte_array::<1>();
    check_byte_array::<80>();
    check_byte_array::<192>();
    check_byte_array::<253>();
    check_byte_array::<296>();
    check_byte_array::<580>();
    check_byte_array::<601>();
    check_byte_array::<{ crate::work::equihash::SOLUTION_SIZE }>();
    check_byte_array::<{ crate::work::equihash::REGTEST_SOLUTION_SIZE }>();
    check_byte_array::<{ zakura_mmr_tree::MAX_ENTRY_SIZE }>();
}

#[test]
fn byte_arrays_reject_wrong_lengths_and_invalid_bytes() {
    for json in ["[]", "[0]", "[0,1,2]", "[0,256]", "[0,-1]", "[0,null]"] {
        assert!(
            serde_json::from_str::<ByteArray<2>>(json).is_err(),
            "{json}"
        );
    }
    assert!(bincode::deserialize::<ByteArray<2>>(&[0]).is_err());
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct HumanDuration(#[serde(with = "super::duration")] Duration);

#[derive(Debug, Default, Deserialize, PartialEq, Serialize)]
struct OptionalDuration {
    #[serde(default, with = "super::optional_duration")]
    duration: Option<Duration>,
}

#[test]
fn duration_strings_and_binary_formats_are_unchanged() {
    let value = HumanDuration(Duration::new(15, 123));
    let string = humantime::format_duration(value.0).to_string();
    assert_eq!(
        serde_json::to_string(&value).unwrap(),
        serde_json::to_string(&string).unwrap()
    );
    assert_eq!(
        bincode::serialize(&value).unwrap(),
        bincode::serialize(&string).unwrap()
    );
    assert_eq!(
        bincode::deserialize::<HumanDuration>(&bincode::serialize(&string).unwrap()).unwrap(),
        value
    );
    assert_eq!(
        serde_json::from_str::<HumanDuration>("\"15 seconds\"").unwrap(),
        HumanDuration(Duration::from_secs(15))
    );

    for json in ["15", "null", "\"bad\"", "\"-1s\""] {
        assert!(
            serde_json::from_str::<HumanDuration>(json).is_err(),
            "{json}"
        );
    }
}

#[test]
fn optional_durations_preserve_some_none_and_missing_fields() {
    let value: OptionalDuration = serde_json::from_str(r#"{"duration":"15 seconds"}"#).unwrap();
    assert_eq!(value.duration, Some(Duration::from_secs(15)));
    assert_eq!(
        serde_json::to_string(&value).unwrap(),
        r#"{"duration":"15s"}"#
    );
    assert_eq!(
        serde_json::from_str::<OptionalDuration>(r#"{"duration":null}"#).unwrap(),
        OptionalDuration::default()
    );
    assert_eq!(
        serde_json::from_str::<OptionalDuration>("{}").unwrap(),
        OptionalDuration::default()
    );
    assert_eq!(
        serde_json::to_string(&OptionalDuration::default()).unwrap(),
        r#"{"duration":null}"#
    );
    for duration in [None, Some(Duration::new(15, 123))] {
        let value = OptionalDuration { duration };
        let old_format = duration.map(|value| humantime::format_duration(value).to_string());
        let encoded = bincode::serialize(&old_format).unwrap();
        assert_eq!(bincode::serialize(&value).unwrap(), encoded);
        assert_eq!(
            bincode::deserialize::<OptionalDuration>(&encoded).unwrap(),
            value
        );
    }
    assert!(serde_json::from_str::<OptionalDuration>(r#"{"duration":15}"#).is_err());
}
