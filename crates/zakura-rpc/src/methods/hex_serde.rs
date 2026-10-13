//! Hex strings nested in optional RPC fields and lists.

use std::fmt;

use hex::FromHex;
use serde::{ser::SerializeSeq, Deserialize, Deserializer, Serialize, Serializer};

struct Hex<T>(T);

impl<T: AsRef<[u8]>> Serialize for Hex<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        hex::serialize(&self.0, serializer)
    }
}

impl<'de, T> Deserialize<'de> for Hex<T>
where
    T: FromHex,
    T::Error: fmt::Display,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        hex::deserialize(deserializer).map(Self)
    }
}

pub(super) fn serialize_option<S, T>(value: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: AsRef<[u8]>,
{
    value.as_ref().map(Hex).serialize(serializer)
}

pub(super) fn deserialize_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: FromHex,
    T::Error: fmt::Display,
{
    Option::<Hex<T>>::deserialize(deserializer).map(|value| value.map(|hex| hex.0))
}

pub(super) fn serialize_vec<S, T>(values: &[T], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: AsRef<[u8]>,
{
    let mut sequence = serializer.serialize_seq(Some(values.len()))?;
    for value in values {
        sequence.serialize_element(&Hex(value))?;
    }
    sequence.end()
}

pub(super) fn deserialize_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: FromHex,
    T::Error: fmt::Display,
{
    Vec::<Hex<T>>::deserialize(deserializer)
        .map(|values| values.into_iter().map(|hex| hex.0).collect())
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Fields {
        #[serde(
            default,
            serialize_with = "super::serialize_option",
            deserialize_with = "super::deserialize_option"
        )]
        optional: Option<[u8; 2]>,
        #[serde(
            serialize_with = "super::serialize_vec",
            deserialize_with = "super::deserialize_vec"
        )]
        fixed: Vec<[u8; 2]>,
        #[serde(
            serialize_with = "super::serialize_vec",
            deserialize_with = "super::deserialize_vec"
        )]
        variable: Vec<Vec<u8>>,
    }

    #[test]
    fn nested_hex_preserves_json_shapes() {
        let value = Fields {
            optional: Some([0xab, 0xcd]),
            fixed: vec![[0, 0xff]],
            variable: vec![vec![], vec![0xab]],
        };
        let expected = json!({"optional": "abcd", "fixed": ["00ff"], "variable": ["", "ab"]});
        assert_eq!(serde_json::to_value(&value).unwrap(), expected);
        assert_eq!(serde_json::from_value::<Fields>(expected).unwrap(), value);

        let absent = Fields {
            optional: None,
            fixed: vec![],
            variable: vec![],
        };
        assert_eq!(
            serde_json::to_value(&absent).unwrap(),
            json!({"optional": null, "fixed": [], "variable": []})
        );
        for optional in [json!({}), json!({"optional": null})] {
            let mut input = json!({"fixed": [], "variable": []});
            input
                .as_object_mut()
                .unwrap()
                .extend(optional.as_object().unwrap().clone());
            assert_eq!(serde_json::from_value::<Fields>(input).unwrap(), absent);
        }
    }

    #[test]
    fn nested_hex_accepts_uppercase_and_rejects_invalid_encodings() {
        let input = json!({"optional": "ABCD", "fixed": ["00FF"], "variable": ["aB"]});
        let decoded: Fields = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(decoded.optional, Some([0xab, 0xcd]));
        assert_eq!(decoded.fixed, vec![[0, 0xff]]);
        assert_eq!(decoded.variable, vec![vec![0xab]]);

        for invalid in [
            json!("ab"),
            json!("abcdef"),
            json!("abc"),
            json!("zzzz"),
            json!([0, 1]),
        ] {
            let mut input = input.clone();
            input["optional"] = invalid.clone();
            assert!(serde_json::from_value::<Fields>(input).is_err());
            let mut input = json!({"fixed": [], "variable": []});
            input["fixed"] = json!([invalid]);
            assert!(serde_json::from_value::<Fields>(input).is_err());
        }
    }
}
