//! Load the node's TOML file and environment overrides.

use std::{collections::BTreeMap, fs, path::PathBuf};

use serde::{
    de::{self, value::Error, IntoDeserializer, Visitor},
    Deserialize, Deserializer,
};

use super::{is_sensitive_leaf_key, ZakuradConfig};
use crate::BoxError;

enum Value {
    Nil,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Array(Vec<Value>),
    Table(BTreeMap<String, Value>),
}

impl From<toml::Value> for Value {
    fn from(value: toml::Value) -> Self {
        match value {
            toml::Value::Boolean(value) => Self::Boolean(value),
            toml::Value::Integer(value) => Self::Integer(value),
            toml::Value::Float(value) => Self::Float(value),
            toml::Value::String(value) => Self::String(value),
            toml::Value::Datetime(value) => Self::String(value.to_string()),
            toml::Value::Array(values) => Self::Array(values.into_iter().map(Self::from).collect()),
            toml::Value::Table(values) => Self::Table(
                values
                    .into_iter()
                    .map(|(key, value)| (key, Self::from(value)))
                    .collect(),
            ),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum LoadError {
    #[error("could not read configuration file {path:?}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("could not parse configuration file {path:?}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("{0}")]
    Environment(String),
}

/// Load the file, then apply prefixes from lowest to highest precedence.
pub(super) fn load(
    path: Option<PathBuf>,
    env_prefixes: &[&str],
) -> Result<ZakuradConfig, BoxError> {
    let mut values = match path {
        Some(mut path) => {
            if !path.is_file() {
                path.as_mut_os_string().push(".toml");
            }
            let bytes = fs::read(&path).map_err(|source| LoadError::Read {
                path: path.clone(),
                source,
            })?;
            // Match the previous loader's BOM handling and UTF-8 replacement.
            let text =
                String::from_utf8_lossy(bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(&bytes));
            let table = toml::from_str::<toml::Table>(&text)
                .map_err(|source| LoadError::Parse { path, source })?;
            Value::from(toml::Value::Table(table))
        }
        None => Value::Table(BTreeMap::new()),
    };

    for prefix in env_prefixes {
        let prefix_pattern = format!("{prefix}_");
        let mut overrides = BTreeMap::new();
        for (key, value) in std::env::vars() {
            if let Some(key) = key.strip_prefix(&prefix_pattern) {
                let leaf = key.rsplit("__").next().unwrap_or(key);
                if is_sensitive_leaf_key(leaf) {
                    return Err(LoadError::Environment(format!(
                        "Environment variable '{prefix_pattern}{key}' contains sensitive key \
                         '{leaf}' which cannot be overridden via environment variables. \
                         Use the configuration file instead to prevent process table exposure."
                    ))
                    .into());
                }
                overrides.insert(
                    key.to_lowercase().replace("__", "."),
                    parse_environment(value),
                );
            }
        }
        if prefix.starts_with("ZEBRA") && !overrides.is_empty() {
            tracing::warn!(
                "ZEBRA_* config environment variables are deprecated; use ZAKURA_* instead"
            );
        }
        for (key, value) in overrides {
            insert_override(&mut values, &key, value)?;
        }
    }
    Ok(ZakuradConfig::deserialize(ConfigValue(values))?)
}

/// Insert a legacy environment path, including dotted keys and array indexes.
fn insert_override(root: &mut Value, path: &str, value: Value) -> Result<(), BoxError> {
    let invalid_path = || LoadError::Environment(format!("invalid environment config key: {path}"));
    let mut remaining = path;
    let mut current = root;
    loop {
        let key_len = remaining
            .bytes()
            .take_while(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'-')
            .count();
        if key_len == 0 {
            return Err(invalid_path().into());
        }
        let (key, rest) = remaining.split_at(key_len);
        if !matches!(current, Value::Table(_)) {
            *current = Value::Table(BTreeMap::new());
        }
        let Value::Table(table) = current else {
            unreachable!()
        };
        current = table.entry(key.to_owned()).or_insert(Value::Nil);
        remaining = rest;
        while let Some(index_text) = remaining.strip_prefix('[') {
            let (index_text, rest) = index_text.split_once(']').ok_or_else(invalid_path)?;
            let index: isize = index_text.trim().parse().map_err(|_| invalid_path())?;
            if !matches!(current, Value::Array(_)) {
                *current = Value::Array(Vec::new());
            }
            let Value::Array(array) = current else {
                unreachable!()
            };
            let index = if index >= 0 {
                usize::try_from(index)?
            } else if let Some(index) = array.len().checked_sub(index.unsigned_abs()) {
                index
            } else {
                let padding = index.unsigned_abs() - array.len();
                array.try_reserve(padding)?;
                array.splice(0..0, std::iter::repeat_with(|| Value::Nil).take(padding));
                0
            };
            let length = index.checked_add(1).ok_or_else(invalid_path)?;
            if length > array.len() {
                array.try_reserve(length - array.len())?;
                array.resize_with(length, || Value::Nil);
            }
            current = &mut array[index];
            remaining = rest;
        }
        if remaining.is_empty() {
            *current = value;
            return Ok(());
        }
        remaining = remaining.strip_prefix('.').ok_or_else(invalid_path)?;
    }
}

fn parse_environment(value: String) -> Value {
    if let Ok(value) = value.to_lowercase().parse::<bool>() {
        Value::Boolean(value)
    } else if let Ok(value) = value.parse::<i64>() {
        Value::Integer(value)
    } else if let Ok(value) = value.parse::<f64>() {
        Value::Float(value)
    } else {
        Value::String(value)
    }
}

/// Preserve the former loader's scalar conversions while using Serde's map
/// and sequence adapters for the configuration structure.
struct ConfigValue(Value);

impl<'de> IntoDeserializer<'de, Error> for ConfigValue {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self {
        self
    }
}

fn boolean_alias(value: &str) -> Option<bool> {
    match value.to_lowercase().as_str() {
        "true" | "on" | "yes" | "1" => Some(true),
        "false" | "off" | "no" | "0" => Some(false),
        _ => None,
    }
}

impl ConfigValue {
    fn signed(self) -> Result<i64, Error> {
        match self.0 {
            Value::Integer(value) => Ok(value),
            Value::Boolean(value) => Ok(i64::from(value)),
            // Rust's float casts saturate; rounding matches legacy config-rs.
            Value::Float(value) => Ok(value.round() as i64),
            Value::String(value) => boolean_alias(&value)
                .map(i64::from)
                .map(Ok)
                .unwrap_or_else(|| value.parse().map_err(de::Error::custom)),
            _ => Err(de::Error::custom("expected an integer")),
        }
    }

    fn unsigned(self) -> Result<u64, Error> {
        match self.0 {
            Value::Float(value) => {
                // Preserve legacy rounding and saturating float conversion.
                Ok(value.round() as u64)
            }
            Value::String(value) => boolean_alias(&value)
                .map(u64::from)
                .map(Ok)
                .unwrap_or_else(|| value.parse().map_err(de::Error::custom)),
            _ => self.signed()?.try_into().map_err(de::Error::custom),
        }
    }

    fn float(self) -> Result<f64, Error> {
        match self.0 {
            Value::Float(value) => Ok(value),
            // Integer-to-float rounding is retained for legacy compatibility.
            Value::Integer(value) => Ok(value as f64),
            Value::Boolean(value) => Ok(f64::from(u8::from(value))),
            Value::String(value) => boolean_alias(&value)
                .map(u8::from)
                .map(f64::from)
                .map(Ok)
                .unwrap_or_else(|| value.parse().map_err(de::Error::custom)),
            _ => Err(de::Error::custom("expected a number")),
        }
    }
}

macro_rules! deserialize_integer {
    ($method:ident, $visit:ident, $convert:ident) => {
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
            visitor.$visit(self.$convert()?.try_into().map_err(de::Error::custom)?)
        }
    };
}

impl<'de> Deserializer<'de> for ConfigValue {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        match self.0 {
            Value::Boolean(value) => visitor.visit_bool(value),
            Value::Integer(value) => visitor.visit_i64(value),
            Value::Float(value) => visitor.visit_f64(value),
            Value::String(value) => visitor.visit_string(value),
            Value::Nil => visitor.visit_unit(),
            Value::Array(values) => visitor.visit_seq(de::value::SeqDeserializer::new(
                values.into_iter().map(ConfigValue),
            )),
            Value::Table(values) => visitor.visit_map(de::value::MapDeserializer::new(
                values
                    .into_iter()
                    .map(|(key, value)| (key, ConfigValue(value))),
            )),
        }
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let value = match self.0 {
            Value::Boolean(value) => value,
            Value::Integer(value) => value != 0,
            Value::Float(value) => value != 0.0,
            Value::String(value) => boolean_alias(&value)
                .ok_or_else(|| <Error as de::Error>::custom("expected a boolean"))?,
            _ => return Err(de::Error::custom("expected a boolean")),
        };
        visitor.visit_bool(value)
    }

    deserialize_integer!(deserialize_i8, visit_i8, signed);
    deserialize_integer!(deserialize_i16, visit_i16, signed);
    deserialize_integer!(deserialize_i32, visit_i32, signed);
    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_i64(self.signed()?)
    }
    deserialize_integer!(deserialize_u8, visit_u8, unsigned);
    deserialize_integer!(deserialize_u16, visit_u16, unsigned);
    deserialize_integer!(deserialize_u32, visit_u32, unsigned);
    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_u64(self.unsigned()?)
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        // Preserve legacy IEEE-754 narrowing, including overflow to infinity.
        visitor.visit_f32(self.float()? as f32)
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_f64(self.float()?)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let value = match self.0 {
            Value::String(value) => value,
            Value::Boolean(value) => value.to_string(),
            Value::Integer(value) => value.to_string(),
            Value::Float(value) => value.to_string(),

            _ => return Err(de::Error::custom("expected a string")),
        };
        visitor.visit_string(value)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        self.deserialize_str(visitor)
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        match self.0 {
            Value::Nil => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        match self.0 {
            Value::String(value) => value
                .into_deserializer()
                .deserialize_enum(name, variants, visitor),
            Value::Table(values) => {
                de::value::MapAccessDeserializer::new(de::value::MapDeserializer::new(
                    values
                        .into_iter()
                        .map(|(key, value)| (key, ConfigValue(value))),
                ))
                .deserialize_enum(name, variants, visitor)
            }
            _ => Err(de::Error::custom("expected an enum string or table")),
        }
    }

    serde::forward_to_deserialize_any! {
        char bytes byte_buf seq map struct unit identifier ignored_any
        unit_struct tuple_struct tuple i128 u128
    }
}
