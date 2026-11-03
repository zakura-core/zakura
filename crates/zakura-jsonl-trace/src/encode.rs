//! Direct serde-to-CSV row encoding.
//!
//! Serializing through [`serde_json::Value`] and flattening it allocated a
//! string for every key and value on the emitting thread. This serializer
//! writes each field straight into its column instead. It produces the same
//! bytes as [`crate::render_csv_row`]: nested objects become dotted columns,
//! sequences become embedded JSON, and undeclared fields go to the extra
//! column.

use std::{
    fmt::{self, Write as _},
    io::Write as _,
};

use serde::{ser, Serialize};
use serde_json::{Map, Value};

use crate::{write_csv_field, write_csv_value, ENVELOPE_COLUMNS};

/// Encode `row` as one CSV line aligned to `header`, or `None` when `row` is
/// not an object or cannot be serialized.
pub(crate) fn encode_csv_row<T>(header: &'static [&'static str], row: &T) -> Option<Vec<u8>>
where
    T: Serialize + ?Sized,
{
    let mut encoder = RowEncoder::new(header);
    row.serialize(FieldSerializer {
        row: &mut encoder,
        top: true,
    })
    .ok()?;
    Some(encoder.finish())
}

struct RowEncoder {
    header: &'static [&'static str],
    /// Byte ranges in `data` for each envelope and header column.
    slots: Vec<Option<(usize, usize)>>,
    data: Vec<u8>,
    extra: Map<String, Value>,
    /// The dotted name of the field being serialized.
    name: String,
    /// The column after the last match. Fields usually arrive in header order.
    hint: usize,
    scratch: String,
}

impl RowEncoder {
    fn new(header: &'static [&'static str]) -> Self {
        Self {
            header,
            slots: vec![None; ENVELOPE_COLUMNS.len() + header.len()],
            data: Vec::with_capacity(256),
            extra: Map::new(),
            name: String::new(),
            hint: 0,
            scratch: String::new(),
        }
    }

    fn column_name(&self, index: usize) -> &'static str {
        ENVELOPE_COLUMNS
            .get(index)
            .copied()
            .unwrap_or_else(|| self.header[index - ENVELOPE_COLUMNS.len()])
    }

    fn column(&mut self) -> Option<usize> {
        let count = self.slots.len();
        for offset in 0..count {
            let index = (self.hint + offset) % count;
            if self.column_name(index) == self.name {
                self.hint = index + 1;
                return Some(index);
            }
        }
        None
    }

    /// Store a scalar: `csv` renders it into a declared column, and `json`
    /// builds it for the extra column.
    fn put(&mut self, csv: impl FnOnce(&mut Vec<u8>), json: impl FnOnce() -> Value) {
        match self.column() {
            Some(index) => {
                let start = self.data.len();
                csv(&mut self.data);
                self.slots[index] = Some((start, self.data.len()));
            }
            None => {
                self.extra.insert(self.name.clone(), json());
            }
        }
    }

    /// Store a value that was serialized through `serde_json`.
    fn put_value(&mut self, value: Value) {
        match value {
            Value::Object(fields) => {
                let prefix = self.name.len();
                for (key, value) in fields {
                    self.push_key(prefix, &key);
                    self.put_value(value);
                }
                self.name.truncate(prefix);
            }
            value => match self.column() {
                Some(index) => {
                    let start = self.data.len();
                    write_csv_value(&mut self.data, &value);
                    self.slots[index] = Some((start, self.data.len()));
                }
                None => {
                    self.extra.insert(self.name.clone(), value);
                }
            },
        }
    }

    /// Set the current name to `key` under the first `prefix` bytes.
    fn push_key(&mut self, prefix: usize, key: &str) {
        self.name.truncate(prefix);
        if prefix > 0 {
            self.name.push('.');
        }
        self.name.push_str(key);
    }

    /// Serialize one field value, falling back to `serde_json` for shapes
    /// that become embedded JSON.
    fn field<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        match value.serialize(FieldSerializer {
            row: self,
            top: false,
        }) {
            Err(Error::Fallback) => {
                let value = serde_json::to_value(value).map_err(|_| Error::Invalid)?;
                self.put_value(value);
                Ok(())
            }
            result => result,
        }
    }

    fn finish(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.data.len() + self.slots.len() + 1);
        for (index, slot) in self.slots.iter().enumerate() {
            if index > 0 {
                out.push(b',');
            }
            if let Some((start, end)) = slot {
                out.extend_from_slice(&self.data[*start..*end]);
            }
        }
        out.push(b',');
        if !self.extra.is_empty() {
            write_csv_field(&mut out, &Value::Object(self.extra).to_string());
        }
        out
    }
}

#[derive(Debug)]
enum Error {
    /// The value becomes embedded JSON; reserialize it through `serde_json`.
    Fallback,
    Invalid,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("trace row cannot be encoded as CSV")
    }
}

impl std::error::Error for Error {}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(_: T) -> Self {
        Self::Invalid
    }
}

struct FieldSerializer<'a> {
    row: &'a mut RowEncoder,
    /// Only an object may be serialized at the top level.
    top: bool,
}

impl<'a> FieldSerializer<'a> {
    fn scalar(
        self,
        csv: impl FnOnce(&mut Vec<u8>),
        json: impl FnOnce() -> Value,
    ) -> Result<(), Error> {
        if self.top {
            return Err(Error::Invalid);
        }
        self.row.put(csv, json);
        Ok(())
    }

    fn integer<N>(self, value: N) -> Result<(), Error>
    where
        N: fmt::Display + Into<Value> + Copy,
    {
        self.scalar(
            |out| {
                write!(out, "{value}").expect("writing to a Vec cannot fail");
            },
            || value.into(),
        )
    }

    fn json<T>(self, value: T) -> Result<(), Error>
    where
        T: Serialize,
    {
        if self.top {
            return Err(Error::Invalid);
        }
        let value = serde_json::to_value(value).map_err(|_| Error::Invalid)?;
        self.row.put_value(value);
        Ok(())
    }

    fn nested(self) -> Result<NestedSerializer<'a>, Error> {
        let prefix = self.row.name.len();
        Ok(NestedSerializer {
            row: self.row,
            prefix,
        })
    }
}

impl<'a> ser::Serializer for FieldSerializer<'a> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = ser::Impossible<(), Error>;
    type SerializeTuple = ser::Impossible<(), Error>;
    type SerializeTupleStruct = ser::Impossible<(), Error>;
    type SerializeTupleVariant = ser::Impossible<(), Error>;
    type SerializeMap = NestedSerializer<'a>;
    type SerializeStruct = NestedSerializer<'a>;
    type SerializeStructVariant = ser::Impossible<(), Error>;

    fn serialize_bool(self, value: bool) -> Result<(), Error> {
        self.scalar(
            |out| out.extend_from_slice(if value { b"true" } else { b"false" }),
            || Value::Bool(value),
        )
    }

    fn serialize_i8(self, value: i8) -> Result<(), Error> {
        self.integer(value)
    }

    fn serialize_i16(self, value: i16) -> Result<(), Error> {
        self.integer(value)
    }

    fn serialize_i32(self, value: i32) -> Result<(), Error> {
        self.integer(value)
    }

    fn serialize_i64(self, value: i64) -> Result<(), Error> {
        self.integer(value)
    }

    fn serialize_u8(self, value: u8) -> Result<(), Error> {
        self.integer(value)
    }

    fn serialize_u16(self, value: u16) -> Result<(), Error> {
        self.integer(value)
    }

    fn serialize_u32(self, value: u32) -> Result<(), Error> {
        self.integer(value)
    }

    fn serialize_u64(self, value: u64) -> Result<(), Error> {
        self.integer(value)
    }

    fn serialize_i128(self, value: i128) -> Result<(), Error> {
        self.json(value)
    }

    fn serialize_u128(self, value: u128) -> Result<(), Error> {
        self.json(value)
    }

    fn serialize_f32(self, value: f32) -> Result<(), Error> {
        self.json(value)
    }

    fn serialize_f64(self, value: f64) -> Result<(), Error> {
        self.json(value)
    }

    fn serialize_char(self, value: char) -> Result<(), Error> {
        self.serialize_str(value.encode_utf8(&mut [0; 4]))
    }

    fn serialize_str(self, value: &str) -> Result<(), Error> {
        self.scalar(
            |out| write_csv_field(out, value),
            || Value::String(value.to_owned()),
        )
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<(), Error> {
        self.json(value)
    }

    fn serialize_none(self) -> Result<(), Error> {
        self.scalar(|_| {}, || Value::Null)
    }

    fn serialize_some<T>(self, value: &T) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), Error> {
        self.serialize_none()
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), Error> {
        self.serialize_none()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> Result<(), Error> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        if self.top {
            return Err(Error::Invalid);
        }
        let prefix = self.row.name.len();
        self.row.push_key(prefix, variant);
        let result = self.row.field(value);
        self.row.name.truncate(prefix);
        result
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
        Err(self.fallback())
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Error> {
        Err(self.fallback())
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Error> {
        Err(self.fallback())
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Error> {
        Err(self.fallback())
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Error> {
        self.nested()
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Error> {
        self.nested()
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Error> {
        Err(self.fallback())
    }

    fn collect_str<T>(self, value: &T) -> Result<(), Error>
    where
        T: fmt::Display + ?Sized,
    {
        let mut text = std::mem::take(&mut self.row.scratch);
        text.clear();
        write!(text, "{value}").map_err(|_| Error::Invalid)?;
        let result = FieldSerializer {
            row: &mut *self.row,
            top: self.top,
        }
        .serialize_str(&text);
        self.row.scratch = text;
        result
    }
}

impl FieldSerializer<'_> {
    fn fallback(&self) -> Error {
        if self.top {
            Error::Invalid
        } else {
            Error::Fallback
        }
    }
}

struct NestedSerializer<'a> {
    row: &'a mut RowEncoder,
    prefix: usize,
}

impl ser::SerializeStruct for NestedSerializer<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        self.row.push_key(self.prefix, key);
        self.row.field(value)
    }

    fn end(self) -> Result<(), Error> {
        self.row.name.truncate(self.prefix);
        Ok(())
    }
}

impl ser::SerializeMap for NestedSerializer<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        let Value::String(key) = serde_json::to_value(MapKey(key)).map_err(|_| Error::Invalid)?
        else {
            return Err(Error::Invalid);
        };
        self.row.push_key(self.prefix, &key);
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        self.row.field(value)
    }

    fn serialize_entry<K, V>(&mut self, key: &K, value: &V) -> Result<(), Error>
    where
        K: Serialize + ?Sized,
        V: Serialize + ?Sized,
    {
        let mut name = std::mem::take(&mut self.row.scratch);
        name.clear();
        key.serialize(KeySerializer(&mut name))?;
        self.row.push_key(self.prefix, &name);
        self.row.scratch = name;
        self.row.field(value)
    }

    fn end(self) -> Result<(), Error> {
        self.row.name.truncate(self.prefix);
        Ok(())
    }
}

/// Converts a map key the way `serde_json` does, for [`ser::SerializeMap::serialize_key`].
struct MapKey<'a, T: ?Sized>(&'a T);

impl<T> Serialize for MapKey<'_, T>
where
    T: Serialize + ?Sized,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        let mut key = String::new();
        self.0
            .serialize(KeySerializer(&mut key))
            .map_err(|_| ser::Error::custom("unsupported map key"))?;
        serializer.serialize_str(&key)
    }
}

/// Writes a string or integer map key into a buffer, like `serde_json`.
struct KeySerializer<'a>(&'a mut String);

impl KeySerializer<'_> {
    fn display(self, value: impl fmt::Display) -> Result<(), Error> {
        write!(self.0, "{value}").map_err(|_| Error::Invalid)
    }
}

impl ser::Serializer for KeySerializer<'_> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = ser::Impossible<(), Error>;
    type SerializeTuple = ser::Impossible<(), Error>;
    type SerializeTupleStruct = ser::Impossible<(), Error>;
    type SerializeTupleVariant = ser::Impossible<(), Error>;
    type SerializeMap = ser::Impossible<(), Error>;
    type SerializeStruct = ser::Impossible<(), Error>;
    type SerializeStructVariant = ser::Impossible<(), Error>;

    fn serialize_str(self, value: &str) -> Result<(), Error> {
        self.0.push_str(value);
        Ok(())
    }

    fn serialize_char(self, value: char) -> Result<(), Error> {
        self.0.push(value);
        Ok(())
    }

    fn serialize_i8(self, value: i8) -> Result<(), Error> {
        self.display(value)
    }

    fn serialize_i16(self, value: i16) -> Result<(), Error> {
        self.display(value)
    }

    fn serialize_i32(self, value: i32) -> Result<(), Error> {
        self.display(value)
    }

    fn serialize_i64(self, value: i64) -> Result<(), Error> {
        self.display(value)
    }

    fn serialize_i128(self, value: i128) -> Result<(), Error> {
        self.display(value)
    }

    fn serialize_u8(self, value: u8) -> Result<(), Error> {
        self.display(value)
    }

    fn serialize_u16(self, value: u16) -> Result<(), Error> {
        self.display(value)
    }

    fn serialize_u32(self, value: u32) -> Result<(), Error> {
        self.display(value)
    }

    fn serialize_u64(self, value: u64) -> Result<(), Error> {
        self.display(value)
    }

    fn serialize_u128(self, value: u128) -> Result<(), Error> {
        self.display(value)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> Result<(), Error> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self)
    }

    fn collect_str<T>(self, value: &T) -> Result<(), Error>
    where
        T: fmt::Display + ?Sized,
    {
        self.display(value)
    }

    fn serialize_bool(self, _value: bool) -> Result<(), Error> {
        Err(Error::Invalid)
    }

    fn serialize_f32(self, _value: f32) -> Result<(), Error> {
        Err(Error::Invalid)
    }

    fn serialize_f64(self, _value: f64) -> Result<(), Error> {
        Err(Error::Invalid)
    }

    fn serialize_bytes(self, _value: &[u8]) -> Result<(), Error> {
        Err(Error::Invalid)
    }

    fn serialize_none(self) -> Result<(), Error> {
        Err(Error::Invalid)
    }

    fn serialize_some<T>(self, _value: &T) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        Err(Error::Invalid)
    }

    fn serialize_unit(self) -> Result<(), Error> {
        Err(Error::Invalid)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), Error> {
        Err(Error::Invalid)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        Err(Error::Invalid)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
        Err(Error::Invalid)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Error> {
        Err(Error::Invalid)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Error> {
        Err(Error::Invalid)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Error> {
        Err(Error::Invalid)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Error> {
        Err(Error::Invalid)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Error> {
        Err(Error::Invalid)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Error> {
        Err(Error::Invalid)
    }
}
