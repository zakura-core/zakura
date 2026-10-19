//! JSON-RPC 2.0 requests, parameters, responses, and errors.
//!
//! Requests are parsed and responses are encoded exactly like `jsonrpsee` 0.26, which Zakura used
//! before, so clients see the same bytes. `server/tests/wire.rs` checks this.

use std::{borrow::Cow, fmt, io, marker::PhantomData};

use serde::{
    de::{self, DeserializeOwned, Deserializer, Unexpected, Visitor},
    ser::{SerializeStruct, Serializer},
    Deserialize, Serialize,
};
use serde_json::value::RawValue;

/// The result of an RPC method.
pub type RpcResult<T> = Result<T, ErrorObject>;

/// A JSON-RPC 2.0 error object, serialized as `{"code": ..., "message": ...}`.
///
/// RPC methods never send the optional `data` member, so this type doesn't have one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ErrorObject {
    code: i32,
    message: String,
}

impl ErrorObject {
    /// Returns an error with `code` and `message`.
    pub fn new(code: impl Into<ErrorCode>, message: impl Into<String>) -> Self {
        Self {
            code: code.into().code(),
            message: message.into(),
        }
    }

    /// The error code.
    pub fn code(&self) -> i32 {
        self.code
    }

    /// The error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ErrorObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ErrorObject {}

impl From<ErrorCode> for ErrorObject {
    fn from(code: ErrorCode) -> Self {
        Self::new(code, code.message())
    }
}

/// The error codes defined by the JSON-RPC 2.0 specification, and other server error codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    /// The request isn't valid JSON.
    ParseError,
    /// The request isn't a valid JSON-RPC request.
    InvalidRequest,
    /// The method doesn't exist or isn't available.
    MethodNotFound,
    /// The method parameters are invalid.
    InvalidParams,
    /// An internal JSON-RPC error.
    InternalError,
    /// Any other error code.
    ServerError(i32),
}

impl ErrorCode {
    /// The numeric error code.
    pub const fn code(&self) -> i32 {
        match *self {
            Self::ParseError => -32700,
            Self::InvalidRequest => -32600,
            Self::MethodNotFound => -32601,
            Self::InvalidParams => -32602,
            Self::InternalError => -32603,
            Self::ServerError(code) => code,
        }
    }

    /// The standard message for this error code.
    pub const fn message(&self) -> &'static str {
        match self {
            Self::ParseError => "Parse error",
            Self::InvalidRequest => "Invalid request",
            Self::MethodNotFound => "Method not found",
            Self::InvalidParams => "Invalid params",
            Self::InternalError => "Internal error",
            Self::ServerError(_) => "Server error",
        }
    }
}

impl From<i32> for ErrorCode {
    fn from(code: i32) -> Self {
        Self::ServerError(code)
    }
}

/// The error code for a request larger than the server's limit.
const OVERSIZED_REQUEST_CODE: i32 = -32007;
/// The error message for a request larger than the server's limit.
const OVERSIZED_REQUEST_MSG: &str = "Request is too big";
/// The error code for a response larger than the server's limit.
pub(crate) const OVERSIZED_RESPONSE_CODE: i32 = -32008;
/// The error message for a response larger than the server's limit.
const OVERSIZED_RESPONSE_MSG: &str = "Response is too big";
/// The error code for a batch with more entries than the server's limit.
pub(crate) const TOO_BIG_BATCH_REQUEST_CODE: i32 = -32010;
/// The error message for a batch with more entries than the server's limit.
const TOO_BIG_BATCH_REQUEST_MSG: &str = "The batch request was too large";
/// The error code for a batch response larger than the server's limit.
pub(crate) const TOO_BIG_BATCH_RESPONSE_CODE: i32 = -32011;
/// The error message for a batch response larger than the server's limit.
const TOO_BIG_BATCH_RESPONSE_MSG: &str = "The batch response was too large";

/// An error object as it is sent, including the optional `data` member that only the server's
/// own errors use.
#[derive(Serialize)]
struct WireError<'a> {
    code: i32,
    message: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<&'a str>,
}

/// The `"jsonrpc": "2.0"` member of requests and responses.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TwoPointZero;

impl<'de> Deserialize<'de> for TwoPointZero {
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D,
    ) -> ::core::result::Result<Self, D::Error> {
        struct TwoPointZeroVisitor;

        impl Visitor<'_> for TwoPointZeroVisitor {
            type Value = TwoPointZero;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str(r#"a string "2.0""#)
            }

            fn visit_str<E: de::Error>(self, s: &str) -> Result<TwoPointZero, E> {
                match s {
                    "2.0" => Ok(TwoPointZero),
                    _ => Err(de::Error::invalid_value(Unexpected::Str(s), &self)),
                }
            }
        }

        deserializer.deserialize_str(TwoPointZeroVisitor)
    }
}

impl Serialize for TwoPointZero {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str("2.0")
    }
}

/// A request ID.
///
/// IDs that are fractional, negative, or larger than a `u64` aren't valid, so requests with them
/// are notifications.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(untagged)]
pub enum Id<'a> {
    /// `null`
    Null,
    /// A number.
    Number(u64),
    /// A string.
    #[serde(borrow)]
    Str(Cow<'a, str>),
}

/// A method call.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Request<'a> {
    /// Always `"2.0"`.
    pub jsonrpc: TwoPointZero,
    /// The request ID.
    #[serde(borrow)]
    pub id: Id<'a>,
    /// The method name.
    #[serde(borrow)]
    pub method: Cow<'a, str>,
    /// The method parameters, if any.
    #[serde(borrow)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Cow<'a, RawValue>>,
}

impl<'a> Request<'a> {
    /// Returns a request for `method` with `params` and `id`.
    pub fn new(method: impl Into<Cow<'a, str>>, params: Option<Box<RawValue>>, id: Id<'a>) -> Self {
        Self {
            jsonrpc: TwoPointZero,
            id,
            method: method.into(),
            params: params.map(Cow::Owned),
        }
    }

    /// The method name.
    pub fn method_name(&self) -> &str {
        &self.method
    }

    /// The method parameters.
    pub fn params(&self) -> Params<'_> {
        Params::new(self.params.as_deref().map(RawValue::get))
    }
}

/// A method call without an ID, which gets no response.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Notification<'a> {
    /// Always `"2.0"`.
    pub jsonrpc: TwoPointZero,
    /// The method name.
    #[serde(borrow)]
    pub method: Cow<'a, str>,
    /// The method parameters, if any.
    #[serde(borrow)]
    pub params: Option<Cow<'a, RawValue>>,
}

impl<'a> Notification<'a> {
    /// Returns a notification for `method` with `params`.
    pub fn new(method: impl Into<Cow<'a, str>>, params: Option<Box<RawValue>>) -> Self {
        Self {
            jsonrpc: TwoPointZero,
            method: method.into(),
            params: params.map(Cow::Owned),
        }
    }
}

/// The ID of a request that isn't a valid call or notification.
#[derive(Deserialize)]
struct InvalidRequest<'a> {
    #[serde(borrow)]
    id: Id<'a>,
}

/// One entry of a batch request.
pub(crate) enum BatchEntry<'a> {
    /// A method call.
    Call(Request<'a>),
    /// A notification.
    Notification(Notification<'a>),
    /// An entry that isn't a call or notification, with its ID if it has a valid one.
    Invalid(Id<'a>),
}

/// A parsed request body.
pub(crate) enum Body<'a> {
    /// A single method call.
    Call(Request<'a>),
    /// A single notification.
    Notification(Notification<'a>),
    /// A batch of entries.
    Batch(Vec<BatchEntry<'a>>),
    /// A request that can't be handled, and the response to it.
    Error(MethodResponse),
}

impl<'a> Body<'a> {
    /// Parses a request body that starts with `{` if `is_single`, or `[` otherwise.
    pub(crate) fn parse(body: &'a [u8], is_single: bool, max_batch_entries: usize) -> Self {
        if is_single {
            if let Ok(request) = serde_json::from_slice(body) {
                Self::Call(request)
            } else if let Ok(notification) = serde_json::from_slice(body) {
                Self::Notification(notification)
            } else {
                let (id, code) = match serde_json::from_slice::<InvalidRequest>(body) {
                    Ok(InvalidRequest { id }) => (id, ErrorCode::InvalidRequest),
                    Err(_) => (Id::Null, ErrorCode::ParseError),
                };
                Self::Error(MethodResponse::error(id, code.into()))
            }
        } else {
            let Ok(entries) = serde_json::from_slice::<Vec<&RawValue>>(body) else {
                return Self::Error(MethodResponse::error(
                    Id::Null,
                    ErrorCode::ParseError.into(),
                ));
            };

            if entries.len() > max_batch_entries {
                return Self::Error(MethodResponse::server_error(
                    Id::Null,
                    TOO_BIG_BATCH_REQUEST_CODE,
                    TOO_BIG_BATCH_REQUEST_MSG,
                    max_batch_entries,
                ));
            }

            Self::Batch(
                entries
                    .into_iter()
                    .map(|entry| {
                        let entry = entry.get();
                        if let Ok(request) = serde_json::from_str(entry) {
                            BatchEntry::Call(request)
                        } else if let Ok(notification) = serde_json::from_str(entry) {
                            BatchEntry::Notification(notification)
                        } else {
                            let id = serde_json::from_str::<InvalidRequest>(entry)
                                .map_or(Id::Null, |invalid| invalid.id);
                            BatchEntry::Invalid(id)
                        }
                    })
                    .collect(),
            )
        }
    }
}

/// The raw JSON parameters of a method call.
#[derive(Clone, Copy, Debug)]
pub struct Params<'a>(Option<&'a str>);

impl<'a> Params<'a> {
    /// Returns the parameters in `raw`, or no parameters.
    pub fn new(raw: Option<&'a str>) -> Self {
        Self(raw.map(str::trim))
    }

    /// Whether the parameters are a JSON object, which is decoded by parameter name.
    pub fn is_object(&self) -> bool {
        self.0.is_some_and(|json| json.starts_with('{'))
    }

    /// Returns the positional parameters, one at a time.
    pub fn sequence(&self) -> ParamsSequence<'a> {
        ParamsSequence(match self.0 {
            Some("[]") | None => "",
            Some(json) => json,
        })
    }

    /// Deserializes all the parameters, or `null` if there aren't any.
    pub fn parse<T: Deserialize<'a>>(&self) -> RpcResult<T> {
        serde_json::from_str(self.0.unwrap_or("null")).map_err(|_| invalid_params())
    }
}

/// Positional parameters that haven't been decoded yet.
///
/// Parameters after the last decoded one are ignored, and they don't have to be valid JSON.
#[derive(Clone, Copy, Debug)]
pub struct ParamsSequence<'a>(&'a str);

impl<'a> ParamsSequence<'a> {
    /// Decodes the next parameter, or returns `None` if there are no more.
    fn next_inner<T: Deserialize<'a>>(&mut self) -> Option<RpcResult<T>> {
        let mut json = self.0;
        match json.as_bytes().first()? {
            b']' => {
                self.0 = "";
                return None;
            }
            b'[' | b',' => json = &json[1..],
            _ => return Some(Err(invalid_params())),
        }

        let mut values = serde_json::Deserializer::from_str(json).into_iter::<T>();
        match values.next()? {
            Ok(value) => {
                self.0 = json[values.byte_offset()..].trim_start();
                Some(Ok(value))
            }
            Err(_) => {
                self.0 = "";
                Some(Err(invalid_params()))
            }
        }
    }

    /// Decodes the next parameter, which must be present.
    pub fn required_next<T: Deserialize<'a>>(&mut self) -> RpcResult<T> {
        self.next_inner().unwrap_or_else(|| Err(invalid_params()))
    }

    /// Decodes the next parameter, or returns `None` if there are no more.
    pub fn optional_next<T: Deserialize<'a>>(&mut self) -> RpcResult<Option<T>> {
        self.next_inner::<Option<T>>().unwrap_or(Ok(None))
    }
}

/// The error for parameters that can't be decoded.
fn invalid_params() -> ErrorObject {
    ErrorCode::InvalidParams.into()
}

/// Decodes the next positional parameter of type `T`.
///
/// A trailing `Option` parameter can be left out, so [`NextParam`] decodes `Option<T>` with
/// [`ParamsSequence::optional_next`], and other types with [`ParamsSequence::required_next`].
/// Calling `(&NextParam::<T>::new()).next_param(sequence)` picks the first one that applies.
pub struct NextParam<T>(PhantomData<fn() -> T>);

impl<T> NextParam<T> {
    /// Returns a decoder for a parameter of type `T`.
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Default for NextParam<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for NextParam<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for NextParam<T> {}

/// Decodes an optional positional parameter.
pub trait NextOptionalParam {
    /// The parameter type.
    type Param;

    /// Decodes the parameter, or returns `None` if there are no more parameters.
    fn next_param(self, params: &mut ParamsSequence<'_>) -> RpcResult<Self::Param>;
}

impl<T: DeserializeOwned> NextOptionalParam for &NextParam<Option<T>> {
    type Param = Option<T>;

    fn next_param(self, params: &mut ParamsSequence<'_>) -> RpcResult<Option<T>> {
        params.optional_next()
    }
}

/// Decodes a required positional parameter.
pub trait NextRequiredParam {
    /// The parameter type.
    type Param;

    /// Decodes the parameter, which must be present.
    fn next_param(self, params: &mut ParamsSequence<'_>) -> RpcResult<Self::Param>;
}

impl<T: DeserializeOwned> NextRequiredParam for NextParam<T> {
    type Param = T;

    fn next_param(self, params: &mut ParamsSequence<'_>) -> RpcResult<T> {
        params.required_next()
    }
}

/// Returns whether `key` names the parameter `name`, as written, in snake case, or in lower
/// camel case.
///
/// For example, `_allow_high_fees` also matches `allow_high_fees` and `allowHighFees`.
pub fn param_name_matches(key: &str, name: &str) -> bool {
    let words = || name.split('_').filter(|word| !word.is_empty());

    if key == name {
        return true;
    }

    // Snake case: the words joined by `_`, without leading or repeated underscores.
    let mut snake_case = words().flat_map(|word| ["_", word]).skip(1);
    if key_matches(key, &mut snake_case) {
        return true;
    }

    // Lower camel case: the first word as is, and the rest capitalized.
    let mut rest = key;
    for (index, word) in words().enumerate() {
        let mut chars = word.chars();
        let first = chars.next().expect("words are not empty");
        let first = if index == 0 {
            first
        } else {
            first.to_ascii_uppercase()
        };

        let Some(after_first) = rest.strip_prefix(first) else {
            return false;
        };
        let Some(after_word) = after_first.strip_prefix(chars.as_str()) else {
            return false;
        };
        rest = after_word;
    }

    rest.is_empty()
}

/// Returns whether `key` is the concatenation of `parts`.
fn key_matches<'a>(key: &str, parts: &mut impl Iterator<Item = &'a str>) -> bool {
    let mut rest = key;
    for part in parts {
        let Some(after) = rest.strip_prefix(part) else {
            return false;
        };
        rest = after;
    }

    rest.is_empty()
}

/// Deserializes a parameter that is missing from a parameters object.
///
/// An `Option` parameter is `None`, and other types are a "missing field" error.
pub struct MissingParam<E> {
    name: &'static str,
    error: PhantomData<E>,
}

impl<E> MissingParam<E> {
    /// Returns a deserializer for the missing parameter `name`.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            error: PhantomData,
        }
    }
}

impl<'de, E: de::Error> Deserializer<'de> for MissingParam<E> {
    type Error = E;

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, E> {
        Err(E::missing_field(self.name))
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, E> {
        visitor.visit_none()
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

/// Logs a parameter that couldn't be decoded.
#[cold]
pub fn log_invalid_param(name: &str, ty: &str, optional: bool) {
    let optional = if optional { "optional " } else { "" };
    tracing::debug!("Error parsing {optional}\"{name}\" as \"{ty}\": invalid params");
}

/// Logs a parameters object that couldn't be decoded.
#[cold]
pub fn log_invalid_params_object() {
    tracing::debug!("Failed to parse JSON-RPC params as object: invalid params");
}

/// A response to a method call, notification, or batch, encoded as JSON.
#[derive(Debug)]
pub struct MethodResponse {
    json: Box<RawValue>,
    error_code: Option<i32>,
    is_notification: bool,
}

/// A response, which is serialized with its members in `jsonrpsee`'s order.
struct Response<'a, T> {
    id: &'a Id<'a>,
    payload: Payload<'a, T>,
}

/// The result or error of a response.
enum Payload<'a, T> {
    Result(&'a T),
    Error(WireError<'a>),
}

impl<T: Serialize> Serialize for Response<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut response = serializer.serialize_struct("Response", 3)?;
        response.serialize_field("jsonrpc", &TwoPointZero)?;
        response.serialize_field("id", self.id)?;
        match &self.payload {
            Payload::Result(result) => response.serialize_field("result", result)?,
            Payload::Error(error) => response.serialize_field("error", error)?,
        }
        response.end()
    }
}

impl MethodResponse {
    /// Returns the response to a method call with `id` that returned `result`.
    ///
    /// A response longer than `max_response_size` bytes is replaced by an error.
    pub fn response<T: Serialize>(id: Id, result: &RpcResult<T>, max_response_size: usize) -> Self {
        let (payload, error_code) = match result {
            Ok(result) => (Payload::Result(result), None),
            Err(error) => (
                Payload::Error(WireError {
                    code: error.code,
                    message: &error.message,
                    data: None,
                }),
                Some(error.code),
            ),
        };

        let mut writer = BoundedWriter::new(max_response_size);
        match serde_json::to_writer(&mut writer, &Response { id: &id, payload }) {
            Ok(()) => Self {
                json: writer.into_json(),
                error_code,
                is_notification: false,
            },
            Err(error) if error.is_io() => {
                tracing::error!("Error serializing response: {error:?}");
                Self::server_error(
                    id,
                    OVERSIZED_RESPONSE_CODE,
                    OVERSIZED_RESPONSE_MSG,
                    max_response_size,
                )
            }
            Err(error) => {
                tracing::error!("Error serializing response: {error:?}");
                Self::error(id, ErrorCode::InternalError.into())
            }
        }
    }

    /// Returns an error response with `id`.
    pub fn error(id: Id, error: ErrorObject) -> Self {
        Self::encode_error(
            &id,
            WireError {
                code: error.code,
                message: &error.message,
                data: None,
            },
        )
    }

    /// Returns a response with one of the server's own errors, which says what `limit` was
    /// exceeded.
    fn server_error(id: Id, code: i32, message: &str, limit: usize) -> Self {
        Self::encode_error(
            &id,
            WireError {
                code,
                message,
                data: Some(&format!("Exceeded max limit of {limit}")),
            },
        )
    }

    /// Returns the response to a request body longer than `limit` bytes.
    pub(crate) fn too_large_request(limit: u32) -> Self {
        Self::server_error(
            Id::Null,
            OVERSIZED_REQUEST_CODE,
            OVERSIZED_REQUEST_MSG,
            // A `u32` fits in a `usize` on supported platforms.
            limit as usize,
        )
    }

    fn encode_error(id: &Id, error: WireError) -> Self {
        let code = error.code;
        let json = serde_json::value::to_raw_value(&Response::<()> {
            id,
            payload: Payload::Error(error),
        })
        .expect("error responses only contain strings and integers");

        Self {
            json,
            error_code: Some(code),
            is_notification: false,
        }
    }

    /// Returns the response to a notification, which is `null`.
    pub fn notification() -> Self {
        Self {
            json: RawValue::NULL.to_owned(),
            error_code: None,
            is_notification: true,
        }
    }

    /// Whether this is an error response.
    pub fn is_error(&self) -> bool {
        self.error_code.is_some()
    }

    /// The error code of an error response.
    pub fn as_error_code(&self) -> Option<i32> {
        self.error_code
    }

    /// Whether this is the response to a notification.
    pub fn is_notification(&self) -> bool {
        self.is_notification
    }

    /// The response JSON.
    pub fn as_json(&self) -> &RawValue {
        &self.json
    }

    /// Returns the response JSON.
    pub fn into_json(self) -> Box<RawValue> {
        self.json
    }
}

/// Builds the response to a batch request, as long as it is shorter than a limit.
pub(crate) struct BatchResponseBuilder {
    json: String,
    max_response_size: usize,
}

impl BatchResponseBuilder {
    /// Returns an empty batch response that can be up to `max_response_size` bytes long.
    pub(crate) fn new(max_response_size: usize) -> Self {
        let mut json = String::with_capacity(2048);
        json.push('[');

        Self {
            json,
            max_response_size,
        }
    }

    /// Appends `response`, or returns an error response if the batch response gets too long.
    pub(crate) fn append(&mut self, response: MethodResponse) -> Result<(), MethodResponse> {
        let json = response.json.get();
        if self.json.len() + json.len() + 1 > self.max_response_size {
            return Err(MethodResponse::server_error(
                Id::Null,
                TOO_BIG_BATCH_RESPONSE_CODE,
                TOO_BIG_BATCH_RESPONSE_MSG,
                self.max_response_size,
            ));
        }

        self.json.push_str(json);
        self.json.push(',');
        Ok(())
    }

    /// Whether no responses have been appended.
    pub(crate) fn is_empty(&self) -> bool {
        self.json.len() <= 1
    }

    /// Returns the batch response, or an invalid request error if it is empty.
    pub(crate) fn finish(mut self) -> MethodResponse {
        if self.is_empty() {
            return MethodResponse::error(Id::Null, ErrorCode::InvalidRequest.into());
        }

        self.json.pop();
        self.json.push(']');

        MethodResponse {
            json: RawValue::from_string(self.json).expect("a list of JSON responses is valid JSON"),
            error_code: None,
            is_notification: false,
        }
    }
}

/// A writer that fails when more than `max_len` bytes are written.
struct BoundedWriter {
    max_len: usize,
    buf: Vec<u8>,
}

impl BoundedWriter {
    fn new(max_len: usize) -> Self {
        Self {
            max_len,
            buf: Vec::with_capacity(128),
        }
    }

    fn into_json(self) -> Box<RawValue> {
        let json = String::from_utf8(self.buf).expect("serde_json writes UTF-8");
        RawValue::from_string(json).expect("serde_json writes valid JSON")
    }
}

impl io::Write for &mut BoundedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.buf.len() + buf.len() > self.max_len {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "Memory capacity exceeded",
            ));
        }

        self.buf.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Defines the RPC method trait, and how JSON-RPC calls are dispatched to its methods.
///
/// The trait is defined as written, with `async` methods made object-safe by `async_trait`.
/// The macro also implements [`RpcMethods`](super::rpc_service::RpcMethods) for every type that
/// implements the trait. That implementation decodes each method's parameters from either:
/// - a JSON array, in order, where trailing `Option` parameters can be left out, or
/// - a JSON object, by parameter name in snake case or lower camel case, where `Option`
///   parameters can be left out.
///
/// Methods without parameters ignore any parameters they are given.
macro_rules! rpc_trait {
    (
        $(#[doc = $trait_doc:literal])*
        pub trait $trait:ident {
            $(
                $(#[doc = $doc:literal])*
                #[method(name = $name:literal)]
                $(#[doc = $doc_after:literal])*
                $(async $(@$async:tt)?)? fn $method:ident(&self $(, $arg:ident: $ty:ty)* $(,)?)
                    -> $ret:ty;
            )*
        }
    ) => {
        $(#[doc = $trait_doc])*
        #[::async_trait::async_trait]
        pub trait $trait {
            $(
                $(#[doc = $doc])*
                $(#[doc = $doc_after])*
                $(async $($async)?)? fn $method(&self $(, $arg: $ty)*) -> $ret;
            )*
        }

        impl<T> $crate::server::rpc_service::RpcMethods for T
        where
            T: $trait + Clone + Send + Sync + 'static,
        {
            const METHOD_NAMES: &'static [&'static str] = &[$($name),*];

            fn call<'a>(
                &'a self,
                method: &str,
                params: $crate::server::jsonrpc::Params<'a>,
                id: $crate::server::jsonrpc::Id<'a>,
                max_response_size: usize,
            ) -> Option<
                std::pin::Pin<
                    Box<
                        dyn std::future::Future<Output = $crate::server::jsonrpc::MethodResponse>
                            + Send
                            + 'a,
                    >,
                >,
            > {
                match method {
                    $(
                        $name => Some(Box::pin(async move {
                            let result = match $crate::server::jsonrpc::rpc_trait!(
                                @decode params; $($arg: $ty),*
                            ) {
                                Ok(($($arg,)*)) => $crate::server::jsonrpc::rpc_trait!(
                                    @call self.$method($($arg),*) $(, async $($async)?)?
                                ),
                                Err(error) => Err(error),
                            };

                            $crate::server::jsonrpc::MethodResponse::response(
                                id,
                                &result,
                                max_response_size,
                            )
                        })),
                    )*
                    _ => None,
                }
            }
        }

        /// Methods that decode their parameters exactly like the [`RpcMethods`] implementation
        /// for this trait does, then return `null` without running the method.
        ///
        /// Fuzz targets use it to reach parameter decoding without a node.
        ///
        /// [`RpcMethods`]: $crate::server::rpc_service::RpcMethods
        #[cfg(feature = "fuzz-impl")]
        #[derive(Clone, Copy, Debug)]
        pub struct DecodeParamsOnly;

        #[cfg(feature = "fuzz-impl")]
        impl $crate::server::rpc_service::RpcMethods for DecodeParamsOnly {
            const METHOD_NAMES: &'static [&'static str] = &[$($name),*];

            fn call<'a>(
                &'a self,
                method: &str,
                params: $crate::server::jsonrpc::Params<'a>,
                id: $crate::server::jsonrpc::Id<'a>,
                max_response_size: usize,
            ) -> Option<
                std::pin::Pin<
                    Box<
                        dyn std::future::Future<Output = $crate::server::jsonrpc::MethodResponse>
                            + Send
                            + 'a,
                    >,
                >,
            > {
                match method {
                    $(
                        $name => Some(Box::pin(async move {
                            let result = $crate::server::jsonrpc::rpc_trait!(
                                @decode params; $($arg: $ty),*
                            )
                            .map(|_| ());

                            $crate::server::jsonrpc::MethodResponse::response(
                                id,
                                &result,
                                max_response_size,
                            )
                        })),
                    )*
                    _ => None,
                }
            }
        }
    };

    (@call $call:expr, async) => { $call.await };
    (@call $call:expr) => { $call };

    (@decode $params:ident;) => {
        $crate::server::jsonrpc::RpcResult::Ok(())
    };
    (@decode $params:ident; $($arg:ident: $ty:ty),+) => {
        if $params.is_object() {
            struct ParamsObject {
                $($arg: $ty,)+
            }

            impl<'de> ::serde::Deserialize<'de> for ParamsObject {
                fn deserialize<D: ::serde::Deserializer<'de>>(
                    deserializer: D,
                ) -> ::core::result::Result<Self, D::Error> {
                    struct ParamsVisitor;

                    impl<'de> ::serde::de::Visitor<'de> for ParamsVisitor {
                        type Value = ParamsObject;

                        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                            f.write_str("struct ParamsObject")
                        }

                        fn visit_map<A: ::serde::de::MapAccess<'de>>(
                            self,
                            mut map: A,
                        ) -> ::core::result::Result<ParamsObject, A::Error> {
                            $(let mut $arg: Option<$ty> = None;)+

                            while let Some(key) = map.next_key::<std::borrow::Cow<'de, str>>()? {
                                $(
                                    if $crate::server::jsonrpc::param_name_matches(
                                        &key,
                                        stringify!($arg),
                                    ) {
                                        if $arg.is_some() {
                                            return Err(::serde::de::Error::duplicate_field(
                                                stringify!($arg),
                                            ));
                                        }
                                        $arg = Some(map.next_value()?);
                                        continue;
                                    }
                                )+
                                map.next_value::<::serde::de::IgnoredAny>()?;
                            }

                            Ok(ParamsObject {
                                $(
                                    $arg: match $arg {
                                        Some(value) => value,
                                        None => ::serde::Deserialize::deserialize(
                                            $crate::server::jsonrpc::MissingParam::new(
                                                stringify!($arg),
                                            ),
                                        )?,
                                    },
                                )+
                            })
                        }
                    }

                    deserializer.deserialize_map(ParamsVisitor)
                }
            }

            match $params.parse::<ParamsObject>() {
                Ok(parsed) => Ok(($(parsed.$arg,)+)),
                Err(error) => {
                    $crate::server::jsonrpc::log_invalid_params_object();
                    Err(error)
                }
            }
        } else {
            #[allow(unused_imports)]
            use $crate::server::jsonrpc::{NextOptionalParam as _, NextRequiredParam as _};

            let mut sequence = $params.sequence();
            (|| -> $crate::server::jsonrpc::RpcResult<_> {
                $(
                    let $arg: $ty = (&$crate::server::jsonrpc::NextParam::<$ty>::new())
                        .next_param(&mut sequence)
                        .inspect_err(|_| {
                            $crate::server::jsonrpc::log_invalid_param(
                                stringify!($arg),
                                stringify!($ty),
                                stringify!($ty).starts_with("Option"),
                            )
                        })?;
                )+

                Ok(($($arg,)+))
            })()
        }
    };
}

pub(crate) use rpc_trait;
