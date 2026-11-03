//! Interpreting supported RPC formats after HTTP request admission.

use std::{borrow::Cow, io};

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use super::{Body, Id, MethodResponse, Notification, Request, TwoPointZero};

/// A decoded message and the response format selected by the original request.
pub(in crate::server) struct PreparedRequest<'a> {
    pub(in crate::server) message: Body<'a>,
    pub(in crate::server) response_format: ResponseFormat,
}

/// Failures that occur before a JSON-RPC message can be dispatched.
#[derive(Debug)]
pub(in crate::server) enum PreparationError {
    /// The raw fallback does not start with an object or array in the first 128 bytes.
    MalformedBody,
    /// The raw or normalized request would exceed the inner HTTP body limit.
    InnerBodyTooLarge(ResponseFormat),
}

/// The response envelope expected by a supported client.
#[derive(Clone, Copy, Debug)]
pub(in crate::server) enum ResponseFormat {
    Bitcoind,
    Lightwalletd,
    TwoPointZero,
    Unknown,
}

/// Decodes a collected body without constructing and reparsing a replacement request.
///
/// The caller must hold its request permit. Recognized single calls retain the old
/// normalization rules; batches and unrecognized messages use the strict raw parser.
pub(in crate::server) fn prepare(
    body: &[u8],
    max_batch_entries: usize,
    max_body_size: usize,
) -> Result<PreparedRequest<'_>, PreparationError> {
    if let Some(call) = borrow_plain_call(body) {
        if body.len() > max_body_size {
            return Err(PreparationError::InnerBodyTooLarge(
                ResponseFormat::TwoPointZero,
            ));
        }
        return Ok(PreparedRequest {
            message: message(
                call.method,
                call.params.map(Cow::Borrowed),
                call.id.and_then(|id| serde_json::from_str(id.get()).ok()),
            ),
            response_format: ResponseFormat::TwoPointZero,
        });
    }

    if let Ok(request) = serde_json::from_slice::<CompatibleRequest>(body) {
        let response_format = request.response_format();
        if !matches!(response_format, ResponseFormat::Unknown) {
            return Ok(PreparedRequest {
                message: request.into_message(max_body_size, response_format)?,
                response_format,
            });
        }
    }

    if body.len() > max_body_size {
        return Err(PreparationError::InnerBodyTooLarge(ResponseFormat::Unknown));
    }

    // Recognized calls previously lost their leading whitespace during conversion.
    // Only the unchanged fallback retains the HTTP reader's first-128-byte rule.
    let Some((start, first)) = body
        .iter()
        .take(128)
        .enumerate()
        .find(|(_, byte)| !byte.is_ascii_whitespace())
    else {
        return Err(PreparationError::MalformedBody);
    };
    let is_single = match first {
        b'{' => true,
        b'[' => false,
        _ => return Err(PreparationError::MalformedBody),
    };
    Ok(PreparedRequest {
        message: Body::parse(&body[start..], is_single, max_batch_entries),
        response_format: ResponseFormat::Unknown,
    })
}

/// Selects the format for an HTTP-level error that precedes JSON-RPC dispatch.
///
/// This is used only for an oversized declared content length; ordinary requests are
/// prepared once, while HTTP method and content-type errors remain plain text.
pub(in crate::server) fn response_format(body: &[u8]) -> ResponseFormat {
    if borrow_plain_call(body).is_some() {
        ResponseFormat::TwoPointZero
    } else {
        serde_json::from_slice::<CompatibleRequest>(body)
            .map_or(ResponseFormat::Unknown, |request| request.response_format())
    }
}

/// Borrows a JSON-RPC 2.0 call whose request fields need no normalization.
///
/// The server reads a few things differently from their re-encoded form: whitespace inside
/// positional parameters, duplicate or reordered named parameters, leading whitespace, a `null`
/// ID (which re-encoding drops) and unknown fields (which re-encoding drops). So this accepts
/// only a body that starts with `{`, has no other fields, has a non-`null` ID or none, and has
/// no parameters, an empty array, or one unescaped string argument. This avoids copying and
/// re-encoding a block's hex in `submitblock` without inspecting each array element.
fn borrow_plain_call(body: &[u8]) -> Option<PlainJsonRpc2Call<'_>> {
    if body.first() != Some(&b'{') {
        return None;
    }

    let call = serde_json::from_slice::<'_, PlainJsonRpc2Call<'_>>(body).ok()?;

    if call.jsonrpc != "2.0" {
        return None;
    }

    let id_is_absent_or_string_or_number = call.id.is_none_or(|id| {
        // RawValue is valid JSON; the first byte distinguishes strings and numbers.
        matches!(id.get().as_bytes().first(), Some(b'"' | b'-' | b'0'..=b'9'))
    });

    (id_is_absent_or_string_or_number && call.params.is_none_or(params_can_pass_through_unchanged))
        .then_some(call)
}

/// Returns whether parameters are `[]` or one compact, unescaped string argument.
///
/// `RawValue` already contains valid JSON. Checking its delimiters and string contents avoids
/// allocating per argument or parsing and serializing values before falling back to conversion.
fn params_can_pass_through_unchanged(params: &RawValue) -> bool {
    let json = params.get();

    if json == "[]" {
        return true;
    }

    let Some(value) = json
        .strip_prefix("[\"")
        .and_then(|value| value.strip_suffix("\"]"))
    else {
        return false;
    };

    !value.bytes().any(|byte| matches!(byte, b'"' | b'\\'))
}

/// A JSON-RPC 2.0 call with only the standard fields, borrowed from the request body.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PlainJsonRpc2Call<'a> {
    #[serde(borrow)]
    jsonrpc: Cow<'a, str>,
    /// Required, so that a call without a method isn't treated as a plain call.
    #[serde(borrow)]
    method: Cow<'a, str>,
    #[serde(borrow, default)]
    params: Option<&'a RawValue>,
    /// `Some` whenever the field is present, including `null`.
    #[serde(borrow, default, deserialize_with = "present")]
    id: Option<&'a RawValue>,
}

/// Deserializes a field that is present, even if it is `null`, as `Some`.
fn present<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<&'de RawValue>, D::Error> {
    <&RawValue>::deserialize(deserializer).map(Some)
}

/// The existing compatibility interpretation of a single request.
#[derive(Deserialize)]
struct CompatibleRequest {
    jsonrpc: Option<String>,
    method: String,
    params: Option<serde_json::Value>,
    id: Option<serde_json::Value>,
}

impl CompatibleRequest {
    fn response_format(&self) -> ResponseFormat {
        match (self.jsonrpc.as_deref(), &self.params, &self.id) {
            (
                Some("2.0"),
                _,
                None
                | Some(
                    serde_json::Value::Null
                    | serde_json::Value::String(_)
                    | serde_json::Value::Number(_),
                ),
            ) => ResponseFormat::TwoPointZero,
            (Some("1.0"), Some(_), Some(_)) => ResponseFormat::Lightwalletd,
            (None, Some(_), Some(_)) => ResponseFormat::Bitcoind,
            _ => ResponseFormat::Unknown,
        }
    }

    fn into_message(
        self,
        max_body_size: usize,
        response_format: ResponseFormat,
    ) -> Result<Body<'static>, PreparationError> {
        let (params, body_size) = self.normalize_and_measure();
        if body_size > max_body_size {
            return Err(PreparationError::InnerBodyTooLarge(response_format));
        }

        let id = match self.id {
            Some(serde_json::Value::String(id)) => Some(Id::Str(Cow::Owned(id))),
            Some(serde_json::Value::Number(id)) => id.as_u64().map(Id::Number),
            _ => None,
        };
        let method = Cow::Owned(self.method);
        let params = params.map(Cow::Owned);
        Ok(message(method, params, id))
    }

    /// Normalizes parameters and measures the request without creating a replacement envelope.
    fn normalize_and_measure(&self) -> (Option<Box<RawValue>>, usize) {
        let params = self
            .params
            .as_ref()
            .map(serde_json::value::to_raw_value)
            .transpose()
            .expect("JSON values serialize as valid JSON");

        // Count only envelope metadata; the already-normalized parameters contribute their
        // byte length directly. No replacement request document is allocated or reparsed.
        #[derive(Serialize)]
        struct Metadata<'a> {
            jsonrpc: &'static str,
            method: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            id: Option<&'a serde_json::Value>,
        }
        let mut size = ByteCounter::default();
        serde_json::to_writer(
            &mut size,
            &Metadata {
                jsonrpc: "2.0",
                method: &self.method,
                id: self.id.as_ref(),
            },
        )
        .expect("request metadata contains valid JSON values");
        if let Some(params) = &params {
            size.0 = size
                .0
                .saturating_add(b",\"params\":".len())
                .saturating_add(params.get().len());
        }
        (params, size.0)
    }
}

/// Constructs the same call or ignored notification as the strict request decoder.
fn message<'a>(
    method: Cow<'a, str>,
    params: Option<Cow<'a, RawValue>>,
    id: Option<Id<'a>>,
) -> Body<'a> {
    match id {
        Some(id) => Body::Call(Request {
            jsonrpc: TwoPointZero,
            id,
            method,
            params,
        }),
        None => Body::Notification(Notification {
            jsonrpc: TwoPointZero,
            method,
            params,
        }),
    }
}

#[derive(Default)]
struct ByteCounter(usize);

impl io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Borrowed response fields, in the existing compatibility serializer's order.
#[derive(Deserialize, Serialize)]
struct CompatibleResponse<'a> {
    #[serde(borrow, skip_serializing_if = "Option::is_none")]
    jsonrpc: Option<Cow<'a, str>>,
    #[serde(borrow)]
    id: &'a RawValue,
    #[serde(borrow, skip_serializing_if = "Option::is_none")]
    result: Option<&'a RawValue>,
    #[serde(borrow, skip_serializing_if = "Option::is_none")]
    error: Option<&'a RawValue>,
}

impl ResponseFormat {
    /// Formats a JSON-RPC response before it becomes an HTTP body.
    pub(in crate::server) fn format_response(self, mut response: MethodResponse) -> MethodResponse {
        if matches!(self, Self::TwoPointZero) {
            return response;
        }
        let Ok(mut wire) = serde_json::from_str::<CompatibleResponse>(response.json.get()) else {
            return response;
        };
        match self {
            Self::Bitcoind => {
                wire.jsonrpc = None;
                wire.result = wire.result.or(Some(RawValue::NULL));
                wire.error = wire.error.or(Some(RawValue::NULL));
            }
            Self::Lightwalletd => {
                wire.jsonrpc = Some(Cow::Borrowed("1.0"));
                wire.result = wire.result.or(Some(RawValue::NULL));
                wire.error = wire.error.or(Some(RawValue::NULL));
            }
            // The old unknown-version response conversion omitted explicit null fields.
            Self::Unknown | Self::TwoPointZero => (),
        }
        response.json = serde_json::value::to_raw_value(&wire)
            .expect("borrowed response fields serialize as valid JSON");
        response
    }
}

#[cfg(test)]
mod tests;
