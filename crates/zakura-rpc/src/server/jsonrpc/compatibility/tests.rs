//! Compatibility semantics at the admitted JSON decoder boundary.

use super::*;
use crate::server::jsonrpc::ErrorObject;

const INNER_LIMIT: usize = 10 * 1024 * 1024;

#[test]
fn large_block_string_stays_borrowed() {
    let block = "00".repeat(2_000_000);
    let body = format!(r#"{{"jsonrpc":"2.0","method":"submitblock","params":["{block}"],"id":1}}"#);
    let parsed =
        prepare(body.as_bytes(), 1000, INNER_LIMIT).unwrap_or_else(|_| panic!("valid call"));
    let Body::Call(call) = parsed.message else {
        panic!("expected a call")
    };
    assert!(matches!(call.params, Some(Cow::Borrowed(_))));
    let value: &str = call.params().sequence().required_next().unwrap();
    assert_eq!(value, block);
}

#[test]
fn parameters_keep_existing_normalization() {
    for (params, normalized) in [
        ("[ ]", "[]"),
        (r#"["\u0041"]"#, r#"["A"]"#),
        ("[1e2]", "[100.0]"),
        (r#"{"a":1,"a":2}"#, r#"{"a":2}"#),
        (r#"["a","b"]"#, r#"["a","b"]"#),
    ] {
        let body = format!(r#"{{"jsonrpc":"2.0","method":"getinfo","params":{params},"id":1}}"#);
        let parsed =
            prepare(body.as_bytes(), 1000, INNER_LIMIT).unwrap_or_else(|_| panic!("valid call"));
        let Body::Call(call) = parsed.message else {
            panic!("expected a call")
        };
        assert_eq!(call.params.as_deref().unwrap().get(), normalized);
    }
}

#[test]
fn near_limit_numeric_array_uses_one_normalized_parameter_document() {
    let numbers = "0,".repeat(1_999_999);
    let body =
        format!(r#"{{"jsonrpc":"2.0","method":"submitblock","params":[{numbers}1e2],"id":1}}"#);
    let parsed =
        prepare(body.as_bytes(), 1000, INNER_LIMIT).unwrap_or_else(|_| panic!("valid call"));
    let Body::Call(call) = parsed.message else {
        panic!("expected a call")
    };
    assert_eq!(
        call.params.as_deref().unwrap().get(),
        format!("[{numbers}100.0]")
    );
}

#[test]
fn normalization_retains_the_inner_body_limit() {
    let numbers = "1e9,".repeat(200);
    let body = format!(r#"{{"jsonrpc":"2.0","method":"x","params":[{numbers}0],"id":1}}"#);
    assert!(body.len() < 1024);
    assert!(matches!(
        prepare(body.as_bytes(), 1000, 1024),
        Err(PreparationError::InnerBodyTooLarge(_))
    ));
}

#[test]
fn recognized_calls_and_raw_fallback_keep_different_leading_whitespace_rules() {
    for (version, recognized) in [("2.0", true), ("3.0", false)] {
        let body = format!(
            r#"{}{{"jsonrpc":"{version}","method":"getinfo","params":[],"id":1}}"#,
            " ".repeat(200)
        );
        assert_eq!(
            prepare(body.as_bytes(), 1000, INNER_LIMIT).is_ok(),
            recognized
        );
    }
}

#[test]
fn null_and_invalid_numeric_ids_remain_notifications() {
    for id in ["null", "-1", "1.5", "1e2", "18446744073709551616"] {
        let body = format!(r#"{{"jsonrpc":"2.0","method":"getinfo","params":[],"id":{id}}}"#);
        let parsed =
            prepare(body.as_bytes(), 1000, INNER_LIMIT).unwrap_or_else(|_| panic!("valid body"));
        assert!(matches!(parsed.message, Body::Notification(_)), "{id}");
    }
}

#[test]
fn legacy_and_unknown_response_bytes_stay_compatible() {
    for (format, expected) in [
        (
            ResponseFormat::Bitcoind,
            r#"{"id":1,"result":null,"error":null}"#,
        ),
        (
            ResponseFormat::Lightwalletd,
            r#"{"jsonrpc":"1.0","id":1,"result":null,"error":null}"#,
        ),
        (
            ResponseFormat::TwoPointZero,
            r#"{"jsonrpc":"2.0","id":1,"result":null}"#,
        ),
        (ResponseFormat::Unknown, r#"{"jsonrpc":"2.0","id":1}"#),
    ] {
        let response =
            MethodResponse::response(Id::Number(1), &Ok::<_, ErrorObject>(()), INNER_LIMIT);
        assert_eq!(format.format_response(response).as_json().get(), expected);
    }
}
