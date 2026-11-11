use super::*;

fn template() -> GetBlockTemplateResponse {
    serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/vectors/getblocktemplate_response_template.json"
    )))
    .unwrap()
}

#[test]
fn live_json_adapter_preserves_complete_responses_and_limits() {
    let mut result = Ok(template());
    for submit_old in [None, Some(false), Some(true)] {
        if let Ok(GetBlockTemplateResponse::TemplateMode(template)) = &mut result {
            template.submit_old = submit_old;
            template.work_id = "work\"\\\n".to_owned();
        }
        let normal = MethodResponse::response(Id::Str("id\"\\\n".into()), &result, usize::MAX);
        let limit = normal.as_json().get().len();
        for _ in 0..2 {
            let actual = encode_response(Id::Str("id\"\\\n".into()), &result, limit);
            assert_eq!(actual.as_json().get(), normal.as_json().get());
            assert!(encode_response(Id::Str("id\"\\\n".into()), &result, limit - 1).is_error());
        }
    }
}

#[test]
fn changed_coinbase_bytes_are_not_reused() {
    let mut result = Ok(template());
    let original = encode_response(Id::Null, &result, usize::MAX);
    if let Ok(GetBlockTemplateResponse::TemplateMode(template)) = &mut result {
        let mut bytes = template.coinbase_txn.data.as_ref().to_vec();
        bytes.push(0);
        template.coinbase_txn.data = bytes.into();
    }
    let changed = encode_response(Id::Null, &result, usize::MAX);
    assert_ne!(changed.as_json().get(), original.as_json().get());
    assert_eq!(
        changed.as_json().get(),
        MethodResponse::response(Id::Null, &result, usize::MAX)
            .as_json()
            .get()
    );
}

#[test]
fn proposal_results_and_errors_keep_their_wire_format() {
    for json in ["null", "\"rejected\""] {
        let result = Ok(serde_json::from_str::<GetBlockTemplateResponse>(json).unwrap());
        assert_eq!(
            encode_response(Id::Number(7), &result, usize::MAX)
                .as_json()
                .get(),
            MethodResponse::response(Id::Number(7), &result, usize::MAX)
                .as_json()
                .get()
        );
    }
    let error = Err(crate::server::jsonrpc::ErrorCode::InvalidParams.into());
    assert_eq!(
        encode_response(Id::Number(7), &error, usize::MAX)
            .as_json()
            .get(),
        MethodResponse::response::<GetBlockTemplateResponse>(Id::Number(7), &error, usize::MAX)
            .as_json()
            .get()
    );
}
