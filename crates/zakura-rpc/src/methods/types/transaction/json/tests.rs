use super::*;
use zakura_chain::{
    amount::NonNegative,
    transaction::{AuthDigest, Hash},
};

fn transaction(bytes: Vec<u8>) -> TransactionTemplate<NonNegative> {
    TransactionTemplate::new(
        bytes.into(),
        Hash([1; 32]),
        AuthDigest([2; 32]),
        vec![],
        800u64.try_into().unwrap(),
        0,
        false,
    )
}

#[test]
fn ordinary_serializers_do_not_initialize_json() {
    let tx = transaction(vec![0, 0x22, 0x5c, 0xff]);
    let normal = serde_json::to_string(&tx).unwrap();
    assert!(tx.data.0.json.get().is_none());
    assert_eq!(
        serde_json::to_string(&JsonTransaction(&tx)).unwrap(),
        normal
    );
    assert!(tx.data.0.json.get().is_some());
    assert_eq!(serde_json::to_value(&tx).unwrap()["data"], "00225cff");
    let decoded: TransactionTemplate<NonNegative> = serde_json::from_str(&normal).unwrap();
    assert_eq!(decoded, tx);
    assert!(decoded.data.0.json.get().is_none());
}

#[test]
fn cloned_templates_share_payload_but_keep_fresh_metadata() {
    let tx = transaction(vec![0, 0xff]);
    let mut cloned = tx.clone();
    assert!(Arc::ptr_eq(&tx.data.0, &cloned.data.0));
    cloned.depends = vec![1, 3];
    cloned.fee = 1_600u64.try_into().unwrap();
    cloned.sigops = 3;
    let value = serde_json::to_value(JsonTransaction(&cloned)).unwrap();
    assert_eq!(value["data"], "00ff");
    assert_eq!(value["depends"], serde_json::json!([1, 3]));
    assert_eq!(value["fee"], 1_600);
    assert_eq!(value["sigops"], 3);
    assert!(tx.depends.is_empty());
    let different = transaction(vec![0, 0xfe]);
    assert_ne!(
        serde_json::to_string(&JsonTransaction(&different)).unwrap(),
        serde_json::to_string(&JsonTransaction(&tx)).unwrap()
    );
}

#[test]
fn concurrent_first_readers_share_one_checked_fragment() {
    let tx = transaction(vec![0xff; 9_166]);
    let barrier = std::sync::Barrier::new(8);
    std::thread::scope(|scope| {
        let readers: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    tx.data.json()
                })
            })
            .collect();
        for reader in readers {
            assert!(std::ptr::eq(reader.join().unwrap(), tx.data.json()));
        }
    });
}

#[test]
fn discarded_templates_release_bytes_and_json_with_the_last_reader() {
    let tx = transaction(vec![0xff; 9_166]);
    let weak = Arc::downgrade(&tx.data.0);
    let reader = tx.clone();
    assert!(tx.data.0.json.get().is_none());
    drop(tx);
    assert!(weak.upgrade().is_some());
    reader.data.json();
    drop(reader);
    assert!(weak.upgrade().is_none());
}
