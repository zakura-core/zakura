//! Typed JSONL events emitted by the zakurad Zakura sync drivers.

pub(crate) mod block_driver;
pub(crate) mod chain_tip_mirror;
pub(crate) mod header_driver;

#[cfg(test)]
mod tests {
    #[test]
    fn driver_core_uses_only_semantic_trace_calls() {
        for (name, source) in [
            ("block_sync_driver", include_str!("block_sync_driver.rs")),
            ("header_sync_driver", include_str!("header_sync_driver.rs")),
        ] {
            for forbidden in [
                ".emit_event(",
                "emit_commit_state",
                "insert_cs_",
                "commit_state_trace",
                "serde_json::",
            ] {
                assert!(
                    !source.contains(forbidden),
                    "{name} must not contain trace construction `{forbidden}`"
                );
            }
        }
    }
}
