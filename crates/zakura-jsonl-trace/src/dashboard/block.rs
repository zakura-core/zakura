//! Correlated boundaries for one measured block-processing stage.

use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_STAGE: AtomicU64 = AtomicU64::new(1);

/// A stage occurrence, separate from driver or state admission attempt IDs.
/// Missing completion remains incomplete rather than becoming a zero duration.
pub struct BlockStage(Option<Stage>);

struct Stage {
    hash: String,
    name: &'static str,
    token: u64,
}

impl BlockStage {
    /// Start a stage. The hash formatter is only called when telemetry is enabled.
    pub fn start(hash: impl FnOnce() -> String, name: &'static str) -> Self {
        if !super::enabled() {
            return Self(None);
        }
        let stage = Stage {
            hash: hash(),
            name,
            token: NEXT_STAGE.fetch_add(1, Ordering::Relaxed),
        };
        super::emit(|| {
            serde_json::json!({
                "event": "block_stage_started", "hash": stage.hash,
                "stage": stage.name, "stage_token": stage.token,
            })
        });
        Self(Some(stage))
    }

    /// Finish this occurrence, retaining failure without exposing error contents.
    pub fn finish(self, success: bool) {
        if let Some(stage) = self.0 {
            super::emit(|| {
                serde_json::json!({
                    "event": "block_stage_finished", "hash": stage.hash,
                    "stage": stage.name, "stage_token": stage.token, "success": success,
                })
            });
        }
    }
}
