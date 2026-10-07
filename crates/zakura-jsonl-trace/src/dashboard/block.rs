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
        Self::start_if(true, hash, name)
    }

    /// Skip replay or speculative work without formatting a hash or emitting events.
    pub fn start_if(include: bool, hash: impl FnOnce() -> String, name: &'static str) -> Self {
        if !include || !super::enabled() {
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

#[cfg(test)]
mod tests {
    use super::BlockStage;

    #[test]
    fn excluded_stage_never_formats_or_emits() {
        let stage = BlockStage::start_if(false, || panic!("excluded hash formatter"), "replay");
        assert!(stage.0.is_none());
        stage.finish(true);
    }
}
