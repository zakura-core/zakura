//! Shared verification evidence and compatibility with earlier detailed profiles.

use serde::{Deserialize, Serialize};

/// The shielded pool whose bundle is checked.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Pool {
    /// Sapling shielded bundles.
    Sapling,
    /// Orchard shielded bundles.
    Orchard,
    /// Ironwood shielded bundles.
    Ironwood,
}
/// Counts of the work submitted, not transaction payloads.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Workload {
    /// Sapling spend descriptions.
    pub spends: u32,
    /// Sapling output descriptions.
    pub outputs: u32,
    /// Orchard or Ironwood actions.
    pub actions: u32,
}
/// Actual cache decision. Unknown is not evidence of a hit.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Cache {
    #[default]
    /// No cache decision was recorded.
    Unknown,
    /// An existing successful verification was reused.
    Hit,
    /// The key was absent and verification was requested.
    Miss,
    /// No cache key was available.
    Bypass,
}
/// Completion of a measured operation, independent of consensus validity.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// The operation returned success.
    Success,
    /// The operation returned an error or failed verification.
    Failed,
    #[default]
    /// No result was observed before ownership ended.
    Abandoned,
}
/// Fixed-size optional evidence carried by one span.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Detail {
    /// One bundle request, including cache and shared execution links.
    Request {
        /// Pool containing this bundle.
        pool: Pool,
        /// Work represented by this request or the entire shared batch.
        workload: Workload,
        /// Actual cache lookup outcome.
        cache: Cache,
        /// Observed completion status.
        status: Status,
        /// First shared batch, if successfully admitted.
        primary_batch: Option<u64>,
        /// Individual retry batch, if one was recorded.
        fallback_batch: Option<u64>,
        #[serde(default)]
        /// Whether the fallback service was entered, even before batch admission.
        fallback: bool,
        /// Some evidence or ownership links could not be retained.
        partial: bool,
    },
    /// One shared execution projected into a participating block attempt.
    Batch {
        /// Run-unique shared batch identity.
        id: u64,
        /// Work represented by this request or the entire shared batch.
        workload: Workload,
        /// Total submitted requests; partial evidence may include rejected preparation.
        members: u32,
        /// Requests carrying a live block profile context.
        profiled: u32,
        /// Requests without a live block profile context.
        unprofiled: u32,
        /// Sapling member count.
        sapling: u32,
        /// Orchard member count.
        orchard: u32,
        /// Ironwood member count.
        ironwood: u32,
        /// Flush request offset from the run epoch.
        flush_us: Option<u64>,
        /// Worker submission offset from the run epoch.
        dispatch_us: Option<u64>,
        /// Worker execution start offset.
        worker_start_us: Option<u64>,
        /// End of worker setup and start of combined cryptography.
        setup_end_us: Option<u64>,
        /// Combined proof and signature validation completion.
        execution_end_us: Option<u64>,
        /// Result publication offset, absent when abandoned.
        published_us: Option<u64>,
        /// Observed completion status.
        status: Status,
        /// Some evidence or ownership links could not be retained.
        partial: bool,
    },
}
impl Detail {
    /// Validate external metadata without assuming all optional phases were captured.
    pub fn is_valid(&self, start_us: u64, end_us: u64) -> bool {
        if end_us < start_us {
            return false;
        }
        match self {
            Self::Request {
                primary_batch,
                fallback_batch,
                ..
            } => {
                primary_batch.is_none_or(|id| id > 0)
                    && fallback_batch.is_none_or(|id| id > 0)
                    && (fallback_batch.is_none() || primary_batch != fallback_batch)
            }
            Self::Batch {
                id,
                members,
                profiled,
                unprofiled,
                sapling,
                orchard,
                ironwood,
                flush_us,
                dispatch_us,
                worker_start_us,
                setup_end_us,
                execution_end_us,
                published_us,
                ..
            } => {
                let mut previous = start_us;
                *id > 0
                    && u64::from(*profiled) + u64::from(*unprofiled) == u64::from(*members)
                    && u64::from(*sapling) + u64::from(*orchard) + u64::from(*ironwood)
                        == u64::from(*members)
                    && [
                        flush_us,
                        dispatch_us,
                        worker_start_us,
                        setup_end_us,
                        execution_end_us,
                        published_us,
                    ]
                    .into_iter()
                    .flatten()
                    .all(|time| {
                        let valid = *time >= previous && *time <= end_us;
                        previous = *time;
                        valid
                    })
            }
        }
    }
}
