//! Types used in the `getstandardfee` RPC method.

/// Response to a `getstandardfee` RPC request.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GetStandardFeeResponse {
    /// Recommended fee per logical action, in zatoshis.
    pub standard_fee: u64,

    /// Estimator version identifier.
    pub version: u32,
}
