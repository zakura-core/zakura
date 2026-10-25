//! A Prometheus metrics endpoint.
//!
//! The recorder stores metrics from the `metrics` crate, and the endpoint serves them in the
//! Prometheus text format. The output matches `metrics-exporter-prometheus` 0.17 with its
//! default settings, which Zakura used before:
//!
//! - counters and gauges are published as they are;
//! - histograms are published as summaries, with the quantiles in [`summary::QUANTILES`] over
//!   the last minute, and a sum and count since startup;
//! - metric names and labels are sanitized, and descriptions become `# HELP` lines.

use std::{fmt, io, net::SocketAddr};

mod endpoint;
mod recorder;
mod summary;
mod text;

#[cfg(test)]
mod tests;

use recorder::PrometheusRecorder;

/// Installs a recorder for all metrics, and serves them at `addr`.
///
/// Returns an error if `addr` can't be opened, or if a recorder is already installed.
pub(super) fn install(addr: SocketAddr) -> Result<(), InstallError> {
    let recorder = PrometheusRecorder::default();
    endpoint::spawn(addr, recorder.clone()).map_err(InstallError::Listen)?;
    metrics::set_global_recorder(recorder).map_err(|_| InstallError::RecorderAlreadySet)?;
    Ok(())
}

/// Why the metrics endpoint couldn't be started.
#[derive(Debug)]
pub(super) enum InstallError {
    /// The endpoint couldn't listen on its address.
    Listen(io::Error),
    /// Another metrics recorder was already installed.
    RecorderAlreadySet,
}

impl fmt::Display for InstallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Listen(error) => write!(f, "failed to listen: {error}"),
            Self::RecorderAlreadySet => f.write_str("a metrics recorder is already installed"),
        }
    }
}
