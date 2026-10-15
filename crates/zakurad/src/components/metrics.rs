//! An HTTP endpoint for metrics collection.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

/// Starts the Prometheus metrics endpoint, if one is configured.
///
/// # Panics
///
/// If the configured address can't be opened.
#[cfg(feature = "prometheus")]
pub fn start_endpoint(config: &Config) {
    if let Some(addr) = config.endpoint_addr {
        info!("Trying to open metrics endpoint at {}...", addr);

        let endpoint_result = metrics_exporter_prometheus::PrometheusBuilder::new()
            .with_http_listener(addr)
            .install();

        match endpoint_result {
            Ok(()) => {
                // Expose binary metadata to metrics, using a single time series with
                // value 1:
                //     https://www.robustperception.io/exposing-the-software-version-to-prometheus
                metrics::counter!(
                    format!("{}.build.info", env!("CARGO_PKG_NAME")),
                    "version" => env!("CARGO_PKG_VERSION")
                )
                .increment(1);

                info!("Opened metrics endpoint at {}", addr);
            }
            Err(e) => panic!(
                "Opening metrics endpoint listener {addr:?} failed: {e:?}. \
                     Hint: Check if another zakurad or zcashd process is running. \
                     Try changing the metrics endpoint_addr in the Zakura config.",
            ),
        }
    }
}

/// Warns that a configured endpoint can't be started without the `prometheus` feature.
#[cfg(not(feature = "prometheus"))]
pub fn start_endpoint(config: &Config) {
    if let Some(addr) = config.endpoint_addr {
        warn!(
            ?addr,
            "unable to activate configured metrics endpoint: \
                 enable the 'prometheus' feature when compiling zakurad",
        );
    }
}

/// Metrics configuration section.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// The address used for the Prometheus metrics endpoint.
    ///
    /// Install Zebra using `cargo install --features=prometheus` to enable this config.
    ///
    /// The endpoint is disabled if this is set to `None`.
    pub endpoint_addr: Option<SocketAddr>,
}

// we like our default configs to be explicit
#[allow(unknown_lints)]
#[allow(clippy::derivable_impls)]
impl Default for Config {
    fn default() -> Self {
        Self {
            endpoint_addr: None,
        }
    }
}
