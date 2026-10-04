//! OpenTelemetry tracing layer with zero overhead when disabled.
//!
//! This module provides OpenTelemetry distributed tracing support that can be
//! compiled into production builds but only activated at runtime when an
//! endpoint is configured.
//!
//! # Transport
//!
//! Uses HTTP transport with a blocking reqwest client. This works without an
//! async runtime context because:
//! - The BatchSpanProcessor spawns its own dedicated background thread
//! - The reqwest-blocking-client handles HTTP exports synchronously
//!
//! This is important because Zebra's tracing component initializes before the
//! Tokio runtime starts.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::{
    trace::{RandomIdGenerator, Sampler, SdkTracerProvider},
    Resource,
};
use tracing::Subscriber;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{registry::LookupSpan, Layer};

/// Error type for OpenTelemetry layer initialization.
pub type OtelError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Creates an OpenTelemetry layer if an endpoint is configured.
///
/// Returns `(None, None)` with ZERO overhead when endpoint is `None` -
/// no SDK objects are created, no background tasks are spawned.
///
/// When enabled, preserves any installed Rustls crypto provider, or installs
/// `ring` as the process default before creating the HTTP client.
///
/// # Arguments
///
/// * `endpoint` - OTLP HTTP endpoint URL (e.g., "http://localhost:4318")
/// * `service_name` - Service name for traces (defaults to "zakura")
/// * `sample_percent` - Sampling percentage between 0 and 100 (defaults to 100)
///
/// # Errors
///
/// Returns an error if the OTLP exporter fails to initialize.
pub fn layer<S>(
    endpoint: Option<&str>,
    service_name: Option<&str>,
    sample_percent: Option<u8>,
) -> Result<(Option<impl Layer<S>>, Option<SdkTracerProvider>), OtelError>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    // CRITICAL: Check config FIRST - zero-cost path when None
    let endpoint = match endpoint {
        Some(ep) => ep,
        None => return Ok((None, None)), // No SDK objects created
    };

    // Iroh enables Reqwest's rustls-no-provider feature for the shared dependency.
    // An install error only means another provider is already installed.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let endpoint = traces_endpoint(endpoint)?;

    let service_name = service_name.unwrap_or("zakura");
    // Convert percentage (0-100) to rate (0.0-1.0), clamped to valid range
    let sample_rate = f64::from(sample_percent.unwrap_or(100).min(100)) / 100.0;

    // Build the HTTP exporter with blocking client.
    // This works without an async runtime because:
    // 1. reqwest-blocking-client doesn't need tokio
    // 2. BatchSpanProcessor spawns its own background thread for exports
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(endpoint.as_str())
        .build()?;

    // Use ratio-based sampling for production flexibility
    let sampler = if sample_rate >= 1.0 {
        Sampler::AlwaysOn
    } else if sample_rate <= 0.0 {
        Sampler::AlwaysOff
    } else {
        Sampler::TraceIdRatioBased(sample_rate)
    };

    let resource = Resource::builder()
        .with_service_name(service_name.to_owned())
        .build();

    // Use batch exporter - it spawns its own dedicated background thread
    // for collecting and exporting spans, so it doesn't need an external runtime
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(sampler)
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(resource)
        .build();

    let tracer = provider.tracer(service_name.to_owned());
    let layer = OpenTelemetryLayer::new(tracer);

    Ok((Some(layer), Some(provider)))
}

/// Append the traces path to a base URL, preserving queries and already-suffixed paths.
/// Fragments are discarded because they are not part of an HTTP request target.
fn traces_endpoint(endpoint: &str) -> Result<reqwest::Url, OtelError> {
    let mut endpoint = reqwest::Url::parse(endpoint)?;
    if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() {
        return Err("OpenTelemetry endpoint must be an HTTP(S) URL with a host".into());
    }

    let path = endpoint.path().trim_end_matches('/');
    let path = if path.ends_with("/v1/traces") {
        path.to_owned()
    } else {
        format!("{path}/v1/traces")
    };
    endpoint.set_path(&path);
    endpoint.set_fragment(None);
    Ok(endpoint)
}

#[cfg(test)]
mod tests {
    use super::traces_endpoint;

    #[test]
    fn normalize_traces_endpoint() {
        for (input, expected) in [
            ("http://localhost:4318", "http://localhost:4318/v1/traces"),
            ("https://collector.example/", "https://collector.example/v1/traces"),
            ("http://localhost:4318///", "http://localhost:4318/v1/traces"),
            ("https://collector.example/prefix", "https://collector.example/prefix/v1/traces"),
            ("https://collector.example/prefix///", "https://collector.example/prefix/v1/traces"),
            ("http://localhost:4318/v1/traces", "http://localhost:4318/v1/traces"),
            ("http://localhost:4318/v1/traces/", "http://localhost:4318/v1/traces"),
            ("https://collector.example/prefix/v1/traces///", "https://collector.example/prefix/v1/traces"),
            ("http://localhost:4318?sig=example", "http://localhost:4318/v1/traces?sig=example"),
            ("http://localhost:4318/?", "http://localhost:4318/v1/traces?"),
            ("https://collector.example/prefix/?a=one%2Ftwo&a=three+four&empty=", "https://collector.example/prefix/v1/traces?a=one%2Ftwo&a=three+four&empty="),
            ("http://localhost:4318?route=/v1/traces", "http://localhost:4318/v1/traces?route=/v1/traces"),
            ("http://localhost:4318#fragment", "http://localhost:4318/v1/traces"),
            ("http://localhost:4318/prefix#/v1/traces", "http://localhost:4318/prefix/v1/traces"),
            ("http://localhost:4318/v1/traces/?sig=example#fragment", "http://localhost:4318/v1/traces?sig=example"),
            ("https://collector.example/prefix/v1/traces?sig=example#fragment", "https://collector.example/prefix/v1/traces?sig=example"),
            ("https://collector.example/tenant%2Fname/", "https://collector.example/tenant%2Fname/v1/traces"),
            ("https://collector.example/v1/traces-other", "https://collector.example/v1/traces-other/v1/traces"),
            ("http://[::1]:4318/prefix/", "http://[::1]:4318/prefix/v1/traces"),
            ("HTTPS://COLLECTOR.EXAMPLE:443/prefix", "https://collector.example/prefix/v1/traces"),
            ("https://collector.example?value='example'", "https://collector.example/v1/traces?value=%27example%27"),
            ("https://test-user:test-password@collector.example/test-path?token=test-token#test-fragment", "https://test-user:test-password@collector.example/test-path/v1/traces?token=test-token"),
        ] {
            let normalized = traces_endpoint(input).expect("valid HTTP endpoint");
            assert_eq!(normalized.as_str(), expected, "input: {input}");
            assert_eq!(traces_endpoint(normalized.as_str()).unwrap(), normalized);
        }
    }

    #[test]
    fn reject_invalid_traces_endpoints_without_echoing_them() {
        for input in [
            "",
            "test-secret",
            "/test-secret",
            "http://[test-secret",
            "ftp://collector.example/test-secret",
            "file:///test-secret",
            "mailto:test-secret@collector.example",
        ] {
            let error = traces_endpoint(input).expect_err("invalid HTTP endpoint");
            assert!(!format!("{error:?}").contains("test-secret"));
            assert!(!error.to_string().contains("test-secret"));
        }
    }
}
