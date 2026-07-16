# Tracing Zakura

## Dynamic Tracing

Zakura supports dynamic tracing, configured using the config's
[`TracingSection`][tracing_section] and an HTTP RPC endpoint.

Activate this feature using the `filter-reload` compile-time feature,
and the [`filter`][filter] and `endpoint_addr` runtime config options.

If the `endpoint_addr` is specified, `zakurad` will open an HTTP endpoint
allowing dynamic runtime configuration of the tracing filter. For instance,
if the config had `endpoint_addr = '127.0.0.1:3000'`, then

- `curl -X GET localhost:3000/filter` retrieves the current filter string;
- `curl -X POST localhost:3000/filter -d "zakurad=trace"` sets the current filter string.

See the [`filter`][filter] documentation for more details.

## `journald` Logging

Zakura can send tracing spans and events to [systemd-journald][systemd_journald],
on Linux distributions that use `systemd`.

Activate `journald` logging using the `journald` compile-time feature,
and the [`use_journald`][use_journald] runtime config option.

## Flamegraphs

Zakura can generate [flamegraphs] of tracing spans.

Activate flamegraphs using the `flamegraph` compile-time feature,
and the [`flamegraph`][flamegraph] runtime config option.

## OpenTelemetry Export

Official Zakura release builds include OpenTelemetry support. Export is disabled
until you configure an OpenTelemetry endpoint using the tracing config or the
`OTEL_EXPORTER_OTLP_ENDPOINT` environment variable.

## Sentry Production Monitoring

Official Zakura release builds include Sentry support. Sentry is only activated
when the `SENTRY_DSN` environment variable is set.

You can optionally set `SENTRY_ENVIRONMENT` to control the environment name
attached to Sentry events. Zakura also tags events with the git SHA when
available, preferring the runtime `GITHUB_SHA` (full commit SHA on GitHub
Actions) and falling back to the build-baked `SHORT_SHA` or `VERGEN_GIT_SHA`.
When it runs under GitHub Actions it reads standard `GITHUB_*` metadata plus
the optional `CI_TEST_ID` runtime variable for CI context. If `github-slug-action` exports `GITHUB_REF_POINT_SLUG_URL`, Zakura
uses that slugged branch or tag name for the `git.ref` tag, and CI workflows
can pass `CI_PR_NUMBER` and `CI_TEST_ID` for additional correlation. These
values are read at runtime, so container images do not need CI-specific build
arguments, and the `ZAKURA_*` environment namespace remains reserved for Zakura
configuration.

[tracing_section]: https://docs.rs/zakura/latest/zakurad/components/tracing/struct.InnerConfig.html
[filter]: https://docs.rs/zakura/latest/zakurad/components/tracing/struct.InnerConfig.html#structfield.filter
[flamegraph]: https://docs.rs/zakura/latest/zakurad/components/tracing/struct.InnerConfig.html#structfield.flamegraph
[flamegraphs]: http://www.brendangregg.com/flamegraphs.html
[systemd_journald]: https://man7.org/linux/man-pages/man8/systemd-journald.service.8.html
[use_journald]: https://docs.rs/zakura/latest/zakurad/components/tracing/struct.InnerConfig.html#structfield.use_journald
[sentry]: https://sentry.io/welcome/
