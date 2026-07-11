# Running Zakura

You can run Zakura as a backend for [`lightwalletd`][lwd], or a [mining][mining] pool.

[lwd]: <lightwalletd.md>
[mining]: <mining.md>

For Kubernetes and load balancer integrations, Zakura provides simple [HTTP
health endpoints](./health.md).

## Optional Configs & Features

Zakura supports a variety of optional features which you can enable and configure
manually.

### Initializing Configuration File

The command below generates a `zakurad.toml` config file at the default location
for config files on GNU/Linux. The locations for other operating systems are
documented in the [dirs crate documentation][config-locations].

```console
zakurad generate -o ~/.config/zakurad.toml
```

The generated config file contains Zakura's default options, which take place if
no config is present. The contents of the config file is a TOML encoding of the
internal config structure. All config options are documented
in the [ZakuradConfig documentation][config-options].

[config-options]: https://docs.rs/zakurad/latest/zakurad/config/struct.ZakuradConfig.html
[config-locations]: https://docs.rs/dirs/latest/dirs/fn.preference_dir.html

### Configuring Progress Bars

Configure `tracing.progress_bar` in your `zakurad.toml` to show key metrics in
the terminal using progress bars. Progress bars are included in default release
builds. When progress bars are active, Zakura automatically sends logs to a file.
Note that there is a known issue where [progress bar estimates become extremely
large][1]. The `progress_bar = "summary"` config shows a few key metrics, and
`detailed` shows all available metrics.

[1]: https://github.com/console-rs/indicatif/issues/556

### Custom Build Features

Zakura release builds include several features by default:

- `progress-bar` for terminal progress bars (see above)
- `prometheus` for [Prometheus metrics](metrics.md)
- `sentry` for [Sentry monitoring](tracing.md#sentry-production-monitoring)
- `opentelemetry` for [OpenTelemetry trace export](tracing.md#opentelemetry-export)

Additional [Cargo features](https://doc.rust-lang.org/cargo/reference/features.html#command-line-feature-options) that require explicit enabling:

- `elasticsearch` for [experimental Elasticsearch support](elasticsearch.md)
- `internal-miner` for [Regtest internal mining](regtest.md)

You can combine multiple features by listing them as parameters of the
`--features` flag:

```sh
cargo install --features="<feature1> <feature2> ..." ...
```

The full list of all features is in [the API
documentation](https://docs.rs/zakurad/latest/zakurad/index.html#zakura-feature-flags).
Some debugging and monitoring features are disabled in release builds to
increase performance.

## Return Codes

- `0`: Application exited successfully
- `1`: Application exited unsuccessfully
- `2`: Application crashed
- `zakurad` may also return platform-dependent codes.
