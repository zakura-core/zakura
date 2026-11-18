//! `[network.zakura.tx_relay]`.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::machine::{FanOut, ADVERT_DELAY, WINDOW_FULL, WINDOW_SMALL};

/// `tx-relay` settings, nested under the Zakura P2P-v2 config.
///
/// `tx-relay` runs only when it is enabled here, the v2 stack is on, and the
/// mempool runs the pool core (`[mempool] implementation = "core"`).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ZakuraTxRelayConfig {
    /// Whether to move transactions over `tx-relay`. Off by default. At full
    /// load, with 92 peers each announcing a full pool of its own, its
    /// bookkeeping can use about 1.4 GiB of memory.
    pub enabled: bool,

    /// How long a session outside a transaction's fan-out window waits before
    /// it announces the transaction, unless its peer announced it first. Zero
    /// announces to every session at once. Defaults to 2 seconds.
    ///
    /// Experimental.
    #[serde(with = "zakura_chain::serialization::serde_adapters::duration")]
    pub advert_delay: Duration,

    /// How many of the best-ranked sessions announce a transaction of the
    /// largest size, 2,000,000 bytes, or of unknown size, at once. The
    /// window grows in a straight line to `advert_window_small_tx` as the
    /// size falls to 10,000 bytes. At least 1, and at most
    /// `advert_window_small_tx`. Defaults to 4.
    ///
    /// Experimental.
    pub advert_window_full_tx: usize,

    /// How many of the best-ranked sessions announce a transaction of
    /// 10,000 bytes or less at once. At least `advert_window_full_tx`.
    /// Defaults to 16.
    ///
    /// Experimental.
    pub advert_window_small_tx: usize,
}

impl Default for ZakuraTxRelayConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            advert_delay: ADVERT_DELAY,
            advert_window_full_tx: WINDOW_FULL,
            advert_window_small_tx: WINDOW_SMALL,
        }
    }
}

impl ZakuraTxRelayConfig {
    /// The fan-out settings the machine runs with.
    pub(crate) fn fan_out(&self) -> FanOut {
        FanOut {
            advert_delay: self.advert_delay,
            window_small: self.advert_window_small_tx,
            window_full: self.advert_window_full_tx,
        }
    }

    /// Checks the fan-out settings, as the block advert fan-out is checked.
    /// The network config calls it when it loads, so a bad value fails at
    /// startup.
    pub(crate) fn validate(&self) -> Result<(), String> {
        let (full, small) = (self.advert_window_full_tx, self.advert_window_small_tx);
        if full == 0 || full > small {
            return Err(format!(
                "zakura.tx_relay.advert_window_full_tx and zakura.tx_relay.advert_window_small_tx \
                 must satisfy 1 <= advert_window_full_tx <= advert_window_small_tx, \
                 got {full} and {small}"
            ));
        }
        // The relay's deadlines become instants, which a larger delay would
        // overflow.
        if zakura_rt::time::Instant::now()
            .checked_add(self.advert_delay)
            .is_none()
        {
            return Err(
                "zakura.tx_relay.advert_delay exceeds the monotonic clock range".to_owned(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The defaults, a set delay and a zero delay survive a round trip
    /// through TOML, every key is optional, and an unknown key is refused.
    #[test]
    fn the_config_round_trips_through_toml() {
        let round_trip = |config: &ZakuraTxRelayConfig| {
            let written = toml::to_string(config).expect("the config serializes");
            assert_eq!(
                &toml::from_str::<ZakuraTxRelayConfig>(&written).expect("it reads back"),
                config,
            );
        };
        round_trip(&ZakuraTxRelayConfig::default());
        assert_eq!(
            toml::from_str::<ZakuraTxRelayConfig>("").expect("every key is optional"),
            ZakuraTxRelayConfig::default(),
        );
        for (written, delay) in [
            ("500ms", Duration::from_millis(500)),
            ("0s", Duration::ZERO),
        ] {
            let config: ZakuraTxRelayConfig =
                toml::from_str(&format!("advert_delay = \"{written}\""))
                    .expect("a duration is a valid delay");
            assert_eq!(config.advert_delay, delay);
            round_trip(&config);
        }
        assert!(toml::from_str::<ZakuraTxRelayConfig>("advert_delay = \"soon\"").is_err());
        assert!(
            toml::from_str::<ZakuraTxRelayConfig>("unknown_key = 1").is_err(),
            "unknown keys are refused"
        );
    }

    /// The windows read from their keys into the machine's fan-out.
    #[test]
    fn the_windows_read_from_their_keys() {
        let config: ZakuraTxRelayConfig =
            toml::from_str("advert_window_full_tx = 2\nadvert_window_small_tx = 8")
                .expect("both windows are valid");
        let fan_out = config.fan_out();
        assert_eq!((fan_out.window_full, fan_out.window_small), (2, 8));
        assert_eq!(fan_out.window(Some(10_000)), 8);
        assert_eq!(fan_out.window(Some(2_000_000)), 2);
        assert_eq!(fan_out.window(None), 2);
        assert_eq!(ZakuraTxRelayConfig::default().fan_out(), FanOut::default());
    }

    /// The network config accepts a zero delay and equal windows when it
    /// loads, and refuses a zero window, a full-size window larger than the
    /// small-size one, and a delay past the clock's range.
    #[test]
    fn the_network_config_checks_the_fan_out_when_it_loads() {
        let _init_guard = zakura_test::init();
        let load =
            |keys: &str| toml::from_str::<crate::Config>(&format!("[zakura.tx_relay]\n{keys}"));

        let config = load("advert_delay = \"0s\"").expect("a zero delay is valid");
        assert_eq!(config.zakura.tx_relay.advert_delay, Duration::ZERO);
        assert!(load("advert_window_full_tx = 4\nadvert_window_small_tx = 4").is_ok());

        for (keys, named) in [
            ("advert_window_full_tx = 0", "advert_window_full_tx"),
            (
                "advert_window_full_tx = 0\nadvert_window_small_tx = 0",
                "advert_window_full_tx",
            ),
            (
                "advert_window_full_tx = 8\nadvert_window_small_tx = 4",
                "advert_window_full_tx",
            ),
            ("advert_delay = \"18446744073709551615s\"", "advert_delay"),
        ] {
            let error = load(keys).expect_err("the value is refused").to_string();
            assert!(
                error.contains(&format!("zakura.tx_relay.{named}")),
                "{keys}: {error}"
            );
        }
    }
}
