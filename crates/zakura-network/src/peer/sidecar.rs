//! Operator warnings about the protected zcashd-compat sidecar peer.
//!
//! A sidecar that falls behind a network upgrade is disconnected and its wallet silently
//! stops updating, so these warnings are loud but rate-limited: the supervisor keeps
//! restarting and reconnecting the sidecar.

use std::{
    ops::Bound::{Excluded, Unbounded},
    sync::Mutex,
    time::Instant,
};

use zakura_chain::{block, parameters::Network};

use crate::{constants::MIN_PEER_SET_LOG_INTERVAL, protocol::external::types::Version};

/// Allows one warning of a kind per [`MIN_PEER_SET_LOG_INTERVAL`].
pub(crate) struct WarningLimiter(Mutex<Option<Instant>>);

impl WarningLimiter {
    /// Returns a limiter that allows the first warning.
    pub(crate) const fn new() -> Self {
        Self(Mutex::new(None))
    }

    /// Returns whether a warning may be logged now, recording it if so.
    pub(crate) fn allow(&self) -> bool {
        let mut last = self
            .0
            .lock()
            .expect("the warning limiter lock is never poisoned");
        let now = Instant::now();
        if last.is_some_and(|last| now.duration_since(last) < MIN_PEER_SET_LOG_INTERVAL) {
            return false;
        }
        *last = Some(now);
        true
    }
}

/// Rate-limits warnings about a sidecar version that is too old for the next network upgrade.
static UPGRADE_READINESS_WARNINGS: WarningLimiter = WarningLimiter::new();

/// Returns whether a sidecar with `remote_version` supports the next network upgrade that
/// `network` schedules after `tip_height`, setting `zcashd_compat.sidecar.next_upgrade_ready`
/// and logging a rate-limited warning if it does not.
///
/// A sidecar below that upgrade's minimum protocol version is disconnected when the upgrade
/// activates, so the operator must upgrade it beforehand.
pub(crate) fn check_upgrade_readiness(
    network: &Network,
    tip_height: Option<block::Height>,
    remote_version: Version,
) -> bool {
    let tip_height = tip_height.unwrap_or(block::Height(0));
    let next_upgrade = network
        .activation_list()
        .range((Excluded(tip_height), Unbounded))
        .map(|(height, upgrade)| (*height, *upgrade))
        .next();

    let ready = match next_upgrade {
        Some((activation_height, upgrade)) => {
            let required_version = Version::min_specified_for_upgrade(network, upgrade);
            let ready = remote_version >= required_version;
            if !ready && UPGRADE_READINESS_WARNINGS.allow() {
                tracing::warn!(
                    ?upgrade,
                    activation_height = activation_height.0,
                    ?remote_version,
                    ?required_version,
                    "the zcashd-compat sidecar does not support the next network upgrade: upgrade \
                     it before activation, or Zakura will disconnect it and its wallet will stop \
                     updating"
                );
            }
            ready
        }
        None => true,
    };

    set_next_upgrade_ready(ready);
    ready
}

/// Sets the `zcashd_compat.sidecar.next_upgrade_ready` gauge.
pub(crate) fn set_next_upgrade_ready(ready: bool) {
    metrics::gauge!("zcashd_compat.sidecar.next_upgrade_ready").set(if ready { 1.0 } else { 0.0 });
}

/// Warns that the protected sidecar is being disconnected because a network upgrade raised
/// the minimum protocol version above its `remote_version`, and marks it not ready.
pub(crate) fn warn_upgrade_eviction(remote_version: Version, minimum_version: Version) {
    set_next_upgrade_ready(false);
    tracing::warn!(
        ?remote_version,
        ?minimum_version,
        "disconnecting the protected zcashd-compat sidecar: a network upgrade raised the minimum \
         protocol version above its version, so its wallet stops updating until it is upgraded",
    );
}

#[cfg(test)]
mod tests {
    use zakura_chain::parameters::testnet::ConfiguredActivationHeights;

    use super::*;

    /// The limiter allows a first warning and then waits for the interval.
    #[test]
    fn warning_limiter_allows_one_warning_per_interval() {
        let limiter = WarningLimiter::new();
        assert!(limiter.allow());
        assert!(!limiter.allow());
    }

    /// A sidecar needs the next upgrade's minimum version before it activates.
    #[test]
    fn upgrade_readiness_uses_the_next_upgrade_minimum() {
        let _init_guard = zakura_test::init();
        let network = Network::new_regtest(
            ConfiguredActivationHeights {
                nu6_3: Some(1),
                nu7: Some(210),
                ..Default::default()
            }
            .into(),
        );

        // NU7 is next: Regtest uses the Testnet NU7 version, 170180.
        let tip = Some(block::Height(200));
        assert!(!check_upgrade_readiness(&network, tip, Version(170_160)));
        assert!(check_upgrade_readiness(&network, tip, Version(170_180)));

        // After NU7 no upgrade is scheduled.
        let tip = Some(block::Height(210));
        assert!(check_upgrade_readiness(&network, tip, Version(170_160)));
    }
}
