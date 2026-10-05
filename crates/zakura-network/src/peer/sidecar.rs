//! Operator warnings about the protected zcashd-compat sidecar peer.
//!
//! A sidecar that falls behind a network upgrade is disconnected and its wallet silently
//! stops updating, so these warnings are loud but rate-limited: the supervisor keeps
//! restarting and reconnecting the sidecar.

use std::{
    collections::BTreeMap,
    net::IpAddr,
    ops::Bound::{Excluded, Unbounded},
    sync::Mutex,
    time::Instant,
};

use zakura_chain::{block, parameters::Network};

use crate::{
    constants::MIN_PEER_SET_LOG_INTERVAL,
    protocol::external::{canonical_ip, types::Version},
};

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

/// Whether each sidecar supports the next network upgrade, by canonical IP.
pub(crate) struct SidecarReadiness(Mutex<BTreeMap<IpAddr, bool>>);

impl SidecarReadiness {
    /// Returns a record with no sidecars.
    pub(crate) const fn new() -> Self {
        Self(Mutex::new(BTreeMap::new()))
    }

    /// Records whether the sidecar at `ip` supports the next network upgrade, and sets the
    /// `zcashd_compat.sidecar.next_upgrade_ready` gauge to whether every recorded sidecar does,
    /// which it returns. The gauge is set under the lock, so concurrent records publish in order.
    pub(crate) fn record(&self, ip: IpAddr, ready: bool) -> bool {
        let mut readiness = self
            .0
            .lock()
            .expect("the readiness lock is never poisoned: nothing panics while holding it");
        readiness.insert(canonical_ip(ip), ready);
        let all_ready = readiness.values().all(|ready| *ready);
        metrics::gauge!("zcashd_compat.sidecar.next_upgrade_ready").set(if all_ready {
            1.0
        } else {
            0.0
        });
        all_ready
    }
}

/// The readiness that `zcashd_compat.sidecar.next_upgrade_ready` reports.
static SIDECAR_READINESS: SidecarReadiness = SidecarReadiness::new();

/// Returns whether the sidecar at `sidecar_ip` with `remote_version` supports the next network
/// upgrade that `network` schedules after `tip_height`, recording it for
/// `zcashd_compat.sidecar.next_upgrade_ready` and logging a rate-limited warning if it does not.
///
/// A sidecar below that upgrade's minimum protocol version is disconnected when the upgrade
/// activates, so the operator must upgrade it beforehand.
pub(crate) fn check_upgrade_readiness(
    network: &Network,
    tip_height: Option<block::Height>,
    sidecar_ip: IpAddr,
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

    set_next_upgrade_ready(sidecar_ip, ready);
    ready
}

/// Records whether the sidecar at `sidecar_ip` supports the next network upgrade. See
/// [`SidecarReadiness::record`].
pub(crate) fn set_next_upgrade_ready(sidecar_ip: IpAddr, ready: bool) {
    SIDECAR_READINESS.record(sidecar_ip, ready);
}

/// Warns that the protected sidecar at `sidecar_ip` is being disconnected because a network
/// upgrade raised the minimum protocol version above its `remote_version`, and marks it not
/// ready.
pub(crate) fn warn_upgrade_eviction(
    sidecar_ip: IpAddr,
    remote_version: Version,
    minimum_version: Version,
) {
    set_next_upgrade_ready(sidecar_ip, false);
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

    /// The sidecar IP these tests check.
    const SIDECAR: IpAddr = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);

    /// Readiness stays false while any recorded sidecar is not ready.
    #[test]
    fn readiness_covers_every_sidecar() {
        let readiness = SidecarReadiness::new();
        let a = IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1));
        let b = std::net::Ipv4Addr::new(10, 0, 0, 2);
        assert!(!readiness.record(a, false));
        assert!(!readiness.record(IpAddr::V4(b), true));
        assert!(readiness.record(a, true));
        // An IPv4-mapped address is the same sidecar.
        assert!(!readiness.record(IpAddr::V6(b.to_ipv6_mapped()), false));
        assert!(readiness.record(IpAddr::V4(b), true));
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
        assert!(!check_upgrade_readiness(
            &network,
            tip,
            SIDECAR,
            Version(170_160)
        ));
        assert!(check_upgrade_readiness(
            &network,
            tip,
            SIDECAR,
            Version(170_180)
        ));

        // After NU7 no upgrade is scheduled.
        let tip = Some(block::Height(210));
        assert!(check_upgrade_readiness(
            &network,
            tip,
            SIDECAR,
            Version(170_160)
        ));
    }
}
