//! Sizes each legacy block broadcast's advert window from the block's size and the node's
//! measured upload bandwidth.
//!
//! An `inv` is cheap, but the getdata it triggers costs a full body upload, and concurrent
//! uploads share one uplink. Advertising a block of `S` bytes to `W` peers at once delays every
//! copy by about `W·S/B`, where `B` is the upload bandwidth. So once the bandwidth is measured, a
//! broadcast advertises to
//!
//! ```text
//! W = clamp(ceil(B̂ · τ / S), W_min, W_max)
//! ```
//!
//! peers at a time, where `τ` is the target time for that first wave of uploads to finish. A
//! large block reaches a few peers quickly, and they relay it, while a small block goes to many
//! peers at once.
//!
//! `B̂` is [`UploadBandwidth`]: aggregate block upload goodput measured from the bytes that peers
//! acknowledge. Until there is a measurement, and whenever the block's size is unknown, the
//! window is the configured initial window. Only Linux exposes the TCP delivery counters that the
//! measurement needs, so elsewhere the window always stays at the initial value.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    peer::tcp_progress::{TcpSocket, UploadProgress, UploadRateEstimator},
    Config,
};

/// How much busy upload time it takes for a new bandwidth measurement to move the estimate
/// about 63% of the way to its rate.
///
/// A few seconds spans one or two block upload waves, so the estimate follows a changed uplink
/// within a few blocks without swinging on one slow peer.
const UPLOAD_RATE_TIME_CONSTANT: Duration = Duration::from_secs(5);

/// Chooses how many peers one block broadcast advertises to at once.
#[derive(Clone, Debug)]
pub(crate) struct AdvertFanout {
    policy: WindowPolicy,
    bandwidth: UploadBandwidth,
}

#[derive(Clone, Copy, Debug)]
enum WindowPolicy {
    /// Every broadcast uses this window.
    Fixed(usize),
    /// The window follows the block size and the measured upload bandwidth.
    Adaptive {
        target: Duration,
        min: usize,
        initial: usize,
        max: usize,
    },
}

impl AdvertFanout {
    /// Returns the fan-out policy in `config`, which reads its bandwidth from `bandwidth`.
    pub(crate) fn from_config(config: &Config, bandwidth: UploadBandwidth) -> Self {
        let policy = match config.advert_window_fixed {
            Some(window) => WindowPolicy::Fixed(window),
            None => WindowPolicy::Adaptive {
                target: config.advert_target,
                min: config.advert_window_min,
                initial: config.advert_window_initial,
                max: config.advert_window_max,
            },
        };
        Self { policy, bandwidth }
    }

    /// Returns how many peers a broadcast of a block of `size` serialized bytes advertises to
    /// at once, and records the choice in metrics.
    ///
    /// Before the upload bandwidth is measured, or if the size is unknown, this is the initial
    /// window.
    pub(crate) fn window(&self, size: Option<u32>) -> usize {
        let bandwidth = self.bandwidth.bytes_per_second();
        let window = match self.policy {
            WindowPolicy::Fixed(window) => window,
            WindowPolicy::Adaptive {
                target,
                min,
                initial,
                max,
            } => match (bandwidth, size) {
                (Some(bandwidth), Some(size)) => advert_window(bandwidth, target, size, min, max),
                _ => initial,
            },
        };
        if let Some(bandwidth) = bandwidth {
            metrics::gauge!("peer.block_gossip.upload_bandwidth_bytes_per_second").set(bandwidth);
        }
        if let Some(size) = size {
            metrics::histogram!("peer.block_gossip.advert_block_size_bytes")
                .record(f64::from(size));
        }
        // Windows are at most a few hundred peers, so they convert exactly.
        #[allow(clippy::cast_precision_loss)]
        metrics::histogram!("peer.block_gossip.advert_window").record(window as f64);
        window
    }
}

/// Returns `clamp(ceil(bandwidth · target / size), min, max)`, the number of peers to advertise
/// a block of `size` bytes to at once so that their uploads finish within about `target` at
/// `bandwidth` bytes per second.
///
/// A zero size returns `max`: a block that costs nothing to upload can go to every peer at once.
pub(crate) fn advert_window(
    bandwidth: f64,
    target: Duration,
    size: u32,
    min: usize,
    max: usize,
) -> usize {
    if size == 0 {
        return max;
    }
    let window = (bandwidth * target.as_secs_f64() / f64::from(size)).ceil();
    if window.is_nan() {
        return min;
    }
    // A float-to-integer `as` cast saturates, so huge windows become `usize::MAX` and then `max`,
    // and negative windows become zero and then `min`.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let window = window as usize;
    window.clamp(min, max)
}

/// The node's aggregate block upload bandwidth, shared by every legacy connection and block
/// broadcast.
///
/// When measured, each connection's [`BodyUploadWatcher`] reports how many body bytes peers
/// acknowledged, and an [`UploadRateEstimator`] divides them by the time at least one upload
/// was outstanding.
#[derive(Clone, Debug)]
pub(crate) struct UploadBandwidth {
    estimator: Option<Arc<Mutex<UploadRateEstimator>>>,
}

impl UploadBandwidth {
    /// Returns the bandwidth that `config` uses: measured, unless the advert window is fixed.
    pub(crate) fn from_config(config: &Config) -> Self {
        if config.advert_window_fixed.is_some() {
            Self::unmeasured()
        } else {
            Self::measured()
        }
    }

    /// Returns a bandwidth measured from acknowledged uploads.
    pub(crate) fn measured() -> Self {
        Self {
            estimator: Some(Arc::new(Mutex::new(UploadRateEstimator::new(
                UPLOAD_RATE_TIME_CONSTANT,
            )))),
        }
    }

    /// Returns a bandwidth that is never measured.
    pub(crate) fn unmeasured() -> Self {
        Self { estimator: None }
    }

    /// Returns the measured bandwidth in bytes per second, or `None` before the first
    /// measurement and when unmeasured.
    pub(crate) fn bytes_per_second(&self) -> Option<f64> {
        self.estimator.as_ref().and_then(|estimator| {
            estimator
                .lock()
                .expect("upload bandwidth mutex is never poisoned")
                .bytes_per_second()
        })
    }

    /// Returns a watcher that measures block body uploads on `socket`, if this bandwidth is
    /// measured and the platform can read the socket's delivery counters.
    ///
    /// Call it with the connection's socket before the stream is wrapped. On platforms other
    /// than Linux, [`TcpSocket`] is never available, so no watcher is returned.
    pub(crate) fn watcher(
        &self,
        socket: impl FnOnce() -> Option<TcpSocket>,
    ) -> Option<BodyUploadWatcher> {
        self.estimator.as_ref()?;
        Some(BodyUploadWatcher {
            progress: UploadProgress::new(socket()?),
            bandwidth: self.clone(),
        })
    }

    /// Records that a measured upload started at `now`.
    pub(crate) fn upload_started(&self, now: Instant) {
        if let Some(estimator) = &self.estimator {
            estimator
                .lock()
                .expect("upload bandwidth mutex is never poisoned")
                .upload_started(now);
        }
    }

    /// Records that a measured upload ended at `now`, after the peer acknowledged
    /// `acked_bytes` of it.
    pub(crate) fn upload_finished(&self, acked_bytes: u64, now: Instant) {
        let Some(estimator) = &self.estimator else {
            return;
        };
        let bandwidth = {
            let mut estimator = estimator
                .lock()
                .expect("upload bandwidth mutex is never poisoned");
            estimator.record_acked(acked_bytes, now);
            estimator.upload_finished(now);
            estimator.bytes_per_second()
        };
        if let Some(bandwidth) = bandwidth {
            metrics::gauge!("peer.block_gossip.upload_bandwidth_bytes_per_second").set(bandwidth);
        }
    }
}

/// Measures block body uploads on one legacy connection for the shared [`UploadBandwidth`].
///
/// The connection owns this, and with it the only strong handle to its socket.
#[derive(Debug)]
pub(crate) struct BodyUploadWatcher {
    progress: UploadProgress,
    bandwidth: UploadBandwidth,
}

impl BodyUploadWatcher {
    /// Starts measuring a block body of `message_len` encoded bytes that is about to be
    /// written.
    ///
    /// The measurement runs in its own task, sampling the socket only until the peer
    /// acknowledges the body, the connection closes, or the watch times out. Uploads beyond the
    /// connection's watch limit are not measured.
    pub(crate) fn watch_body(&self, message_len: u64) {
        let Some(watch) = self.progress.start(message_len) else {
            return;
        };
        let bandwidth = self.bandwidth.clone();
        bandwidth.upload_started(Instant::now());
        tokio::spawn(async move {
            let report = watch.wait_acked().await;
            bandwidth.upload_finished(report.acked_message_bytes(), Instant::now());
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MB: u32 = 1_000_000;
    const SECOND: Duration = Duration::from_secs(1);

    /// Returns a measured bandwidth whose estimate is `bytes_per_second`, from one upload that
    /// peers acknowledged over one second.
    fn bandwidth_measured_at(bytes_per_second: u64) -> UploadBandwidth {
        let bandwidth = UploadBandwidth::measured();
        let t0 = Instant::now();
        bandwidth.upload_started(t0);
        bandwidth.upload_finished(bytes_per_second, t0 + SECOND);
        bandwidth
    }

    #[test]
    fn window_is_bandwidth_times_target_over_size_rounded_up() {
        let window = |size| advert_window(12_500_000.0, SECOND, size, 1, 264);
        // 12.5 MB/s for 1 s is 8.33 blocks of 1.5 MB, so the ninth peer still starts at once.
        assert_eq!(window(1_500_000), 9);
        // Exactly 10 blocks of 1.25 MB fit, so no extra peer is added.
        assert_eq!(window(1_250_000), 10);
        assert_eq!(window(1_250_001), 10);
        assert_eq!(window(1_249_999), 11);
        assert_eq!(window(2 * MB), 7);
        assert_eq!(window(20 * MB), 1);
        // Small blocks rise to the ceiling, and a block that costs nothing uses it.
        assert_eq!(window(2_000), 264);
        assert_eq!(window(1), 264);
        assert_eq!(window(0), 264);
        // The default floor clamps large blocks.
        assert_eq!(advert_window(12_500_000.0, SECOND, 1_500_000, 16, 264), 16);
    }

    #[test]
    fn window_follows_target_and_bandwidth() {
        let size = 1_500_000;
        assert_eq!(advert_window(12_500_000.0, SECOND / 2, size, 1, 264), 5);
        assert_eq!(advert_window(12_500_000.0, 2 * SECOND, size, 1, 264), 17);
        assert_eq!(advert_window(125_000_000.0, SECOND, size, 1, 264), 84);
        assert_eq!(advert_window(0.0, SECOND, size, 4, 264), 4);
        assert_eq!(advert_window(f64::INFINITY, SECOND, size, 4, 264), 264);
        assert_eq!(advert_window(f64::NAN, SECOND, size, 4, 264), 4);
        // The bounds are inclusive, and a single-value range always returns that value.
        assert_eq!(advert_window(12_500_000.0, SECOND, size, 9, 9), 9);
        assert_eq!(advert_window(12_500_000.0, SECOND, size, 1, 1), 1);
    }

    #[test]
    fn window_is_initial_until_bandwidth_is_measured() {
        let config = Config::default();
        let fanout = AdvertFanout::from_config(&config, UploadBandwidth::from_config(&config));
        for size in [None, Some(0), Some(2_000), Some(1_500_000), Some(10 * MB)] {
            assert_eq!(fanout.window(size), 32, "size {size:?}");
        }
    }

    #[test]
    fn unmeasured_window_stays_initial() {
        // Without TCP delivery counters, as on platforms other than Linux, nothing is measured.
        let config = Config::default();
        let bandwidth = UploadBandwidth::unmeasured();
        let t0 = Instant::now();
        bandwidth.upload_started(t0);
        bandwidth.upload_finished(12_500_000, t0 + SECOND);
        assert_eq!(bandwidth.bytes_per_second(), None);
        let fanout = AdvertFanout::from_config(&config, bandwidth);
        for size in [Some(2_000), Some(1_500_000), Some(10 * MB)] {
            assert_eq!(fanout.window(size), 32);
        }
    }

    #[test]
    fn measured_window_uses_configured_bounds() {
        let mut config = Config::default();
        let fanout = AdvertFanout::from_config(&config, bandwidth_measured_at(12_500_000));
        // ceil(12.5 MB/s · 1 s / 1.5 MB) = 9 clamps to the default floor of 16.
        assert_eq!(fanout.window(Some(1_500_000)), 16);
        assert_eq!(fanout.window(Some(10 * MB)), 16);
        assert_eq!(fanout.window(Some(2_000)), 264);
        assert_eq!(
            fanout.window(None),
            32,
            "an unknown size uses the initial window"
        );

        config.advert_window_min = 1;
        let fanout = AdvertFanout::from_config(&config, bandwidth_measured_at(12_500_000));
        assert_eq!(
            fanout.window(Some(1_500_000)),
            9,
            "unclamped below the floor"
        );
    }

    #[test]
    fn fixed_window_ignores_block_size_and_measures_nothing() {
        let config = Config {
            advert_window_fixed: Some(8),
            ..Config::default()
        };
        let bandwidth = UploadBandwidth::from_config(&config);
        assert!(
            bandwidth
                .watcher(|| unreachable!("a fixed window measures nothing"))
                .is_none(),
            "a fixed window does not measure bandwidth"
        );
        let fanout = AdvertFanout::from_config(&config, bandwidth);
        for size in [None, Some(0), Some(2_000), Some(1_500_000), Some(10 * MB)] {
            assert_eq!(fanout.window(size), 8);
        }
        let fanout = AdvertFanout::from_config(&config, bandwidth_measured_at(12_500_000));
        assert_eq!(fanout.window(Some(2_000)), 8);
    }

    #[test]
    fn bandwidth_is_unknown_until_an_upload_is_acknowledged() {
        let t0 = Instant::now();
        let bandwidth = UploadBandwidth::measured();
        assert_eq!(bandwidth.bytes_per_second(), None);

        // Two overlapping 1.5 MB uploads, both acknowledged within one busy second.
        bandwidth.upload_started(t0);
        bandwidth.upload_started(t0 + SECOND / 4);
        assert_eq!(
            bandwidth.bytes_per_second(),
            None,
            "outstanding uploads are not a measurement"
        );
        bandwidth.upload_finished(1_500_000, t0 + SECOND / 2);
        bandwidth.upload_finished(1_500_000, t0 + SECOND);
        // The first observation is 1.5 MB in 0.5 s, and the second moves the estimate toward
        // 1.5 MB in the next 0.5 s.
        let measured = bandwidth.bytes_per_second().expect("uploads were measured");
        assert!(
            (measured - 3_000_000.0).abs() < 1.0,
            "aggregate goodput is 3 MB/s: {measured}"
        );

        // The window follows the measurement: 3 MB/s for 1 s fits two 1.5 MB uploads.
        let config = Config {
            advert_window_min: 1,
            ..Config::default()
        };
        let fanout = AdvertFanout::from_config(&config, bandwidth.clone());
        assert_eq!(fanout.window(Some(1_500_000)), 2);

        // Idle time between uploads does not dilute the estimate.
        let t1 = t0 + 60 * SECOND;
        bandwidth.upload_started(t1);
        bandwidth.upload_finished(3_000_000, t1 + SECOND);
        let measured = bandwidth.bytes_per_second().expect("uploads were measured");
        assert!((measured - 3_000_000.0).abs() < 1.0, "{measured}");
    }
}
