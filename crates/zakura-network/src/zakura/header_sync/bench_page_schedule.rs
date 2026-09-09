//! Deterministic serving delays for isolated, single-client benchmark fixtures.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        OnceLock,
    },
    time::Duration,
};

use super::ZakuraPeerId;

#[derive(Clone, Copy, Debug)]
struct Schedule {
    free_pages: u64,
    delay_ms: u64,
}

impl Schedule {
    fn parse(value: &str) -> Result<Self, &'static str> {
        let (free_pages, delay_ms) = value
            .split_once(':')
            .ok_or("expected free_pages:delay_ms")?;
        let free_pages = free_pages
            .parse::<u64>()
            .map_err(|_| "invalid free page count")?;
        let delay_ms = delay_ms.parse::<u64>().map_err(|_| "invalid delay")?;
        if free_pages > 1000 || delay_ms > 5000 {
            return Err("free pages must be at most 1000 and delay at most 5000 ms");
        }
        Ok(Self {
            free_pages,
            delay_ms,
        })
    }

    fn delay(&self, page: u64) -> Duration {
        Duration::from_millis(if page <= self.free_pages {
            0
        } else {
            self.delay_ms
        })
    }
}

pub(super) async fn before_page_completion(peer: &ZakuraPeerId, request_id: u64) {
    static SCHEDULE: OnceLock<Option<Schedule>> = OnceLock::new();
    static PAGE: AtomicU64 = AtomicU64::new(0);
    let Some(schedule) =
        SCHEDULE.get_or_init(|| match std::env::var("SYNC_BENCH_HEADER_SCHEDULE") {
            Ok(value) => Some(
                Schedule::parse(&value)
                    .unwrap_or_else(|error| panic!("invalid benchmark header schedule: {error}")),
            ),
            Err(std::env::VarError::NotPresent) => None,
            Err(std::env::VarError::NotUnicode(_)) => {
                panic!("benchmark header schedule must be Unicode")
            }
        })
    else {
        return;
    };
    let previous = PAGE
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_add(1))
        })
        .expect("the page counter update always supplies a value");
    let page = previous.saturating_add(1);
    let delay = schedule.delay(page);
    tracing::info!(target: "sync_fixture", ?peer, request_id, page, delay_ms = schedule.delay_ms,
        delayed = !delay.is_zero(), phase = "header_page_ready");
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
    tracing::info!(target: "sync_fixture", ?peer, request_id, page, phase = "header_page_released");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_pages_have_exact_delay() {
        let schedule = Schedule::parse("2:2500").unwrap();
        assert_eq!(schedule.delay(1), Duration::ZERO);
        assert_eq!(schedule.delay(2), Duration::ZERO);
        assert_eq!(schedule.delay(3), Duration::from_millis(2500));
        assert_eq!(schedule.delay(u64::MAX), Duration::from_millis(2500));
        assert_eq!(Schedule::parse("0:0").unwrap().delay(1), Duration::ZERO);
    }

    #[test]
    fn rejects_unbounded_or_ambiguous_schedules() {
        for value in ["", "1", "1:2:3", "-1:2500", "0:5001", "1001:0"] {
            assert!(Schedule::parse(value).is_err(), "accepted {value}");
        }
    }
}
