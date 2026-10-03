//! Native inbound transport accounting. Owners retire with QUIC state.

use crate::zakura::canonical_ip;
use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{Arc, Mutex},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Keep one eighth available to outbound work, without increasing the total.
/// A single effective slot remains shared for compatibility.
pub(super) fn inbound_capacity(total: usize) -> usize {
    let total = total.max(1);
    if total == 1 {
        total
    } else {
        total - (total / 8).max(1)
    }
}

#[derive(Clone, Debug)]
pub(super) struct IncomingTransportBudget(Arc<Budget>);

#[derive(Debug)]
struct Budget {
    global: Arc<Semaphore>,
    inbound: Arc<Semaphore>,
    per_source: usize,
    sources: Mutex<HashMap<IpAddr, usize>>,
    #[cfg(test)]
    accounting_errors: std::sync::atomic::AtomicUsize,
}

struct TransportOwner {
    _source: SourceOwner,
    _inbound: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
}

struct SourceOwner {
    budget: Arc<Budget>,
    source: IpAddr,
}

impl IncomingTransportBudget {
    pub(super) fn new(global: Arc<Semaphore>, total: usize, per_ip: usize) -> Self {
        let inbound = inbound_capacity(total);
        Self(Arc::new(Budget {
            global,
            inbound: Arc::new(Semaphore::new(inbound)),
            // One replacement may authenticate while its incumbent still owns state.
            per_source: per_ip.saturating_add(1).min(inbound),
            sources: Mutex::new(HashMap::new()),
            #[cfg(test)]
            accounting_errors: std::sync::atomic::AtomicUsize::new(0),
        }))
    }

    /// Called only after address validation. Rejected sources must not take even
    /// transient ownership of capacity needed by concurrent outbound dials.
    pub(super) fn reserve(&self, source: IpAddr) -> Option<Box<dyn std::any::Any + Send + Sync>> {
        let source = canonical_ip(source);
        let mut sources = self
            .0
            .sources
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if sources.get(&source).copied().unwrap_or(0) >= self.0.per_source {
            metrics::counter!("zakura.p2p.conn.rejected.source_cap").increment(1);
            return None;
        }
        let Ok(inbound) = self.0.inbound.clone().try_acquire_owned() else {
            metrics::counter!("zakura.p2p.conn.rejected.inbound_share").increment(1);
            return None;
        };
        let Ok(global) = self.0.global.clone().try_acquire_owned() else {
            metrics::counter!("zakura.p2p.conn.rejected.transport_admission").increment(1);
            return None;
        };
        *sources.entry(source).or_default() += 1;
        drop(sources);
        Some(Box::new(TransportOwner {
            _global: global,
            _inbound: inbound,
            _source: SourceOwner {
                budget: self.0.clone(),
                source,
            },
        }))
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self) -> (usize, usize, usize, usize) {
        let sources = self
            .0
            .sources
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        (
            self.0.inbound.available_permits(),
            sources.len(),
            sources.values().sum(),
            self.0
                .accounting_errors
                .load(std::sync::atomic::Ordering::SeqCst),
        )
    }
}

impl Drop for SourceOwner {
    fn drop(&mut self) {
        let mut sources = self
            .budget
            .sources
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let valid = match sources.get_mut(&self.source) {
            Some(count) if *count > 1 => {
                *count -= 1;
                true
            }
            Some(1) => {
                sources.remove(&self.source);
                true
            }
            _ => false,
        };
        drop(sources);
        if !valid {
            #[cfg(test)]
            self.budget
                .accounting_errors
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            metrics::counter!("zakura.p2p.conn.admission.accounting_error").increment(1);
            tracing::error!("native transport source accounting invariant violated");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserves_and_config_zero_semantics() {
        for (total, inbound) in [
            (0, 1),
            (1, 1),
            (2, 1),
            (4, 3),
            (8, 7),
            (32, 28),
            (256, 224),
            (usize::MAX, usize::MAX - usize::MAX / 8),
        ] {
            assert_eq!(inbound_capacity(total), inbound);
        }
        let config = crate::zakura::ZakuraConfig {
            max_connections_per_ip: 0,
            ..Default::default()
        };
        let budget = IncomingTransportBudget::new(
            Arc::new(Semaphore::new(256)),
            256,
            config.max_connections_per_ip(),
        );
        assert_eq!(budget.0.per_source, 17);
        let saturated = IncomingTransportBudget::new(Arc::new(Semaphore::new(4)), 4, usize::MAX);
        assert_eq!(saturated.0.per_source, 3);
    }

    #[test]
    fn source_aliases_rejections_and_churn_return_all_capacity() {
        let global = Arc::new(Semaphore::new(4));
        let budget = IncomingTransportBudget::new(global.clone(), 4, 1);
        let one = budget.reserve("127.0.0.1".parse().unwrap()).unwrap();
        let two = budget.reserve("::ffff:127.0.0.1".parse().unwrap()).unwrap();
        assert!(budget.reserve("127.0.0.1".parse().unwrap()).is_none());
        assert_eq!(global.available_permits(), 2);
        let three = budget.reserve("::1".parse().unwrap()).unwrap();
        assert!(budget.reserve("::2".parse().unwrap()).is_none());
        let outgoing = global.clone().try_acquire_owned().unwrap();
        assert_eq!(global.available_permits(), 0);
        drop((one, two, three, outgoing));
        for address in 1..=255 {
            drop(
                budget
                    .reserve(IpAddr::V4(std::net::Ipv4Addr::new(192, 0, 2, address)))
                    .unwrap(),
            );
        }
        assert_eq!(global.available_permits(), 4);
        assert_eq!(budget.snapshot(), (3, 0, 0, 0));
        let all = global.clone().try_acquire_many_owned(4).unwrap();
        assert!(budget.reserve("::1".parse().unwrap()).is_none());
        assert_eq!(budget.snapshot(), (3, 0, 0, 0));
        drop(all);
    }

    #[test]
    fn owner_cleanup_recovers_a_poisoned_map() {
        let global = Arc::new(Semaphore::new(2));
        let budget = IncomingTransportBudget::new(global.clone(), 2, 1);
        let owner = budget.reserve("::1".parse().unwrap()).unwrap();
        let inner = budget.0.clone();
        assert!(std::thread::spawn(move || {
            let _guard = inner.sources.lock().unwrap();
            panic!("poison test");
        })
        .join()
        .is_err());
        drop(owner);
        assert_eq!(global.available_permits(), 2);
        assert_eq!(budget.snapshot(), (1, 0, 0, 0));
    }
}
