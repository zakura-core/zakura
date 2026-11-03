//! Tokio's `Semaphore` with preemption points, simulator build.
//!
//! Each acquire and each `available_permits` or `is_closed` read is a point,
//! and so is the moment right after permits go back. Tokio's semaphore stays
//! fair: a release hands permits to waiters in the order they asked, as in
//! production.

use std::{fmt, future::Future, panic::Location, sync::Arc};

use tokio::sync::{AcquireError, TryAcquireError};

use crate::sim::{point, Op};

/// Tokio's `Semaphore`, with preemption points.
pub struct Semaphore {
    // Tokio's owned permits need an `Arc` of its semaphore.
    inner: Arc<tokio::sync::Semaphore>,
}

impl Semaphore {
    /// The most permits a semaphore can hold, as tokio's.
    pub const MAX_PERMITS: usize = tokio::sync::Semaphore::MAX_PERMITS;

    /// A semaphore with `permits` permits.
    pub fn new(permits: usize) -> Self {
        Semaphore {
            inner: Arc::new(tokio::sync::Semaphore::new(permits)),
        }
    }

    /// Permits free now.
    #[track_caller]
    pub fn available_permits(&self) -> usize {
        point(Op::Acquire, Location::caller());
        self.inner.available_permits()
    }

    /// Adds `n` permits, waking waiters in order.
    #[track_caller]
    pub fn add_permits(&self, n: usize) {
        self.inner.add_permits(n);
        point(Op::Release, Location::caller());
    }

    /// Removes up to `n` free permits and returns how many it removed.
    #[track_caller]
    pub fn forget_permits(&self, n: usize) -> usize {
        point(Op::Acquire, Location::caller());
        self.inner.forget_permits(n)
    }

    /// Waits for one permit.
    #[track_caller]
    pub fn acquire(&self) -> impl Future<Output = Result<SemaphorePermit<'_>, AcquireError>> + '_ {
        self.acquire_many_at(1, Location::caller())
    }

    /// Waits for `n` permits at once.
    #[track_caller]
    pub fn acquire_many(
        &self,
        n: u32,
    ) -> impl Future<Output = Result<SemaphorePermit<'_>, AcquireError>> + '_ {
        self.acquire_many_at(n, Location::caller())
    }

    /// [`Semaphore::acquire_many`], reporting `site` as the call site.
    async fn acquire_many_at(
        &self,
        n: u32,
        site: &'static Location<'static>,
    ) -> Result<SemaphorePermit<'_>, AcquireError> {
        point(Op::Acquire, site);
        let inner = self.inner.acquire_many(n).await?;
        Ok(SemaphorePermit {
            inner: Some(inner),
            site,
        })
    }

    /// Takes one permit if one is free now.
    #[track_caller]
    pub fn try_acquire(&self) -> Result<SemaphorePermit<'_>, TryAcquireError> {
        self.try_acquire_many_at(1, Location::caller())
    }

    /// Takes `n` permits if they're free now.
    #[track_caller]
    pub fn try_acquire_many(&self, n: u32) -> Result<SemaphorePermit<'_>, TryAcquireError> {
        self.try_acquire_many_at(n, Location::caller())
    }

    /// [`Semaphore::try_acquire_many`], reporting `site` as the call site.
    fn try_acquire_many_at(
        &self,
        n: u32,
        site: &'static Location<'static>,
    ) -> Result<SemaphorePermit<'_>, TryAcquireError> {
        point(Op::Acquire, site);
        let inner = self.inner.try_acquire_many(n)?;
        Ok(SemaphorePermit {
            inner: Some(inner),
            site,
        })
    }

    /// Waits for one permit that keeps the semaphore alive.
    #[track_caller]
    pub fn acquire_owned(
        self: Arc<Self>,
    ) -> impl Future<Output = Result<OwnedSemaphorePermit, AcquireError>> {
        self.acquire_many_owned_at(1, Location::caller())
    }

    /// Waits for `n` permits that keep the semaphore alive.
    #[track_caller]
    pub fn acquire_many_owned(
        self: Arc<Self>,
        n: u32,
    ) -> impl Future<Output = Result<OwnedSemaphorePermit, AcquireError>> {
        self.acquire_many_owned_at(n, Location::caller())
    }

    /// [`Semaphore::acquire_many_owned`], reporting `site` as the call site.
    async fn acquire_many_owned_at(
        self: Arc<Self>,
        n: u32,
        site: &'static Location<'static>,
    ) -> Result<OwnedSemaphorePermit, AcquireError> {
        point(Op::Acquire, site);
        let inner = Arc::clone(&self.inner).acquire_many_owned(n).await?;
        Ok(OwnedSemaphorePermit {
            inner: Some(inner),
            semaphore: self,
            site,
        })
    }

    /// Takes one permit that keeps the semaphore alive, if one is free now.
    #[track_caller]
    pub fn try_acquire_owned(self: Arc<Self>) -> Result<OwnedSemaphorePermit, TryAcquireError> {
        self.try_acquire_many_owned_at(1, Location::caller())
    }

    /// Takes `n` permits that keep the semaphore alive, if they're free now.
    #[track_caller]
    pub fn try_acquire_many_owned(
        self: Arc<Self>,
        n: u32,
    ) -> Result<OwnedSemaphorePermit, TryAcquireError> {
        self.try_acquire_many_owned_at(n, Location::caller())
    }

    /// [`Semaphore::try_acquire_many_owned`], reporting `site` as the call
    /// site.
    fn try_acquire_many_owned_at(
        self: Arc<Self>,
        n: u32,
        site: &'static Location<'static>,
    ) -> Result<OwnedSemaphorePermit, TryAcquireError> {
        point(Op::Acquire, site);
        let inner = Arc::clone(&self.inner).try_acquire_many_owned(n)?;
        Ok(OwnedSemaphorePermit {
            inner: Some(inner),
            semaphore: self,
            site,
        })
    }

    /// Fails every pending and future acquire.
    #[track_caller]
    pub fn close(&self) {
        point(Op::Acquire, Location::caller());
        self.inner.close();
    }

    /// True once closed.
    #[track_caller]
    pub fn is_closed(&self) -> bool {
        point(Op::Acquire, Location::caller());
        self.inner.is_closed()
    }
}

impl fmt::Debug for Semaphore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

/// Permits borrowed from a [`Semaphore`]. Dropping them gives them back.
#[must_use]
pub struct SemaphorePermit<'a> {
    inner: Option<tokio::sync::SemaphorePermit<'a>>,
    site: &'static Location<'static>,
}

impl SemaphorePermit<'_> {
    /// Keeps the permits out of the semaphore for good.
    pub fn forget(mut self) {
        if let Some(inner) = self.inner.take() {
            inner.forget();
        }
    }

    /// Adds `other`'s permits to these.
    ///
    /// # Panics
    ///
    /// If the permits come from different semaphores.
    pub fn merge(&mut self, mut other: Self) {
        let other = other
            .inner
            .take()
            .expect("a permit's inner permit is only taken by consuming it");
        self.inner
            .as_mut()
            .expect("a permit's inner permit is only taken by consuming it")
            .merge(other);
    }

    /// Splits `n` permits off into a new permit.
    pub fn split(&mut self, n: usize) -> Option<Self> {
        // The inner permit is only taken by consuming methods.
        let inner = self.inner.as_mut()?.split(n)?;
        Some(SemaphorePermit {
            inner: Some(inner),
            site: self.site,
        })
    }

    /// How many permits this holds.
    pub fn num_permits(&self) -> usize {
        self.inner.as_ref().map_or(0, |p| p.num_permits())
    }
}

impl Drop for SemaphorePermit<'_> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            drop(inner);
            point(Op::Release, self.site);
        }
    }
}

impl fmt::Debug for SemaphorePermit<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SemaphorePermit")
            .field("permits", &self.num_permits())
            .finish()
    }
}

/// Permits from a [`Semaphore`] that keep it alive. Dropping them gives them
/// back.
#[must_use]
pub struct OwnedSemaphorePermit {
    inner: Option<tokio::sync::OwnedSemaphorePermit>,
    // Held so `Arc::strong_count` and `Weak` on the semaphore count permits,
    // as they do with tokio's.
    semaphore: Arc<Semaphore>,
    site: &'static Location<'static>,
}

impl OwnedSemaphorePermit {
    /// Keeps the permits out of the semaphore for good.
    pub fn forget(mut self) {
        if let Some(inner) = self.inner.take() {
            inner.forget();
        }
    }

    /// Adds `other`'s permits to these.
    ///
    /// # Panics
    ///
    /// If the permits come from different semaphores.
    pub fn merge(&mut self, mut other: Self) {
        let other = other
            .inner
            .take()
            .expect("a permit's inner permit is only taken by consuming it");
        self.inner
            .as_mut()
            .expect("a permit's inner permit is only taken by consuming it")
            .merge(other);
    }

    /// Splits `n` permits off into a new permit.
    pub fn split(&mut self, n: usize) -> Option<Self> {
        // The inner permit is only taken by consuming methods.
        let inner = self.inner.as_mut()?.split(n)?;
        Some(OwnedSemaphorePermit {
            inner: Some(inner),
            semaphore: Arc::clone(&self.semaphore),
            site: self.site,
        })
    }

    /// The semaphore these permits came from.
    pub fn semaphore(&self) -> &Arc<Semaphore> {
        &self.semaphore
    }

    /// How many permits this holds.
    pub fn num_permits(&self) -> usize {
        self.inner.as_ref().map_or(0, |p| p.num_permits())
    }
}

impl Drop for OwnedSemaphorePermit {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            drop(inner);
            point(Op::Release, self.site);
        }
    }
}

impl fmt::Debug for OwnedSemaphorePermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedSemaphorePermit")
            .field("permits", &self.num_permits())
            .finish()
    }
}
