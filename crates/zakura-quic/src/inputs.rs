//! Where an endpoint's randomness comes from (API-2, ADM-10, TLS-9).
//!
//! [`crate::QuicEndpoint::bind`] keeps every production source. A
//! deterministic simulation passes [`EndpointInputs`] to
//! [`crate::QuicEndpoint::bind_with`] so the same inputs give the same
//! datagrams. Each input is one of `quinn-proto`'s or rustls' own injection
//! points, and this module only stores and applies them.

use std::{fmt, sync::Arc};

use quinn_proto::{
    crypto::{HandshakeTokenKey, HmacKey},
    ClientConfig, ConnectionId, ConnectionIdGenerator, EndpointConfig, ServerConfig,
};
use rustls::crypto::{SecureRandom, SupportedKxGroup};

/// Builds the connection ID generator for one socket's `quinn_proto::Endpoint`.
pub type CidGeneratorFactory = Arc<dyn Fn() -> Box<dyn ConnectionIdGenerator> + Send + Sync>;

/// Picks the destination connection ID of a dial's first Initial packet.
pub type InitialCidProvider = Arc<dyn Fn() -> ConnectionId + Send + Sync>;

/// Hears about each TLS provider built from an endpoint's inputs. The
/// simulator's seeded inputs use it to tell when the last TLS user of a
/// leaked `&'static` source is gone.
#[cfg(any(test, zakura_sim))]
pub(crate) trait ProviderWatch: Send + Sync {
    /// Called with each provider built from the injected TLS inputs.
    fn built(&self, provider: &Arc<rustls::crypto::CryptoProvider>);
}

/// The randomness an endpoint uses, input by input.
///
/// An input left unset keeps its production source, so
/// `EndpointInputs::default()` behaves exactly as [`crate::QuicEndpoint::bind`].
/// A deterministic run sets all of them. The node key is the `secret` passed
/// to `bind_with`, and both clocks come from the runtime, so neither is here.
#[derive(Clone, Default)]
pub struct EndpointInputs {
    rng_seed: Option<[u8; 32]>,
    reset_key: Option<Arc<dyn HmacKey>>,
    cid_generator: Option<CidGeneratorFactory>,
    initial_dst_cid: Option<InitialCidProvider>,
    token_key: Option<Arc<dyn HandshakeTokenKey>>,
    tls_random: Option<&'static dyn SecureRandom>,
    kx_groups: Option<Vec<&'static dyn SupportedKxGroup>>,
    /// Set only by the simulator. No public method replaces it.
    #[cfg(any(test, zakura_sim))]
    provider_watch: Option<Arc<dyn ProviderWatch>>,
}

impl fmt::Debug for EndpointInputs {
    /// Names the inputs that are set. Keys and generators don't print.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EndpointInputs")
            .field("rng_seed", &self.rng_seed.is_some())
            .field("reset_key", &self.reset_key.is_some())
            .field("cid_generator", &self.cid_generator.is_some())
            .field("initial_dst_cid", &self.initial_dst_cid.is_some())
            .field("token_key", &self.token_key.is_some())
            .field("tls_random", &self.tls_random.is_some())
            .field("kx_groups", &self.kx_groups.as_ref().map(Vec::len))
            .finish()
    }
}

impl EndpointInputs {
    /// Seeds each socket's endpoint RNG, from which `quinn-proto` derives
    /// every connection's RNG. Every socket of the endpoint gets this seed.
    ///
    /// Unset, each socket seeds itself from the OS.
    pub fn rng_seed(mut self, seed: [u8; 32]) -> Self {
        self.rng_seed = Some(seed);
        self
    }

    /// The stateless-reset key, shared by every socket of the endpoint
    /// (ADM-10).
    ///
    /// Unset, each socket gets a random ring HMAC key at bind.
    pub fn reset_key(mut self, key: Arc<dyn HmacKey>) -> Self {
        self.reset_key = Some(key);
        self
    }

    /// The factory each socket calls once for its connection ID generator.
    ///
    /// Unset, sockets use `quinn-proto`'s `HashedConnectionIdGenerator`,
    /// which draws from the thread RNG.
    pub fn cid_generator(mut self, factory: CidGeneratorFactory) -> Self {
        self.cid_generator = Some(factory);
        self
    }

    /// The destination connection ID of each dial's first Initial packet.
    /// RFC 9000 §7.2 requires at least 8 unpredictable bytes on a real
    /// network.
    ///
    /// Unset, each dial draws 20 bytes from the thread RNG.
    pub fn initial_dst_cid(mut self, provider: InitialCidProvider) -> Self {
        self.initial_dst_cid = Some(provider);
        self
    }

    /// The key that seals Retry tokens (ADM-10).
    ///
    /// Unset, `serve` derives a random HKDF key from the thread RNG.
    pub fn token_key(mut self, key: Arc<dyn HandshakeTokenKey>) -> Self {
        self.token_key = Some(key);
        self
    }

    /// The random source for TLS handshake randoms (TLS-9).
    ///
    /// Unset, rustls' ring provider uses ring's `SystemRandom`.
    pub fn tls_random(mut self, random: &'static dyn SecureRandom) -> Self {
        self.tls_random = Some(random);
        self
    }

    /// The TLS key-exchange groups, in preference order (TLS-9). ring's own
    /// groups draw their ephemeral keys from `SystemRandom` directly, so a
    /// seeded [`Self::tls_random`] alone doesn't fix the key shares.
    ///
    /// Unset, rustls' ring provider offers its default groups.
    pub fn kx_groups(mut self, groups: Vec<&'static dyn SupportedKxGroup>) -> Self {
        self.kx_groups = Some(groups);
        self
    }

    /// Tells `watch` about every TLS provider built from these inputs.
    #[cfg(any(test, zakura_sim))]
    pub(crate) fn provider_watch(mut self, watch: Arc<dyn ProviderWatch>) -> Self {
        self.provider_watch = Some(watch);
        self
    }

    /// Reports a provider built from these inputs to the watch, if any.
    #[cfg(any(test, zakura_sim))]
    pub(crate) fn provider_built(&self, provider: &Arc<rustls::crypto::CryptoProvider>) {
        if let Some(watch) = &self.provider_watch {
            watch.built(provider);
        }
    }

    /// The injected TLS random source, if any.
    pub(crate) fn injected_tls_random(&self) -> Option<&'static dyn SecureRandom> {
        self.tls_random
    }

    /// The injected key-exchange groups, if any.
    pub(crate) fn injected_kx_groups(&self) -> Option<&[&'static dyn SupportedKxGroup]> {
        self.kx_groups.as_deref()
    }

    /// Applies the seed, reset key and CID generator to one socket's
    /// endpoint config.
    pub(crate) fn apply_endpoint(&self, config: &mut EndpointConfig) {
        if let Some(seed) = self.rng_seed {
            config.rng_seed(Some(seed));
        }
        if let Some(key) = &self.reset_key {
            config.reset_key(key.clone());
        }
        if let Some(factory) = &self.cid_generator {
            let factory = factory.clone();
            config.cid_generator(move || factory());
        }
    }

    /// The server config for `crypto`, sealing Retry tokens with the
    /// injected key or a random one.
    pub(crate) fn server_config(
        &self,
        crypto: Arc<dyn quinn_proto::crypto::ServerConfig>,
    ) -> ServerConfig {
        match &self.token_key {
            Some(key) => ServerConfig::new(crypto, key.clone()),
            None => ServerConfig::with_crypto(crypto),
        }
    }

    /// Applies the initial destination CID provider to a dial's config.
    pub(crate) fn apply_client(&self, config: &mut ClientConfig) {
        if let Some(provider) = &self.initial_dst_cid {
            config.initial_dst_cid_provider(provider.clone());
        }
    }
}
