use thiserror::Error;
use zakura_chain::{
    block,
    parameters::{Network, NetworkKind},
    work::equihash,
};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum PowPolicyKind {
    Validate,
    AuthenticatedCustomWaiver,
}

/// Network-bound proof-of-work policy.
/// Callers cannot construct a production waiver.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PowPolicy {
    network: Network,
    kind: PowPolicyKind,
}

/// Invalid attempt to derive a proof-of-work waiver.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum PowPolicyError {
    /// Mainnet and the default public Testnet can never receive a waiver.
    #[error("proof-of-work cannot be disabled for production network {0:?}")]
    ProductionNetwork(NetworkKind),
    /// The authenticated custom configuration does not disable proof of work.
    #[error("custom network configuration does not authenticate a proof-of-work waiver")]
    PowNotDisabled,
}

impl PowPolicy {
    /// Construct a policy that always verifies proof of work for the bound network.
    pub fn validating(network: &Network) -> Self {
        Self {
            network: network.clone(),
            kind: PowPolicyKind::Validate,
        }
    }

    /// Derive the only policy permitted by an authenticated network configuration.
    pub fn for_network(network: &Network) -> Result<Self, PowPolicyError> {
        if network.disable_pow() {
            Self::authenticated_custom_waiver(network)
        } else {
            Ok(Self::validating(network))
        }
    }

    /// Attempt to construct a waiver from an authenticated custom-network configuration.
    pub fn authenticated_custom_waiver(network: &Network) -> Result<Self, PowPolicyError> {
        if matches!(network.kind(), NetworkKind::Mainnet)
            || (matches!(network.kind(), NetworkKind::Testnet) && network.is_default_testnet())
        {
            return Err(PowPolicyError::ProductionNetwork(network.kind()));
        }
        if !network.disable_pow() {
            return Err(PowPolicyError::PowNotDisabled);
        }
        Ok(Self {
            network: network.clone(),
            kind: PowPolicyKind::AuthenticatedCustomWaiver,
        })
    }

    /// Validate solution shape, network parameters, and proof unless this exact custom network
    /// has an authenticated disabled-PoW configuration.
    pub fn validate_solution(&self, header: &block::Header) -> Result<(), equihash::Error> {
        match self.kind {
            PowPolicyKind::Validate => header.solution.check(header, &self.network),
            PowPolicyKind::AuthenticatedCustomWaiver => {
                header.solution.validate_shape(&self.network)
            }
        }
    }

    /// Return true when this exact authenticated custom configuration waives Equihash.
    pub fn is_authenticated_custom_waiver(&self) -> bool {
        self.kind == PowPolicyKind::AuthenticatedCustomWaiver
    }
}
