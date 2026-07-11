//! Trait aliases for block verification Tower services.
//!
//! This trait provides a convenient alias for `tower::Service`
//! implementations that operate on Zebra block verification request and response types.
//!
//! - [`BlockVerifierService`]: for services that handle block verification requests.

use crate::router::Request;
use zakura_chain::block::Hash;
use zakura_node_services::service_traits::ZakuraService;

/// Trait alias for services handling block verification requests.
pub trait BlockVerifierService: ZakuraService<Request, Hash> {}

impl<T> BlockVerifierService for T where T: ZakuraService<Request, Hash> {}
