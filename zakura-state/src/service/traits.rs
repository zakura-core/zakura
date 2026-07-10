//! Trait aliases for state-related Tower services.
//!
//! These traits provide convenient aliases for `tower::Service`
//! implementations that operate on Zebra state request and response types.
//!
//! - [`State`]: for services that handle state-modifying requests.
//! - [`ReadState`]: for services that handle read-only state requests.

use crate::{ReadRequest, ReadResponse, Request, Response};
use zakura_node_services::service_traits::ZakuraService;

/// Trait alias for services handling state-modifying requests.
pub trait State: ZakuraService<Request, Response> {}

impl<T> State for T where T: ZakuraService<Request, Response> {}

/// Trait alias for services handling read-only state requests.
pub trait ReadState: ZakuraService<ReadRequest, ReadResponse> {}

impl<T> ReadState for T where T: ZakuraService<ReadRequest, ReadResponse> {}
