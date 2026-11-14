//! `tx-relay`, the v2 service that moves transactions between peers and
//! hands them to the pool core.
//!
//! Only its contract exists so far: [`model`], an independent model of the
//! service's three owners driving the pool's model, and the cases it must
//! cover. The machines and the IO are built against it.

mod model;
#[cfg(test)]
mod tests;
