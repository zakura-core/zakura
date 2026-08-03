//! Utilities for Zakura development, not for library or application users.
#![doc(html_favicon_url = "https://zakura.com/assets/rustdoc/zakura-favicon-128.png")]
#![doc(html_logo_url = "https://zakura.com/assets/rustdoc/zakura-icon.png")]
#![doc(html_root_url = "https://docs.rs/zakura_utils")]

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Initialise tracing using its defaults.
pub fn init_tracing() {
    tracing_subscriber::Registry::default()
        .with(tracing_error::ErrorLayer::default())
        .init();
}
