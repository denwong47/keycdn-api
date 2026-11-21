//! A library for interacting with the KeyCDN API.
//!
//! Currently only supports the
//! [IP Location Finder API](https://tools.keycdn.com/geo#ip-location-finder-api).

pub mod models;

#[cfg(test)]
#[allow(unused)]
pub(crate) mod logging {
    pub use tracing_subscriber;
    use tracing_subscriber::EnvFilter;

    static INIT_LOG: std::sync::Once = std::sync::Once::new();

    const DEFAULT_LOG_LEVEL: &str = "debug";

    /// Placeholder for crates that are noisy and should be set to a higher log
    /// level than the default.
    pub const NOISY_CRATE_OVERRIDES: [&str; 0] = [];

    fn build_env_filter(default_level: &str) -> EnvFilter {
        let base = std::env::var("RUST_LOG").unwrap_or_else(|_| default_level.to_owned());
        EnvFilter::new(format!("{base},{}", NOISY_CRATE_OVERRIDES.join(",")))
    }

    pub fn init_test_tracing() {
        INIT_LOG.call_once(|| {
            tracing_subscriber::fmt()
                .with_env_filter(build_env_filter("trace"))
                .with_target(true)
                .with_level(true)
                .with_ansi(true)
                .with_writer(std::io::stderr)
                .try_init()
                .ok();
        });
    }
}
