use std::time::Duration;

/// Default connection timeout in seconds.
const CONNECT_TIMEOUT_IN_SECS: u64 = 3;

/// Placement client configuration
#[derive(Debug)]
pub(crate) struct ClientConfig {
    /// Maximum amount of time to wait when creating connections to placement manager servers.
    pub(crate) connect_timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(CONNECT_TIMEOUT_IN_SECS),
        }
    }
}
