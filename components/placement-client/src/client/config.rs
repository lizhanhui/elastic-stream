use std::time::Duration;

/// Default connection timeout in seconds.
const CONNECT_TIMEOUT_IN_SECS: u64 = 3;

const HEARTBEAT_INTERVAL_IN_SECS: u64 = 30;

/// Placement client configuration
#[derive(Debug, Clone)]
pub(crate) struct ClientConfig {
    /// Maximum amount of time to wait when creating connections to placement manager servers.
    pub(crate) connect_timeout: Duration,

    pub(crate) heartbeat_interval: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(CONNECT_TIMEOUT_IN_SECS),
            heartbeat_interval: Duration::from_secs(HEARTBEAT_INTERVAL_IN_SECS),
        }
    }
}
