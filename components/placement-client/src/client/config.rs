use std::{
    process,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

/// Default connection timeout in seconds.
const CONNECT_TIMEOUT_IN_SECS: u64 = 3;

const HEARTBEAT_INTERVAL_IN_SECS: u64 = 30;

const MAX_ATTEMPT: u32 = 3;

static CLIENT_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn client_id() -> String {
    let hostname = gethostname::gethostname()
        .into_string()
        .unwrap_or(String::from("unknown"));
    format!(
        "{}-{}-{}",
        hostname,
        process::id(),
        CLIENT_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// Placement client configuration
#[derive(Debug, Clone)]
pub(crate) struct ClientConfig {
    /// Maximum amount of time to wait when creating connections to placement manager servers.
    pub(crate) connect_timeout: Duration,

    pub(crate) heartbeat_interval: Duration,

    pub(crate) max_attempt: u32,

    pub(crate) client_id: String,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(CONNECT_TIMEOUT_IN_SECS),
            heartbeat_interval: Duration::from_secs(HEARTBEAT_INTERVAL_IN_SECS),
            max_attempt: MAX_ATTEMPT,
            client_id: client_id(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::client_id;

    // Ensure generated client-id are unique.
    #[test]
    fn test_client_id() {
        let mut set = std::collections::HashSet::new();
        for _ in 0..100 {
            let client_id = client_id();
            assert_eq!(false, set.contains(&client_id));
            set.insert(client_id);
        }
        assert_eq!(100, set.len());
    }
}
