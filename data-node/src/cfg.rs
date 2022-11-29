use clap::Parser;

#[derive(Debug, Parser, Clone)]
#[command(author, version, about, long_about = None)]
pub struct ServerConfig {
    /// Listening port
    #[arg(short, long, default_value_t = 3536)]
    pub port: u16,

    /// Number of thread-per-core actors
    #[arg(short, long, default_value_t = 1)]
    pub thread: u16,

    #[arg(short, long, default_value_t = 1024)]
    pub queue_depth: u32,
}

impl ServerConfig {
    pub fn actor_count(&self) -> u16 {
        let processor_number = num_cpus::get() as u16;
        if processor_number < self.thread {
            processor_number
        } else {
            self.thread
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_actor_count() {
        let config = ServerConfig::parse_from(["data-node", "-p", "123", "-q", "2048"]);
        assert_eq!(123, config.port);
        assert_eq!(1, config.thread);
        assert_eq!(2048, config.queue_depth);
    }

    #[test]
    fn test_actor_count_with_excessive_thread() {
        let config =
            ServerConfig::parse_from(["data-node", "-p", "123", "-q", "2048", "-t", "65535"]);
        assert_eq!(123, config.port);
        assert_eq!(num_cpus::get(), config.actor_count() as usize);
        assert_eq!(2048, config.queue_depth);
    }
}
