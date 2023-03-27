use clap::Parser;

pub const DEFAULT_PORT: u16 = 10911;

pub const DEFAULT_QUEUE_DEPTH: u32 = 32768;

pub const DEFAULT_CONCURRENCY: usize = 1;

#[derive(Debug, Parser, Clone)]
#[command(author, version, about, long_about = None)]
pub struct ServerConfig {
    #[arg(long)]
    pub host: String,

    /// Listening port
    #[arg(short, long, default_value_t = DEFAULT_PORT)]
    pub port: u16,

    /// Number of thread-per-core worker nodes
    #[arg(short, long, default_value_t = DEFAULT_CONCURRENCY, value_parser = parse_concurrency)]
    pub concurrency: usize,

    #[arg(short, long, default_value_t = DEFAULT_QUEUE_DEPTH)]
    pub queue_depth: u32,

    #[arg(long)]
    pub placement_manager: String,
}

fn parse_concurrency(s: &str) -> Result<usize, String> {
    let concurrency: usize = s.parse().map_err(|_| format!("{} is not a number", s))?;

    if 0 == concurrency {
        return Err(format!("{} is not a valid concurrency value", s));
    }

    let available_core_num = match core_affinity::get_core_ids() {
        Some(ids) => ids.len(),
        None => return Err("Cores not available".to_owned()),
    };

    let range = 1..=available_core_num;
    if !range.contains(&concurrency) {
        return Err(format!(
            "num_cpu: {}, num_of_available_core: {}, expected_concurrency: {}",
            num_cpus::get(),
            available_core_num,
            concurrency
        ));
    }
    Ok(concurrency)
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_actor_count() {
        let config = ServerConfig::parse_from([
            "data-node",
            "--host",
            "localhost",
            "-p",
            "123",
            "-q",
            "2048",
            "--placement-manager",
            "localhost:2378",
        ]);
        assert_eq!(123, config.port);
        assert_eq!(1, config.concurrency);
        assert_eq!(2048, config.queue_depth);
        assert_eq!("localhost:2378", &config.placement_manager);
    }

    #[test]
    fn test_actor_count_with_excessively_large_num() {
        let config = ServerConfig::parse_from([
            "data-node",
            "--host",
            "localhost",
            "-p",
            "123",
            "-c",
            format!("{}", num_cpus::get()).as_str(),
            "-q",
            "2048",
            "--placement-manager",
            "localhost:2378",
        ]);
        assert_eq!(123, config.port);
        assert_eq!(2048, config.queue_depth);
        assert_eq!(num_cpus::get(), config.concurrency);
    }
}
