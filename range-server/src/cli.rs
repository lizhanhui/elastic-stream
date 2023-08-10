use clap::{Args, Parser, Subcommand};
use config::Configuration;
use log::{info, trace};
use nix::{errno::Errno, ifaddrs::getifaddrs};
use std::{
    fs::File,
    net::{Ipv4Addr, SocketAddr},
    path::Path,
    str::FromStr,
};

#[derive(Debug, Parser, Clone)]
#[command(author, about, version, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Clone, Args)]
pub struct StartArgs {
    /// The address that the range server binds and listens to.
    ///
    /// Default value is `127.0.0.1:10911`.
    ///
    /// If the range server is running inside a container, specify the address as `0.0.0.0:10911`.
    #[arg(long, env = "ES_ADDR")]
    addr: Option<String>,

    /// Range server advertising address for clients to connect
    ///
    /// Default value is the same to `addr`.
    #[arg(long, env = "ES_ADVERTISE_ADDR")]
    advertise_addr: Option<String>,

    /// The address of placement-driver service: `domain-name:port`
    ///
    /// Default value: `127.0.0.1:12378`
    #[arg(long, env = "ES_PD")]
    pd: Option<String>,

    /// Base path of the store, containing lock, immutable properties and other configuration files
    /// It could be absolute or relative to the current working directory
    ///
    /// Default value: `/data/store`
    #[arg(long, env = "ES_STORE_PATH")]
    store_path: Option<String>,

    /// Path to the configuration file in YAML format.
    #[arg(long, env = "ES_CONFIG")]
    config: Option<String>,

    /// Path to the log4rs configuration file in YAML format.
    #[arg(long, env = "ES_LOG_CONFIG")]
    log: Option<String>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Commands {
    Start(StartArgs),
    BuildInfo,
}

impl StartArgs {
    pub fn init_log(&self) -> anyhow::Result<()> {
        let config = self
            .log
            .as_deref()
            .unwrap_or("/etc/range-server/range-server-log.yaml");
        let config_path = Path::new(config);

        if !config_path.exists() {
            eprintln!("Log configuration file {} does not exist", config);
            // Exit with errno set
            std::process::exit(2);
        };

        if !config_path.is_file() {
            eprintln!("{} is not a file", config);
            // Exit with errno set
            std::process::exit(22);
        }

        log4rs::init_file(config_path, Default::default())?;
        info!("Log initialized");
        Ok(())
    }

    pub fn create_config(&self) -> anyhow::Result<Configuration> {
        let path = Path::new(
            self.config
                .as_deref()
                .unwrap_or("/etc/range-server/range-server.yaml"),
        );
        let mut configuration = if path.exists() && path.is_file() {
            serde_yaml::from_reader(File::open(path)?)?
        } else {
            Configuration::default()
        };

        configuration.placement_driver = match &self.pd {
            Some(pd) => pd.clone(),
            None => String::from("127.0.0.1:12378"),
        };

        configuration.server.addr = match &self.addr {
            Some(addr) => addr.clone(),
            None => String::from("0.0.0.0:10911"),
        };

        let socket_addr = SocketAddr::from_str(&configuration.server.addr)?;
        let port = socket_addr.port();

        match &self.advertise_addr {
            Some(advertise_addr) => {
                configuration.server.advertise_addr = advertise_addr.clone();
            }
            None => {
                let addr = pick_ip()?;
                configuration.server.advertise_addr = addr
                    .map_or(format!("127.0.0.1:{}", port), |addr| {
                        format!("{}:{}", addr, port)
                    });
                info!("Advertise address: {}", configuration.server.advertise_addr);
            }
        }

        let base_path = match &self.store_path {
            Some(store_path) => store_path.clone(),
            None => String::from("/data/store"),
        };
        configuration.store.path.set_base(&base_path);

        configuration.check_and_apply()?;
        Ok(configuration)
    }
}

/// Pick an address among interfaces
///
/// IP addresses are pick in the following order
/// * Global IP address
/// * Shared IP address
/// * Private IP address
/// * Other IP address kinds
pub(crate) fn pick_ip() -> Result<Option<Ipv4Addr>, Errno> {
    let addrs = getifaddrs()?;
    let mut shared = vec![];
    let mut private = vec![];
    let mut other = vec![];

    for ifaddr in addrs {
        if let Some(address) = ifaddr.address {
            if let Some(addr) = address.as_sockaddr_in() {
                let ip = addr.ip();
                let ipv4_addr = Ipv4Addr::from(ip);
                if ipv4_addr.is_loopback() {
                    continue;
                }

                if ipv4_addr.is_global() {
                    trace!("Host has a global IP: {addr}");
                    return Ok(Some(ipv4_addr));
                }

                if ipv4_addr.is_shared() {
                    trace!("Host has a shared IP: {addr}");
                    shared.push(ipv4_addr);
                    continue;
                }

                if ipv4_addr.is_private() {
                    trace!("Host has a private IP: {addr}");
                    private.push(ipv4_addr);
                    continue;
                }

                trace!("Host has an IP: {addr}");
                other.push(ipv4_addr);
            }
        }
    }

    if !shared.is_empty() {
        return Ok(shared.pop());
    }

    if !private.is_empty() {
        return Ok(private.pop());
    }

    if !other.is_empty() {
        return Ok(other.pop());
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use log::debug;
    use nix::errno::Errno;

    #[test]
    fn test_pick_ip() -> Result<(), Errno> {
        ulog::try_init_log();
        let ip = super::pick_ip()?;
        if let Some(addr) = ip {
            debug!("Picked IP: {addr}");
        } else {
            panic!("Failed to pick a non-loopback address");
        }
        Ok(())
    }
}
