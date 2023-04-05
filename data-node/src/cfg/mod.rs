pub(crate) mod server_config;

use serde::{Deserialize, Serialize};

use crate::error::LaunchError;

#[derive(Debug, Serialize, Deserialize)]
pub struct Server {
    pub host: String,
    pub port: u16,
    pub concurrency: usize,

    #[serde(rename = "queue-depth")]
    pub queue_depth: u16,

    #[serde(rename = "placement-manager")]
    pub placement_manager: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Path {
    pub wal: String,
    pub metadata: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Store {
    pub path: Path,

    #[serde(rename = "queue-depth")]
    pub queue_depth: u16,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Configuration {
    pub server: Server,
    pub store: Store,
}

impl Configuration {
    pub fn validate(&self) -> Result<(), LaunchError> {
        let total_processor_num = num_cpus::get();
        if self.server.concurrency + 1 > total_processor_num {
            return Err(LaunchError::NoCoresAvailable);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Configuration;
    use std::{error::Error, fs::File, io::Read, path::Path};

    #[test]
    fn test_yaml() -> Result<(), Box<dyn Error>> {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
        let path = Path::new(&manifest_dir);
        let path = path.parent().unwrap().join("etc/config.yaml");
        let mut file = File::open(path.as_path())?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;
        let config: Configuration = serde_yaml::from_str(&content)?;
        assert_eq!(10911, config.server.port);
        assert_eq!(1, config.server.concurrency);
        assert_eq!(128, config.server.queue_depth);
        Ok(())
    }
}
