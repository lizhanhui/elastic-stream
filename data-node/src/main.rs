use clap::Parser;
use data_node::ServerConfig;

fn main() {
    let server_config = ServerConfig::parse();
    if let Err(e) = data_node::server::launch(&server_config) {
        eprintln!("Failed to start data-node: {:?}", e);
    }
}
