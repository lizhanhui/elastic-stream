use clap::Parser;
use data_node::cfg::ServerConfig;

fn main() {
    let server_config = ServerConfig::parse();
    data_node::server::launch(&server_config);
}
