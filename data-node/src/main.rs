use clap::Parser;
use data_node::cfg::ServerConfig;

fn main() {
    let server_config = ServerConfig::parse();
    println!("{:?}", server_config);
}
