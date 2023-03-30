#![feature(result_option_inspect)]

pub mod cfg;
pub(crate) mod connection_tracker;
mod delegate_task;
pub mod error;
pub mod handler;
mod node;
mod node_config;
pub mod server;
pub(crate) mod stream_manager;

pub use crate::cfg::server_config::ServerConfig;

#[cfg(test)]
mod tests {
    #[test]
    fn test_thread_affinity() {
        let core_ids = core_affinity::get_core_ids().unwrap();
        core_ids.into_iter().for_each(|id| {
            println!("{}", id.id);
        });
    }
}
