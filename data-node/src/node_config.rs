use crate::ServerConfig;
use core_affinity::CoreId;
use std::os::fd::RawFd;

pub(crate) struct NodeConfig {
    pub(crate) core_id: CoreId,
    pub(crate) server_config: ServerConfig,
    pub(crate) sharing_uring: RawFd,
    pub(crate) primary: bool,
}
