use core_affinity::CoreId;
use std::{os::fd::RawFd, sync::Arc};

use config::Configuration;

pub(crate) struct NodeConfig {
    pub(crate) core_id: CoreId,
    pub(crate) server_config: Arc<Configuration>,
    pub(crate) sharing_uring: RawFd,
    pub(crate) primary: bool,
}
