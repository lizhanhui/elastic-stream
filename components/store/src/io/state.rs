use tokio::sync::oneshot;

use crate::{error::AppendError, ops::append::AppendResult};

use super::{buf::RecordBuf, task::IoTask};

pub(crate) struct OpState {
    pub(crate) task: IoTask,
    pub(crate) record_buf: RecordBuf,
}
