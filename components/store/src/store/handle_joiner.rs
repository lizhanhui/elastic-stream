use std::{sync::Mutex, thread::JoinHandle};

use slog::{Logger, info};

pub(crate) struct HandleJoiner {
    log: Logger,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl HandleJoiner {
    pub(crate) fn new(log: Logger) -> Self {
        Self {
            log,
            handles: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn push(&mut self, handle: JoinHandle<()>) {
        if let Ok(mut handles) = self.handles.lock() {
            handles.push(handle);
        }
    }
}

impl Drop for HandleJoiner {
    fn drop(&mut self) {
        if let Ok(mut handles) = self.handles.lock() {
            while let Some(handle) = handles.pop() {
                info!(self.log, "Joining a thread handle");
                let _ = handle.join();
            }
        }
    }
}
