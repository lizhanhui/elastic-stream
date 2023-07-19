use std::{sync::Mutex, thread::JoinHandle};

use log::info;

#[derive(Debug)]
pub struct HandleJoiner {
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl HandleJoiner {
    pub fn new() -> Self {
        Self {
            handles: Mutex::new(Vec::new()),
        }
    }

    pub fn push(&mut self, handle: JoinHandle<()>) {
        if let Ok(mut handles) = self.handles.lock() {
            handles.push(handle);
        }
    }
}

impl Default for HandleJoiner {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for HandleJoiner {
    fn drop(&mut self) {
        if let Ok(mut handles) = self.handles.lock() {
            while let Some(handle) = handles.pop() {
                info!("Joining a thread handle");
                let _ = handle.join();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_joiner() {
        let mut joiner = super::HandleJoiner::default();
        for _ in 0..3 {
            let handle = std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(300));
            });
            joiner.push(handle);
        }
    }
}
