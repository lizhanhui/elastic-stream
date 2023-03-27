use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::fd::{FromRawFd, IntoRawFd, RawFd},
    path::Path,
};

use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use nix::fcntl::{flock, FlockArg};
use slog::{error, info, Logger};

use crate::error::StoreError;

pub(crate) struct Lock {
    log: Logger,
    fd: RawFd,
    id: i32,
}

impl Lock {
    pub(crate) fn new(store_path: &Path, log: &Logger) -> Result<Self, StoreError> {
        let lock_file_path = store_path.join("LOCK");
        let (fd, id) = if lock_file_path.as_path().exists() {
            let mut file = OpenOptions::new()
                .read(true)
                .open(lock_file_path.as_path())?;
            let id = file.read_i32::<BigEndian>()?;
            (file.into_raw_fd(), id)
        } else {
            let mut file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(lock_file_path.as_path())?;
            // TODO: Allocate data node ID from PM.
            let id: i32 = 0;
            file.write_all(&id.to_be_bytes())?;
            file.sync_all()?;
            (file.into_raw_fd(), id)
        };

        info!(
            log,
            "Acquiring store lock: {:?}, id={}",
            lock_file_path.as_path(),
            id
        );

        flock(fd, FlockArg::LockExclusive).map_err(|e| {
            error!(log, "Failed to acquire store lock. errno={}", e);
            StoreError::AcquireLock
        })?;

        info!(log, "Store lock acquired. ID={}", id);

        Ok(Self {
            log: log.clone(),
            fd,
            id,
        })
    }

    pub(crate) fn id(&self) -> i32 {
        self.id
    }
}

impl Drop for Lock {
    fn drop(&mut self) {
        if let Err(e) = flock(self.fd, FlockArg::Unlock) {
            error!(self.log, "Failed to release store lock. errno={}", e);
        }
        let _file = unsafe { File::from_raw_fd(self.fd) };
    }
}

#[cfg(test)]
mod tests {
    use super::Lock;
    use std::error::Error;

    #[test]
    fn test_lock_normal() -> Result<(), Box<dyn Error>> {
        let log = test_util::terminal_logger();
        let path = test_util::create_random_path()?;
        let _guard = test_util::DirectoryRemovalGuard::new(log.clone(), path.as_path());
        let _lock = Lock::new(path.as_path(), &log)?;
        Ok(())
    }

    #[test]
    fn test_lock_after_release() -> Result<(), Box<dyn Error>> {
        let log = test_util::terminal_logger();
        let path = test_util::create_random_path()?;
        let _guard = test_util::DirectoryRemovalGuard::new(log.clone(), path.as_path());
        {
            let _lock = Lock::new(path.as_path(), &log)?;
        }
        {
            let _lock = Lock::new(path.as_path(), &log)?;
        }

        Ok(())
    }
}
