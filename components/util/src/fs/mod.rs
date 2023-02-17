use std::{
    io::{ErrorKind, Result},
    path::Path,
};

pub fn mkdirs_if_missing(path: &str) -> Result<()> {
    let path = Path::new(path);
    if !path.exists() {
        std::fs::create_dir_all(path)
    } else if path.is_dir() {
        Ok(())
    } else {
        Err(std::io::Error::new(
            ErrorKind::NotADirectory,
            "Specified path is not a directory",
        ))
    }
}
