//! Exclusive ownership of one disposable Foyer directory.
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum DirectoryError {
    #[error("cache directory already in use: {0}")]
    DirectoryInUse(PathBuf),
    #[error("cache directory failed: {0}")]
    Foyer(String),
}

/// Process-wide ownership of one physical Foyer directory.
///
/// Foyer's filesystem device creates partition files but does not coordinate
/// two engines pointed at the same directory. Holding an advisory lock on a
/// disposable sidecar for the lifetime of `Inner` makes that misuse fail at
/// construction instead of allowing concurrent index and partition writes.
pub(crate) struct DirectoryOwner {
    /// Open lock file whose descriptor keeps the advisory lock alive.
    _file: std::fs::File,
}

impl DirectoryOwner {
    /// Acquires exclusive ownership of the directory's disposable lock file.
    pub(crate) fn acquire(path: &Path) -> Result<Self, DirectoryError> {
        std::fs::create_dir_all(path)
            .map_err(|error| DirectoryError::Foyer(format!("create cache directory: {error}")))?;
        let lock_path = path.join(".lakeday-foyer-owner.lock");
        let mut options = std::fs::OpenOptions::new();
        options.create(true).read(true).write(true);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;

            // Windows' share mode keeps the lock file exclusive while the
            // handle is alive; a second process cannot open it for writing.
            options.share_mode(0);
        }
        let file = match options.open(&lock_path) {
            Ok(file) => file,
            Err(error)
                if cfg!(windows)
                    && lock_path.exists()
                    && error.kind() == std::io::ErrorKind::PermissionDenied =>
            {
                return Err(DirectoryError::DirectoryInUse(path.to_owned()));
            }
            Err(error) => {
                return Err(DirectoryError::Foyer(format!(
                    "open cache owner lock: {error}"
                )));
            }
        };

        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;

            // SAFETY: `file` is an open regular file and its descriptor remains
            // owned by this guard until the Foyer engine is fully dropped.
            let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if result != 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::WouldBlock {
                    return Err(DirectoryError::DirectoryInUse(path.to_owned()));
                }
                return Err(DirectoryError::Foyer(format!(
                    "acquire cache owner lock: {error}"
                )));
            }
        }

        Ok(Self { _file: file })
    }
}
