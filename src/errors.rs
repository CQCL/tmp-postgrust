use thiserror::Error;

/// UTF-8 captures of stdout and stderr for child processes used by the library.
#[derive(Debug)]
pub struct ProcessCapture {
    /// Capture of stdout from the process
    pub stdout: String,
    /// Capture of stderr from the process
    pub stderr: String,
}

/// Error type for possible postgresql errors.
#[derive(Error, Debug)]
pub enum TmpPostgrustError {
    /// Catchall error for when a subprocess fails to run to completion
    #[error("subprocess failed to execute")]
    ExecSubprocessFailed {
        /// Underlying I/O Error.
        #[source]
        source: std::io::Error,
        /// Debug formatted string of Command that was attempted.
        command: String,
    },
    /// Catchall error for when a subprocess fails to start
    #[error("subprocess failed to spawn")]
    SpawnSubprocessFailed(#[source] std::io::Error),
    /// Error when `initdb` fails to execute.
    #[error("initdb failed")]
    InitDBFailed(ProcessCapture),
    /// Error when a file to be copied is not found.
    #[error("copying cached database failed, file not found")]
    CopyCachedInitDBFailedFileNotFound(#[source] std::io::Error),
    /// Error when the file type cannot be read when copying the cached db.
    #[error("copying cached database failed, could not read file type")]
    CopyCachedInitDBFailedCouldNotReadFileType(#[source] std::io::Error),
    /// Error when the source directory filepath cannot be stripped.
    #[error("copying cached database failed, could not strip path prefix")]
    CopyCachedInitDBFailedCouldNotStripPathPrefix(#[source] std::path::StripPrefixError),
    /// Error when a copy process cannot be joined.
    #[cfg(feature = "tokio-process")]
    #[error("copying cached database failed, failed to join cp process")]
    CopyCachedInitDBFailedJoinError(#[source] tokio::task::JoinError),
    /// Error when `createdb` fails to execute.
    #[error("createdb failed")]
    CreateDBFailed(ProcessCapture),
    /// Error when `postgresql.conf` cannot be written.
    #[error("failed to write postgresql.conf")]
    CreateConfigFailed(#[source] std::io::Error),
    /// Error when the PGDATA directory is empty.
    #[error("failed to find temporary data directory")]
    EmptyDataDirectory,
    /// Error when the temporary unix socket directory cannot be created.
    #[error("failed to create unix socket directory")]
    CreateSocketDirFailed(#[source] std::io::Error),
    /// Error when the cache directory cannot be created.
    #[error("failed to create cache directory")]
    CreateCacheDirFailed(#[source] std::io::Error),
    /// Error when the data directory cannot be created.
    #[error("failed to create cache directory")]
    CreateDataDirFailed(#[source] std::io::Error),
    /// Error when `cp` fails for the initialized database.
    #[error("updating directory permission to non-root failed")]
    UpdatingPermissionsFailed(ProcessCapture),
    /// Error when running migrations failed.
    #[error("failed to run database migrations")]
    MigrationsFailed(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Result type for `TmpPostgrustError`, used by functions in this crate.
pub type TmpPostgrustResult<T> = Result<T, TmpPostgrustError>;
