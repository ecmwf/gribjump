//! Error handling for gribjump.

/// Error type for gribjump operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error from eckit C++ libraries.
    #[error(transparent)]
    Eckit(#[from] eckit::Error),

    /// Invalid argument provided.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Iterator has been exhausted.
    #[error("iterator exhausted")]
    IteratorExhausted,
}

/// Result type alias for gribjump operations.
pub type Result<T> = std::result::Result<T, Error>;

impl From<gribjump_sys::Exception> for Error {
    fn from(e: gribjump_sys::Exception) -> Self {
        Self::Eckit(eckit::Error::from(e))
    }
}
