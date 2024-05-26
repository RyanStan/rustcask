use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    io,
};

#[derive(Debug)]
#[non_exhaustive]
pub struct SetError {
    pub kind: SetErrorKind,
    pub key: Vec<u8>,
}

#[derive(Debug)]
pub enum SetErrorKind {
    Serialize(bincode::Error),
    Io(io::Error),
}

impl Error for SetError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            SetErrorKind::Io(e) => Some(e),
            SetErrorKind::Serialize(e) => Some(e),
        }
    }
}

impl Display for SetError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // TODO [RyanStan 04-29-24] Implement a "pretty print" mode, that when disabled, does
        // not try printing the key.
        write!(
            f,
            "error setting key. Bytes of key interpreted as utf 8: {} ",
            String::from_utf8_lossy(&self.key)
        )
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct RemoveError {
    pub kind: RemoveErrorKind,
    pub key: Vec<u8>,
}

#[derive(Debug)]
pub enum RemoveErrorKind {
    Io(io::Error),
    /// A deserialization error indicates there was an error
    /// retrieving the previous value at the key. This may mean that the
    /// existing data was corrupted.
    Deserialize(bincode::Error),
}

impl Error for RemoveError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            RemoveErrorKind::Io(e) => Some(e),
            RemoveErrorKind::Deserialize(e) => Some(e),
        }
    }
}

impl Display for RemoveError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "error removing key. Bytes of key interpreted as utf8: {}",
            String::from_utf8_lossy(&self.key)
        )
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct OpenError {
    pub kind: OpenErrorKind,
    pub rustcask_dir: String,
}

#[derive(Debug)]
pub enum OpenErrorKind {
    Io(io::Error),
    BadDirectory,
}

impl Error for OpenError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            OpenErrorKind::Io(e) => Some(e),
            OpenErrorKind::BadDirectory => None,
        }
    }
}

impl Display for OpenError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "error opening rustcask directory {}", self.rustcask_dir)
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct GetError<'a> {
    pub kind: GetErrorKind,
    pub key: &'a Vec<u8>,
}

#[derive(Debug)]
pub enum GetErrorKind {
    Io(io::Error),
    Deserialize(bincode::Error),
}

impl<'a> Error for GetError<'a> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            GetErrorKind::Io(e) => Some(e),
            GetErrorKind::Deserialize(e) => Some(e),
        }
    }
}

impl<'a> Display for GetError<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "error getting value.  Bytes of key interpreted as utf8: {}",
            String::from_utf8_lossy(&self.key)
        )
    }
}
