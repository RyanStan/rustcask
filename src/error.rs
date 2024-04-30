use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    io, path::Path,
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
    DiskWrite(io::Error),
    SeekError(io::Error),
}

impl Error for SetError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            SetErrorKind::DiskWrite(e) => Some(e),
            SetErrorKind::Serialize(e) => Some(e),
            SetErrorKind::SeekError(e) => Some(e),
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
    // The key that we failed to remove
    pub key: Vec<u8>,
}

#[derive(Debug)]
pub enum RemoveErrorKind {
    DiskWrite(io::Error),
}

impl Error for RemoveError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            RemoveErrorKind::DiskWrite(e) => Some(e),
        }
    }
}

impl Display for RemoveError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // TODO: Print the key that we failed to remove
        write!(
            f,
            "error removing key. Bytes of key interpreted as utf: {} ",
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
    ListFiles(io::Error),
    CreateActiveWriter(io::Error),
    CreateDataFileReaders(io::Error),
}

impl Error for OpenError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            OpenErrorKind::ListFiles(e) => Some(e),
            OpenErrorKind::CreateActiveWriter(e) => Some(e),
            OpenErrorKind::CreateDataFileReaders(e) => Some(e),
        }
    }
}

impl Display for OpenError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "error opening rustcask directory {}",
            self.rustcask_dir
        )
    }
}