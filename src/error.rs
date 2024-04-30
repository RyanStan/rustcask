use std::{error::Error, fmt::{self, Display, Formatter}, io};

// Implement source for error traits but not display too

#[derive(Debug)]
#[non_exhaustive]
pub struct SetError {
    kind: SetErrorKind,
    // The key and value that we failed to set
    key: Vec<u8>,
    value: Vec<u8>,
}


#[derive(Debug)]
pub enum SetErrorKind {
    Serialize(bincode::Error),
    DiskWrite(io::Error),
}

impl Error for SetError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            SetErrorKind::DiskWrite(e) => Some(e),
            SetErrorKind::Serialize(e) => Some(e),
        }
    }
}

impl Display for SetError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // TODO [RyanStan 04-29-24] Implement a "pretty print" mode, that when disabled, does
        // not try printing the key.
        write!(f, "error setting key. Bytes of key interpreted as utf 8: {} ", String::from_utf8_lossy(&self.key))
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct RemoveError {
    kind: RemoveErrorKind,
    // The key that we failed to remove
    key: Vec<u8>,
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
        write!(f, "error removing key. Bytes of key interpreted as utf: {} ", String::from_utf8_lossy(&self.key))
    }
}