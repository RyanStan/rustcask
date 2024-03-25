use std::path::{Path, PathBuf};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum RustcaskError {
    #[error("Rustcask error")]
    Error,

    #[error("Open must be passed a valid and pre-existing directory. Invalid directory: {0}")]
    BadRustcaskDirectory(PathBuf),
}
