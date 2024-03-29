use std::{
    backtrace::Backtrace,
    io,
    path::{Path, PathBuf},
};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum RustcaskError {
    #[error("Rustcask error")]
    Error,

    #[error("Open must be passed a valid and pre-existing directory. Invalid directory: {0}")]
    BadRustcaskDirectory(PathBuf),
    // TODO [RyanStan 3-25-24]: Learn how to use this crate and add an IO error.
}
