use thiserror::Error;

#[derive(Error, Debug)]
pub enum RustcaskError {
    #[error("Generic error")]
    Error(),
}