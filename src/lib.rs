//! This crate contains... TODO [RyanStan 04-07-24] Write crate description

pub mod rustcask;
mod bufio;
mod error;
mod keydir;
mod logfile;
mod utils;

#[inline]
pub fn fibonacci(n: u64) -> u64 {
    match n {
        0 => 1,
        1 => 1,
        n => fibonacci(n-1) + fibonacci(n-2),
    }
}