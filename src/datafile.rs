use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct DataFileEntry {
    //TODO [RyanStan 03/05/24] Add CRC and timestamp
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

// TODO: data file iterator.
// The iterator should just accept a file path, then open the file and do the reads. 

pub struct DataFile {
    path: PathBuf
}

impl Iterator for DataFile {
    
}