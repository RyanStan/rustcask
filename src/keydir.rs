use crate::GenerationNumber;
use std::collections::HashMap;

pub struct KeyDir {
    keydir: HashMap<Vec<u8>, KeyDirEntry>,
}

struct KeyDirEntry {
    data_file: GenerationNumber,
    pos: u64,
    size: u64,
}

impl KeyDir {
    pub fn new() -> Self {
        KeyDir {
            keydir: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: Vec<u8>, data_file: GenerationNumber, entry_pos: u64, entry_size: u64) {
        let keydir_entry = KeyDirEntry {
            data_file,
            pos: entry_pos,
            size: entry_size,
        };
        self.keydir.insert(key, keydir_entry);
    }
}