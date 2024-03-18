use crate::GenerationNumber;
use std::collections::HashMap;

pub struct KeyDir {
    keydir: HashMap<Vec<u8>, KeyDirEntry>,
}

pub struct KeyDirEntry {
    pub data_file_gen: GenerationNumber,
    pub pos: u64,
    pub size: u64,
}

impl KeyDir {
    pub fn new() -> Self {
        KeyDir {
            keydir: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: Vec<u8>, data_file: GenerationNumber, entry_pos: u64, entry_size: u64) {
        let keydir_entry = KeyDirEntry {
            data_file_gen: data_file,
            pos: entry_pos,
            size: entry_size,
        };
        self.keydir.insert(key, keydir_entry);
    }

    pub fn get(&mut self, key: &Vec<u8>) -> Option<&KeyDirEntry> {
        self.keydir.get(key)
    }
}