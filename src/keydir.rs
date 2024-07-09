use std::collections::HashMap;

use crate::{logfile::LogIndex, GenerationNumber};

#[derive(Debug)]
pub struct KeyDir {
    keydir: HashMap<Vec<u8>, KeyDirEntry>,
}

#[derive(Debug)]
pub struct KeyDirEntry {
    pub data_file_gen: GenerationNumber,
    pub index: LogIndex,
}

impl KeyDir {
    pub fn new() -> Self {
        KeyDir {
            keydir: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: Vec<u8>, data_file: GenerationNumber, log_index: LogIndex) {
        let keydir_entry = KeyDirEntry {
            data_file_gen: data_file,
            index: log_index,
        };
        self.keydir.insert(key, keydir_entry);
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<&KeyDirEntry> {
        self.keydir.get(key)
    }

    /// Removes a key from the keydir, returning the entry at the key
    /// if the key was previously in the map.
    pub fn remove(&mut self, key: &Vec<u8>) -> Option<KeyDirEntry> {
        self.keydir.remove(key)
    }
}

impl<'a> IntoIterator for &'a KeyDir {
    type Item = (&'a Vec<u8>, &'a KeyDirEntry);
    type IntoIter = std::collections::hash_map::Iter<'a, Vec<u8>, KeyDirEntry>;

    fn into_iter(self) -> Self::IntoIter {
        self.keydir.iter()
    }
}

impl IntoIterator for KeyDir {
    type Item = (Vec<u8>, KeyDirEntry);
    type IntoIter = std::collections::hash_map::IntoIter<Vec<u8>, KeyDirEntry>;

    fn into_iter(self) -> Self::IntoIter {
        self.keydir.into_iter()
    }
}