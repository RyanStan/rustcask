use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use crate::{
    error::{OpenError, OpenErrorKind},
    logfile::{LogFileIterator, LogIndex},
    utils::{data_file_path, list_generations},
    GenerationNumber,
};

#[derive(Debug)]
pub struct KeyDir {
    keydir: HashMap<Vec<u8>, KeyDirEntry>,
}

#[derive(Debug)]
pub struct KeyDirEntry {
    pub data_file_gen: GenerationNumber,
    pub index: LogIndex,
}

// TODO [RyanStan 3-25-24] Implement hint files.
impl KeyDir {
    /// Creates a new `KeyDir` instance by parsing the data files in the given RustCask directory.
    ///
    /// This function reads all the data files in the RustCask directory, ordered by generation number.
    /// It populates the `KeyDir` with the key-value pairs from each data file.
    ///
    /// # Arguments
    ///
    /// * `rustcask_dir` - The path to the RustCask directory containing the data files.
    ///
    /// # Returns
    ///
    /// * `Ok(KeyDir)` - A `KeyDir` instance populated with the key-value pairs from the data files.
    /// * `Err(OpenError)` - An error if the RustCask directory cannot be read or parsed.
    ///     
    pub fn new(rustcask_dir: &Path) -> Result<Self, OpenError> {
        let mut generations: Vec<GenerationNumber> =
            list_generations(&rustcask_dir).map_err(|err| OpenError {
                kind: OpenErrorKind::Io(err),
                rustcask_dir: rustcask_dir.to_string_lossy().to_string(),
            })?;
        generations.sort_unstable();

        let mut keydir = KeyDir {
            keydir: HashMap::new(),
        };

        for gen in generations {
            let data_file = data_file_path(rustcask_dir, &gen);
            populate_keydir_with_data_file(data_file, &mut keydir, gen);
        }

        Ok(keydir)
    }

    pub fn new_empty() -> Self {
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

fn populate_keydir_with_data_file(
    data_file: PathBuf,
    keydir: &mut KeyDir,
    data_file_gen: GenerationNumber,
) {
    let log_iter = LogFileIterator::new(data_file).unwrap_or_else(|_| {
        panic!(
            "Unable to create a log file iterator for generation {}. \
            This iterator is used to populate the keydir on data store open.",
            data_file_gen
        )
    });

    for (entry, index) in log_iter {
        if entry.value.is_none() {
            keydir.remove(&entry.key);
        } else {
            keydir.set(entry.key, data_file_gen, index);
        }
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

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Write};

    use tempfile::TempDir;

    use crate::{
        logfile::{LogFileEntry, LogIndex},
        utils::data_file_path,
    };

    use super::{populate_keydir_with_data_file, KeyDir};

    #[test]
    fn test_populate_keydir_with_data_file() {
        let temp_dir = TempDir::new().unwrap();
        let generation = 0;
        let data_file_path = data_file_path(temp_dir.path(), &generation);
        let mut data_file = File::create(data_file_path.clone()).unwrap();

        let key = "key".as_bytes().to_vec();
        let value = "value".as_bytes().to_vec();

        let data_file_entry = LogFileEntry {
            key: key.clone(),
            value: Some(value.clone()),
        };

        let encoded = bincode::serialize(&data_file_entry).unwrap();

        data_file.write_all(&encoded).unwrap();
        data_file.flush().unwrap();

        let mut keydir = KeyDir::new_empty();
        populate_keydir_with_data_file(data_file_path, &mut keydir, generation);

        let entry = keydir.get(&key);
        assert!(matches!(entry, Some(_)));

        let entry = entry.unwrap();

        assert_eq!(entry.data_file_gen, generation);
        assert_eq!(
            entry.index,
            LogIndex {
                offset: 0,
                len: encoded.len() as u64,
            }
        );
    }
}
