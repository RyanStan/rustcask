use crate::error::RustcaskError;
use crate::error::RustcaskError::BadRustcaskDirectory;
use crate::utils::data_file_path;
use crate::bufio::BufReaderWithPos;
use crate::keydir::KeyDir;
use crate::logfile::{LogFileEntry, LogFileIterator, LogIndex};
use regex::Regex;


use std::fs::OpenOptions;

use std::sync::{Arc, Mutex, RwLock};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufWriter, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

pub type GenerationNumber = u64;

pub const MAX_DATA_FILE_SIZE: u64 = 2 * 1024 * 1024 * 1024; // 2 GiB

#[derive(Clone, Debug)]
pub struct RustCask {
    pub(crate) active_generation: GenerationNumber,
    pub(crate) active_data_file_writer: Arc<Mutex<BufWriter<File>>>,

    // TODO [RyanStan 2-28-24] Keeping a file handle for every open file may cause us to hit
    // system open file handle limits. We should use a LRU cache instead.
    //
    // A buffered reader provides benefits when performing sequential reads of the
    // data and hint files during startup
    pub(crate) data_file_readers: HashMap<GenerationNumber, BufReaderWithPos<File>>,

    pub(crate) directory: PathBuf,

    pub(crate) keydir: Arc<RwLock<KeyDir>>,


    pub(crate) max_data_file_size: u64,

    // Bytes written to the active data file
    pub(crate) active_data_file_size: u64,
}

impl RustCask {
    pub fn builder() -> RustCaskBuilder {
        RustCaskBuilder::default()
    }

    /// Inserts a key-value pair into the map.
    /// TODO [RyanStan 3/6/23] Instead of panicking with except or unwrap, we should bubble errors up to the caller.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), RustcaskError> {
        // To maintain correctness with concurrent reads, 'set' must insert an entry into the active data file,
        // and then update the keydir. This way, a concurrent read does not see an entry in the keydir
        // before the corresponding value has been written to the data file.

        let data_file_entry = LogFileEntry {
            key: key.clone(),
            value: Some(value),
        };

        let encoded =
            bincode::serialize(&data_file_entry).expect("Could not serialize data file entry");

        let mut writer = self
            .active_data_file_writer
            .lock()
            .expect("Writer lock was poisoned");
        let file_offset = writer.stream_position().unwrap();
        writer
            .write_all(&encoded)
            .expect("Failed to write data file entry to stream");
        writer.flush().unwrap();
        self.active_data_file_size += encoded.len() as u64;


        self.keydir
            .write()
            .expect("Keydir write lock was poisoned")
            .set(
                key,
                self.active_generation,
                LogIndex {
                    offset: file_offset,
                    len: encoded.len().try_into().unwrap(),
                },
            );

        // TODO [RyanStan]: if the active data file becomes larger than some size, then close it
        // and open a new active data file.
        // E.g. see https://github.com/dragonquest/bitcask/blob/master/src/database.rs#L415-L423.
        if self.active_data_file_size >= self.max_data_file_size {
            // TODO: extract below code into a "rotate active data file" function
            drop(writer);
            self.rotate_active_data_file();
        }

        Ok(())
    }


    /// Increment the active generation and update the writer and readers to reference
    /// the new active data file.
    fn rotate_active_data_file(&mut self) {
        self.active_generation += 1;
    
        let active_data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(data_file_path(&self.directory, &self.active_generation))
            .expect("Error opening active data file");
    
        self.active_data_file_writer = Arc::new(Mutex::new(BufWriter::new(active_data_file)));
            
        let reader = BufReaderWithPos::new(
            File::open(data_file_path(&self.directory, &self.active_generation)).expect(&format!(
                "Unable to open data file for generation {}",
                &self.active_generation
            )),
        )
        .unwrap();
    
        self.data_file_readers.insert(self.active_generation, reader);
        self.active_data_file_size = 0;
    }

    
    pub fn get(&mut self, key: &Vec<u8>) -> Option<Vec<u8>> {
        let keydir = self.keydir.read().unwrap();
        let keydir_entry = keydir.get(key)?;

        let reader = self
            .data_file_readers
            .get_mut(&keydir_entry.data_file_gen)
            .expect(&format!(
                "Could not find reader for generation {}",
                &keydir_entry.data_file_gen
            ));

        read_value(reader, &keydir_entry.index, key)
    }

    /// Removes a key from the store, returning the value at the key
    /// if the key was previously in the map.
    pub fn remove(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, RustcaskError> {
        // TODO: unit test to confirm I remove entry from the keydir

        // Remove from data file before removing from keydir?
        // Add remove into keydir, then update other thing if needed
        let tombstone = LogFileEntry::create_tombstone_entry(key);
        let encoded_tombstone =
            bincode::serialize(&tombstone).expect("Could not serialize tombstone");
        let mut writer = self.active_data_file_writer.lock().unwrap();
        writer.write_all(&encoded_tombstone).unwrap();
        writer.flush().unwrap();

        match self
            .keydir
            .write()
            .expect("Keydir write lock was poisoned")
            .remove(&tombstone.key)
        {
            None => Ok(None),
            Some(keydir_entry) => {
                // TODO [RyanStan 04-06-24] Return the old value instead of None.
                let reader = self
                    .data_file_readers
                    .get_mut(&keydir_entry.data_file_gen)
                    .expect(&format!(
                        "Could not find reader for generation {}",
                        &keydir_entry.data_file_gen
                    ));
    
                Ok(read_value(reader, &keydir_entry.index, &tombstone.key))
            }
        }
    }

    
}

/// Return the value from reader at the given index
fn read_value(reader: &mut BufReaderWithPos<File>, log_index: &LogIndex, key: &Vec<u8>) -> Option<Vec<u8>> {
    reader
        .seek(SeekFrom::Start(log_index.offset))
        .unwrap();
    let data_file_entry: LogFileEntry =
        bincode::deserialize_from(reader).expect("Error deserializing data");

    assert_eq!(
        &data_file_entry.key, key,
        "The deserialized entries key does not match the key passed to get"
    );

    Some(
        data_file_entry.value.expect(
            "We returned a tombstone value from get. We should have instead returned None",
        ),
    )
}

pub struct RustCaskBuilder {
    max_data_file_size: u64
}

impl Default for RustCaskBuilder {
    fn default() -> Self {
        Self {
            max_data_file_size: MAX_DATA_FILE_SIZE,
        }
    }
}

impl RustCaskBuilder {
    pub fn set_max_data_file_size(mut self, max_size: u64) -> Self {
        self.max_data_file_size = max_size;
        self
    }

    pub fn open(self, rustcask_dir: &Path) -> Result<RustCask, RustcaskError> {
        let rustcask_dir = PathBuf::from(&rustcask_dir);

        if !rustcask_dir.is_dir() {
            return Err(BadRustcaskDirectory(rustcask_dir));
        }

        let mut generations: Vec<GenerationNumber> = list_generations(&rustcask_dir);
        generations.sort_unstable();

        let active_generation: GenerationNumber = match generations.last() {
            Some(generation) => *generation,
            None => 0,
        };

        let active_data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(data_file_path(&rustcask_dir, &active_generation))
            .expect("Error opening active data file");

        let active_data_file_writer = Arc::new(Mutex::new(BufWriter::new(active_data_file)));

        let data_file_readers = create_data_file_readers(&rustcask_dir);

        let keydir = Arc::new(RwLock::new(build_keydir(&generations, &rustcask_dir)));

        Ok(RustCask {
            active_generation,
            active_data_file_writer,
            data_file_readers,
            directory: rustcask_dir,
            keydir,
            max_data_file_size: self.max_data_file_size,
            active_data_file_size: 0
        })
        
    }
}

fn list_generations(rustcask_dir: &Path) -> Vec<GenerationNumber> {
    let generations: Vec<GenerationNumber> = fs::read_dir(rustcask_dir)
        .unwrap()
        .map(|entry| -> PathBuf { entry.unwrap().path() })
        .filter(is_data_file)
        .map(parse_generation_number)
        .collect();

    generations
}

fn create_data_file_readers(
    rustcask_dir: &Path,
) -> HashMap<GenerationNumber, BufReaderWithPos<File>> {
    let mut map = HashMap::new();
    let generations = list_generations(&rustcask_dir);
    for generation in generations {
        let reader = BufReaderWithPos::new(
            File::open(data_file_path(rustcask_dir, &generation)).expect(&format!(
                "Unable to open data file for generation {}",
                generation
            )),
        )
        .unwrap();
        map.insert(generation, reader);
    }
    map
}

fn is_data_file(path: &PathBuf) -> bool {
    let file_name = match path.file_name() {
        Some(file) => file,
        None => return false,
    };

    let re = Regex::new(r"^\d+\.rustcask\.data$").unwrap();
    re.is_match(&file_name.to_string_lossy())
}

// TODO [RyanStan 3-25-24] Implement hint files.
fn build_keydir(sorted_generations: &Vec<GenerationNumber>, rustcask_dir: &Path) -> KeyDir {
    let mut keydir = KeyDir::new();
    for gen in sorted_generations {
        let data_file = data_file_path(rustcask_dir, gen);
        populate_keydir_with_data_file(data_file, &mut keydir, *gen);
    }

    keydir
}

fn populate_keydir_with_data_file(
    data_file: PathBuf,
    keydir: &mut KeyDir,
    data_file_gen: GenerationNumber,
) {
    let log_iter = LogFileIterator::new(data_file).unwrap();
    for (entry, index) in log_iter {
        if entry.value.is_none() {
            keydir.remove(&entry.key);
        } else {
            keydir.set(entry.key, data_file_gen, index);
        }
    }
}

/// Returns the generation of a hint or data file
fn parse_generation_number(path: PathBuf) -> GenerationNumber {
    let file_name = path.file_name().unwrap().to_string_lossy();
    let generation = file_name.split('.').next().expect("Unexpected file format");
    let generation: GenerationNumber = generation
        .parse()
        .expect("Failed to parse generation from file name");

    generation
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Write};

    use crate::logfile::LogFileEntry;

    use super::*;
    use tempfile::{tempdir, TempDir};
    use tracing::debug;

    #[test]
    fn test_is_data_file() {
        let dir = tempdir().unwrap();
        let data_file = dir.path().join("/tmp/384304/0.rustcask.data");
        assert!(is_data_file(&data_file));

        let hint_file = dir.path().join("/tmp/384304/0.rustcask.hint");
        assert!(!is_data_file(&hint_file));

        let random_file = dir.path().join("/tmp/3432432/some-lock-file.lock");
        assert!(!is_data_file(&random_file));
    }

    #[test]
    fn test_list_generations() {
        let dir = tempdir().unwrap();

        for number in 0..5 {
            File::create(dir.path().join(format!("{}.rustcask.data", number))).unwrap();
            File::create(dir.path().join(format!("{}.rustcask.hint", number))).unwrap();
        }

        let mut generations = list_generations(dir.path());
        generations.sort_unstable();
        let expected_range = 0..5;
        let expected_gen_values: Vec<u64> = expected_range.collect();
        assert_eq!(generations, expected_gen_values);
    }


    #[test]
    fn test_parse_generation_number() {
        let dir = tempdir().unwrap();
        let data_file = dir.path().join("/tmp/384304/0.rustcask.data");
        assert_eq!(parse_generation_number(data_file), 0);

        let dir = tempdir().unwrap();
        let data_file = dir.path().join("/tmp/384304/1000.rustcask.hint");
        assert_eq!(parse_generation_number(data_file), 1000);
    }

    #[test]
    fn test_open() {
        let dir = tempdir().unwrap();

        for number in 1..=5 {
            File::create(dir.path().join(format!("{}.rustcask.data", number))).unwrap();
            File::create(dir.path().join(format!("{}.rustcask.hint", number))).unwrap();
        }

        let rustcask = RustCask::builder().open(dir.path()).unwrap();

        assert_eq!(rustcask.active_generation, 5);
        assert_eq!(rustcask.data_file_readers.len(), 5);
    }

    #[test]
    fn test_open_on_empty_dir() {
        let dir = tempdir().unwrap();
        let rustcask = RustCask::builder().open(dir.path()).unwrap();
        assert_eq!(rustcask.active_generation, 0);
        assert_eq!(rustcask.data_file_readers.len(), 1);
    }

    #[test]
    fn test_open_non_existent_dir() {
        let dir = tempdir().unwrap();
        let invalid_dir = dir.path().join("invalid-dir");
        let rustcask = RustCask::builder().open(&invalid_dir);
        assert!(matches!(rustcask, Err(BadRustcaskDirectory(_))));
    }

    #[test]
    fn test_populate_keydir_with_data_file() {
        let temp_dir = TempDir::new().unwrap();
        let data_file = data_file_path(temp_dir.path(), &0);
        let mut data_file = File::create(data_file).unwrap();

        let key = "key".as_bytes().to_vec();
        let value = "value".as_bytes().to_vec();

        // encode the entry into the file
        let data_file_entry = LogFileEntry {
            key,
            value: Some(value),
        };

        let encoded = bincode::serialize(&data_file_entry).unwrap();

        data_file.write_all(&encoded);
        data_file.flush().unwrap();
    }

    #[test]
    fn test_data_file_rotation() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let temp_dir_path = temp_dir.path();
        // Force log file rotation by setting the max data file size to one byte
        let mut store = RustCask::builder().set_max_data_file_size(1).open(temp_dir_path).unwrap();


        let keys = ["key1".as_bytes().to_vec(), "key2".as_bytes().to_vec()];
        let values = ["value1".as_bytes().to_vec(), "value2".as_bytes().to_vec()];

        assert_eq!(store.active_generation, 0);
        assert_eq!(store.data_file_readers.len(), 1);
        assert_eq!(store.active_data_file_size, 0);
        

        store.set(keys[0].clone(), values[0].clone()).unwrap();

        assert_eq!(store.active_generation, 1);
        assert_eq!(store.data_file_readers.len(), 2);
        assert_eq!(store.active_data_file_size, 0);
        assert_eq!(store.get(&keys[0].clone()), Some(values[0].clone()));

        let data_files = fs::read_dir(temp_dir_path).unwrap();
        let data_files: Vec<String> = data_files.map(|dir_entry| dir_entry.unwrap().path().file_name().unwrap().to_str().unwrap().to_string()).collect();

        let expected_data_files = vec!["0.rustcask.data", "1.rustcask.data"];
        assert_eq!(data_files, expected_data_files);
    }

}