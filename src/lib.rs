//! `Rustcask` is a fast and efficient key-value storage engine implemented in Rust.
//! It's based on [Bitcask,
//! "A Log-Structured Hash Table for Fast Key/Value Data"](https://riak.com/assets/bitcask-intro.pdf).
//!
//! For more details on the design of Rustcask, see [the README on Github](https://github.com/RyanStan/rustcask).
//!
//! # Example
//! ```
//! # use rustcask::Rustcask;
//! # use tempfile::TempDir;
//! # let temp_dir = TempDir::new().expect("unable to create temporary working directory");
//! # let rustcask_dir = temp_dir.path();
//! let mut store = Rustcask::builder().open(rustcask_dir).unwrap();
//!
//! let key = "leader-node".as_bytes().to_vec();
//! let value = "instance-a".as_bytes().to_vec();
//!
//! store.set(key.clone(), value).unwrap();
//! store.get(&key);
//! ```

use error::{
    GetError, MergeError, MergeErrorKind, OpenError, OpenErrorKind, RemoveError,
    SetError,
};
use keydir::KeyDir;
use logfile::LogFileEntry;
use readers::Readers;

use log::{info, trace};
use writer::Writer;

use std::sync::{Arc, Mutex, RwLock};
use std::{
    io::{Seek, SeekFrom},
    path::{Path, PathBuf},
};

use crate::error::GetErrorKind;

/// Rustcask error types.
pub mod error;

mod bufio;
mod keydir;
mod logfile;
mod readers;
mod utils;
mod writer;

type GenerationNumber = u64;

const MAX_DATA_FILE_SIZE: u64 = 2 * 1024 * 1024 * 1024; // 2 GiB

/// A handle to interact with a Rustcask storage engine.
#[derive(Clone, Debug)]
pub struct Rustcask {
    // Writes to active data file. Performs data file rotation as needed.
    writer: Arc<Mutex<Writer>>,

    // Data file readers
    readers: Readers,

    pub(crate) keydir: Arc<RwLock<KeyDir>>,

    sync_mode: bool,

    pub(crate) directory: Arc<PathBuf>,
}

impl Rustcask {
    /// Returns a Rustcask builder with default configuration values.
    pub fn builder() -> RustcaskBuilder {
        RustcaskBuilder::default()
    }

    /// Inserts a key-value pair into Rustcask.
    ///
    ///  # Arguments
    ///
    /// * `key` - The key to insert, as a `Vec<u8>`.
    /// * `value` - The value to associate with the key, as a `Vec<u8>`.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the key-value pair was successfully inserted.
    /// * `Err(SetError)` if there was an error serializing the entry or writing to the data file.
    ///
    /// # Errors
    ///
    /// This function may return a `SetError` if:
    ///
    /// * The `LogFileEntry` could not be serialized (`SetErrorKind::Serialize`).
    /// * There was an error writing to the active data file.
    ///
    /// # Panics
    ///
    /// This function will panic if another thread crashed while holding the lock on the key directory.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), SetError> {
        trace!(
            "Set called with key (as UTF 8) {}",
            String::from_utf8_lossy(&key)
        );

        let mut writer = self
            .writer
            .lock()
            .expect("Another thread crashed while holding the writer lock. Panicking.");

        writer.set(key, value)
    }

    /// Returns a reference to the value corresponding to the key.
    ///
    /// # Arguments
    ///
    /// * `key` - A reference to the `Vec<u8>` representing the key to look up.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(value))` - If the key is found in the data store, returns the corresponding value as a `Vec<u8>`.
    /// * `Ok(None)` - If the key is not found in the data store.
    /// * `Err(GetError)` - If an error occurs while reading or deserializing the data from the data store.
    ///
    /// # Errors
    ///
    /// This function may return a `GetError` with the following variants:
    ///
    /// * `GetErrorKind::Io(err)` - An I/O error occurred while reading the data file.
    /// * `GetErrorKind::Deserialize(err)` - An error occurred while deserializing the data from the data file.
    pub fn get<'a>(&'a mut self, key: &'a Vec<u8>) -> Result<Option<Vec<u8>>, GetError<'a>> {
        trace!(
            "Get called with key (as UTF 8) {}",
            String::from_utf8_lossy(key)
        );
        let keydir = self
            .keydir
            .read()
            .expect("Another thread panicked while holding the keydir lock. Panicking.");
        let keydir_entry = keydir.get(key);
        if keydir_entry.is_none() {
            return Ok(None);
        }
        let keydir_entry = keydir_entry.unwrap();

        let reader = self
            .readers
            .get_data_file_reader(keydir_entry.data_file_gen);

        // TODO [RyanStan 3-25-24] This code is duplicated in remove. Extract it into a separate function.
        let log_index = &keydir_entry.index;
        reader
            .seek(SeekFrom::Start(log_index.offset))
            .map_err(|err| GetError {
                kind: GetErrorKind::Io(err),
                key,
            })?;

        let data_file_entry: LogFileEntry =
            bincode::deserialize_from(reader).map_err(|err| GetError {
                kind: GetErrorKind::Deserialize(err),
                key,
            })?;

        assert_eq!(
            &data_file_entry.key, key,
            "The deserialized entries key does not match the key passed to get. The data store could corrupted."
        );

        Ok(Some(data_file_entry.value.expect(
            "We returned a tombstone value from get. We should have instead returned None. 
            The data store may not be corrupted - this indicates a programming bug.",
        )))
    }

    /// Removes a key-value pair from the database.
    ///
    /// This function takes a `key` as input and removes the corresponding key-value pair from the
    /// database. If the key exists, it returns the previously associated value. If the key does not
    /// exist, it returns `None`.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to remove, as a `Vec<u8>`.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(value))` if the key existed and was removed, containing the previously associated
    ///   value.
    /// * `Ok(None)` if the key did not exist in the database.
    /// * `Err(RemoveError)` if there was an error removing the key.
    ///
    /// # Errors
    ///
    /// This function may return a `RemoveError` if:
    ///
    /// * There was an I/O error seeking or reading from the data file (`RemoveErrorKind::Io`).
    /// * There was an error deserializing the log entry from the data file (`RemoveErrorKind::Deserialize`).
    ///
    /// # Panics
    ///
    /// This function will panic if another thread crashed while holding the lock on the key directory.
    pub fn remove(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, RemoveError> {
        trace!(
            "Remove called with key (as UTF 8) {}",
            String::from_utf8_lossy(&key)
        );
        let mut writer = self
            .writer
            .lock()
            .expect("Another thread crashed while holding the writer lock. Panicking.");

        writer.remove(key)
    }

    /// Compacts the rustcask directory be writing active key-value pairs
    /// to a new set of data files, and removes old data files which may have contained
    /// dead values.
    /// 
    /// # Errors
    ///
    /// This function may return a `MergeError` with the following variants:
    ///
    /// * `MergeErrorKind::OutsideMergeWindow` - The merge operation was attempted outside of the allowed merge window.
    ///   The `merge_generation` field in this case indicates the next generation number when a merge will be allowed.
    /// * `MergeErrorKind::Io(err)` - An I/O error occurred while reading or writing data files during the merge operation.
    /// 
    /// Reads can be performed concurrently with merges. However, writes will be blocked
    /// until the merge is complete.
    pub fn merge(&mut self) -> Result<(), MergeError> {
        // TODO [RyanStan 07/08/24] Instead of relying on the user to call merge,
        //   the open function should spawn a background thread that performs merging based on
        //   a configured interval.

        // Locking the writer prevents concurrent writes
        let mut writer = self
            .writer
            .lock()
            .expect("Another thread crashed while holding the writer lock. Panicking.");

        if !writer.can_merge() {
            return Err(MergeError {
                kind: MergeErrorKind::OutsideMergeWindow,
                merge_generation: writer.get_active_generation() + 1,
            });
        }

        writer.merge()?;

        // TODO [RyanStan 07/17/24] Output stats about the number of bytes saved.
        info!("Merged data files.");

        Ok(())
    }

    // Get active generation and get active data file size are for testing
    fn get_active_generation(&self) -> GenerationNumber {
        let writer = self.writer.lock().expect(
            "Another thread crashed while holding the writer lock. \
                Panicking because the write lock is required to get the active generation.",
        );
        writer.get_active_generation()
    }

    fn get_active_data_file_size(&self) -> u64 {
        let writer = self.writer.lock().expect(
            "Another thread crashed while holding the writer lock. \
                Panicking because the write lock is required to get the active data file size.",
        );
        writer.get_active_data_file_size()
    }
}

/// Simplifies configuration and creation of Rustcask instances.
/// 
/// # Example
/// ```
/// # use rustcask::Rustcask;
/// # use tempfile::TempDir;
/// # let temp_dir = TempDir::new().unwrap();
/// # let rustcask_dir = temp_dir.path();
/// let store = Rustcask::builder()
///     .set_sync_mode(true)
///     .open(rustcask_dir);
/// ```
pub struct RustcaskBuilder {
    max_data_file_size: u64,

    /// When sync mode is true, writes to the data file
    /// are fsync'ed before returning to the user.
    /// This guarantees that data is durable and persisted to disk immediately,
    /// at the expense of reduced performance
    sync_mode: bool,
}

impl Default for RustcaskBuilder {
    fn default() -> Self {
        Self {
            max_data_file_size: MAX_DATA_FILE_SIZE,
            sync_mode: false,
        }
    }
}

impl RustcaskBuilder {
    /// Sets the maximum data file size. When the active data file
    /// surpasses this size, it will be marked read-only and a new active data file
    /// will be created.
    pub fn set_max_data_file_size(mut self, max_size: u64) -> Self {
        self.max_data_file_size = max_size;
        self
    }

    /// When sync mode is set to true, writes to the data file
    /// are fsync'ed before returning to the user.
    /// This guarantees that data is durable and persisted to disk immediately,
    /// at the expense of reduced performance
    pub fn set_sync_mode(mut self, sync_mode: bool) -> Self {
        self.sync_mode = sync_mode;
        self
    }

    /// Generates a Rustcask instance.
    pub fn open(self, rustcask_dir: &Path) -> Result<Rustcask, OpenError> {
        trace!(
            "Open called on directory {}",
            rustcask_dir.to_string_lossy().to_string()
        );
        let rustcask_dir = Arc::new(PathBuf::from(rustcask_dir));

        if !rustcask_dir.is_dir() {
            return Err(OpenError {
                kind: OpenErrorKind::BadDirectory,
                rustcask_dir: rustcask_dir.to_string_lossy().to_string(),
            });
        }

        let data_file_readers = Readers::new(rustcask_dir.clone()).map_err(|err| OpenError {
            kind: OpenErrorKind::Io(err),
            rustcask_dir: rustcask_dir.to_string_lossy().to_string(),
        })?;

        let keydir = Arc::new(RwLock::new(KeyDir::new(&rustcask_dir)?));

        let writer = Arc::new(Mutex::new(Writer::new(
            self.sync_mode,
            self.max_data_file_size,
            rustcask_dir.clone(),
            keydir.clone(),
            data_file_readers.clone(),
        )?));

        info!(
            "Opened Rustcask directory {}. Max data file size: {}. Number of existing data files: {}. Active generation: {}. Sync mode: {}.",
            rustcask_dir.to_string_lossy().to_string(),
            self.max_data_file_size,
            data_file_readers.data_file_readers.len(),
            writer.lock().unwrap().get_active_generation(),
            self.sync_mode
        );

        Ok(Rustcask {
            readers: data_file_readers,
            directory: rustcask_dir,
            keydir,
            sync_mode: self.sync_mode,
            writer,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::
        fs::File
    ;

    use super::*;
    use logfile::LogFileIterator;
    use tempfile::{tempdir, TempDir};
    use utils::{list_generations, tests::{file_names, get_keys, get_keys_values}};

    #[test]
    fn test_open() {
        let dir = tempdir().unwrap();

        for number in 1..=5 {
            File::create(dir.path().join(format!("{}.rustcask.data", number))).unwrap();
            File::create(dir.path().join(format!("{}.rustcask.hint", number))).unwrap();
        }

        let rustcask = Rustcask::builder().open(dir.path()).unwrap();

        assert_eq!(rustcask.get_active_generation(), 5);
    }

    #[test]
    fn test_open_on_empty_dir() {
        let dir = tempdir().unwrap();
        let rustcask = Rustcask::builder().open(dir.path()).unwrap();
        assert_eq!(rustcask.get_active_generation(), 0);
    }

    #[test]
    fn test_open_non_existent_dir() {
        let dir = tempdir().unwrap();
        let invalid_dir = dir.path().join("invalid-dir");
        let rustcask = Rustcask::builder().open(&invalid_dir);
        assert!(matches!(
            rustcask,
            Err(OpenError {
                kind: OpenErrorKind::BadDirectory,
                ..
            })
        ));
    }

    #[test]
    fn test_data_file_rotation() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let temp_dir_path = temp_dir.path();
        // Force log file rotation by setting the max data file size to one byte
        let mut store = Rustcask::builder()
            .set_max_data_file_size(1)
            .open(temp_dir_path)
            .unwrap();

        let keys = ["key1".as_bytes().to_vec(), "key2".as_bytes().to_vec()];
        let values = ["value1".as_bytes().to_vec(), "value2".as_bytes().to_vec()];

        assert_eq!(store.get_active_generation(), 0);
        assert_eq!(store.get_active_data_file_size(), 0);

        store.set(keys[0].clone(), values[0].clone()).unwrap();

        assert_eq!(store.get_active_generation(), 1);
        assert_eq!(store.get_active_data_file_size(), 0);
        assert_eq!(
            store.get(&keys[0].clone()).unwrap(),
            Some(values[0].clone())
        );

        let data_files = file_names(temp_dir_path);
        assert!(
            data_files.contains(&String::from("0.rustcask.data"))
                && data_files.contains(&String::from("1.rustcask.data"))
        );
    }

    #[test]
    fn test_merge_internal() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let temp_dir_path = temp_dir.path();
        let mut store = Rustcask::builder().open(temp_dir_path).unwrap();

        store
            .set(
                "leader".as_bytes().to_vec(),
                "instance-a".as_bytes().to_vec(),
            )
            .unwrap();
        store
            .set(
                "leader".as_bytes().to_vec(),
                "instance-b".as_bytes().to_vec(),
            )
            .unwrap();

        let expected_data_files = vec!["0.rustcask.data"];
        let data_files = file_names(temp_dir_path);
        assert_eq!(data_files, expected_data_files);

        let log_file_keys = get_keys(temp_dir_path, &data_files[0]);
        assert_eq!(log_file_keys.len(), 2);
        assert_eq!(
            log_file_keys,
            vec!["leader".as_bytes().to_vec(), "leader".as_bytes().to_vec()]
        );

        store.merge().unwrap();

        let expected_data_files = vec!["1.rustcask.data"];
        let data_files = file_names(temp_dir_path);
        assert_eq!(data_files, expected_data_files);

        let log_file_iter = LogFileIterator::new(temp_dir_path.join("1.rustcask.data")).unwrap();

        let log_file_entries: Vec<(Vec<u8>, Vec<u8>)> = log_file_iter
            .map(|x| (x.0.key, x.0.value.unwrap()))
            .collect();

        assert_eq!(log_file_entries.len(), 1);
        assert_eq!(log_file_entries[0].0, "leader".as_bytes().to_vec());
        assert_eq!(log_file_entries[0].1, "instance-b".as_bytes().to_vec());
    }

    #[test]
    fn test_data_file_rotation_cloned_stores() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let temp_dir_path = temp_dir.path();
        // Force log file rotation by setting the max data file size to one byte
        let mut store = Rustcask::builder()
            .set_max_data_file_size(1)
            .open(temp_dir_path)
            .unwrap();
        let mut store_clone = store.clone();

        store
            .set("key1".as_bytes().to_vec(), "value1".as_bytes().to_vec())
            .unwrap();
        store_clone
            .set("key2".as_bytes().to_vec(), "value2".as_bytes().to_vec())
            .unwrap();

        let log_file_keys = get_keys_values(temp_dir_path, &String::from("0.rustcask.data"));
        assert_eq!(log_file_keys.len(), 1);
        assert_eq!(
            log_file_keys,
            vec![("key1".as_bytes().to_vec(), "value1".as_bytes().to_vec())]
        );

        let log_file_keys = get_keys_values(temp_dir_path, &String::from("1.rustcask.data"));
        assert_eq!(log_file_keys.len(), 1);
        assert_eq!(
            log_file_keys,
            vec![("key2".as_bytes().to_vec(), "value2".as_bytes().to_vec())]
        );
    }

    #[test]
    fn test_merge_with_rotate() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let temp_dir_path = temp_dir.path();
        let mut store = Rustcask::builder()
            .set_max_data_file_size(1)
            .open(temp_dir_path)
            .unwrap();

        store
            .set(
                "leader".as_bytes().to_vec(),
                "instance-a".as_bytes().to_vec(),
            )
            .unwrap();
        store
            .set(
                "last-election-ts".as_bytes().to_vec(),
                "00:00".as_bytes().to_vec(),
            )
            .unwrap();
        store
            .set(
                "leader".as_bytes().to_vec(),
                "instance-b".as_bytes().to_vec(),
            )
            .unwrap();

        check_generations(temp_dir_path, vec![0, 1, 2, 3]);
        store.merge().unwrap();
        check_generations(temp_dir_path, vec![4, 5, 6]);

        drop(store);
        let mut store = Rustcask::builder()
            .set_max_data_file_size(1)
            .open(temp_dir_path)
            .unwrap();
        assert_eq!(
            store.get(&"leader".as_bytes().to_vec()).unwrap(),
            Some("instance-b".as_bytes().to_vec())
        );
        assert_eq!(
            store.get(&"last-election-ts".as_bytes().to_vec()).unwrap(),
            Some("00:00".as_bytes().to_vec())
        );
    }

    #[test]
    fn test_active_gen_update() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let temp_dir_path = temp_dir.path();
        let mut store = Rustcask::builder()
            .set_max_data_file_size(1)
            .open(temp_dir_path)
            .unwrap();

        let mut store_b = store.clone();

        store
            .set(
                "leader".as_bytes().to_vec(),
                "instance-a".as_bytes().to_vec(),
            )
            .unwrap();
        assert_eq!(store.get_active_generation(), 1); // Both stores share the same Writer, so they should see the same active generation
        assert_eq!(store_b.get_active_generation(), 1);

        // If the active generation is not correctly shared among stores, then
        // this will update the keydir with the incorrect generation. Thus, the following get for the key
        // will read the wrong data file.
        store_b
            .set("key".as_bytes().to_vec(), "value".as_bytes().to_vec())
            .unwrap();
        assert_eq!(
            store.get(&"key".as_bytes().to_vec()).unwrap(),
            Some("value".as_bytes().to_vec()),
        )
    }

    fn check_generations(temp_dir_path: &Path, expected_generations: Vec<GenerationNumber>) {
        let mut generations: Vec<GenerationNumber> = list_generations(temp_dir_path).unwrap();
        generations.sort_unstable();
        assert_eq!(generations, expected_generations);
    }
}
