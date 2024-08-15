use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{Arc, RwLock},
};

use log::{debug, trace};

use crate::{
    error::{
        MergeError, MergeErrorKind, OpenError, OpenErrorKind, RemoveError, RemoveErrorKind,
        SetError, SetErrorKind,
    },
    keydir::KeyDir,
    logfile::{LogFileEntry, LogIndex},
    readers::Readers,
    utils::{data_file_path, list_generations, KEYDIR_POISON_ERR},
    GenerationNumber,
};

/// The Writer is responsible for writing data to the rustcask directory.
///
/// The Writer is wrapped in an Arc<Mutex<>> within the Rustcask struct to allow for concurrent access.
#[derive(Debug)]
pub struct Writer {
    pub(crate) active_generation: GenerationNumber,
    pub(crate) active_data_file: BufWriter<File>,
    pub(crate) active_data_file_size: u64,
    pub(crate) sync_mode: bool,
    pub(crate) max_data_file_size: u64,
    pub(crate) rustcask_directory: Arc<PathBuf>,
    pub(crate) keydir: Arc<RwLock<KeyDir>>,
    pub(crate) readers: Readers,
}

impl Writer {
    /// Creates a new `Writer` instance for the RustCask database.
    ///
    /// This function initializes a new `Writer` with the provided configuration options and the current
    /// state of the database
    ///
    ///  # Arguments
    ///
    /// * `sync_mode` - A boolean indicating whether to sync data to disk after every write.
    /// * `max_data_file_size` - The maximum size (in bytes) for a single data file.
    /// * `rustcask_directory` - An `Arc<PathBuf>` representing the path to the RustCask directory.
    /// * `keydir` - An `Arc<RwLock<KeyDir>>` representing the key directory.
    /// * `readers` - A `Readers` instance containing the active readers.
    ///
    /// # Returns
    ///
    /// * `Ok(Writer)` - A new `Writer` instance if the initialization was successful.
    /// * `Err(OpenError)` - An `OpenError` if there was an error listing generations or opening the
    ///   active data file.
    ///
    /// # Errors
    ///
    /// This function may return an `OpenError` if:
    ///
    /// * There was an I/O error listing the generations in the RustCask directory.
    /// * There was an I/O error opening the active data file.
    pub fn new(
        sync_mode: bool,
        max_data_file_size: u64,
        rustcask_directory: Arc<PathBuf>,
        keydir: Arc<RwLock<KeyDir>>,
        readers: Readers,
    ) -> Result<Writer, OpenError> {
        let mut generations: Vec<GenerationNumber> = list_generations(&rustcask_directory)
            .map_err(|err| OpenError {
                kind: OpenErrorKind::Io(err),
                rustcask_dir: rustcask_directory.to_string_lossy().to_string(),
            })?;
        generations.sort_unstable();

        let active_generation: GenerationNumber = match generations.last() {
            Some(generation) => *generation,
            None => 0,
        };

        let active_data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(data_file_path(&rustcask_directory, &active_generation))
            .map_err(|err| OpenError {
                kind: OpenErrorKind::Io(err),
                rustcask_dir: rustcask_directory.to_string_lossy().to_string(),
            })?;

        let active_data_file_size = active_data_file.metadata().unwrap().len();

        let buffered_writer = BufWriter::new(active_data_file);

        Ok(Writer {
            active_generation,
            active_data_file: buffered_writer,
            active_data_file_size,
            sync_mode,
            max_data_file_size,
            rustcask_directory,
            keydir,
            readers,
        })
    }

    /// Inserts a key-value pair into the database.
    ///
    /// This function first serializes the `LogFileEntry` containing the key and value, and appends it
    /// to the active data file. It then updates the key directory with the key, generation number, and
    /// log index of the entry in the data file. This ordering ensures that concurrent reads will not
    /// see a key in the key directory before its value is written to the data file, maintaining
    /// correctness.
    ///
    /// # Arguments
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
        // To maintain correctness with concurrent reads, 'set' must insert an entry into the active data file,
        // and then update the keydir. This way, a concurrent read does not see an entry in the keydir
        // before the corresponding value has been written to the data file.
        let data_file_entry = LogFileEntry {
            key,
            value: Some(value),
        };

        let encoded = bincode::serialize(&data_file_entry).map_err(|err| SetError {
            kind: SetErrorKind::Serialize(err),
            key: data_file_entry.key.clone(),
        })?;

        let (log_index, gen) = self.write_to_active_data_file(encoded).unwrap();

        self.keydir
            .write()
            .expect("Another thread crashed while holding keydir lock. Panicking.")
            .set(data_file_entry.key.clone(), gen, log_index);

        Ok(())
    }

    /// Writes the encoded log file entry to the active data file.
    ///
    /// This function appends the encoded log file entry to the active data file. If the active data
    /// file exceeds the maximum size after writing the entry, it rotates to a new data file with an
    /// incremented generation number.
    ///
    /// This function does not modify the keydir. That is up to the caller to do.
    ///
    /// # Arguments
    ///
    /// * `encoded_log_file_entry` - Encoded bytes to write to the active data file.
    ///
    /// # Returns
    ///
    /// A `Result` containing a tuple of the log index and generation number for the written entry on
    /// success, or an `io::Error` on failure.
    ///
    fn write_to_active_data_file(
        &mut self,
        encoded_log_file_entry: Vec<u8>,
    ) -> Result<(LogIndex, GenerationNumber), io::Error> {
        let file_offset = self.active_data_file.stream_position()?;
        self.active_data_file.write_all(&encoded_log_file_entry)?;
        self.active_data_file.flush()?;
        if self.sync_mode {
            // Force the write to disk.
            self.active_data_file.get_ref().sync_all()?;
        }
        let len_encoded_data = encoded_log_file_entry.len();
        self.active_data_file_size += len_encoded_data as u64;

        trace!(
            "Wrote {} bytes to data file (gen={})",
            len_encoded_data,
            self.active_generation
        );

        let written_generation = self.active_generation;

        if self.active_data_file_size >= self.max_data_file_size {
            self.rotate_active_data_file();
        }

        Ok((
            LogIndex {
                offset: file_offset,
                len: len_encoded_data.try_into().unwrap(),
            },
            written_generation,
        ))
    }

    fn rotate_active_data_file(&mut self) {
        // TODO [RyanStan 07/22/24]
        // Errors during rotation should return a "rotation" error so that the caller knows the value was successfully written,
        // but that the rotation didn't work as expected.
        self.active_generation += 1;
        trace!(
            "Rotating active data file. New generation start: {}",
            self.active_generation
        );

        let active_data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(data_file_path(
                &self.rustcask_directory,
                &self.active_generation,
            ))
            .expect("Error opening active data file");

        self.active_data_file = BufWriter::new(active_data_file);

        self.active_data_file_size = 0;
        debug!(
            "Rotated active data file. New active generation: {}",
            self.active_generation
        );
    }

    // TODO [RyanStan 7-8-24] Implement merge window support.
    pub fn can_merge(&self) -> bool {
        true
    }

    /// Performs a merge operation on the log data files.
    ///
    /// The merge operation combines all the live log entries from the existing data files into a new
    /// set of data files with an incremented generation number. After the merge is complete, the
    /// previous generations of data files are deleted.
    ///
    /// This function will update the keydir.
    ///
    /// # Errors
    ///
    /// This function returns a `MergeError` if an error occurs during the merge process, such as an
    /// I/O error or an inconsistency in the data. The `merge_generation` field of the error contains
    /// the generation number of the merge that failed.
    pub fn merge(&mut self) -> Result<(), MergeError> {
        let mut active_merge_gen: u64 = self.get_active_generation() + 1;
        let initial_merge_gen = active_merge_gen;

        let mut keydir_guard = self.keydir.write().expect(KEYDIR_POISON_ERR);
        let keydir = &*keydir_guard;
        let mut new_keydir = KeyDir::new_empty();
        let mut merge_offset: u64 = 0;
        let mut file_size: u64 = 0;

        let previous_generations: Vec<GenerationNumber> =
            list_generations(&self.rustcask_directory).unwrap();

        let mut active_merge_data_file = BufWriter::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(data_file_path(&self.rustcask_directory, &active_merge_gen))
                .unwrap(),
        );

        for (key, val) in keydir {
            let reader = self.readers.get_data_file_reader(val.data_file_gen);
            reader.seek(SeekFrom::Start(val.index.offset)).unwrap();
            let mut buffer: Vec<u8> = vec![0; val.index.len as usize];
            let bytes_read = reader.read(&mut buffer).map_err(|err| MergeError {
                kind: MergeErrorKind::Io(err),
                merge_generation: initial_merge_gen,
            })?;
            assert_eq!(
                bytes_read, val.index.len as usize,
                "Error performing merging: bytes read for live entry does not match expected byte count.
                Aborting merge. However, new data file is still safe to read from."
            );
            active_merge_data_file
                .write_all(&buffer)
                .map_err(|err| MergeError {
                    kind: MergeErrorKind::Io(err),
                    merge_generation: initial_merge_gen,
                })?;

            new_keydir.set(
                key.clone(),
                active_merge_gen,
                LogIndex {
                    offset: merge_offset,
                    len: bytes_read as u64,
                },
            );

            merge_offset += bytes_read as u64;
            file_size += bytes_read as u64;

            // Rotate the active data file if it exceeded the size threshold
            if file_size > self.max_data_file_size {
                active_merge_data_file.flush().map_err(|err| MergeError {
                    kind: MergeErrorKind::Io(err),
                    merge_generation: initial_merge_gen,
                })?;

                self.rotate_merge_data_file(
                    &mut active_merge_gen,
                    &mut active_merge_data_file,
                    &mut file_size,
                    &mut merge_offset,
                )
                .map_err(|err| MergeError {
                    kind: MergeErrorKind::Io(err),
                    merge_generation: initial_merge_gen,
                })?;
            }
        }

        active_merge_data_file.flush().map_err(|err| MergeError {
            kind: MergeErrorKind::Io(err),
            merge_generation: initial_merge_gen,
        })?;

        self.active_generation = active_merge_gen;
        *keydir_guard = new_keydir;

        // TODO [RyanStan 07/29/24] Failures here should return a message that indicates to the user
        // that merge failed during removal of generations.
        self.delete_generations(previous_generations)
            .map_err(|err| MergeError {
                kind: MergeErrorKind::Io(err),
                merge_generation: initial_merge_gen,
            })?;

        Ok(())
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
    /// * `Err(RemoveError)` if there was an error deserializing the log entry or performing I/O
    ///   operations.
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
        let tombstone = LogFileEntry::create_tombstone_entry(key);
        let encoded_tombstone =
            bincode::serialize(&tombstone).expect("Could not serialize tombstone");
        self.write_to_active_data_file(encoded_tombstone).unwrap();

        match self
            .keydir
            .write()
            .expect("Another thread panicked while holding the keydir lock. Panicking.")
            .remove(&tombstone.key)
        {
            // The key was not previously in the map
            None => Ok(None),
            // The key was previously in the map, so we retrieve the overwritten value and return it.
            Some(keydir_entry) => {
                let reader = self
                    .readers
                    .get_data_file_reader(keydir_entry.data_file_gen);

                let log_index = &keydir_entry.index;
                reader
                    .seek(SeekFrom::Start(log_index.offset))
                    .map_err(|err| RemoveError {
                        kind: RemoveErrorKind::Io(err),
                        key: tombstone.key.clone(),
                    })?;

                let data_file_entry: LogFileEntry =
                    bincode::deserialize_from(reader).map_err(|err| RemoveError {
                        kind: RemoveErrorKind::Deserialize(err),
                        key: tombstone.key.clone(),
                    })?;

                Ok(Some(data_file_entry.value.expect(
                    "We returned a tombstone value from get. We should have instead returned None. 
                    The data store may not be corrupted - this indicates a programming bug.",
                )))
            }
        }
    }

    pub fn get_active_generation(&self) -> GenerationNumber {
        self.active_generation
    }

    pub fn get_active_data_file_size(&self) -> u64 {
        self.active_data_file_size
    }

    fn delete_generations(&self, previous_generations: Vec<u64>) -> Result<(), io::Error> {
        for generation in previous_generations {
            debug!(
                "Merge: deleting {}.",
                data_file_path(&self.rustcask_directory, &generation)
                    .to_string_lossy()
                    .to_string()
            );
            fs::remove_file(data_file_path(&self.rustcask_directory, &generation))?;
        }
        Ok(())
    }

    fn rotate_merge_data_file(
        &self,
        active_merge_gen: &mut u64,
        active_merge_data_file: &mut BufWriter<File>,
        file_size: &mut u64,
        active_merge_offset: &mut u64,
    ) -> Result<(), io::Error> {
        *active_merge_gen += 1;
        *active_merge_data_file = BufWriter::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(data_file_path(&self.rustcask_directory, &*active_merge_gen))?,
        );
        *file_size = 0;
        *active_merge_offset = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write,
        path::PathBuf,
        sync::{Arc, RwLock},
    };

    use tempfile::TempDir;

    use crate::{
        keydir::KeyDir,
        readers::Readers,
        utils::{
            data_file_path,
            tests::{file_names, get_keys_values},
        },
    };

    use super::Writer;

    #[test]
    fn test_set_happy_path() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let temp_dir_path = temp_dir.path().to_path_buf();
        let keydir = KeyDir::new_empty();
        let mut writer = create_test_writer(&temp_dir_path, keydir);
        let key = "key".as_bytes().to_vec();
        let value = "value".as_bytes().to_vec();

        writer.set(key.clone(), value.clone()).unwrap();

        let log_file_keys = get_keys_values(&temp_dir_path, &String::from("0.rustcask.data"));
        assert_eq!(log_file_keys.len(), 1);
        assert_eq!(
            log_file_keys,
            vec![("key".as_bytes().to_vec(), "value".as_bytes().to_vec())]
        );
        let data_files = file_names(&temp_dir_path);
        assert!(data_files.contains(&"0.rustcask.data".to_string()));
    }

    #[test]
    fn test_rotate_active_data_file() {
        let temp_dir = TempDir::new().unwrap();
        let rustcask_directory = temp_dir.path().to_path_buf();
        let keydir = KeyDir::new_empty();

        let mut writer = create_test_writer(&rustcask_directory, keydir);

        writer.active_data_file.write_all(b"test data").unwrap();

        let initial_generation = writer.active_generation;

        writer.rotate_active_data_file();

        assert_eq!(writer.active_generation, initial_generation + 1);

        let new_file_path = data_file_path(&rustcask_directory, &writer.active_generation);
        assert!(std::path::Path::new(&new_file_path).exists());

        assert_eq!(writer.active_data_file_size, 0);

        temp_dir.close().unwrap();
    }

    #[test]
    fn test_write_to_active_data_file_with_rotate() {
        let temp_dir = TempDir::new().unwrap();
        let rustcask_directory = temp_dir.path().to_path_buf();
        let keydir = KeyDir::new_empty();

        let mut writer = create_test_writer(&rustcask_directory, keydir);
        writer.max_data_file_size = 1; // Force rotations
        let initial_generation = writer.active_generation;

        let test_bytes: Vec<u8> = "test".to_string().into_bytes();
        let (log_index, generation) = writer
            .write_to_active_data_file(test_bytes.clone())
            .unwrap();

        assert_eq!(writer.active_generation, initial_generation + 1);
        assert_eq!(generation, initial_generation); // The bytes should have been written to the original generation data file.
        assert_eq!(log_index.offset, 0);
        assert_eq!(log_index.len, test_bytes.len().try_into().unwrap());
    }

    #[test]
    fn test_write_to_active_data_file_twice_without_rotate() {
        let temp_dir = TempDir::new().unwrap();
        let rustcask_directory = temp_dir.path().to_path_buf();
        let keydir = KeyDir::new_empty();

        let mut writer = create_test_writer(&rustcask_directory, keydir);
        writer.max_data_file_size = 1024;
        let initial_generation = writer.active_generation;

        let test_bytes: Vec<u8> = "test".to_string().into_bytes();
        let (mut log_index, mut generation) = writer
            .write_to_active_data_file(test_bytes.clone())
            .unwrap();

        assert_eq!(writer.active_generation, initial_generation);
        assert_eq!(generation, initial_generation);
        assert_eq!(log_index.offset, 0);
        assert_eq!(log_index.len, test_bytes.len().try_into().unwrap());

        let more_test_bytes = "more-test-bytes".to_string().into_bytes();
        (log_index, generation) = writer
            .write_to_active_data_file(more_test_bytes.clone())
            .unwrap();
        assert_eq!(log_index.offset, test_bytes.len() as u64);
        assert_eq!(log_index.len, more_test_bytes.len().try_into().unwrap());
        assert_eq!(writer.active_generation, initial_generation);
        assert_eq!(generation, initial_generation);
    }

    fn create_test_writer(rustcask_dir: &PathBuf, keydir: KeyDir) -> Writer {
        // TODO [RyanStan 08/13/24] In the future, we may want to create mock keydir and readers.
        // Then, this function should take a keydir and reader as input.
        // We'll also have to refactor those types to be traits.
        let readers = Readers::new(Arc::new(rustcask_dir.clone())).unwrap();

        let writer = Writer::new(
            false,
            1024,
            Arc::new(rustcask_dir.clone()),
            Arc::new(RwLock::new(keydir)),
            readers,
        )
        .unwrap();

        writer
    }
}
