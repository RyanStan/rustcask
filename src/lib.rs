use crate::error::RustcaskError;
use crate::error::RustcaskError::BadRustcaskDirectory;
use bufio::BufReaderWithPos;
use datafile::DataFileEntry;
use keydir::KeyDir;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::borrow::BorrowMut;
use std::fs::OpenOptions;
use std::ops::Deref;
use std::{
    collections::HashMap,
    fs::{self, File},
    hash::Hash,
    io::{BufReader, BufWriter, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

mod error;
mod keydir;
mod bufio;
mod datafile;

type GenerationNumber = u64;

pub struct RustCask {
    active_generation: GenerationNumber,
    active_data_file_writer: BufWriter<File>,

    // TODO [RyanStan 2-28-24] Keeping a file handle for every open file may cause us to hit
    // system open file handle limits. We should use a LRU cache instead.
    //
    // TODO [RyanStan 3-18-24] Threads are expected to share a RustCask instance. Therefore,
    // they must share this set of BufReaders. This limits parallelism because a read on a data file
    // can only be performed by one thread at a time. We should instead allow each user thread
    // to have its own set of open file handles to the data and hint files. This way we can have multiple concurrent
    // reads on the same file at once.
    //
    // A buffered reader provides benefits when performing sequential reads of the
    // data and hint files during startup
    data_file_readers: HashMap<GenerationNumber, BufReaderWithPos<File>>,

    directory: PathBuf,

    keydir: KeyDir,
}

impl RustCask {
    /// Inserts a key-value pair into the map.
    /// TODO [RyanStan 3/6/23] Instead of panicking with except or unwrap, we should bubble errors up to the caller.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), RustcaskError> {
        // TODO: is this key clone ok?
        let data_file_entry = DataFileEntry {
            key: key.clone(),
            value,
        };

        let encoded =
            bincode::serialize(&data_file_entry).expect("Could not serialize data file entry");

        let file_offset = self.active_data_file_writer.stream_position().unwrap();
        self.active_data_file_writer
            .write_all(&encoded)
            .expect("Failed to write data file entry to stream");
        self.active_data_file_writer.flush().unwrap();

        self.keydir.set(
            key,
            self.active_generation,
            file_offset,
            encoded.len().try_into().unwrap(),
        );

        Ok(())

        // TODO: if the file is larger than some size, is this where we throw it into a data file? I think
        // E.g. see https://github.com/dragonquest/bitcask/blob/master/src/database.rs#L415-L423.
    }

    pub fn get(&mut self, key: &Vec<u8>) -> Option<Vec<u8>> {
        let keydir_entry = self.keydir.get(key)?;

        let reader = self
            .data_file_readers
            .get_mut(&keydir_entry.data_file_gen)
            .expect(&format!(
                "Could not find reader for generation {}",
                &keydir_entry.data_file_gen
            ));

        reader.seek(SeekFrom::Start(keydir_entry.pos));
        let data_file_entry: DataFileEntry =
            bincode::deserialize_from(reader).expect("Error deserializing data");

        assert_eq!(
            &data_file_entry.key, key,
            "The deserialized entries key does not match the key passed to get"
        );

        Some(data_file_entry.value)
    }

    /// Removes a key from the store, returning the value at the key
    /// if the key was previously in the map.
    pub fn remove(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, RustcaskError> {
        // TODO: implement
        Ok(None)
    }

    pub fn open(rustcask_dir: &Path) -> Result<RustCask, RustcaskError> {
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
            .open(data_file_path(&rustcask_dir, active_generation))
            .expect("Error opening active data file");

        let active_data_file_writer = BufWriter::new(active_data_file);

        let mut data_file_readers = create_data_file_readers(&rustcask_dir);

        //let keydir = build_keydir(&mut data_file_readers, &generations);

        Ok(RustCask {
            active_generation,
            active_data_file_writer,
            data_file_readers,
            directory: rustcask_dir,
            keydir: KeyDir::new(),
        })
    }
}

fn build_keydir(
    sorted_generations: &Vec<GenerationNumber>, rustcask_dir: &Path
) -> KeyDir {
    let mut keydir = KeyDir::new();
    for gen in sorted_generations {
        let data_file = data_file_path(rustcask_dir, gen);
        populate_keydir_with_data_file(data_file, &mut keydir);
    }

    keydir
}

// TODO [RyanStan 03-23-24] I should have a data file iterator. I think it would help simplify this code.
fn populate_keydir_with_data_file(data_file: &mut BufReaderWithPos<File>, keydir: &mut KeyDir, data_file_gen: GenerationNumber)
{
    // TODO: this shoudl just accept a path and should open an iterator...
    data_file.seek(SeekFrom::Start(0));
    loop {
        let pos = data_file.pos();
        match bincode::deserialize_from::<_, DataFileEntry>(data_file) {
            Ok(data_file_entry) => {
                // TODO: handle tombstones and removes
                // TODO: implement stats like live keys, dead keys, dead bytes, etc. Would be cool to get reports
                let len = data_file.pos() - pos;
                keydir.set(data_file_entry.key, data_file_gen, pos, len);
            }
            Err(err) => match err.as_ref() {
                bincode::ErrorKind::Io(io_error) => match io_error.kind() {
                    std::io::ErrorKind::UnexpectedEof => break,
                    _ => panic!("Error deserializing data file: {:?}", io_error)
                }
                _ => panic!("Error deserializing data file: {:?}", err)
            }
        }
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

fn is_data_file(path: &PathBuf) -> bool {
    let file_name = match path.file_name() {
        Some(file) => file,
        None => return false,
    };

    let re = Regex::new(r"^\d+\.rustcask\.data$").unwrap();
    re.is_match(&file_name.to_string_lossy())
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

fn create_data_file_readers(rustcask_dir: &Path) -> HashMap<GenerationNumber, BufReaderWithPos<File>> {
    let mut map = HashMap::new();
    let generations = list_generations(&rustcask_dir);
    for generation in generations {
        let reader = BufReaderWithPos::new(File::open(data_file_path(rustcask_dir, generation)).expect(
            &format!("Unable to open data file for generation {}", generation),
        )).unwrap();
        map.insert(generation, reader);
    }
    map
}

fn data_file_path(rustcask_dir: &Path, generation: &GenerationNumber) -> PathBuf {
    rustcask_dir.join(format!("{}.rustcask.data", generation))
}

fn hint_file_path(rustcask_dir: &Path, generation: GenerationNumber) -> PathBuf {
    rustcask_dir.join(format!("{}.rustcask.hint", generation))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{tempdir, TempDir};

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
    fn test_parse_generation_number() {
        let dir = tempdir().unwrap();
        let data_file = dir.path().join("/tmp/384304/0.rustcask.data");
        assert_eq!(parse_generation_number(data_file), 0);

        let dir = tempdir().unwrap();
        let data_file = dir.path().join("/tmp/384304/1000.rustcask.hint");
        assert_eq!(parse_generation_number(data_file), 1000);
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
    fn test_open() {
        let dir = tempdir().unwrap();

        for number in 1..=5 {
            File::create(dir.path().join(format!("{}.rustcask.data", number))).unwrap();
            File::create(dir.path().join(format!("{}.rustcask.hint", number))).unwrap();
        }

        let rustcask = RustCask::open(dir.path()).unwrap();

        assert_eq!(rustcask.active_generation, 5);
        assert_eq!(rustcask.data_file_readers.len(), 5);
    }

    #[test]
    fn test_open_on_empty_dir() {
        let dir = tempdir().unwrap();
        let rustcask = RustCask::open(dir.path()).unwrap();
        assert_eq!(rustcask.active_generation, 0);
        assert_eq!(rustcask.data_file_readers.len(), 1);
    }

    #[test]
    fn test_open_non_existent_dir() {
        let dir = tempdir().unwrap();
        let invalid_dir = dir.path().join("invalid-dir");
        let rustcask = RustCask::open(&invalid_dir);
        assert!(matches!(rustcask, Err(BadRustcaskDirectory(_))));
    }

    #[test]
    fn test_populate_keydir_with_data_file() {
        let temp_dir = TempDir::new().unwrap();
        let data_file = data_file_path(temp_dir.path(), 0);
        let mut data_file = File::create(data_file).unwrap();

        let key = "key".as_bytes().to_vec();
        let value = "value".as_bytes().to_vec();

        // encode the entry into the file
        let data_file_entry = DataFileEntry {
            key,
            value,
        };

        let encoded =
            bincode::serialize(&data_file_entry).unwrap();

        data_file.write_all(&encoded);
        data_file.flush().unwrap();

    }
}
