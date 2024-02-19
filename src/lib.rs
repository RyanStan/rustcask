use std::{collections::HashMap, fs::{self, File}, hash::Hash, io::{BufReader, BufWriter}, path::{Path, PathBuf}};
use crate::error::RustcaskError;
use std::fs::OpenOptions;
mod error;
use regex::Regex;

type GenerationNumber = u64;

pub struct RustCask<K, V> {
    map: HashMap<K, V>,
    active_generation: GenerationNumber,
    active_data_file: BufWriter<File>,

    // TODO [RyanStan 2-28-24] Keeping a file handle for every open file may cause us to hit
    // system open file handle limits. We should use a LRU cache instead.
    non_active_data_files: HashMap<GenerationNumber, BufReader<File>>,

    directory: PathBuf
}

impl<K, V> RustCask<K, V>
where
    K: Eq + PartialEq,
    K: Hash,
{

    /// Inserts a key-value pair into the map.
    pub fn set(&mut self, key: K, value: V) -> Result<Option<V>, RustcaskError> {
        Ok(self.map.insert(key, value))
    }

    /// Returns a reference to the value corresponding to the key.
    pub fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }

    /// Removes a key from the store, returning the value at the key
    /// if the key was previously in the map.
    pub fn remove(&mut self, key: &K) -> Result<Option<V>, RustcaskError> {
        Ok(self.map.remove(key))
    }

    pub fn open(rustcask_dir: &Path) -> Result<RustCask<K, V>, RustcaskError> {
        
        let rustcask_dir = PathBuf::from(&rustcask_dir);

        let mut generations: Vec<GenerationNumber> = list_generations(&rustcask_dir);
        generations.sort_unstable();

        let active_generation: GenerationNumber = match generations.last() {
            Some(generation) => *generation,
            None => 0,
        };

        let active_data_file = OpenOptions::new().read(true).write(true).create(true)
                                                .open(data_file_path(&rustcask_dir, active_generation))
                                                .expect("Error opening active data file");

        let active_data_file = BufWriter::new(active_data_file);

        let non_active_data_files = create_non_active_data_file_readers(&rustcask_dir, generations);


        Ok(RustCask {
            map: HashMap::new(),
            active_generation,
            active_data_file,
            non_active_data_files,
            directory: rustcask_dir,
        })

    }
}

fn list_generations(rustcask_dir: &Path) -> Vec<GenerationNumber> {
    let generations: Vec<GenerationNumber> = fs::read_dir(rustcask_dir).unwrap()
        .map(|entry| -> PathBuf { entry.unwrap().path() })
        .filter(is_data_file).map(parse_generation_number).collect();

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
    let generation: GenerationNumber = generation.parse().expect("Failed to parse generation from file name");

    generation
}

fn create_non_active_data_file_readers(rustcask_dir: &Path, generations: Vec<GenerationNumber>) -> HashMap<GenerationNumber, BufReader<File>> {
    let mut map = HashMap::new();
    for generation in generations {
        let reader = BufReader::new(File::open(data_file_path(rustcask_dir, generation))
            .expect(&format!("Unable to open data file for generation {}", generation)));
        map.insert(generation, reader);
    }
    map
}

fn data_file_path(rustcask_dir: &Path, generation: GenerationNumber) -> PathBuf {
    rustcask_dir.join(format!("{}.rustcask.data", generation))
}

fn hint_file_path(rustcask_dir: &Path, generation: GenerationNumber) -> PathBuf {
    rustcask_dir.join(format!("{}.rustcask.hint", generation))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

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

        let rustcask = RustCask::<String, String>::open(dir.path()).unwrap();

        assert_eq!(rustcask.active_generation, 5);
        assert_eq!(rustcask.non_active_data_files.len(), 5);
    }
}