use regex::Regex;

use crate::GenerationNumber;
use std::{
    fs::{self},
    io,
    path::{Path, PathBuf},
};

pub const KEYDIR_POISON_ERR: &str = "Another thread crashed while holding keydir lock. Panicking.";

pub fn data_file_path(rustcask_dir: &Path, generation: &GenerationNumber) -> PathBuf {
    rustcask_dir.join(format!("{}.rustcask.data", generation))
}

pub fn hint_file_path(rustcask_dir: &Path, generation: GenerationNumber) -> PathBuf {
    rustcask_dir.join(format!("{}.rustcask.hint", generation))
}

/// Returns the generations that are present within a directory.
pub fn list_generations(rustcask_dir: &Path) -> Result<Vec<GenerationNumber>, io::Error> {
    let mut generations: Vec<GenerationNumber> = Vec::new();
    let entries = fs::read_dir(rustcask_dir)?;
    for entry in entries {
        let entry = entry?.path();
        if is_data_file(&entry) {
            let gen: GenerationNumber = parse_generation_number(entry);
            generations.push(gen);
        }
    }

    Ok(generations)
}

pub fn is_data_file(path: &Path) -> bool {
    let file_name = match path.file_name() {
        Some(file) => file,
        None => return false,
    };

    let re = Regex::new(r"^\d+\.rustcask\.data$").unwrap();
    re.is_match(&file_name.to_string_lossy())
}

/// Returns the generation of a hint or data file
pub fn parse_generation_number(path: PathBuf) -> GenerationNumber {
    let file_name = path.file_name().unwrap().to_string_lossy();
    let generation = file_name.split('.').next().expect("Unexpected file format");
    let generation: GenerationNumber = generation
        .parse()
        .expect("Failed to parse generation from file name");

    generation
}

#[cfg(test)]
pub mod tests {
    use std::{
        fs::{self, File},
        path::Path,
    };

    use tempfile::tempdir;

    use crate::{
        logfile::LogFileIterator,
        utils::{is_data_file, list_generations, parse_generation_number},
    };

    /// Return the names of the files within a directory
    pub fn file_names(temp_dir_path: &Path) -> Vec<String> {
        let data_files = fs::read_dir(temp_dir_path).unwrap();
        let data_files: Vec<String> = data_files
            .map(|dir_entry| {
                dir_entry
                    .unwrap()
                    .path()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        data_files
    }

    /// Return the keys within a log file
    pub fn get_keys(temp_dir_path: &Path, log_file: &String) -> Vec<Vec<u8>> {
        let log_file_iter = LogFileIterator::new(temp_dir_path.join(log_file)).unwrap();

        let log_file_keys: Vec<Vec<u8>> = log_file_iter.map(|x| x.0.key).collect();

        log_file_keys
    }

    type KeyBytes = Vec<u8>;
    type ValueBytes = Vec<u8>;

    /// Return key value pairs from a log file
    pub fn get_keys_values(temp_dir_path: &Path, log_file: &String) -> Vec<(KeyBytes, ValueBytes)> {
        let log_file_iterator = LogFileIterator::new(temp_dir_path.join(log_file));
        let log_file_iter = log_file_iterator.unwrap();

        let log_file_kvs: Vec<(KeyBytes, ValueBytes)> = log_file_iter
            .map(|x| {
                // Throws an error if there is a tombstone value
                (x.0.key, x.0.value.unwrap())
            })
            .collect();

        log_file_kvs
    }

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

        let mut generations = list_generations(dir.path()).unwrap();
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
}
