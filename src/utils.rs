use regex::Regex;

use crate::GenerationNumber;
use std::{
    fs::{self},
    io,
    path::{Path, PathBuf},
};

pub fn data_file_path(rustcask_dir: &Path, generation: &GenerationNumber) -> PathBuf {
    rustcask_dir.join(format!("{}.rustcask.data", generation))
}

pub fn hint_file_path(rustcask_dir: &Path, generation: GenerationNumber) -> PathBuf {
    rustcask_dir.join(format!("{}.rustcask.hint", generation))
}

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
mod tests {
    use std::fs::File;

    use tempfile::tempdir;

    use crate::utils::{is_data_file, list_generations, parse_generation_number};

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
