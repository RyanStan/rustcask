use crate::rustcask::GenerationNumber;
use std::path::{Path, PathBuf};

pub fn data_file_path(rustcask_dir: &Path, generation: &GenerationNumber) -> PathBuf {
    rustcask_dir.join(format!("{}.rustcask.data", generation))
}

pub fn hint_file_path(rustcask_dir: &Path, generation: GenerationNumber) -> PathBuf {
    rustcask_dir.join(format!("{}.rustcask.hint", generation))
}
