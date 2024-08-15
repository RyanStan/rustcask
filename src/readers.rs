use std::collections::hash_map::Entry;
use std::io::{self};
use std::sync::Arc;
use std::{collections::HashMap, fs::File, path::PathBuf};

use crate::utils::list_generations;
use crate::{bufio::BufReaderWithPos, utils::data_file_path, GenerationNumber};

// TODO [RyanStan 07-02-24] Extend this class (or restructure and create new classes) to support hint files.
//
// TODO [RyanStan 07/29/24] This type should encapsulate all reading logic.
#[derive(Debug)]
pub struct Readers {
    // TODO [RyanStan 2-28-24] Keeping a file handle for every open file may cause us to hit
    // system open file handle limits. We should use a LRU cache instead.
    //
    // A buffered reader provides benefits when performing sequential reads of the
    // data and hint files during startup
    pub(crate) data_file_readers: HashMap<GenerationNumber, BufReaderWithPos<File>>,
    rustcask_dir: Arc<PathBuf>,
}

impl Clone for Readers {
    fn clone(&self) -> Self {
        // TODO [RyanStan 07-01-24] Iterate over readers
        //   and create a BufReaderWithPos for each generation.
        Self {
            data_file_readers: HashMap::new(),
            rustcask_dir: self.rustcask_dir.clone(),
        }
    }
}

impl Readers {
    pub fn new(rustcask_dir: Arc<PathBuf>) -> Result<Self, io::Error> {
        let readers = Readers::create_data_file_readers(rustcask_dir.clone())?;
        Ok(Self {
            data_file_readers: readers,
            rustcask_dir,
        })
    }

    fn create_data_file_readers(
        rustcask_dir: Arc<PathBuf>,
    ) -> Result<HashMap<GenerationNumber, BufReaderWithPos<File>>, io::Error> {
        let mut readers = HashMap::new();
        let generations = list_generations(&rustcask_dir)?;
        for generation in generations {
            let reader = BufReaderWithPos::new(
                File::open(data_file_path(&rustcask_dir, &generation)).expect(&format!(
                    "Unable to open data file for generation {}.",
                    generation
                )),
            )?;
            readers.insert(generation, reader);
        }
        Ok(readers)
    }

    pub fn get_data_file_reader(&mut self, gen: GenerationNumber) -> &mut BufReaderWithPos<File> {
        match self.data_file_readers.entry(gen) {
            Entry::Vacant(entry) => {
                let reader = BufReaderWithPos::new(
                    File::open(data_file_path(&self.rustcask_dir, &gen))
                        .expect(&format!("Unable to open data file for generation {}", gen)),
                )
                .unwrap();
                entry.insert(reader)
            }
            Entry::Occupied(entry) => entry.into_mut(),
        }
    }
}
