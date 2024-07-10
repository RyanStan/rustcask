use std::{fs::File, io, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::bufio::BufReaderWithPos;

// TODO: on errors, just clone the key in memory. I think it's fine.
// If I get fancy, I can probably avoid doing that, but I don't want to get too fancy.

/// Represents an entry in the data or hint files.
#[derive(Serialize, Clone, Deserialize, Debug, PartialEq)]
pub struct LogFileEntry {
    //TODO [RyanStan 03/05/24] Add CRC and timestamp
    pub key: Vec<u8>,

    // None is used as a tombstone marker
    pub value: Option<Vec<u8>>,
}

impl LogFileEntry {
    pub fn create_tombstone_entry(key: Vec<u8>) -> Self {
        Self { key, value: None }
    }
}

#[derive(Debug, PartialEq)]
pub struct LogIndex {
    // Offset of log entry in bytes
    pub offset: u64,
    // Length of log entry in bytes
    pub len: u64,
}

pub struct LogFileIterator {
    log_path: PathBuf,
    reader: BufReaderWithPos<File>,
}

impl LogFileIterator {
    pub fn new(log_path: PathBuf) -> io::Result<Self> {
        let reader = BufReaderWithPos::new(File::open(&log_path)?)?;
        Ok(Self { log_path, reader })
    }
}

impl Iterator for LogFileIterator {
    // TODO [RyanStan 03-25-24] Wrap this in a Result so that we can return and catch errors
    // instead of just panicking or returning None.
    type Item = (LogFileEntry, LogIndex);

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.reader.pos();
        match bincode::deserialize_from::<_, LogFileEntry>(&mut self.reader) {
            Ok(log_file_entry) => {
                let len = self.reader.pos() - offset;
                Some((log_file_entry, LogIndex { offset, len }))
            }
            Err(err) => match err.as_ref() {
                bincode::ErrorKind::Io(io_error) => match io_error.kind() {
                    std::io::ErrorKind::UnexpectedEof => None,
                    _ => panic!("Error deserializing data file: {:?}", io_error),
                },
                _ => panic!("Error deserializing data file: {:?}", err),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Write, path::PathBuf};

    use tempfile::TempDir;

    use crate::{
        logfile::{LogFileEntry, LogFileIterator, LogIndex},
        utils::data_file_path,
    };

    fn setup_data_file(entries: Vec<LogFileEntry>) -> (TempDir, PathBuf, Vec<usize>, Vec<usize>) {
        let temp_dir = TempDir::new().unwrap();
        let data_file_path = data_file_path(temp_dir.path(), &0);
        let mut data_file = File::create(&data_file_path).unwrap();

        let mut encoded_lens = Vec::new();
        let mut offsets = Vec::new();

        let mut offset = 0;

        for entry in entries {
            let encoded = bincode::serialize(&entry).unwrap();
            let entry_len = encoded.len();
            encoded_lens.push(encoded.len());
            offsets.push(offset);
            offset += entry_len;
            let _ = data_file.write_all(&encoded).unwrap();
        }

        let _ = data_file.flush().unwrap();

        (temp_dir, data_file_path, encoded_lens, offsets)
    }

    #[test]
    fn test_log_iter_single_entry() {
        let entry = LogFileEntry {
            key: "key".as_bytes().to_vec(),
            value: Some("value".as_bytes().to_vec()),
        };
        let mut entries = Vec::new();
        entries.push(entry);
        let expected_num_entries = entries.len();

        let (_temp_dir, data_file_path, entry_lens, entry_offsets) =
            setup_data_file(entries.clone());
        let log_iter = LogFileIterator::new(data_file_path).unwrap();
        let data_entries: Vec<(LogFileEntry, LogIndex)> = log_iter.collect();

        assert_eq!(data_entries.len(), expected_num_entries);
        assert_eq!(data_entries[0].0, entries[0]);
        assert_eq!(
            data_entries[0].1,
            LogIndex {
                offset: entry_offsets[0] as u64,
                len: entry_lens[0] as u64
            }
        )
    }

    #[test]
    fn test_log_iter_two_entries() {
        let entries = Vec::from([
            LogFileEntry {
                key: "key".as_bytes().to_vec(),
                value: Some("value".as_bytes().to_vec()),
            },
            LogFileEntry {
                key: "key2".as_bytes().to_vec(),
                value: Some("value2".as_bytes().to_vec()),
            },
        ]);
        let expected_num_entries = entries.len();

        let (_temp_dir, data_file_path, entry_lens, entry_offsets) =
            setup_data_file(entries.clone());
        let log_iter = LogFileIterator::new(data_file_path).unwrap();
        let data_entries: Vec<(LogFileEntry, LogIndex)> = log_iter.collect();

        assert_eq!(data_entries.len(), expected_num_entries);
        for (i, entry) in data_entries.iter().enumerate() {
            assert_eq!(entry.0, entries[i]);
            assert_eq!(
                entry.1,
                LogIndex {
                    len: entry_lens[i] as u64,
                    offset: entry_offsets[i] as u64
                }
            )
        }
    }
}
