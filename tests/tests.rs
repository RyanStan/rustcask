use rustcask::Rustcask;

use std::fs::{self};

use std::os::linux::fs::MetadataExt;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;

use tempfile::TempDir;

// Many of the tests in lib.rs could be considered integration tests.
// However, they're not included here because they make use of private types like LogFileIterator to validate
// behavior and internal state.

#[test]
fn get_stored_value() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = Rustcask::builder().open(temp_dir.path()).unwrap();

    let keys = ["key1".as_bytes().to_vec(), "key2".as_bytes().to_vec()];
    let values = ["value1".as_bytes().to_vec(), "value2".as_bytes().to_vec()];

    store.set(keys[0].clone(), values[0].clone()).unwrap();
    store.set(keys[1].clone(), values[1].clone()).unwrap();

    assert_eq!(store.get(&keys[0]).unwrap(), Some(values[0].clone()));

    assert_eq!(store.get(&keys[1]).unwrap(), Some(values[1].clone()));

    drop(store);

    // Open from disk and check persistent data
    let mut store = Rustcask::builder().open(temp_dir.path()).unwrap();
    assert_eq!(store.get(&keys[0]).unwrap(), Some(values[0].clone()));
    assert_eq!(store.get(&keys[1]).unwrap(), Some(values[1].clone()));
}

#[test]
fn overwrite_value() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = Rustcask::builder().open(temp_dir.path()).unwrap();

    let key = "key".as_bytes().to_vec();
    let values = ["value0".as_bytes().to_vec(), "value1".as_bytes().to_vec()];

    store.set(key.clone(), values[0].clone()).unwrap();
    assert_eq!(store.get(&key.clone()).unwrap(), Some(values[0].clone()));

    store.set(key.clone(), values[1].clone()).unwrap();
    assert_eq!(store.get(&key.clone()).unwrap(), Some(values[1].clone()));

    drop(store);
    let mut store = Rustcask::builder().open(temp_dir.path()).unwrap();
    assert_eq!(store.get(&key.clone()).unwrap(), Some(values[1].clone()));
}

#[test]
fn get_non_existent_value() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = Rustcask::builder().open(temp_dir.path()).unwrap();

    let key = "key".as_bytes().to_vec();
    assert_eq!(store.get(&key.clone()).unwrap(), None);
}

#[test]
fn remove_key() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = Rustcask::builder().open(temp_dir.path()).unwrap();

    let keys = ["key1".as_bytes().to_vec(), "key2".as_bytes().to_vec()];
    let values = ["value1".as_bytes().to_vec(), "value2".as_bytes().to_vec()];

    store.set(keys[0].clone(), values[0].clone()).unwrap();
    store.set(keys[1].clone(), values[1].clone()).unwrap();

    store.remove(keys[0].clone()).unwrap();
    assert_eq!(store.get(&keys[0].clone()).unwrap(), None);
    assert_eq!(store.get(&keys[1]).unwrap(), Some(values[1].clone()));

    drop(store);
    let mut store = Rustcask::builder().open(temp_dir.path()).unwrap();
    assert_eq!(store.get(&keys[0].clone()).unwrap(), None);
    assert_eq!(store.get(&keys[1]).unwrap(), Some(values[1].clone()));
}

#[test]
fn remove_non_existent_key() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = Rustcask::builder().open(temp_dir.path()).unwrap();

    assert!(matches!(
        store.remove("bad-key".as_bytes().to_vec()),
        Ok(None)
    ));
}

#[test]
fn concurrent_reads() {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let num_keyvals = 64;

    for _ in 0..num_keyvals {
        let key: Vec<u8> = (0..16).map(|_| rand::random::<u8>()).collect(); // 16-byte key
        keys.push(key);
    }

    for _ in 0..num_keyvals {
        let value: Vec<u8> = (0..32).map(|_| rand::random::<u8>()).collect(); // 32-byte value
        values.push(value);
    }

    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = Rustcask::builder().open(temp_dir.path()).unwrap();

    // Fill store
    for i in 0..num_keyvals {
        store.set(keys[i].clone(), values[i].clone()).unwrap();
    }

    // Run concurrent read tasks
    let num_tasks = 64;
    let mut handles = Vec::with_capacity(num_tasks);
    let barrier = Arc::new(Barrier::new(num_tasks + 1));

    for i in 0..num_tasks {
        let barrier = Arc::clone(&barrier);
        let key = keys[i].clone();
        let expected_val = values[i].clone();
        let mut store = store.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            let val = store.get(&key).unwrap();
            assert_eq!(val, Some(expected_val));
        }));
    }

    barrier.wait();

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_merge() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let temp_dir_path = temp_dir.path();
    let mut store = Rustcask::builder()
        .set_sync_mode(true)
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
            "leader".as_bytes().to_vec(),
            "instance-b".as_bytes().to_vec(),
        )
        .unwrap();

    let rustcask_dir_size = get_total_directory_size(temp_dir_path);
    store.merge().unwrap();
    let new_rustcask_dir_size = get_total_directory_size(temp_dir_path);
    assert!(new_rustcask_dir_size < rustcask_dir_size);

    assert_eq!(
        store.get(&"leader".as_bytes().to_vec()).unwrap(),
        Some("instance-b".as_bytes().to_vec())
    );
}

#[test]
fn test_merge_with_rotate() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let temp_dir_path = temp_dir.path();
    let mut store = Rustcask::builder()
        .set_sync_mode(true)
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
            "leader".as_bytes().to_vec(),
            "instance-b".as_bytes().to_vec(),
        )
        .unwrap();

    let rustcask_dir_size = get_total_directory_size(temp_dir_path);
    store.merge().unwrap();
    let new_rustcask_dir_size = get_total_directory_size(temp_dir_path);
    assert!(new_rustcask_dir_size < rustcask_dir_size);

    assert_eq!(
        store.get(&"leader".as_bytes().to_vec()).unwrap(),
        Some("instance-b".as_bytes().to_vec())
    );
}

/// Calculates the total size of files in a given directory and its subdirectories.
///
/// # Arguments
///
/// * `path` - A reference to the `Path` of the directory to calculate the size for.
///
/// # Returns
///
/// The total size of files in the directory and its subdirectories, in bytes.
fn get_total_directory_size(path: &Path) -> u64 {
    let mut total_size: u64 = 0;

    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries {
            if let Ok(entry) = entry {
                let entry_path = entry.path();
                if entry_path.is_dir() {
                    total_size += get_total_directory_size(&entry_path);
                } else {
                    if let Ok(metadata) = entry_path.metadata() {
                        total_size += metadata.st_size();
                    }
                }
            }
        }
    }

    total_size
}
