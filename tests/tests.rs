use rustcask::error::GetError;
use rustcask::RustCask;

use std::fs::{self};

use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;

use tempfile::TempDir;


#[test]
fn get_stored_value() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = RustCask::builder().open(temp_dir.path()).unwrap();

    let keys = ["key1".as_bytes().to_vec(), "key2".as_bytes().to_vec()];
    let values = ["value1".as_bytes().to_vec(), "value2".as_bytes().to_vec()];

    store.set(keys[0].clone(), values[0].clone()).unwrap();
    store.set(keys[1].clone(), values[1].clone()).unwrap();

    assert_eq!(store.get(&keys[0]).unwrap(), Some(values[0].clone()));

    assert_eq!(store.get(&keys[1]).unwrap(), Some(values[1].clone()));

    drop(store);

    // Open from disk and check persistent data
    let mut store = RustCask::builder().open(temp_dir.path()).unwrap();
    assert_eq!(store.get(&keys[0]).unwrap(), Some(values[0].clone()));
    assert_eq!(store.get(&keys[1]).unwrap(), Some(values[1].clone()));
}

#[test]
fn overwrite_value() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = RustCask::builder().open(temp_dir.path()).unwrap();

    let key = "key".as_bytes().to_vec();
    let values = ["value0".as_bytes().to_vec(), "value1".as_bytes().to_vec()];

    store.set(key.clone(), values[0].clone()).unwrap();
    assert_eq!(store.get(&key.clone()).unwrap(), Some(values[0].clone()));

    store.set(key.clone(), values[1].clone()).unwrap();
    assert_eq!(store.get(&key.clone()).unwrap(), Some(values[1].clone()));

    drop(store);
    let mut store = RustCask::builder().open(temp_dir.path()).unwrap();
    assert_eq!(store.get(&key.clone()).unwrap(), Some(values[1].clone()));
}

#[test]
fn get_non_existent_value() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = RustCask::builder().open(temp_dir.path()).unwrap();

    let key = "key".as_bytes().to_vec();
    assert_eq!(store.get(&key.clone()).unwrap(), None);
}

#[test]
fn remove_key() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = RustCask::builder().open(temp_dir.path()).unwrap();

    let keys = ["key1".as_bytes().to_vec(), "key2".as_bytes().to_vec()];
    let values = ["value1".as_bytes().to_vec(), "value2".as_bytes().to_vec()];

    store.set(keys[0].clone(), values[0].clone()).unwrap();
    store.set(keys[1].clone(), values[1].clone()).unwrap();

    store.remove(keys[0].clone()).unwrap();
    assert_eq!(store.get(&keys[0].clone()).unwrap(), None);
    assert_eq!(store.get(&keys[1]).unwrap(), Some(values[1].clone()));

    drop(store);
    let mut store = RustCask::builder().open(temp_dir.path()).unwrap();
    assert_eq!(store.get(&keys[0].clone()).unwrap(), None);
    assert_eq!(store.get(&keys[1]).unwrap(), Some(values[1].clone()));
}

/*
#[test]
fn remove_non_existent_key() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store: RustCask<String, String> = RustCask::builder().open(temp_dir.path()).unwrap();

    assert!(store.remove(&String::from("empty_key")).is_err());
}

*/

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
    let mut store = RustCask::builder().open(temp_dir.path()).unwrap();

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
            // Read the data for ith key and we later confirm it was the correct
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

fn print_files_in_dir<P>(dir: P)
where
    P: AsRef<Path>,
{
    for entry in fs::read_dir(&dir).unwrap() {
        let entry = entry.unwrap();
        println!("Here is an entry in the directory: {:?}", entry.path())
    }
}
