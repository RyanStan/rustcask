use assert_cmd::prelude::*;


use rustcask::RustCask;
use std::fs::{self};

use std::path::Path;

use tempfile::{TempDir};


#[test]
fn get_stored_value() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = RustCask::open(temp_dir.path()).unwrap();

    let keys = ["key1".as_bytes().to_vec(), "key2".as_bytes().to_vec()];
    let values = ["value1".as_bytes().to_vec(), "value2".as_bytes().to_vec()];

    store.set(keys[0].clone(), values[0].clone()).unwrap();
    store.set(keys[1].clone(), values[1].clone()).unwrap();

    assert_eq!(store.get(&keys[0]), Some(values[0].clone()));

    assert_eq!(store.get(&keys[1]), Some(values[1].clone()));

    drop(store);

    // Open from disk and check persistent data
    let mut store = RustCask::open(temp_dir.path()).unwrap();
    assert_eq!(store.get(&keys[0]), Some(values[0].clone()));
    assert_eq!(store.get(&keys[1]), Some(values[1].clone()));
}

#[test]
fn overwrite_value() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = RustCask::open(temp_dir.path()).unwrap();

    let key = "key".as_bytes().to_vec();
    let values = ["value0".as_bytes().to_vec(), "value1".as_bytes().to_vec()];

    store.set(key.clone(), values[0].clone()).unwrap();
    assert_eq!(store.get(&key.clone()), Some(values[0].clone()));

    store.set(key.clone(), values[1].clone()).unwrap();
    assert_eq!(store.get(&key.clone()), Some(values[1].clone()));

    drop(store);
    let mut store = RustCask::open(temp_dir.path()).unwrap();
    assert_eq!(store.get(&key.clone()), Some(values[1].clone()));
}

#[test]
fn get_non_existent_value() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = RustCask::open(temp_dir.path()).unwrap();

    let key = "key".as_bytes().to_vec();
    assert_eq!(store.get(&key.clone()), None);
}

#[test]
fn remove_key() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = RustCask::open(temp_dir.path()).unwrap();

    let keys = ["key1".as_bytes().to_vec(), "key2".as_bytes().to_vec()];
    let values = ["value1".as_bytes().to_vec(), "value2".as_bytes().to_vec()];

    store.set(keys[0].clone(), values[0].clone()).unwrap();
    store.set(keys[1].clone(), values[1].clone()).unwrap();

    store.remove(keys[0].clone()).unwrap();
    assert_eq!(store.get(&keys[0].clone()), None);
    assert_eq!(store.get(&keys[1]), Some(values[1].clone()));

    drop(store);
    let mut store = RustCask::open(temp_dir.path()).unwrap();
    assert_eq!(store.get(&keys[0].clone()), None);
    assert_eq!(store.get(&keys[1]), Some(values[1].clone()));
}

/*
#[test]
fn remove_non_existent_key() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store: RustCask<String, String> = RustCask::open(temp_dir.path()).unwrap();

    assert!(store.remove(&String::from("empty_key")).is_err());
}

#[test]
fn open_active_file_exists() {
    let dir = tempdir().unwrap();
    let mut active_data_file = File::create(dir.path().join("2.rustcask.data")).unwrap();
    writeln!(active_data_file, "active data file").unwrap();

    let mut old_data_file = File::create(dir.path().join("1.rustcask.data")).unwrap();
    writeln!(old_data_file, "old data file").unwrap();

    let mut old_data_file = File::create(dir.path().join("0.rustcask.data")).unwrap();
    writeln!(old_data_file, "oldest data file").unwrap();

    //print_files_in_dir(&dir);
    let mut rustcask: RustCask<String, String> = RustCask::open(dir.path()).unwrap();


    drop(active_data_file);
    dir.close().unwrap();
}
*/

fn print_files_in_dir<P>(dir: P)
where
    P: AsRef<Path>,
{
    for entry in fs::read_dir(&dir).unwrap() {
        let entry = entry.unwrap();
        println!("Here is an entry in the directory: {:?}", entry.path())
    }
}
