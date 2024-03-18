use assert_cmd::prelude::*;
use predicates::str::{contains, is_empty};
use predicates::ord::eq;
use rustcask::RustCask;
use std::fs::{self, File};
use std::path::Path;
use std::process::Command;
use tempfile::{tempdir, TempDir};
use walkdir::WalkDir;
use std::io::{Write};

// `kvs` with no args should exit with a non-zero code.
/* 
#[test]
fn cli_no_args() {
    Command::cargo_bin("rustcask").unwrap().assert().failure();
}

#[test]
fn cli_version() {
    Command::cargo_bin("rustcask")
        .unwrap()
        .args(&["-V"])
        .assert()
        .stdout(contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn cli_invalid_get() {
    Command::cargo_bin("rustcask")
        .unwrap()
        .args(&["get"])
        .assert()
        .failure();

    Command::cargo_bin("rustcask")
        .unwrap()
        .args(&["get", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_set() {
    Command::cargo_bin("rustcask")
        .unwrap()
        .args(&["set"])
        .assert()
        .failure();

    Command::cargo_bin("rustcask")
        .unwrap()
        .args(&["set", "missing_field"])
        .assert()
        .failure();

    Command::cargo_bin("rustcask")
        .unwrap()
        .args(&["set", "extra", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_rm() {
    Command::cargo_bin("rustcask")
        .unwrap()
        .args(&["rm"])
        .assert()
        .failure();

    Command::cargo_bin("rustcask")
        .unwrap()
        .args(&["rm", "extra", "field"])
        .assert()
        .failure();
}

#[test]
fn cli_invalid_subcommand() {
    Command::cargo_bin("rustcask")
        .unwrap()
        .args(&["unknown", "subcommand"])
        .assert()
        .failure();
}


#[test]
fn cli_get_non_existent_key() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("Key not found"));
}

#[test]
fn cli_rm_non_existent_key() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["rm", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .failure()
        .stdout(eq("Key not found"));
}

#[test]
fn cli_set() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["set", "key1", "value1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());
}

#[test]
fn cli_get_stored() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let mut store = RustCask::open(temp_dir.path()).unwrap();
    store.set("key1".to_owned(), "value1".to_owned()).unwrap();
    store.set("key2".to_owned(), "value2".to_owned()).unwrap();
    drop(store);

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("value1"));

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "key2"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("value2"));
}

#[test]
fn cli_rm_stored() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let mut store = RustCask::open(temp_dir.path()).unwrap();
    store.set("key1".to_owned(), "value1".to_owned()).unwrap();
    drop(store);

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["rm", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());

    Command::cargo_bin("kvs")
        .unwrap()
        .args(&["get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(eq("Key not found"));
}
*/

// TODO: Failing to start from empty state dir I think
#[test]
fn get_stored_value() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = RustCask::open(temp_dir.path()).unwrap();

    let keys = ["key1".as_bytes().to_vec(), "key2".as_bytes().to_vec()];
    let values = ["value1".as_bytes().to_vec(), "value2".as_bytes().to_vec()];

    store.set(keys[0].clone(), values[0].clone()).unwrap();
    //store.set("key2".to_owned(), "value2".to_owned());

    assert_eq!(
        store.get(&keys[0]),
        Some(values[0].clone())
    );
    /*
    assert_eq!(
        store.get(&String::from(keys[1])),
        Some(&String::from(values[1]))
    );
    */

    drop(store);

    // Open from disk and check persistent data
    /*
    let mut store = RustCask::open(temp_dir.path()).unwrap();
    assert_eq!(
        store.get(&String::from(keys[0])),
        Some(&String::from(values[0]))
    );
    /
    assert_eq!(
        store.get(&String::from(keys[1])),
        Some(&String::from(values[1]))
    );
    */
}

/*
#[test]
fn overwrite_value() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = RustCask::open(temp_dir.path()).unwrap();

    let keys = ["key1", "key2"];
    let values = ["value1", "value2"];

    store.set(String::from(keys[0]), String::from(values[0]));
    assert_eq!(
        store.get(&String::from(keys[0])),
        Some(&String::from(values[0]))
    );

    store.set(String::from(keys[0]), String::from(values[1]));
    assert_eq!(
        store.get(&String::from(keys[0])),
        Some(&String::from(values[1]))
    );

    drop(store);

    let mut store = RustCask::open(temp_dir.path()).unwrap();
    assert_eq!(
        store.get(&String::from(keys[0])),
        Some(&String::from(values[1]))
    );
}

#[test]
fn get_non_existent_value() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store: RustCask<String, String> = RustCask::open(temp_dir.path()).unwrap();

    assert_eq!(store.get(&String::from("empty_key")), None);

    drop(store);
    let mut store: RustCask<String, String> = RustCask::open(temp_dir.path()).unwrap();
    assert_eq!(store.get(&String::from("empty_key")), None);
}

#[test]
fn remove_key() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = RustCask::open(temp_dir.path()).unwrap();

    let key = String::from("key");
    let value = String::from("value");

    store.set(key.clone(), value);
    store.remove(&key);
    assert_eq!(store.get(&key), None);

    drop(store);
    let mut store: RustCask<String, String> = RustCask::open(temp_dir.path()).unwrap();
    assert_eq!(store.get(&key), None);
}

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