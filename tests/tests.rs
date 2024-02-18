use assert_cmd::prelude::*;
use predicates::str::contains;
use rustcask::KvStore;
use std::process::Command;

// `kvs` with no args should exit with a non-zero code.
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
fn cli_get() {
    Command::cargo_bin("rustcask")
        .unwrap()
        .args(&["get", "key1"])
        .assert()
        .failure()
        .stderr(contains("unimplemented"));
}

#[test]
fn cli_set() {
    Command::cargo_bin("rustcask")
        .unwrap()
        .args(&["set", "key1", "value1"])
        .assert()
        .failure()
        .stderr(contains("unimplemented"));
}

#[test]
fn cli_rm() {
    Command::cargo_bin("rustcask")
        .unwrap()
        .args(&["rm", "key1"])
        .assert()
        .failure()
        .stderr(contains("unimplemented"));
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
fn get_stored_value() {
    let mut store = KvStore::new();

    let keys = ["key1", "key2"];
    let values = ["value1", "value2"];

    store.set(String::from(keys[0]), String::from(values[0]));
    store.set("key2".to_owned(), "value2".to_owned());

    assert_eq!(
        store.get(&String::from(keys[0])),
        Some(&String::from(values[0]))
    );
    assert_eq!(
        store.get(&String::from(keys[1])),
        Some(&String::from(values[1]))
    );
}

#[test]
fn overwrite_value() {
    let mut store = KvStore::new();

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
}

#[test]
fn get_non_existent_value() {
    let mut store: KvStore<String, String> = KvStore::new();

    assert_eq!(store.get(&String::from("empty_key")), None);
}

#[test]
fn remove_key() {
    let mut store = KvStore::new();

    let key = String::from("key");
    let value = String::from("value");

    store.set(key.clone(), value);
    store.remove(&key);
    assert_eq!(store.get(&key), None);
}
