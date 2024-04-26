use divan::Bencher;
use rand::{distributions::{Distribution, Uniform}, Rng};
use rustcask::rustcask::RustCask;
use tempfile::TempDir;
use divan::counter::BytesCount;
use rand::prelude::SliceRandom;


fn main() {
    divan::main();
}

/* Measure single write, single read

Things that the bitcask authors cared about:
- Low write per-item write latency
- Low per-read latency


    N sequential writes
    N random reads (set up is filling db with N items).
        (N must be less than mem size)
    
    Max write throughput benchmark

    In the readme, mention that performance depends on the workload and underlying disk device, and that
    and users should test their workloads against different storage engines.

    A performance test for hint files.... startup time.
    Need a workload that generates lots of dead keys via overwrites.... I can do that. Just randomly pick keys to write to with random values from a set list.

It would be cool to have a "real-life" benchmark. Some real application that uses a key-value store.
 
*/

const COUNT_KV_PAIRS: usize = 1000;
const OVERWRITE_COUNT: usize = 30;
const KEY_SIZE: usize = 1024; // 1 KiB
const VAL_SIZE: usize = 8096; // 8 KiB

#[derive(Clone)]
struct KeyValuePair(Vec<u8>, Vec<u8>);

impl KeyValuePair {
    fn random<R: Rng>(rng: &mut R, key_size: usize, value_size: usize) -> KeyValuePair {
        let key: Vec<u8> = (0..key_size).map(|_| rng.gen::<u8>()).collect();
        let value: Vec<u8> = (0..value_size).map(|_| rng.gen::<u8>()).collect();
        KeyValuePair(key, value)
    }

    fn random_many<R: Rng>(
        rng: &mut R,
        count_kv_pairs: usize,
        max_key_size: usize,
        max_val_size: usize,
    ) -> Vec<KeyValuePair> {
        let key_dist = Uniform::from(1..max_key_size);
        let val_dist = Uniform::from(1..max_val_size);
        (0..count_kv_pairs)
            .map(|_| {
                let key_size = key_dist.sample(rng);
                let value_size = val_dist.sample(rng);
                KeyValuePair::random(rng, key_size, value_size)
            })
            .collect()
    }
}


#[divan::bench]
fn bench_writes(bencher: Bencher) {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = RustCask::builder().open(temp_dir.path()).unwrap();

    bencher
        .with_inputs(move || {
            let mut rng = rand::thread_rng();
            let store = store.clone();
            (KeyValuePair::random(&mut rng, KEY_SIZE, VAL_SIZE), store)
        })
        .input_counter(|(kv_pair, _)| BytesCount::new(kv_pair.0.len() + kv_pair.1.len()))
        .bench_values(|(kv_pair, mut store)| {
            store.set(kv_pair.0, kv_pair.1)
        });
}

#[divan::bench]
fn bench_writes_sync_mode(bencher: Bencher) {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = RustCask::builder().set_sync_mode(true).open(temp_dir.path()).unwrap();

    bencher
        .with_inputs(move || {
            let mut rng = rand::thread_rng();
            let store = store.clone();
            (KeyValuePair::random(&mut rng, KEY_SIZE, VAL_SIZE), store)
        })
        .input_counter(|(kv_pair, _)| BytesCount::new(kv_pair.0.len() + kv_pair.1.len()))
        .bench_values(|(kv_pair, mut store)| {
            store.set(kv_pair.0, kv_pair.1)
        });
}

#[divan::bench]
fn bench_random_reads(bencher: Bencher) {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = RustCask::builder().open(temp_dir.path()).unwrap();
    let mut rng: rand::prelude::ThreadRng = rand::thread_rng();
    let kv_pairs = KeyValuePair::random_many(&mut rng, COUNT_KV_PAIRS, KEY_SIZE, VAL_SIZE);
    for kv_pair in kv_pairs.clone() {
        store.set(kv_pair.0, kv_pair.1).unwrap();
    }

    bencher
        .with_inputs(move || {
            let store = store.clone();
            let mut rng = rand::thread_rng();
            let rand_index = rng.gen_range(0..kv_pairs.len());
            let rand_kv_pair = kv_pairs[rand_index].clone();
            (rand_kv_pair, store)
        })
        .input_counter(|(rand_kv_pair, _)| BytesCount::new(rand_kv_pair.0.len() + rand_kv_pair.1.len()))
        .bench_values(|(kv_pair, mut store)| {
            store.get(&kv_pair.0)
        });
}

#[divan::bench()]
fn bench_open_hint_files_disabled(bencher: Bencher) {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = RustCask::builder().open(temp_dir.path()).unwrap();
    let mut rng: rand::prelude::ThreadRng = rand::thread_rng();
    let kv_pairs = KeyValuePair::random_many(&mut rng, COUNT_KV_PAIRS, KEY_SIZE, VAL_SIZE);
    for kv_pair in kv_pairs.clone() {
        store.set(kv_pair.0, kv_pair.1).unwrap();
    }

    // Overwrite a number of keys.
    // This should make the benefits of hint files more obvious.
    let sample_kv_pairs: Vec<&KeyValuePair> = kv_pairs.choose_multiple(&mut rng, OVERWRITE_COUNT).collect();
    for kv_pair in sample_kv_pairs {
        let rand_value = (0..VAL_SIZE).map(|_| rng.gen::<u8>()).collect();
        store.set(kv_pair.0.clone(), rand_value).unwrap();
    }

    drop(store);

    bencher
        .bench_local( || {
            RustCask::builder().open(temp_dir.path()).unwrap();
        });
}