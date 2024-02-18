use std::{collections::HashMap, hash::Hash};

/// An in-memory key-value store.
pub struct KvStore<K, V> {
    map: HashMap<K, V>,
}

impl<K, V> KvStore<K, V>
where
    K: Eq + PartialEq,
    K: Hash,
{
    pub fn new() -> Self {
        KvStore {
            map: HashMap::new(),
        }
    }

    /// Inserts a key-value pair into the map.
    pub fn set(&mut self, key: K, value: V) {
        self.map.insert(key, value);
    }

    /// Returns a reference to the value corresponding to the key.
    pub fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }

    /// Removes a key from the store, returning the value at the key
    /// if the key was previously in the map.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.map.remove(key)
    }
}

impl<K, V> Default for KvStore<K, V>
where
    K: Eq + PartialEq,
    K: Hash,
{
    fn default() -> Self {
        Self::new()
    }
}
