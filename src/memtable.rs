//! In-memory sorted view of the newest value for each key
//! Folds the write log down to "current state": newest seq wins, tombstone hide keys.
//! Single-write, single-version

use crate::record::{Kind, Record};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub seq: u64,
    pub kind: Kind,
    pub value: Vec<u8>,
}

#[derive(Debug, Default)]
pub struct MemTable {
    map: BTreeMap<Vec<u8>, Entry>,
    size_bytes: usize,
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable::default()
    }

    // Apply one record to the in-memory state.
    //A record is ignored if we already hold a newer version of that key.

    pub fn apply(&mut self, record: Record) {
        if let Some(existing) = self.map.get(&record.key) {
            if existing.seq >= record.seq {
                return;
            }
            self.size_bytes -= entry_size(&record.key, existing);
        }

        let entry = Entry {
            seq: record.seq,
            kind: record.kind,
            value: record.value,
        };
        self.size_bytes += entry_size(&record.key, &entry);
        self.map.insert(record.key, entry);
    }

    // Look up the current value of a key.
    // Some(value) -> key exists with this value
    // None -> key was never written or its newes version is a tombstone
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        match self.map.get(key) {
            Some(entry) if entry.kind == Kind::Put => Some(&entry.value),
            _ => None,
        }
    }

    pub fn approx_size_bytes(&self) -> usize {
        self.size_bytes
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    // Ordered iteration over the key-value pairs in the memtable.
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Entry)> {
        self.map.iter()
    }

    pub fn lookup(&self, key: &[u8]) -> Option<(Kind, Vec<u8>)> {
        self.map.get(key).map(|e| (e.kind.clone(), e.value.clone()))
    }
}

fn entry_size(key: &[u8], entry: &Entry) -> usize {
    key.len() + entry.value.len() + std::mem::size_of::<Entry>()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(seq: u64, key: &str, val: &str) -> Record {
        Record { seq, kind: Kind::Put, key: key.into(), value: val.into() }
    }
    fn del(seq: u64, key: &str) -> Record {
        Record { seq, kind: Kind::Tombstone, key: key.into(), value: Vec::new() }
    }

    #[test]
    fn put_then_get() {
        let mut m = MemTable::new();
        m.apply(put(1, "a", "alice"));
        assert_eq!(m.get(b"a"), Some(&b"alice"[..]));
        assert_eq!(m.get(b"missing"), None);
    }

    #[test]
    fn newer_write_wins() {
        let mut m = MemTable::new();
        m.apply(put(1, "a", "alice"));
        m.apply(put(2, "a", "alicia"));
        assert_eq!(m.get(b"a"), Some(&b"alicia"[..]));
    }

    #[test]
    fn tombstone_hides_key() {
        let mut m = MemTable::new();
        m.apply(put(1, "a", "alice"));
        m.apply(del(2, "a"));
        assert_eq!(m.get(b"a"), None); // deleted reads as absent
    }

    #[test]
    fn rewrite_after_delete() {
        let mut m = MemTable::new();
        m.apply(put(1, "a", "alice"));
        m.apply(del(2, "a"));
        m.apply(put(3, "a", "al"));
        assert_eq!(m.get(b"a"), Some(&b"al"[..]));
    }

    #[test]
    fn stale_write_is_ignored() {
        // Out-of-order apply: a lower-seq record must not clobber a higher-seq one.
        let mut m = MemTable::new();
        m.apply(put(5, "a", "new"));
        m.apply(put(2, "a", "old")); // arrives late, lower seq
        assert_eq!(m.get(b"a"), Some(&b"new"[..]));
    }

    #[test]
    fn iter_is_sorted() {
        let mut m = MemTable::new();
        m.apply(put(1, "banana", "1"));
        m.apply(put(2, "apple", "2"));
        m.apply(put(3, "cherry", "3"));
        let keys: Vec<Vec<u8>> = m.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b"apple".to_vec(), b"banana".to_vec(), b"cherry".to_vec()]);
    }
}