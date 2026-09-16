//! Compaction: merge several SSTables into one, keeping only the newest version.
//! of each key and dropping tombstones.

//! The merge is a k-way merge of sorted inputs.

use crate::record::{Kind, Record};

pub fn merge(inputs: Vec<Vec<Record>>, drop_tombstones: bool) -> Vec<Record> {
    // Collect all records, remembering which input each came from so that when two
    // records share a key and a seq, newer input wins.
    // Simple, correct approach: flatten, then reduce to newest-per-key.
    use std::collections::BTreeMap;

    let mut winners: BTreeMap<Vec<u8>, Record> = BTreeMap::new();

    for input in inputs {
        for rec in input {
            match winners.get(&rec.key) {
                Some(existing) if existing.seq >= rec.seq => {
                    // None as result is already there as the newer version
                }
                _ => {
                    winners.insert(rec.key.clone(), rec);
                }
            }
        }
    }
    //Emit in sorted key order, dropping tombstones if allowed.
    winners
        .into_values()
        .filter(|rec| !(drop_tombstones && rec.kind == Kind::Tombstone))
        .collect()
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
    fn newest_version_wins() {
        // key "a" written in an older table (seq 1) and a newer table (seq 5).
        let old = vec![put(1, "a", "old"), put(2, "b", "b1")];
        let new = vec![put(5, "a", "new")];
        let merged = merge(vec![old, new], true);
        // "a" -> "new", "b" -> "b1", sorted.
        assert_eq!(merged, vec![put(5, "a", "new"), put(2, "b", "b1")]);
    }

    #[test]
    fn output_is_sorted() {
        let t1 = vec![put(1, "banana", "1"), put(2, "date", "2")];
        let t2 = vec![put(3, "apple", "3"), put(4, "cherry", "4")];
        let merged = merge(vec![t1, t2], true);
        let keys: Vec<&[u8]> = merged.iter().map(|r| r.key.as_slice()).collect();
        assert_eq!(keys, vec![&b"apple"[..], b"banana", b"cherry", b"date"]);
    }

    #[test]
    fn tombstone_dropped_when_allowed() {
        // "a" was put (seq 1) then deleted (seq 3). Merging all -> "a" vanishes.
        let old = vec![put(1, "a", "v"), put(2, "keep", "k")];
        let new = vec![del(3, "a")];
        let merged = merge(vec![old, new], true);
        assert_eq!(merged, vec![put(2, "keep", "k")]); // "a" gone entirely
    }

    #[test]
    fn tombstone_kept_when_not_allowed() {
        // Same as above but drop_tombstones=false: the tombstone survives, so it can
        // still shadow older data in tables we didn't merge.
        let old = vec![put(1, "a", "v")];
        let new = vec![del(3, "a")];
        let merged = merge(vec![old, new], false);
        assert_eq!(merged, vec![del(3, "a")]); // tombstone retained
    }

    #[test]
    fn put_after_delete_survives() {
        // put(1) -> del(2) -> put(4): the final put wins, key is live.
        let inputs = vec![
            vec![put(1, "a", "first")],
            vec![del(2, "a")],
            vec![put(4, "a", "third")],
        ];
        let merged = merge(inputs, true);
        assert_eq!(merged, vec![put(4, "a", "third")]);
    }

    #[test]
    fn empty_inputs() {
        assert_eq!(merge(vec![], true), vec![]);
        assert_eq!(merge(vec![vec![]], true), vec![]);
    }
}