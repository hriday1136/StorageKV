//! The database: owns the WAl adn memtable, routes all writes WAL-first,
//! and rebuilds in-memory state by replaying the WAL on open.

use crate::error::Result;
use crate::memtable::MemTable;
use crate::wal::Wal;
use crate::record::{Kind, Record};
use std::path::Path;

pub struct Db {
    wal: Wal,
    memtable: MemTable,
    next_seq: u64,
}

impl Db {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Db> {
        let wal = Wal::open(&path)?;

        let mut memtable = MemTable::new();
        let mut max_seq = 0u64;
        for record in wal.replay()?{
            max_seq = max_seq.max(record.seq);
            memtable.apply(record);
        }

        Ok(Db {
            wal,
            memtable,
            next_seq: max_seq + 1
        })
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write(Kind::Put, key, value.to_vec())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write(Kind::Tombstone, key, Vec::new())
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.memtable.get(key)
    }

    pub fn write(&mut self, kind: Kind, key: &[u8], value: Vec<u8>) -> Result<()> {
        let seq = self.next_seq;
        let record = Record { seq, kind, key: key.to_vec(), value };

        self.wal.append(&record)?;
        self.memtable.apply(record);
        self.next_seq += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("storagekv_db_test_{name}_{}.log", std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn put_get_delete() {
        let path = temp_path("put_get_delete");
        let mut db = Db::open(&path).unwrap();

        db.put(b"name", b"alice").unwrap();
        assert_eq!(db.get(b"name"), Some(&b"alice"[..]));

        db.put(b"name", b"alicia").unwrap(); // overwrite
        assert_eq!(db.get(b"name"), Some(&b"alicia"[..]));

        db.delete(b"name").unwrap();
        assert_eq!(db.get(b"name"), None); // gone

        assert_eq!(db.get(b"never"), None);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn survives_reopen() {
        let path = temp_path("survives_reopen");

        {
            let mut db = Db::open(&path).unwrap();
            db.put(b"a", b"1").unwrap();
            db.put(b"b", b"2").unwrap();
            db.put(b"a", b"11").unwrap(); // overwrite before "crash"
            db.delete(b"b").unwrap();
        } // db dropped == process died. Nothing flushed anywhere but the WAL.

        // Reopen: state must be reconstructed purely from the durable log.
        let db = Db::open(&path).unwrap();
        assert_eq!(db.get(b"a"), Some(&b"11"[..])); // newest value survived
        assert_eq!(db.get(b"b"), None); // deletion survived
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn seq_continues_after_reopen() {
        let path = temp_path("seq_continues");
        {
            let mut db = Db::open(&path).unwrap();
            db.put(b"a", b"1").unwrap(); // seq 1
            db.put(b"a", b"2").unwrap(); // seq 2
        }
        // After reopen, a new write must get seq 3 — not restart at 1 and get
        // ignored by the memtable's seq guard.
        let mut db = Db::open(&path).unwrap();
        db.put(b"a", b"3").unwrap();
        assert_eq!(db.get(b"a"), Some(&b"3"[..])); // proves the new write won
        std::fs::remove_file(&path).unwrap();
    }
}