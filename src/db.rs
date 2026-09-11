//! The database: owns the WAl adn memtable, routes all writes WAL-first,
//! and rebuilds in-memory state by replaying the WAL on open.

use crate::error::Result;
use crate::memtable::MemTable;
use crate::wal::Wal;
use crate::record::{Kind, Record};
use crate::sstable::{self, SSTable};
use std::path::{Path, PathBuf};
use crate::manifest::{Manifest, SstEntry};
use std::fs;

// Flush when the memtable exceeds this many bytes.
const DEFAULT_FLUSH_THRESHOLD: usize = 1024; // Intentionally small for testing.

pub struct Db {
    dir: PathBuf,
    wal: Wal,
    memtable: MemTable,
    sstables: Vec<SSTable>,
    manifest: Manifest,
    next_seq: u64,
    next_sst_number: u64,
    flush_threshold: usize,
}

impl Db {
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Db> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut sst_numbers: Vec<u64> = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if let Some(num) = parse_sst_number(&entry.file_name().to_string_lossy()) {
                sst_numbers.push(num);
            }
        }
        sst_numbers.sort_unstable();

        let mut sstables =  Vec::new();
        let manifest = Manifest::load(manifest_path(&dir))?;
        let mut max_seq = 0u64;
        for num in &sst_numbers {
            let sst = SSTable::open(sst_path(&dir, *num))?;
            max_seq = max_seq.max(sst.max_seq());
            sstables.push(sst);
        }
        let next_sst_number = sst_numbers.last().map_or(1, |n| n + 1);

        let wal = Wal::open(wal_path(&dir))?;
        let mut memtable = MemTable::new();
        for record in wal.replay()? {
            max_seq = max_seq.max(record.seq);
            memtable.apply(record);
        }

        Ok(Db {
            dir,
            wal,
            memtable,
            sstables,
            manifest,
            next_seq: max_seq + 1,
            next_sst_number,
            flush_threshold: DEFAULT_FLUSH_THRESHOLD,
        })

    }

     fn flush(&mut self) -> Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }
    

        let num = self.next_sst_number;
        let path = sst_path(&self.dir, num);

        // SSTable Durable
        sstable::write_from_memtable(&path, &self.memtable)?;
        let sst = SSTable::open(&path)?;
        let max_seq = sst.max_seq();
        self.sstables.push(sst);
        self.next_sst_number += 1;

        // Recording in Manifest
        self.manifest.ssts.push(SstEntry { number: num, max_seq });
        self.manifest.next_sst = self.next_sst_number;
        self.manifest.next_seq = self.next_seq;
        self.manifest.save(manifest_path(&self.dir))?;

        self.wal.reset()?;
        self.memtable = MemTable::new();

        Ok(())
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
            self.write(Kind::Put, key, value.to_vec())
        }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write(Kind::Tombstone, key, Vec::new())
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some((kind, value)) = self.memtable.lookup(key) {
            return Ok(match kind {
                Kind::Put => Some(value),
                Kind::Tombstone => None,
            });
        }

        

        for sst in self.sstables.iter_mut().rev() {
            if let Some(record) = sst.get(key)? {
                return Ok(match record.kind {
                    Kind::Put => Some(record.value),
                    Kind::Tombstone => None,
                });
            }
        }
        Ok(None)
    }

    fn write(&mut self, kind: Kind, key: &[u8], value: Vec<u8>) -> Result<()> {
        let seq = self.next_seq;
        let record = Record { seq, kind, key: key.to_vec(), value };

        self.wal.append(&record)?;
        self.memtable.apply(record);
        self.next_seq += 1;

        if self.memtable.approx_size_bytes() >= self.flush_threshold {
            self.flush()?;
        }
        Ok(())
    }
}

fn wal_path(dir: &Path) -> PathBuf {
    dir.join("wal.log")
}

fn sst_path(dir: &Path, num: u64) -> PathBuf {
    dir.join(format!("{:06}.sst", num))
}

fn manifest_path(dir: &Path) -> PathBuf {
    dir.join("MANIFEST")
}

fn parse_sst_number(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(".sst")?;
    stem.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("storagekv_db_{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&p);
        p
    }

    #[test]
    fn put_get_delete() {
        let dir = temp_dir("put_get_delete");
        let mut db = Db::open(&dir).unwrap();
        db.put(b"name", b"alice").unwrap();
        assert_eq!(db.get(b"name").unwrap(), Some(b"alice".to_vec()));
        db.put(b"name", b"alicia").unwrap();
        assert_eq!(db.get(b"name").unwrap(), Some(b"alicia".to_vec()));
        db.delete(b"name").unwrap();
        assert_eq!(db.get(b"name").unwrap(), None);
        assert_eq!(db.get(b"never").unwrap(), None);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn survives_reopen_memtable_only() {
        let dir = temp_dir("reopen_mem");
        {
            let mut db = Db::open(&dir).unwrap();
            db.put(b"a", b"1").unwrap();
            db.put(b"a", b"11").unwrap();
            db.delete(b"b").unwrap();
        }
        let mut db = Db::open(&dir).unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"11".to_vec()));
        assert_eq!(db.get(b"b").unwrap(), None);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn flush_happens_and_data_is_readable_from_sstable() {
        let dir = temp_dir("flush");
        let mut db = Db::open(&dir).unwrap();
        db.flush_threshold = 256; // force early flushes

        // Write enough distinct keys to trigger at least one flush.
        for i in 0..100u64 {
            let k = format!("key{:04}", i);
            db.put(k.as_bytes(), format!("val{}", i).as_bytes()).unwrap();
        }
        // At least one SSTable should now exist on disk.
        let sst_count = fs::read_dir(&dir).unwrap()
            .filter_map(|e| parse_sst_number(&e.unwrap().file_name().to_string_lossy()))
            .count();
        assert!(sst_count >= 1, "expected at least one flushed SSTable");

        // Every key still reads correctly, whether it's in an SSTable or the memtable.
        for i in 0..100u64 {
            let k = format!("key{:04}", i);
            assert_eq!(db.get(k.as_bytes()).unwrap(), Some(format!("val{}", i).into_bytes()));
        }
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn survives_reopen_after_flush() {
        let dir = temp_dir("reopen_flush");
        {
            let mut db = Db::open(&dir).unwrap();
            db.flush_threshold = 256;
            for i in 0..100u64 {
                let k = format!("key{:04}", i);
                db.put(k.as_bytes(), format!("v{}", i).as_bytes()).unwrap();
            }
            db.delete(b"key0005").unwrap();
        } // drop == crash; data lives across SSTables + WAL

        let mut db = Db::open(&dir).unwrap();
        assert_eq!(db.get(b"key0000").unwrap(), Some(b"v0".to_vec()));
        assert_eq!(db.get(b"key0099").unwrap(), Some(b"v99".to_vec()));
        assert_eq!(db.get(b"key0005").unwrap(), None); // delete survived across layers
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn newer_sstable_shadows_older() {
        let dir = temp_dir("shadow");
        let mut db = Db::open(&dir).unwrap();
        db.flush_threshold = 1; // flush after essentially every write

        db.put(b"k", b"old").unwrap();  // -> SSTable 1
        db.put(b"k", b"new").unwrap();  // -> SSTable 2 (newer)
        assert_eq!(db.get(b"k").unwrap(), Some(b"new".to_vec())); // newest wins

        db.delete(b"k").unwrap();       // tombstone -> SSTable 3
        assert_eq!(db.get(b"k").unwrap(), None); // newest layer shadows both
        fs::remove_dir_all(&dir).unwrap();
    }
}