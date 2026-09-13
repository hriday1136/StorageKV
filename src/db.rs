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

    /// Open a database directory, recovering to the exact committed pre-crash state.
    ///
    /// Recovery procedure (order matters):
    ///   1. Load the MANIFEST — the authoritative set of live SSTables and the
    ///      sequence/SSTable counters as of the last flush. A missing manifest
    ///      means a brand-new database.
    ///   2. Open exactly the SSTables the manifest names (oldest -> newest).
    ///      Files on disk that the manifest does not list are NOT trusted.
    ///   3. Replay the WAL on top. Its records are newer than any SSTable and
    ///      advance the sequence counter past all recovered data.
    ///   4. Delete crash litter (orphan .tmp files, .sst files not in the manifest)
    ///      — only now, once the live set is known.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Db> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let manifest = Manifest::load(manifest_path(&dir))?;

        let mut sstables = Vec::new();
        for entry in &manifest.ssts {
            let sst = SSTable::open(sst_path(&dir, entry.number))?;
            sstables.push(sst);
        }

        let mut next_seq = manifest.next_seq;
        let next_sst_number = manifest.next_sst;

        let wal = Wal::open(wal_path(&dir))?;
        let mut memtable = MemTable::new();
        for record in wal.replay()? {
            next_seq = next_seq.max(record.seq + 1);
            memtable.apply(record);
        }

        cleanup_orphans(&dir, &manifest.ssts)?;

        Ok(Db {
            dir,
            wal,
            memtable,
            sstables,
            next_seq,
            next_sst_number,
            manifest,
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

    #[cfg(test)]
    fn sstables_contains_number(&self, num: u64) -> bool {
        self.manifest.ssts.iter().any(|e| e.number == num)
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

/// Remove crash litter
fn cleanup_orphans(dir: &Path, live: &[SstEntry]) -> Result<()> {
    let live_numbers: std::collections::HashSet<u64> = live.iter().map(|e| e.number).collect();

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();

        let is_orphan = if name.ends_with(".tmp") {
            true // any temp file is leftover from an interrupted atomic write
        } else if let Some(num) = parse_sst_number(&name) {
            !live_numbers.contains(&num) // an .sst not in the manifest
        } else {
            false
        };

        if is_orphan {
            let _ = fs::remove_file(entry.path());
        }
    }
    Ok(())
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

    #[test]
    fn stray_sstable_not_in_manifest_is_ignored() {
        let dir = temp_dir("stray");
        {
            let mut db = Db::open(&dir).unwrap();
            db.flush_threshold = 256;
            for i in 0..50u64 {
                db.put(format!("key{:04}", i).as_bytes(), b"v").unwrap();
            }
        }
        // Simulate crash litter: a plausible-looking SSTable the manifest never recorded.
        // (Copy an existing real one to a high, unreferenced number.)
        let real = sst_path(&dir, 1);
        let stray = sst_path(&dir, 9999);
        std::fs::copy(&real, &stray).unwrap();

        // Recovery must ignore the stray file entirely — it isn't in the manifest.
        let db = Db::open(&dir).unwrap();
        assert!(!db.sstables_contains_number(9999), "stray SSTable must not be loaded");
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn recovery_deletes_orphan_files() {
        let dir = temp_dir("cleanup");
        {
            let mut db = Db::open(&dir).unwrap();
            db.flush_threshold = 256;
            for i in 0..50u64 {
                db.put(format!("key{:04}", i).as_bytes(), b"v").unwrap();
            }
        }
        // Plant crash litter: a stray .sst not in the manifest, and a .tmp file.
        let stray_sst = sst_path(&dir, 9999);
        std::fs::copy(sst_path(&dir, 1), &stray_sst).unwrap();
        let stray_tmp = dir.join("000001.sst.tmp");
        std::fs::write(&stray_tmp, b"garbage").unwrap();

        // Reopen: recovery should delete both.
        let _db = Db::open(&dir).unwrap();
        assert!(!stray_sst.exists(), "orphan .sst should be deleted");
        assert!(!stray_tmp.exists(), "orphan .tmp should be deleted");

        // And a real, manifest-listed SSTable must survive.
        assert!(sst_path(&dir, 1).exists(), "live SSTable must not be deleted");
        fs::remove_dir_all(&dir).unwrap();
    }
}