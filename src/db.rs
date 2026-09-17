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
const COMPACTION_TRIGGER: usize = 4;

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

        if self.sstables.len() > COMPACTION_TRIGGER {
            self.compact()?;
        }

        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        if self.sstables.len() < 2 {
            return Ok(()); // Nothing to merge
        }

        let mut inputs: Vec<Vec<Record>> = Vec::new();
        for sst in self.sstables.iter_mut() {
            inputs.push(sst.records()?);
        }

        let merged = crate::compaction::merge(inputs, true);

        let old_numbers: Vec<u64> = self.manifest.ssts.iter().map(|e| e.number).collect();
        let new_num = self.next_sst_number;
        let new_path = sst_path(&self.dir, new_num);
        sstable::write_records(&new_path, &merged)?;
        let new_sst = SSTable::open(&new_path)?;
        let new_max_seq = new_sst.max_seq();
        self.next_sst_number += 1;

        self.manifest.ssts = vec![SstEntry { number: new_num, max_seq: new_max_seq, }];
        self.manifest.next_sst = self.next_sst_number;
        self.manifest.next_seq = self.next_seq;
        self.manifest.save(manifest_path(&self.dir))?;

        self.sstables = vec![new_sst];

        for num in old_numbers {
            let _ = std::fs::remove_file(sst_path(&self.dir, num));
        }

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

        /// Test/tuning hook: set the memtable flush threshold in bytes.
    /// Exposed so tests can force frequent flushes and compaction.
    pub fn set_flush_threshold_for_test(&mut self, bytes: usize) {
        self.flush_threshold = bytes;
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
        // Plant a stray SSTable at a number the manifest will never reference.
        // Contents don't matter: recovery must ignore it because it's not in the manifest.
        let stray = sst_path(&dir, 999999);
        std::fs::write(&stray, b"not a real sstable").unwrap();

        let db = Db::open(&dir).unwrap();
        assert!(!db.sstables_contains_number(999999), "stray SSTable must not be loaded");
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
        // Plant crash litter at numbers/paths the manifest never references.
        let stray_sst = sst_path(&dir, 999999);
        std::fs::write(&stray_sst, b"garbage").unwrap();
        let stray_tmp = dir.join("000123.sst.tmp");
        std::fs::write(&stray_tmp, b"garbage").unwrap();

        let _db = Db::open(&dir).unwrap();
        assert!(!stray_sst.exists(), "orphan .sst should be deleted");
        assert!(!stray_tmp.exists(), "orphan .tmp should be deleted");

        // A manifest-listed SSTable must survive — check whichever number the manifest holds now.
        let live = _db.manifest.ssts.first().map(|e| e.number);
        if let Some(num) = live {
            assert!(sst_path(&dir, num).exists(), "live SSTable must not be deleted");
        }
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn compaction_reduces_sstable_count_and_preserves_data() {
        let dir = temp_dir("compact");
        let mut db = Db::open(&dir).unwrap();
        db.flush_threshold = 128; // many small flushes -> triggers compaction

        // Write enough to flush several times and cross the compaction trigger.
        for i in 0..300u64 {
            db.put(format!("key{:04}", i).as_bytes(), b"v1").unwrap();
        }
        // Overwrite a subset (creates shadowed old versions to be reclaimed).
        for i in 0..50u64 {
            db.put(format!("key{:04}", i).as_bytes(), b"v2").unwrap();
        }
        // Delete a subset (creates tombstones to be dropped).
        for i in 50..80u64 {
            db.delete(format!("key{:04}", i).as_bytes()).unwrap();
        }
        db.compact().unwrap(); // force a final compaction

        // After compacting all tables, there should be exactly one.
        assert_eq!(db.sstables.len(), 1, "all tables should merge into one");

        // Correctness preserved across the merge:
        assert_eq!(db.get(b"key0000").unwrap(), Some(b"v2".to_vec())); // overwritten
        assert_eq!(db.get(b"key0100").unwrap(), Some(b"v1".to_vec())); // untouched
        assert_eq!(db.get(b"key0060").unwrap(), None);                 // deleted, tombstone dropped
        assert_eq!(db.get(b"key0299").unwrap(), Some(b"v1".to_vec())); // last key
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn crash_after_new_table_before_manifest_swap_recovers() {
        // Window (a): the merged table exists on disk, but the manifest was never
        // updated to reference it (crash before manifest.save). Recovery must fall
        // back to the OLD tables the manifest still names, lose nothing, and treat
        // the new table as orphan litter.
        let dir = temp_dir("crash_pre_manifest");
        {
            let mut db = Db::open(&dir).unwrap();
            db.flush_threshold = 128;
            for i in 0..200u64 {
                db.put(format!("key{:04}", i).as_bytes(), b"v1").unwrap();
            }
            // Do NOT compact. Manifest currently names several small tables.
        }

        // Simulate the crash-window litter: a plausible new merged table at the next
        // number, which the manifest does not reference.
        let orphan_new = sst_path(&dir, 999999);
        std::fs::write(&orphan_new, b"partial merged table (not in manifest)").unwrap();

        // Recover: must ignore the orphan and read all data from the real tables.
        let mut db = Db::open(&dir).unwrap();
        for i in 0..200u64 {
            assert_eq!(db.get(format!("key{:04}", i).as_bytes()).unwrap(), Some(b"v1".to_vec()));
        }
        assert!(!orphan_new.exists(), "orphan merged table should be cleaned up");
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn crash_after_manifest_swap_before_old_delete_recovers() {
        // Window (b): compaction completed the manifest swap (new table is now
        // authoritative) but crashed before deleting the old files. Recovery must
        // use the new table, read everything, and clean up the leftover old files.
        let dir = temp_dir("crash_post_manifest");
        let mut db = Db::open(&dir).unwrap();
        db.flush_threshold = 128;
        for i in 0..200u64 {
            db.put(format!("key{:04}", i).as_bytes(), b"v1").unwrap();
        }
        db.compact().unwrap(); // completes fully: new table + manifest + delete old

        // Simulate a leftover old file that a crash would have prevented deleting:
        // plant a stray .sst not referenced by the (post-compaction) manifest.
        let leftover_old = sst_path(&dir, 999998);
        std::fs::write(&leftover_old, b"old table that should have been deleted").unwrap();
        drop(db); // "crash"

        let mut db = Db::open(&dir).unwrap();
        for i in 0..200u64 {
            assert_eq!(db.get(format!("key{:04}", i).as_bytes()).unwrap(), Some(b"v1".to_vec()));
        }
        assert!(!leftover_old.exists(), "leftover old table should be cleaned up");
        assert_eq!(db.sstables.len(), 1, "exactly the merged table remains");
        fs::remove_dir_all(&dir).unwrap();
    }
}