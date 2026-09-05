//! Immutable, sorted on-disk table
//! Layout: [data: sorted Record::encode() ...][sparse index][footer]
//! Footer (fixed 28 bytes at EOF): [index_offset u64][index_len u64][count u64][magic u32]

use crate::error::{Error, Result};
use crate::memtable::MemTable;
use crate::record::Record;
use std::fs::File;
use std::io::{BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;

const MAGIC: u32 = 0x5353_5442; // SSTB
const FOOTER_LEN: usize = 28; // 8+8+8+4
const INDEX_INTERVAL: usize = 16; // sparse index interval, index every 16th record

// Flush a memtable to an SSTbale file at 'path', in sorted key order
pub fn write_from_memtable<P: AsRef<Path>>(path: P, memtable: &MemTable) -> Result<()> {
    let path = path.as_ref();
    let tmp_path = tmp_path_for(path);

    let file = File::create(&tmp_path)?;
    let mut w = BufWriter::new(file);

    let mut index: Vec<(Vec<u8>, u64)> = Vec::new();
    let mut offset: u64 = 0;
    let mut count: u64 = 0;

    for(i, (key, entry)) in memtable.iter().enumerate()  {
        let record = Record {
            seq: entry.seq,
            kind: entry.kind.clone(),
            key: key.clone(),
            value: entry.value.clone(),
        };
        let bytes = record.encode();
        if i % INDEX_INTERVAL == 0 {
            index.push((key.clone(), offset));
        }
        w.write_all(&bytes)?;
        offset += bytes.len() as u64;
        count += 1;
    }
    let index_offset = offset;

    let mut index_bytes = Vec::new();
    for (key, off) in &index {
        index_bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
        index_bytes.extend_from_slice(key);
        index_bytes.extend_from_slice(&off.to_le_bytes());
    }
    w.write_all(&index_bytes)?;
    let index_len = index_bytes.len() as u64;

    let mut footer = Vec::with_capacity(FOOTER_LEN);
    footer.extend_from_slice(&index_offset.to_le_bytes());
    footer.extend_from_slice(&index_len.to_le_bytes());
    footer.extend_from_slice(&count.to_le_bytes());
    footer.extend_from_slice(&MAGIC.to_le_bytes());
    w.write_all(&footer)?;

    let file = w.into_inner().map_err(|e| Error::Io(e.into_error()))?;
    file.sync_all()?;
    drop(file);

    std::fs::rename(&tmp_path, path)?;

    fsync_dir(path)?;
    Ok(())
}

/// Temp path for atomic write: "foo.sst" -> "foo.sst.tmp"
fn tmp_path_for(path: &Path) -> std::path::PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    std::path::PathBuf::from(s)
}

/// fsync directory; turning a rename into a durable operation.
fn fsync_dir(path: &Path) -> Result<()> {
    let dir = path.parent().filter(|p| !p.as_os_str().is_empty());
    let dir = match dir {
        Some(d) => d.to_path_buf(),
        None => std::path::PathBuf::from("."), // path had no parent -> current dir
    };
    let dir_file = File::open(&dir)?;
    dir_file.sync_all()?;
    Ok(())
}

pub struct SSTable {
    file: File,
    index: Vec<(Vec<u8>, u64)>, // sorted by key: (indexed key, data offset)
    index_offset: u64,
}

impl SSTable {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<SSTable> {
        let mut file = File::open(&path)?;
        let file_len = file.seek(SeekFrom::End(0))?;
        if file_len < FOOTER_LEN as u64 {
            return Err(Error::Corruption("sstable smaller than footer".into()));
        }

        // Read the footer from the end.
        file.seek(SeekFrom::End(-(FOOTER_LEN as i64)))?;
        let mut footer = [0u8; FOOTER_LEN];
        file.read_exact(&mut footer)?;
        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let index_len = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let _count = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        let magic = u32::from_le_bytes(footer[24..28].try_into().unwrap());
        if magic != MAGIC {
            return Err(Error::Corruption("bad sstable magic".into()));
        }

        // Read and parse the sparse index
        file.seek(SeekFrom::Start(index_offset))?;
        let mut index_bytes = vec![0u8; index_len as usize];
        file.read_exact(&mut index_bytes)?;

        let mut index = Vec::new();
        let mut cur = Cursor::new(index_bytes);
        loop {
            let mut klen_buf = [0u8; 4];
            if cur.read_exact(&mut klen_buf).is_err() {
                break;
            }
            let klen = u32::from_le_bytes(klen_buf) as usize;
            let mut key = vec![0u8; klen];
            cur.read_exact(&mut key)?;
            let mut off_buf = [0u8; 8];
            cur.read_exact(&mut off_buf)?;
            index.push((key, u64::from_le_bytes(off_buf)));
        }

        Ok(SSTable { file, index, index_offset })
    }

    /// Look up a key in the SSTable
    /// Ok(Some(record)) -> present
    /// Ok(None) -> table does not contain the key
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Record>> {
        if self.index.is_empty() || key < self.index[0].0.as_slice() {
            return Ok(None);
        }

        // Binary Search
        let mut lo = 0usize;
        let mut hi = self.index.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.index[mid].0.as_slice() <= key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let block_idx = lo - 1;
        let scan_start = self.index[block_idx].1;
        let scan_end = if block_idx + 1 < self.index.len() {
            self.index[block_idx + 1].1
        } else {
            self.index_offset
        };

        // Read that specific block and scan
        self.file.seek(SeekFrom::Start(scan_start))?;
        let mut block = vec![0u8; (scan_end - scan_start) as usize];
        self.file.read_exact(&mut block)?;

        let mut cur = Cursor::new(block);
        while let Some(record) = Record::decode(&mut cur)? {
            if record.key.as_slice() == key {
                return Ok(Some(record));
            }
            if record.key.as_slice() > key {
                return Ok(None); // sorted
            }
        }
        Ok(None)
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{Kind, Record};
    use std::path::PathBuf;

    fn temp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("storagekv_sst_test_{name}_{}.sst", std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    fn memtable_with(pairs: &[(&str, Option<&str>)]) -> MemTable {
        // Some(v) => put, None => tombstone
        let mut m = MemTable::new();
        let mut seq = 1u64;
        for (k, v) in pairs {
            let rec = match v {
                Some(val) => Record { seq, kind: Kind::Put, key: k.as_bytes().to_vec(), value: val.as_bytes().to_vec() },
                None => Record { seq, kind: Kind::Tombstone, key: k.as_bytes().to_vec(), value: Vec::new() },
            };
            m.apply(rec);
            seq += 1;
        }
        m
    }

    #[test]
    fn write_read_basic() {
        let path = temp_path("basic");
        let m = memtable_with(&[("apple", Some("red")), ("banana", Some("yellow")), ("cherry", Some("dark"))]);
        write_from_memtable(&path, &m).unwrap();

        let mut sst = SSTable::open(&path).unwrap();
        assert_eq!(sst.get(b"apple").unwrap().unwrap().value, b"red");
        assert_eq!(sst.get(b"banana").unwrap().unwrap().value, b"yellow");
        assert_eq!(sst.get(b"cherry").unwrap().unwrap().value, b"dark");
        assert!(sst.get(b"missing").unwrap().is_none());
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn tombstone_is_stored() {
        let path = temp_path("tomb");
        let m = memtable_with(&[("a", Some("1")), ("b", None), ("c", Some("3"))]);
        write_from_memtable(&path, &m).unwrap();

        let mut sst = SSTable::open(&path).unwrap();
        let b = sst.get(b"b").unwrap().unwrap();
        assert_eq!(b.kind, Kind::Tombstone); // 'b' is present, as a tombstone
        assert!(sst.get(b"zzz").unwrap().is_none()); // genuinely absent
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn many_keys_exercise_sparse_index() {
        let path = temp_path("many");
        // 50 keys, zero-padded so lexical order matches numeric order.
        let mut m = MemTable::new();
        for i in 0..50u64 {
            m.apply(Record {
                seq: i + 1,
                kind: Kind::Put,
                key: format!("key{:04}", i).into_bytes(),
                value: format!("val{}", i).into_bytes(),
            });
        }
        write_from_memtable(&path, &m).unwrap();

        let mut sst = SSTable::open(&path).unwrap();
        // Spread across block boundaries (index entries land at 0,16,32,48).
        for i in [0u64, 1, 15, 16, 17, 31, 32, 48, 49] {
            let k = format!("key{:04}", i);
            assert_eq!(sst.get(k.as_bytes()).unwrap().unwrap().value, format!("val{}", i).into_bytes());
        }
        assert!(sst.get(b"key9999").unwrap().is_none()); // after last
        assert!(sst.get(b"aaaa").unwrap().is_none());     // before first
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn install_is_atomic_no_partial_file() {
        let path = temp_path("atomic");
        let m = memtable_with(&[("a", Some("1")), ("b", Some("2"))]);
        write_from_memtable(&path, &m).unwrap();

        // The real file exists and is fully readable...
        let mut sst = SSTable::open(&path).unwrap();
        assert_eq!(sst.get(b"a").unwrap().unwrap().value, b"1");

        // ...and no temp file is left behind after a successful flush.
        let tmp = super::tmp_path_for(&path);
        assert!(!tmp.exists(), "temp file should have been renamed away");

        std::fs::remove_file(&path).unwrap();
    }
}