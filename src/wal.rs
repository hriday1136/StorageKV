//! Write-Ahead Log: an append-only, fsync-on-append file of encoded records.
//! Every mutation is durably recorded here BEFORE it is applied anywhere else,
//! so a crash can always be recovered by replaying this log.

use crate::error::Result;
use crate::record::Record;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};

pub struct Wal {
    file: File,
    path: PathBuf,
}

impl Wal {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Wal> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok(Wal { file, path })
    }

    // Append one record and make it durable before returning.
    pub fn append(&mut self, record: &Record) -> Result<()> {
        let bytes = record.encode();
        self.file.write_all(&bytes)?; // hand bytes to the OS
        self.file.sync_all()?; // Force them to the physical device (fsync)
        Ok(())
    }

    // Replay every valid record from the start of the log, in order.
    pub fn replay(&self) -> Result<Vec<Record>> {
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut records = Vec::new();
        while let Some(record) = Record::decode(&mut reader)? {
            records.push(record);
        }
        Ok(records)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Kind;
    use std::path::PathBuf;

    // Give each test its own file path so they don't collide when run in parallel.
    fn temp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("storagekv_wal_test_{name}_{}.log", std::process::id()));
        let _ = std::fs::remove_file(&p); // start clean
        p
    }

    fn rec(seq: u64, key: &str, val: &str) -> Record {
        Record { seq, kind: Kind::Put, key: key.into(), value: val.into() }
    }

    #[test]
    fn append_then_replay() {
        let path = temp_path("append_then_replay");
        let recs = vec![rec(1, "a", "1"), rec(2, "b", "2"), rec(3, "c", "3")];

        {
            let mut wal = Wal::open(&path).unwrap();
            for r in &recs {
                wal.append(r).unwrap();
            }
        } // wal dropped -> file closed

        let wal = Wal::open(&path).unwrap();
        assert_eq!(wal.replay().unwrap(), recs);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn reopen_and_append_is_cumulative() {
        let path = temp_path("reopen");
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&rec(1, "a", "1")).unwrap();
        }
        {
            let mut wal = Wal::open(&path).unwrap(); // reopen appends, not truncates
            wal.append(&rec(2, "b", "2")).unwrap();
        }
        let wal = Wal::open(&path).unwrap();
        assert_eq!(wal.replay().unwrap(), vec![rec(1, "a", "1"), rec(2, "b", "2")]);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn replay_stops_at_torn_tail() {
        let path = temp_path("torn");
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&rec(1, "a", "1")).unwrap();
            wal.append(&rec(2, "b", "2")).unwrap();
        }
        // Simulate a crash mid-write: chop the last few bytes off the file.
        let data = std::fs::read(&path).unwrap();
        std::fs::write(&path, &data[..data.len() - 3]).unwrap();

        let wal = Wal::open(&path).unwrap();
        // The first record survives; the torn second one is silently dropped.
        assert_eq!(wal.replay().unwrap(), vec![rec(1, "a", "1")]);
        std::fs::remove_file(&path).unwrap();
    }
}