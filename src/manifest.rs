//! The manifest: authoritative record of database state.
//! Lists the live SSTables by number, with each one's max sequence number
//! Written atomically

use crate::error::{Error, Result};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstEntry {
    pub number: u64,
    pub max_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Manifest {
    pub next_seq: u64,
    pub next_sst: u64,
    pub ssts: Vec<SstEntry>
}

impl Manifest {
    pub fn new() -> Manifest {
        Manifest { next_seq: 1, next_sst: 1, ssts: Vec::new() }
    }

    // Serialization to text format
    fn encode(&self) -> String {
        let mut s = String::new();
        s.push_str(&format!("next_seq {}\n", self.next_seq));
        s.push_str(&format!("next_sst {}\n", self.next_sst));
        for e in &self.ssts {
            s.push_str(&format!("sst {} {}\n", e.number, e.max_seq));
        }
        s
    }

    // Parse text format
    fn decode(text: &str) -> Result<Manifest> {
        let mut next_seq: Option<u64> = None;
        let mut next_sst: Option<u64> = None;
        let mut ssts = Vec::new();

        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            match parts.as_slice() {
                ["next_seq", v] => next_seq = Some(parse_u64(v)?),
                ["next_sst", v] => next_sst = Some(parse_u64(v)?),
                ["sst", num, seq] => {
                    ssts.push(SstEntry { number: parse_u64(num)?, max_seq: parse_u64(seq)? });
                }
                _ => return Err(Error::Corruption(format!("bad manifest line: {line}"))),
            }
        }

        Ok(Manifest {
            next_seq: next_seq.ok_or_else(|| Error::Corruption("manifest missing next_seq".into()))?,
            next_sst: next_sst.ok_or_else(|| Error::Corruption("manifest missing next_sst".into()))?,
            ssts,
        })
    }

    /// Load the manifest at a specific path
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Manifest> {
        match std::fs::read_to_string(path.as_ref()) {
            Ok(text) => Manifest::decode(&text),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Manifest::new()),
            Err(e) => Err(Error::Io(e)),
        }
    }

    // Persist automatically
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();
        let tmp = tmp_path_for(path);

        let mut file = File::create(&tmp)?;
        file.write_all(self.encode().as_bytes())?;
        file.sync_all()?;
        drop(file);

        std::fs::rename(&tmp, path)?;
        fsync_dir(path)?;
        Ok(())
    }
}

fn parse_u64(s: &str) -> Result<u64> {
    s.parse::<u64>().map_err(|_| Error::Corruption(format!("bad number in manifest: {s}")))
}

fn tmp_path_for(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    PathBuf::from(s)
}

fn fsync_dir(path: &Path) -> Result<()> {
    let dir = match path.parent().filter(|p| !p.as_os_str().is_empty()) {
        Some(d) => d.to_path_buf(),
        None => PathBuf::from("."),
    };
    File::open(&dir)?.sync_all()?;
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("storagekv_manifest_{name}_{}.manifest", std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn new_is_empty() {
        let m = Manifest::new();
        assert_eq!(m.next_seq, 1);
        assert_eq!(m.next_sst, 1);
        assert!(m.ssts.is_empty());
    }

    #[test]
    fn encode_decode_round_trip() {
        let m = Manifest {
            next_seq: 42,
            next_sst: 7,
            ssts: vec![
                SstEntry { number: 1, max_seq: 15 },
                SstEntry { number: 2, max_seq: 31 },
                SstEntry { number: 5, max_seq: 40 },
            ],
        };
        let decoded = Manifest::decode(&m.encode()).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn save_then_load() {
        let path = temp_path("save_load");
        let m = Manifest {
            next_seq: 100,
            next_sst: 9,
            ssts: vec![SstEntry { number: 3, max_seq: 99 }],
        };
        m.save(&path).unwrap();
        assert_eq!(Manifest::load(&path).unwrap(), m);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn load_missing_is_fresh() {
        let path = temp_path("missing");
        // File does not exist -> a brand-new empty database.
        assert_eq!(Manifest::load(&path).unwrap(), Manifest::new());
    }

    #[test]
    fn save_overwrites_atomically() {
        let path = temp_path("overwrite");
        Manifest { next_seq: 1, next_sst: 1, ssts: vec![] }.save(&path).unwrap();

        let updated = Manifest {
            next_seq: 50,
            next_sst: 4,
            ssts: vec![SstEntry { number: 1, max_seq: 49 }],
        };
        updated.save(&path).unwrap();

        assert_eq!(Manifest::load(&path).unwrap(), updated); // second save won
        assert!(!tmp_path_for(&path).exists());              // no temp left behind
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn malformed_line_is_corruption() {
        assert!(Manifest::decode("next_seq 1\nnext_sst 1\ngarbage line here\n").is_err());
        assert!(Manifest::decode("next_sst 1\n").is_err()); // missing next_seq
    }
}