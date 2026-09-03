use crate::encode::crc32;
use crate::error::{Error, Result};
use std::io::Read;

// a corrupted length field is not covered by the payload crc, so a garbage value
// could cause issues if not properly validated.
const MAX_RECORD_LEN: usize = 64*1024*1024; // 64 MiB

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    Put,
    Tombstone,
}

impl Kind {
    fn to_byte(&self) -> u8 {
        match self {
            Kind::Put => 0,
            Kind::Tombstone => 1,
        }
    }

    fn from_byte(b: u8) -> Result<Kind> {
        match b {
            0 => Ok(Kind::Put),
            1 => Ok(Kind::Tombstone),
            other => Err(Error::Corruption(format!("invalid record kind: {other}"))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub seq: u64,
    pub kind: Kind,
    pub key: Vec<u8>,
    pub value: Vec<u8>, //empty for tombstone records
}

impl Record {
    // Serialize into a self-contained, checksummed byte buffer
    pub fn encode(&self) -> Vec<u8> {
        // Build Payload
        let mut payload = Vec::new();
        payload.extend_from_slice(&self.seq.to_le_bytes());
        payload.push(self.kind.to_byte());
        payload.extend_from_slice(&(self.key.len() as u32).to_le_bytes());
        payload.extend_from_slice(&self.key);
        payload.extend_from_slice(&(self.value.len() as u32).to_le_bytes());
        payload.extend_from_slice(&self.value);

        // Checksum the payload
        let crc = crc32(&payload);
        let total_len = payload.len() as u32;

        let mut buf = Vec::with_capacity(8+payload.len());
        buf.extend_from_slice(&crc.to_le_bytes());
        buf.extend_from_slice(&total_len.to_le_bytes());
        buf.extend_from_slice(&payload);
        buf
    }

    // Read one record from `r`
    // OK(Some(rec)) -> a valid record
    // Ok(None) -> clean EOF, OR a torn trailing record
    //Err(Corruption) -> a fully-read record whose checkdum doesn't match
    pub fn decode<R: Read>(r: &mut R) -> Result<Option<Record>> {
        // --- header: [crc u32][total_len u32] ---
        let mut header =[0u8; 8];
        if !read_exact_or_none(r, &mut header)? {
            return Ok(None);
        }
        let crc_expected = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let total_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;

        if total_len > MAX_RECORD_LEN {
            return Ok(None);
        }

        //--- payload ---
        let mut payload = vec![0u8; total_len];
        if !read_exact_or_none(r, &mut payload)? {
            return Ok(None); // torn payload
        }

        // Verify checksum
        let crc_actual = crc32(&payload);
        if crc_actual != crc_expected {
            return Err(Error::Corruption(format!("crc mismatch: expected {crc_expected:#010x}, got {crc_actual:#010x}")));
        }

        // CRC passed -> the payload is byte-identical to what encode() wrote
        // so the fixed-offset reads are guaranteed in-bounds.
        let mut off = 0usize;
        let seq = u64::from_le_bytes(payload[off..off+8].try_into().unwrap());
        off += 8;
        let kind = Kind::from_byte(payload[off])?;
        off += 1;
        let key_len = u32::from_le_bytes(payload[off..off+4].try_into().unwrap()) as usize;
        off += 4;
        let key = payload[off..off + key_len].to_vec();
        off += key_len;
        let value_len = u32::from_le_bytes(payload[off..off+4].try_into().unwrap()) as usize;
        off += 4;
        let value = payload[off..off + value_len].to_vec();

        Ok(Some(Record { seq, kind, key, value }))
    }
}

fn read_exact_or_none<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<bool> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = r.read(&mut buf[filled..])?;
        if n == 0 {
            return Ok(false);
        }
        filled += n;
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn put_round_trip() {
        let rec = Record { seq: 42, kind: Kind::Put, key: b"hello".to_vec(), value: b"world".to_vec() };
        let mut cur = Cursor::new(rec.encode());
        assert_eq!(Record::decode(&mut cur).unwrap().unwrap(), rec);
    }

    #[test]
    fn tombstone_round_trip() {
        let rec = Record { seq: 7, kind: Kind::Tombstone, key: b"gone".to_vec(), value: Vec::new() };
        let mut cur = Cursor::new(rec.encode());
        assert_eq!(Record::decode(&mut cur).unwrap().unwrap(), rec);
    }

    #[test]
    fn multiple_records_stream() {
        let recs = vec![
            Record { seq: 1, kind: Kind::Put, key: b"a".to_vec(), value: b"1".to_vec() },
            Record { seq: 2, kind: Kind::Put, key: b"b".to_vec(), value: b"22".to_vec() },
            Record { seq: 3, kind: Kind::Tombstone, key: b"a".to_vec(), value: Vec::new() },
        ];
        let mut buf = Vec::new();
        for r in &recs {
            buf.extend_from_slice(&r.encode());
        }
        let mut cur = Cursor::new(buf);
        let mut out = Vec::new();
        while let Some(r) = Record::decode(&mut cur).unwrap() {
            out.push(r);
        }
        assert_eq!(out, recs);
    }

    #[test]
    fn torn_trailing_record_is_ignored() {
        let rec = Record { seq: 1, kind: Kind::Put, key: b"k".to_vec(), value: b"v".to_vec() };
        let mut buf = rec.encode();
        let n = buf.len();
        buf.truncate(n - 3); // Simulate a crash mid-write
        let mut cur = Cursor::new(buf);
        assert_eq!(Record::decode(&mut cur).unwrap(), None);
    }

    #[test]
    fn corrupt_payload_is_detected() {
        let rec = Record { seq: 1, kind: Kind::Put, key: b"k".to_vec(), value: b"v".to_vec() };
        let mut buf = rec.encode();
        buf[8] ^= 0xFF; // flip a bit in the payload (byte 8 = first payload byte)
        let mut cur = Cursor::new(buf);
        assert!(matches!(Record::decode(&mut cur), Err(Error::Corruption(_))));
    }
}