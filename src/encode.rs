//! Hand-rolled IEEE CRC-32 (reflected, polynomial 0xEDB88320).
//! Zero dependencies. Standard check value: crc32(b"123456789") == 0xCBF43926.

use std::sync::OnceLock;

// The lookup table is expesive-ish to build (256 * 8 iterations), so build it first
// once on first use and reuse it for every checksum after.
static CRC_TABLE: OnceLock<[u32; 256]> = OnceLock::new();

fn make_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

pub fn crc32(bytes: &[u8]) -> u32 {
    let table = CRC_TABLE.get_or_init(make_table);
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in bytes {
        let idx = ((crc ^ b as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ table[idx];
    }
    crc ^ 0xFFFF_FFFF // standard final XOR
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_known_answer() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn crc32_empty() {
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn crc32_detect_single_bit_flip() {
        let a = crc32(b"hello world");
        let b = crc32(b"hallo world"); // single bit flip in the first character
        assert_ne!(a, b);
    }
}