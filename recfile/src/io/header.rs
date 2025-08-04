use crate::util::{aligned_alloc, get_page_size};

pub trait TRffHeader {
    fn num_records(&self) -> usize;
}

const RFF_HEADER_MAGIC_NUMBER: &'static str = "RFF_V003";
pub struct RffHeader {
    magic_number: String,
    buf_size: usize,
    num_records: usize,
}

impl RffHeader {
    pub fn new(num_records: usize, buf_size: usize) -> Self {
        Self {
            magic_number: RFF_HEADER_MAGIC_NUMBER.to_string(),
            buf_size,
            num_records,
        }
    }

    pub fn check_valid(&self) {
        assert!(self.magic_number.eq(RFF_HEADER_MAGIC_NUMBER));
    }

    pub fn buf_size(&self) -> usize {
        self.buf_size
    }

    pub fn num_records(&self) -> usize {
        self.num_records
    }

    pub fn to_bytes(&self, expected_len: usize) -> Vec<u8> {
        let mut result = aligned_alloc(expected_len, get_page_size());
        result[0..8].copy_from_slice(self.magic_number.as_bytes());
        result[8..16].copy_from_slice(self.num_records.to_le_bytes().as_slice());
        result[16..24].copy_from_slice(self.buf_size.to_le_bytes().as_slice());
        result
    }
}

impl From<&[u8]> for RffHeader {
    fn from(value: &[u8]) -> Self {
        let magic_number = String::from_utf8(value[..8].to_vec()).unwrap();
        let mut num_records = [0_u8; 8];
        num_records.copy_from_slice(&value[8..16]);
        let num_records = usize::from_le_bytes(num_records);
        let mut buf_size = [0_u8; 8];
        buf_size.copy_from_slice(&value[16..24]);
        let buf_size = usize::from_le_bytes(buf_size);
        Self {
            magic_number,
            buf_size,
            num_records,
        }
    }
}

const INDEXED_RFF_HEADER_V2_MAGIC_NUMBER: &'static str = "INDEXED_RFF_V001";

pub struct IndexedRffReaderHeader {
    magic_number: String,
    buf_size: usize,
}
impl IndexedRffReaderHeader {
    pub fn new(buf_size: usize) -> Self {
        Self {
            magic_number: INDEXED_RFF_HEADER_V2_MAGIC_NUMBER.to_string(),
            buf_size,
        }
    }

    pub fn check_valid(&self) {
        assert!(self.magic_number.eq(INDEXED_RFF_HEADER_V2_MAGIC_NUMBER));
    }
    pub fn buf_size(&self) -> usize {
        self.buf_size
    }

    pub fn to_bytes(&self, expected_len: usize) -> Vec<u8> {
        let mut result = aligned_alloc(expected_len, get_page_size());
        result[0..16].copy_from_slice(self.magic_number.as_bytes());
        result[16..24].copy_from_slice(self.buf_size.to_le_bytes().as_slice());
        result
    }
}

impl From<&[u8]> for IndexedRffReaderHeader {
    fn from(value: &[u8]) -> Self {
        let magic_number = String::from_utf8(value[..16].to_vec()).unwrap();
        let mut buf_size = [0_u8; 8];
        buf_size.copy_from_slice(&value[16..24]);
        let buf_size = usize::from_le_bytes(buf_size);
        Self {
            magic_number,
            buf_size,
        }
    }
}
