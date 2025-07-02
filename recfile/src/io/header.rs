use crate::util::{aligned_alloc, get_page_size};

pub trait TRffHeader {
    fn num_records(&self) -> usize;
}

const RFF_HEADER_V2_MAGIC_NUMBER: &'static str = "RFF_V002";
pub struct RffHeaderV2 {
    magic_number: String,
    num_records: usize,
}

impl RffHeaderV2 {
    pub fn new(num_records: usize) -> Self {
        Self {
            magic_number: RFF_HEADER_V2_MAGIC_NUMBER.to_string(),
            num_records,
        }
    }

    pub fn check_valid(&self) {
        assert!(self.magic_number.eq(RFF_HEADER_V2_MAGIC_NUMBER));
    }

    pub fn num_records(&self) -> usize {
        self.num_records
    }

    pub fn to_bytes(&self, expected_len: usize) -> Vec<u8> {
        let mut result = aligned_alloc(expected_len, get_page_size());
        result[0..8].copy_from_slice(self.magic_number.as_bytes());
        result[8..16].copy_from_slice(self.num_records.to_le_bytes().as_slice());
        result
    }
}

impl From<&[u8]> for RffHeaderV2 {
    fn from(value: &[u8]) -> Self {
        let magic_number = String::from_utf8(value[..8].to_vec()).unwrap();
        let mut num_records = [0_u8; 8];
        num_records.copy_from_slice(&value[8..16]);
        let num_records = usize::from_le_bytes(num_records);
        Self {
            magic_number,
            num_records,
        }
    }
}

const INDEXED_RFF_HEADER_V2_MAGIC_NUMBER: &'static str = "INDEXED_RFF_V001";

pub struct IndexedRffReaderHeader {
    magic_number: String,
}
impl IndexedRffReaderHeader {
    pub fn new() -> Self {
        Self {
            magic_number: INDEXED_RFF_HEADER_V2_MAGIC_NUMBER.to_string(),
        }
    }

    pub fn check_valid(&self) {
        assert!(self.magic_number.eq(INDEXED_RFF_HEADER_V2_MAGIC_NUMBER));
    }

    pub fn to_bytes(&self, expected_len: usize) -> Vec<u8> {
        let mut result = aligned_alloc(expected_len, get_page_size());
        result[0..16].copy_from_slice(self.magic_number.as_bytes());
        result
    }
}

impl From<&[u8]> for IndexedRffReaderHeader {
    fn from(value: &[u8]) -> Self {
        let magic_number = String::from_utf8(value[..16].to_vec()).unwrap();
        Self { magic_number }
    }
}
