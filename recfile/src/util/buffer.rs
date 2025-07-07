use std::ops::{Deref, DerefMut};

use crate::util::aligned_alloc;

#[derive(Debug, Clone)]
pub struct AlignedVecU8 {
    vec: Vec<u8>,
}

impl AlignedVecU8 {
    pub fn new(buf_size: usize, page_size: usize) -> Self {
        let vec = aligned_alloc(buf_size, page_size);
        Self { vec }
    }
}

impl Deref for AlignedVecU8 {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.vec
    }
}

impl DerefMut for AlignedVecU8 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.vec
    }
}

#[derive(Debug, Clone)]
pub struct Buffer {
    vec: AlignedVecU8,
    cap: usize,
    size: usize,
}

impl Buffer {
    pub fn new(buf_size: usize, page_size: usize) -> Self {
        let vec = AlignedVecU8::new(buf_size, page_size);
        Self {
            vec,
            cap: buf_size,
            size: 0,
        }
    }

    pub fn push(&mut self, data: &[u8]) -> usize {
        let push_len = data.len().min(self.cap - self.size);
        self.vec.vec[self.size..self.size + push_len].copy_from_slice(&data[..push_len]);
        // self. (&data[..push_len]);
        self.size += push_len;
        data.len() - push_len
    }

    pub fn is_full(&self) -> bool {
        assert!(self.size <= self.cap);
        self.size == self.cap
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.vec.vec.as_mut_ptr()
    }

    pub fn size(&self) -> u32 {
        self.size as u32
    }
    pub fn cap(&self) -> u32 {
        self.cap as u32
    }

    pub fn clear(&mut self) -> &mut Self {
        self.size = 0;
        self
    }

    pub fn get_slice(&self, start: usize, end: usize) -> &[u8] {
        &self.vec.vec[start..end]
    }
}
