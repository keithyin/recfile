use bincode::config::Configuration;

pub mod header;
pub mod indexed_rw;
pub mod v1;
pub mod v2;

#[derive(Debug, Clone, Copy)]
pub enum BufferStatus {
    ReadyForRead,
    ReadyForSqe, // 可以用于提交 io_uring 读请求
    Invalid,
}
impl BufferStatus {
    pub fn ready4read(&self) -> bool {
        matches!(self, BufferStatus::ReadyForRead)
    }

    pub fn ready4sqe(&self) -> bool {
        matches!(self, BufferStatus::ReadyForSqe)
    }

    pub fn is_invalid(&self) -> bool {
        matches!(self, BufferStatus::Invalid)
    }

    pub fn set_ready4read(&mut self) {
        *self = BufferStatus::ReadyForRead;
    }
    pub fn set_ready4sqe(&mut self) {
        *self = BufferStatus::ReadyForSqe;
    }

    pub fn set_invalid(&mut self) {
        *self = BufferStatus::Invalid;
    }
}
impl Default for BufferStatus {
    fn default() -> Self {
        BufferStatus::ReadyForSqe
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DataLocation {
    pub buf_idx: usize,
    pub offset: usize,
}

pub fn get_bincode_cfg() -> Configuration {
    bincode::config::standard()
        .with_little_endian()
        .with_variable_int_encoding()
}
