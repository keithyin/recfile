use std::{
    cell::RefCell,
    fs::{self, OpenOptions},
    io::{Read, Seek, Write},
    num::NonZero,
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::Path,
};

use io_uring::{IoUring, opcode, types};

use crate::{
    io::header::RffHeaderV2,
    util::{
        buffer::{AlignedVecU8, Buffer},
        get_page_size,
        stack::FixedSizeStack,
    },
};

pub fn write_rff_meta(
    file: &mut fs::File,
    version: u32,
    record_len: usize,
    page_size: usize,
) -> std::io::Result<()> {
    file.seek(std::io::SeekFrom::Start(0)).expect("seek error");
    let mut buf = AlignedVecU8::new(4 * 1024, page_size);
    buf.fill(0);
    // 10 bytes for magic number
    buf[0..10].copy_from_slice(format!("RFF_V{:05}", version).as_bytes());
    // 8 bytes for record length
    buf[10..18].copy_from_slice(record_len.to_le_bytes().as_ref());
    file.write_all(&buf).expect("write_rff_meta. write error");
    Ok(())
}

/// 存储序列化的对象，核心实现是二级存储
/// 开头留 4k 的空间用来存放 元数据（magic number， 以及一些其它信息）。
/// 4k 之后，用来存放实际内容，结构体的二进制都通过 二进制大小+真实二进制的方式来存储
pub struct RffWriter {
    file: fs::File,
    #[allow(unused)]
    meta_preserve_size: usize,

    #[allow(unused)]
    io_depth: usize,
    #[allow(unused)]
    page_size: usize,

    #[allow(unused)]
    buf_size: usize,
    buffers: Vec<RefCell<Buffer>>,
    active_buffer_index: Option<usize>,
    free_buffer_indices: FixedSizeStack,
    ring: IoUring,
    pending_io: usize,
    write_position: u64,
    write_cnt: usize,
}

impl RffWriter {
    ///
    /// sender is used for to send data to be written. the data should be bytes stream
    pub fn new_writer<P>(p: P, io_depth: NonZero<usize>) -> Self
    where
        P: AsRef<Path>,
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(libc::O_DIRECT)
            .open(p.as_ref())
            .unwrap();
        let io_depth = io_depth.get();
        let page_size = get_page_size();
        if page_size == 0 {
            panic!("Failed to get page size");
        }
        let meta_preserve_size = 4 * 1024; // 4KB for metadata

        file.write_all(&RffHeaderV2::new(0).to_bytes(meta_preserve_size))
            .expect("write header error");

        let buf_size = 4 * 1024 * 1024; // 4MB buffer size

        let ring = IoUring::new(io_depth as u32).unwrap();

        let buffers = (0..io_depth)
            .map(|_| RefCell::new(Buffer::new(buf_size, page_size)))
            .collect::<Vec<_>>();

        Self {
            file,
            meta_preserve_size,
            io_depth,
            page_size,
            buf_size,
            buffers: buffers,
            active_buffer_index: None,
            free_buffer_indices: FixedSizeStack::new(io_depth).fill_stack(),
            ring: ring,
            pending_io: 0,
            write_position: meta_preserve_size as u64,
            write_cnt: 0,
        }
    }

    pub fn write_serialized_data(&mut self, data: &[u8]) -> std::io::Result<()> {
        // Write the data to the file
        self.write(data.len().to_le_bytes().as_ref())?;
        self.write(data)?;
        self.write_cnt += 1;
        Ok(())
    }

    fn write(&mut self, data: &[u8]) -> std::io::Result<()> {
        // println!("write here");

        let mut data_start = 0;
        let data_end = data.len();
        loop {
            let active_idx = self.active_buffer_index;

            // println!("loop here");

            if let Some(active_index) = active_idx {
                let remaining = {
                    let mut active_buf = self.buffers[active_index].borrow_mut();
                    active_buf.push(&data[data_start..])
                };

                if remaining > 0 {
                    // buffer is full
                    // eprintln!("buffer is full, remaining: {}", remaining);
                    data_start = data_end - remaining;
                    self.submit_buffer();
                    continue;
                } else {
                    if self.buffers[active_index].borrow().is_full() {
                        // buffer is full, submit it
                        self.submit_buffer();
                    }
                    break;
                }
            } else {
                // active_buffer_index is None
                if let Some(idx) = self.free_buffer_indices.pop() {
                    self.active_buffer_index = Some(idx);
                } else {
                    // No free buffer, wait for one to become available
                    assert!(self.pending_io > 0);
                    self.ring.submit_and_wait(1)?;
                    let cqe = self
                        .ring
                        .completion()
                        .next()
                        .expect("No completion event found");
                    self.free_buffer_indices.push(cqe.user_data() as usize);
                    self.buffers[cqe.user_data() as usize].borrow_mut().clear();
                    self.pending_io -= 1;
                }
                continue;
            }
        }
        Ok(())
    }

    fn submit_buffer(&mut self) {
        let idx = self.active_buffer_index.unwrap();
        let mut buf = self.buffers[idx].borrow_mut();
        if buf.size() == 0 {
            // nothing to write
            self.active_buffer_index = None;
            return;
        }
        // println!("before:{:?}", buf.get_slice(0, 100));
        if buf.size() < buf.cap() {
            // if the buffer is not full, we need to pad it with zeros
            let padding_size = buf.cap() - buf.size();
            // println!("Padding size: {}", padding_size);
            buf.push(vec![0; padding_size as usize].as_slice());
        }

        // println!("{:?}", buf.get_slice(0, 100));

        let sqe = opcode::Write::new(
            types::Fd(self.file.as_raw_fd()),
            buf.as_mut_ptr(),
            buf.cap(), // it must be cap. the actual size may not aligned!!
        )
        .offset(self.write_position)
        .build()
        .user_data(idx as u64);

        unsafe {
            self.ring
                .submission()
                .push(&sqe)
                .expect("Failed to push submission queue entry");
        }

        self.write_position += buf.cap() as u64;
        self.pending_io += 1;
        self.active_buffer_index = None;
    }

    fn recycle_buffer(&mut self) {
        if self.pending_io == 0 {
            return;
        }
        self.ring.submit_and_wait(1).unwrap();
        let cqe = self
            .ring
            .completion()
            .next()
            .expect("No completion event found");
        let idx = cqe.user_data() as usize;
        self.free_buffer_indices.push(idx);
        self.buffers[idx].borrow_mut().clear();

        self.pending_io -= 1;
    }
}

impl Drop for RffWriter {
    fn drop(&mut self) {
        // Ensure all pending IO operations are completed
        if self.active_buffer_index.is_some() {
            // println!("Flushing remaining data to disk");
            self.submit_buffer();
        }

        while self.pending_io > 0 {
            // println!("Waiting for pending IO operations to complete");
            self.recycle_buffer();
        }
        let num_records = self.write_cnt;

        self.file
            .seek(std::io::SeekFrom::Start(0))
            .expect("seek error");
        self.file
            .write_all(&RffHeaderV2::new(num_records).to_bytes(4096))
            .expect("write header error");

        self.file.sync_all().expect("Failed to sync file");
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DataLocation {
    buf_idx: usize,
    offset: usize,
}

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

pub struct RffReader {
    file: fs::File,
    file_size: u64,
    #[allow(unused)]
    read_position: u64,
    header: RffHeaderV2,
    io_depth: usize,
    buff_size: usize,
    ring: IoUring,
    buffers: Vec<RefCell<Buffer>>,
    buffers_flag: Vec<BufferStatus>,
    data_location: DataLocation, // 即将要读取的 位置和 idx
    file_offset_of_buffers: Vec<u64>,
    pending_io: usize,
    init_flag: bool,
}

impl RffReader {
    pub fn new_reader<P>(p: P, io_depth: NonZero<usize>) -> Self
    where
        P: AsRef<Path>,
    {
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(p.as_ref())
            .unwrap();
        let file_size = file.metadata().unwrap().len();

        let page_size = get_page_size();
        if page_size == 0 {
            panic!("Failed to get page size");
        }
        let read_position = (4 * 1024) as u64; // skip the metadata area

        let mut header_bytes = AlignedVecU8::new(4 * 1024, page_size);
        file.seek(std::io::SeekFrom::Start(0)).expect("seek error");
        file.read_exact(&mut header_bytes).expect("read file error");
        let header: RffHeaderV2 = (header_bytes.as_slice()).into();

        let io_depth = io_depth.get();
        let ring = IoUring::new(io_depth as u32).unwrap();

        let buff_size = 4 * 1024 * 1024; // 4MB buffer size
        let buffers_flag = vec![BufferStatus::default(); io_depth];
        let buffers = (0..io_depth)
            .map(|_| RefCell::new(Buffer::new(buff_size, page_size)))
            .collect();
        Self {
            file,
            file_size,
            read_position,
            header,
            io_depth,
            buff_size,
            ring,
            buffers,
            buffers_flag,
            data_location: DataLocation::default(),
            file_offset_of_buffers: (0..io_depth)
                .map(|i| (read_position + i as u64 * buff_size as u64) % file_size)
                .collect(),
            pending_io: 0,
            init_flag: false,
        }
    }

    pub fn num_records(&self) -> usize {
        self.header.num_records()
    }

    pub fn read_serialized_data(&mut self) -> Option<Vec<u8>> {
        let record_len = self.read_record_length();
        if record_len == 0 {
            return None; // No more records to read
        }
        let mut data = vec![0_u8; record_len];
        if let Some(()) = self.read_exact(&mut data) {
            Some(data)
        } else {
            None
        }
    }

    pub fn read_serialized_data_to_buf(&mut self, buf: &mut [u8]) -> Option<usize> {
        let record_len = self.read_record_length();
        if record_len == 0 {
            return None; // No more records to read
        }
        assert!(buf.len() >= record_len, "buf too short");
        if let Some(()) = self.read_exact(&mut buf[..record_len]) {
            Some(record_len)
        } else {
            None
        }
    }

    fn read_record_length(&mut self) -> usize {
        let mut length_buf = [0u8; 8];
        if let Some(()) = self.read_exact(&mut length_buf) {
            usize::from_le_bytes(length_buf)
        } else {
            0
        }
    }

    // fn read_exact(&mut self, data)

    fn read_exact(&mut self, data: &mut [u8]) -> Option<()> {
        let record_len = data.len();
        let mut data_start = 0;
        // println!("buf_idx:{}", self.data_location.buf_idx);
        while data_start < record_len {
            let buf_idx = self.data_location.buf_idx;
            if self.wait_buf_ready4read(buf_idx).is_none() {
                return None; // No more data to read
            }
            let expected_data_size = record_len - data_start;

            let (fill_size, current_buf_remaining) = {
                let buf = self.buffers[buf_idx].borrow();

                let current_buf_remaining = (buf.cap() - self.data_location.offset as u32) as usize;
                let fill_size = current_buf_remaining.min(expected_data_size);
                data[data_start..data_start + fill_size].copy_from_slice(buf.get_slice(
                    self.data_location.offset,
                    self.data_location.offset + fill_size,
                ));
                (fill_size, current_buf_remaining)
            };

            self.data_location.offset += fill_size;
            data_start += fill_size;
            if expected_data_size >= current_buf_remaining {
                // current buffer is not enough, need to read next buffer
                // self.ready4sqe_buffer_indices.push(buf_idx);
                let next_buf_idx: usize = (buf_idx + 1) % self.io_depth;
                self.data_location.buf_idx = next_buf_idx;
                self.data_location.offset = 0;

                self.buffers_flag[buf_idx].set_ready4sqe();
                self.submit_read_event(buf_idx);
            }
        }

        Some(())
    }

    fn wait_buf_ready4read(&mut self, buf_idx: usize) -> Option<()> {
        if self.buffers_flag[buf_idx].ready4read() {
            return Some(());
        }
        // initial state
        if !self.init_flag {
            for idx in 0..self.io_depth {
                self.submit_read_event(idx);
            }
            self.init_flag = true;
        }
        while self.pending_io > 0 {
            self.ring
                .submit_and_wait(1)
                .expect("Failed to submit and wait");
            let cqe = self
                .ring
                .completion()
                .next()
                .expect("No completion event found");
            self.pending_io -= 1;

            let idx = cqe.user_data() as usize;
            self.buffers_flag[idx].set_ready4read();

            if self.buffers_flag[buf_idx].ready4read() {
                return Some(());
            }
        }

        None
    }

    fn submit_read_event(&mut self, buf_idx: usize) -> Option<()> {
        if self.file_offset_of_buffers[buf_idx] >= self.file_size {
            // no more data to read
            return None;
        }
        assert!(self.buffers_flag[buf_idx].ready4sqe());

        let sqe = opcode::Read::new(
            types::Fd(self.file.as_raw_fd()),
            self.buffers[buf_idx].borrow_mut().as_mut_ptr(),
            self.buff_size as u32,
        )
        .offset(self.file_offset_of_buffers[buf_idx])
        .build()
        .user_data(buf_idx as u64);

        unsafe {
            self.ring
                .submission()
                .push(&sqe)
                .expect("Failed to push submission queue entry");
        }
        self.pending_io += 1;
        self.set_buffer_next_file_offset(buf_idx);
        Some(())
    }

    fn set_buffer_next_file_offset(&mut self, buf_idx: usize) {
        self.file_offset_of_buffers[buf_idx] += self.buff_size as u64 * self.io_depth as u64;
        if self.file_offset_of_buffers[buf_idx] >= self.file_size {
            // no more data to read
            self.buffers_flag[buf_idx].set_invalid();
        }
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZero;

    // use gskits::pbar::{DEFAULT_INTERVAL, get_bar_pb};
    use tempfile::NamedTempFile;

    #[test]
    fn test_rff_rw() {
        let num_records = 1024 * 1024;
        let named_file = NamedTempFile::new().unwrap();
        let mut writer = super::RffWriter::new_writer(named_file.path(), NonZero::new(2).unwrap());
        let data = b"Hello, world! today is a good day.\n";

        for _ in 0..num_records {
            writer.write_serialized_data(data).unwrap();
        }
        drop(writer);

        let mut reader = super::RffReader::new_reader(named_file.path(), NonZero::new(2).unwrap());
        assert_eq!(reader.num_records(), num_records);
        let mut cnt = 0;
        while let Some(record) = reader.read_serialized_data() {
            assert_eq!(record, data);
            cnt += 1;
        }

        assert_eq!(
            cnt, num_records,
            "Number of records read does not match written"
        );
    }

    // #[test]
    // fn test_rff_read() {
    //     let named_file = "data.rff";
    //     let mut reader = super::RffReader::new_reader(named_file, NonZero::new(2).unwrap());
    //     while let Some(record) = reader.read_serialized_data() {
    //         println!("{}", String::from_utf8_lossy(&record));
    //     }
    // }

    // #[test]
    // fn test_rff_write() {
    //     let named_file = "data.rff";
    //     let mut writer = super::RffWriter::new_writer(named_file, NonZero::new(2).unwrap());
    //     let data = b"Hel m\n";
    //     let tot = 1000000;
    //     let pb = get_bar_pb(format!("runing..."), DEFAULT_INTERVAL, tot);
    //     for _idx in 0..tot {
    //         pb.inc(1);
    //         writer.write_serialized_data(data).unwrap();
    //     }
    //     pb.finish();
    //     drop(writer);
    // }
}
