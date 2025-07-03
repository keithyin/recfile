use anyhow;
use memmap2::{self, Mmap};
use std::{
    borrow::Cow,
    cell::RefCell,
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Seek, Write},
    num::NonZero,
    os::{
        fd::AsRawFd,
        unix::fs::{FileExt, OpenOptionsExt},
    },
    path::Path,
    sync::Arc,
};

use io_uring::{IoUring, opcode, types};

use crate::{
    io::{BufferStatus, DataLocation, header::IndexedRffReaderHeader},
    util::{
        buffer::{AlignedVecU8, Buffer},
        get_buffer_size, get_page_size,
        stack::FixedSizeStack,
    },
};

pub fn build_index_filepath(data_fileapth: &str) -> String {
    format!("{}.index", data_fileapth)
}

/// 开头留 4k 的空间用来存放 元数据（magic number， 以及一些其它信息）。
/// 4k 之后，用来存放实际内容，仅存数据信息。数据的元信息存储在独立的 .index 文件中
/// Rff. Record file format
pub struct IndexedRffWriter {
    file: fs::File,
    index_file_writer: BufWriter<fs::File>,

    #[allow(unused)]
    io_depth: usize,

    #[allow(unused)]
    buf_size: usize,
    buffers: Vec<RefCell<Buffer>>,
    active_buffer_index: Option<usize>,
    free_buffer_indices: FixedSizeStack,
    ring: IoUring,
    pending_io: usize,
    write_position: u64,
    record_write_position: u64,
    write_cnt: usize,
}

impl IndexedRffWriter {
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

        file.write_all(&IndexedRffReaderHeader::new().to_bytes(page_size))
            .expect("write header error");

        let buf_size = get_buffer_size(); // 4MB buffer size

        let ring = IoUring::new(io_depth as u32).unwrap();

        let buffers = (0..io_depth)
            .map(|_| RefCell::new(Buffer::new(buf_size, page_size)))
            .collect::<Vec<_>>();

        let iovecs = buffers
            .iter()
            .map(|buf| {
                let buf_len = buf.borrow().cap();
                libc::iovec {
                    iov_base: buf.borrow_mut().as_mut_ptr() as *mut _,
                    iov_len: buf_len as usize,
                }
            })
            .collect::<Vec<_>>();

        unsafe {
            ring.submitter()
                .register_buffers(iovecs.as_slice())
                .expect("register buffers error");
            ring.submitter()
                .register_files(&[file.as_raw_fd()])
                .expect("register file error");
        }

        let index_file_writer = BufWriter::new(
            fs::File::create(build_index_filepath(p.as_ref().to_str().unwrap())).unwrap(),
        );
        Self {
            file,
            index_file_writer,
            io_depth,
            buf_size,
            buffers: buffers,
            active_buffer_index: None,
            free_buffer_indices: FixedSizeStack::new(io_depth).fill_stack(),
            ring: ring,
            pending_io: 0,
            write_position: page_size as u64,
            record_write_position: page_size as u64,
            write_cnt: 0,
        }
    }

    /// caller need to make sure, the key is unique !!!!
    pub unsafe fn write_serialized_data(&mut self, data: &[u8], key: &str) -> std::io::Result<()> {
        // Write the data to the file
        writeln!(
            &mut self.index_file_writer,
            "{}\t{}\t{}",
            key,
            self.record_write_position,
            data.len()
        )
        .unwrap();
        self.write(data)?;
        self.record_write_position += data.len() as u64;
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

        if buf.size() < buf.cap() {
            let padding_size = buf.cap() - buf.size();
            buf.push(vec![0; padding_size as usize].as_slice());
        }

        let sqe = opcode::WriteFixed::new(
            types::Fixed(0),
            buf.as_mut_ptr(),
            buf.cap(), // it must be cap. the actual size may not aligned!!
            idx as u16,
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

impl Drop for IndexedRffWriter {
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
        // let num_records = self.write_cnt;

        self.file
            .seek(std::io::SeekFrom::Start(0))
            .expect("seek error");
        self.file
            .write_all(&IndexedRffReaderHeader::new().to_bytes(4096))
            .expect("write header error");

        self.file.sync_all().expect("Failed to sync file");
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DataMeta {
    offset: usize,
    size: usize,
}

pub fn build_index(index_filepath: &str) -> HashMap<String, DataMeta> {
    let mut result = HashMap::new();

    let file = BufReader::new(fs::File::open(index_filepath).unwrap());
    for line in file.lines() {
        let line = line.unwrap();
        if line.trim().eq("") {
            continue;
        }

        let items = line.split("\t").collect::<Vec<_>>();
        let key = items[0].to_string();
        let offset = items[1].parse::<usize>().unwrap();
        let size = items[2].parse::<usize>().unwrap();
        result.insert(key, DataMeta { offset, size });
    }

    result
}

#[derive(Debug, Clone, Default)]
pub struct IndexedRffReaderSequentialMeta {
    metas: Vec<(Arc<String>, DataMeta)>,
}

pub fn build_index_sequential(
    index_filepath: &str,
    chunk_size: usize,
) -> Vec<IndexedRffReaderSequentialMeta> {
    let mut result = vec![];
    let mut inner = vec![];

    let file = BufReader::new(fs::File::open(index_filepath).unwrap());
    for line in file.lines() {
        let line = line.unwrap();
        if line.trim().eq("") {
            continue;
        }

        let items = line.split("\t").collect::<Vec<_>>();
        let key = items[0].to_string();
        let offset = items[1].parse::<usize>().unwrap();
        let size = items[2].parse::<usize>().unwrap();
        inner.push((Arc::new(key), DataMeta { offset, size }));
        if inner.len() >= chunk_size {
            result.push(IndexedRffReaderSequentialMeta { metas: inner });
            inner = vec![]
        }
    }

    if !inner.is_empty() {
        result.push(IndexedRffReaderSequentialMeta { metas: inner });
    }
    result
}

pub struct IndexedRffReader {
    file: fs::File,
    mmap_file: Option<Mmap>,
}

impl IndexedRffReader {
    pub fn new_reader<P>(p: P, use_mmap: bool) -> Self
    where
        P: AsRef<Path>,
    {
        let file = File::open(p.as_ref()).unwrap();
        let mut header_bytes = vec![0_u8; get_page_size()];
        file.read_exact_at(&mut header_bytes, 0)
            .expect("read meta error");
        let header: IndexedRffReaderHeader = (header_bytes.as_slice()).into();
        header.check_valid();

        let mmap_file = if use_mmap {
            Some(unsafe { Mmap::map(&file).unwrap() })
        } else {
            None
        };

        Self { file, mmap_file }
    }

    pub fn read_serialized_data(&self, data_meta: &DataMeta) -> anyhow::Result<Cow<[u8]>> {
        return if let Some(mmap_file) = &self.mmap_file {
            Ok(Cow::Borrowed(
                &mmap_file[data_meta.offset..(data_meta.offset + data_meta.size)],
            ))

            // buf.copy_from_slice(&mmap_file[data_meta.offset..(data_meta.offset + data_meta.size)]);
        } else {
            // self.file
            //     .seek(std::io::SeekFrom::Start(data_meta.offset as u64))?;
            // self.file.read_exact(&mut buf)?;

            // 采用 read_exact_at 而不是 seek + read_exact 方案是为了 解决多线程场景问题。
            // read_exact_at 底层是 pread. 无需修改线程偏移量，是线程安全的
            let mut buf = vec![0; data_meta.size];
            self.file.read_exact_at(&mut buf, data_meta.offset as u64)?;

            Ok(Cow::Owned(buf))
        };
    }

    pub fn read_data_index(filepath: &str) -> HashMap<String, DataMeta> {
        let index_filepath = build_index_filepath(filepath);
        build_index(&index_filepath)
    }
}

pub struct IndexedRffSequentialReader {
    #[allow(unused)]
    file: fs::File, // 不能删掉。要保证文件是打开的！
    end_position: u64,
    io_depth: usize,
    buff_size: usize,
    ring: IoUring,
    buffers: Vec<RefCell<Buffer>>,
    buffers_flag: Vec<BufferStatus>,
    data_location: DataLocation, // 即将要读取的 buffer 以及 offset
    file_offset_of_buffers: Vec<u64>,
    pending_io: usize,
    init_flag: bool,
    sequential_meta: IndexedRffReaderSequentialMeta,
    cur_data_idx: usize, // 当前读取到的 meta 索引
}

impl IndexedRffSequentialReader {
    pub fn new_reader<P>(
        p: P,
        sequential_meta: IndexedRffReaderSequentialMeta,
        io_depth: NonZero<usize>,
    ) -> Self
    where
        P: AsRef<Path>,
    {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(p.as_ref())
            .unwrap();

        let page_size = get_page_size();
        if page_size == 0 {
            panic!("Failed to get page size");
        }

        let mut header_bytes = AlignedVecU8::new(page_size, page_size);
        file.read_exact_at(&mut header_bytes, 0)
            .expect("read meta error");
        let header: IndexedRffReaderHeader = (header_bytes.as_slice()).into();
        header.check_valid();
        let io_depth = io_depth.get();
        let ring = IoUring::new(io_depth as u32).unwrap();

        let buff_size = get_buffer_size(); // 4MB buffer size
        let buffers: Vec<RefCell<Buffer>> = (0..io_depth)
            .map(|_| RefCell::new(Buffer::new(buff_size, page_size)))
            .collect();

        let iovecs = buffers
            .iter()
            .map(|buf| {
                let buf_len = buf.borrow().cap();
                libc::iovec {
                    iov_base: buf.borrow_mut().as_mut_ptr() as *mut _,
                    iov_len: buf_len as usize,
                }
            })
            .collect::<Vec<_>>();

        let read_position = sequential_meta.metas[0].1.offset / get_page_size() * get_page_size();
        let read_position = read_position as u64;
        let buf_offset = sequential_meta.metas[0].1.offset % get_page_size();

        let data_location = DataLocation {
            buf_idx: 0,
            offset: buf_offset as usize,
        };

        let end_position = sequential_meta
            .metas
            .last()
            .map_or(read_position, |(_, meta)| {
                meta.offset as u64 + meta.size as u64
            });
        let file_offset_of_buffers: Vec<u64> = (0..io_depth)
            .map(|i| read_position + i as u64 * buff_size as u64)
            .collect();

        let buffers_flag = file_offset_of_buffers
            .iter()
            .map(|&file_offset| {
                if file_offset < end_position {
                    BufferStatus::ReadyForSqe
                } else {
                    BufferStatus::Invalid
                }
            })
            .collect::<Vec<_>>();

        unsafe {
            ring.submitter()
                .register_buffers(iovecs.as_slice())
                .expect("register buffers error");
            ring.submitter()
                .register_files(&[file.as_raw_fd()])
                .expect("register file error");
        }

        Self {
            file,
            end_position: end_position,
            io_depth,
            buff_size,
            ring,
            buffers,
            buffers_flag,
            data_location: data_location,
            file_offset_of_buffers: file_offset_of_buffers,
            pending_io: 0,
            init_flag: false,
            sequential_meta: sequential_meta,
            cur_data_idx: 0,
        }
    }

    pub fn read_serialized_data(&mut self) -> Option<(Arc<String>, Vec<u8>)> {
        let record_len = self.read_record_length();
        if record_len == 0 {
            return None; // No more records to read
        }
        let name = self
            .sequential_meta
            .metas
            .get(self.cur_data_idx)
            .unwrap()
            .0
            .clone();
        let mut data = vec![0_u8; record_len];
        if let Some(()) = self.read_exact(&mut data) {
            Some((name, data))
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
        let cur_data_idx = self.cur_data_idx;
        if cur_data_idx > self.sequential_meta.metas.len() - 1 {
            return 0; // No more records to read
        }
        let length = self.sequential_meta.metas[cur_data_idx].1.size;
        self.cur_data_idx += 1; // Move to the next record
        length
    }

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
        if self.file_offset_of_buffers[buf_idx] >= self.end_position {
            // no more data to read
            return None;
        }
        assert!(self.buffers_flag[buf_idx].ready4sqe());

        let buf_cap = self.buffers[buf_idx].borrow().cap();
        let sqe = opcode::ReadFixed::new(
            types::Fixed(0),
            self.buffers[buf_idx].borrow_mut().as_mut_ptr(),
            buf_cap,
            buf_idx as u16,
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
        if self.file_offset_of_buffers[buf_idx] >= self.end_position {
            // no more data to read
            self.buffers_flag[buf_idx].set_invalid();
        }
    }
    pub fn read_data_index(
        filepath: &str,
        chunk_size: usize,
    ) -> Vec<IndexedRffReaderSequentialMeta> {
        let index_filepath = build_index_filepath(filepath);
        build_index_sequential(&index_filepath, chunk_size)
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZero;

    use gskits::pbar::{DEFAULT_INTERVAL, get_bar_pb};
    // use gskits::pbar::{DEFAULT_INTERVAL, get_bar_pb};
    use tempfile::NamedTempFile;

    #[test]
    fn test_rff_rw() {
        let num_records = 1024 * 1024;
        let named_file = NamedTempFile::new().unwrap();
        let mut writer =
            super::IndexedRffWriter::new_writer(named_file.path(), NonZero::new(2).unwrap());
        let data = b"Hello, world! today is a good day.\n";

        for i in 0..num_records {
            unsafe {
                writer
                    .write_serialized_data(data, &format!("record_{}", i))
                    .unwrap();
            }
        }
        drop(writer);

        let data_index =
            super::IndexedRffReader::read_data_index(named_file.path().to_str().unwrap());
        let reader = super::IndexedRffReader::new_reader(named_file.path(), true);
        let mut cnt = 0;
        data_index.iter().for_each(|(_key, data_meta)| {
            let record = reader.read_serialized_data(data_meta).unwrap();
            assert_eq!(record, data.as_ref());
            cnt += 1;
        });

        assert_eq!(
            cnt, num_records,
            "Number of records read does not match written"
        );
    }

    #[test]
    #[ignore = "no file"]
    fn test_rff_write() {
        let named_file = "data.rff";
        let mut writer = super::IndexedRffWriter::new_writer(named_file, NonZero::new(2).unwrap());
        let data = b"Hel m\n";
        let tot = 1000000;
        let pb = get_bar_pb(format!("runing..."), DEFAULT_INTERVAL, tot);
        for idx in 0..tot {
            pb.inc(1);
            unsafe {
                writer
                    .write_serialized_data(data, &format!("record_{}", idx))
                    .unwrap();
            }
        }
        pb.finish();
        drop(writer);
    }

    #[test]
    #[ignore = "no file"]
    fn test_rff_read() {
        let named_file = "data.rff";

        let data_index = super::IndexedRffReader::read_data_index(named_file);
        let reader = super::IndexedRffReader::new_reader(named_file, true);
        let mut cnt = 0;
        data_index.iter().for_each(|(_key, data_meta)| {
            let record = reader.read_serialized_data(data_meta).unwrap();
            println!("{}", String::from_utf8(record.into_owned()).unwrap());
            cnt += 1;
        });
    }
}
