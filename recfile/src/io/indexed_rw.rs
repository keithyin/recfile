use anyhow;
use memmap2::{self, Mmap};
use std::{
    cell::RefCell,
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Seek, Write},
    num::NonZero,
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path::Path,
};

use io_uring::{IoUring, opcode, types};

use crate::{
    io::header::IndexedRffReaderHeader,
    util::{buffer::Buffer, get_buffer_size, get_page_size, stack::FixedSizeStack},
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
        // println!("before:{:?}", buf.get_slice(0, 100));
        if buf.size() < buf.cap() {
            // if the buffer is not full, we need to pad it with zeros
            let padding_size = buf.cap() - buf.size();
            // println!("Padding size: {}", padding_size);
            buf.push(vec![0; padding_size as usize].as_slice());
        }

        // println!("{:?}", buf.get_slice(0, 100));

        // let sqe = opcode::Write::new(
        //     types::Fd(self.file.as_raw_fd()),
        //     buf.as_mut_ptr(),
        //     buf.cap(), // it must be cap. the actual size may not aligned!!
        // )
        // .offset(self.write_position)
        // .build()
        // .user_data(idx as u64);

        // let buf_len = buf.cap();
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

pub struct IndexedRffReader {
    file: fs::File,
    mmap_file: Option<Mmap>,
}

impl IndexedRffReader {
    pub fn new_reader<P>(p: P, use_mmap: bool) -> Self
    where
        P: AsRef<Path>,
    {
        let mut file = File::open(p.as_ref()).unwrap();

        let mut header_bytes = vec![0_u8; get_page_size()];
        file.seek(std::io::SeekFrom::Start(0)).expect("seek error");
        file.read_exact(&mut header_bytes).expect("read file error");
        let header: IndexedRffReaderHeader = (header_bytes.as_slice()).into();
        header.check_valid();

        let mmap_file = if use_mmap {
            Some(unsafe { Mmap::map(&file).unwrap() })
        } else {
            None
        };

        Self { file, mmap_file }
    }

    pub fn read_serialized_data(&mut self, data_meta: &DataMeta) -> anyhow::Result<Vec<u8>> {
        let mut buf = vec![0; data_meta.size];
        if let Some(mmap_file) = &self.mmap_file {
            buf.copy_from_slice(&mmap_file[data_meta.offset..(data_meta.offset + data_meta.size)]);
        } else {
            self.file
                .seek(std::io::SeekFrom::Start(data_meta.offset as u64))?;
            self.file.read_exact(&mut buf)?;
        }
        Ok(buf)
    }

    pub fn read_data_index(filepath: &str) -> HashMap<String, DataMeta> {
        let index_filepath = build_index_filepath(filepath);
        build_index(&index_filepath)
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
        let mut reader = super::IndexedRffReader::new_reader(named_file.path(), true);
        let mut cnt = 0;
        data_index.iter().for_each(|(_key, data_meta)| {
            let record = reader.read_serialized_data(data_meta).unwrap();
            assert_eq!(record, data);
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
        let mut reader = super::IndexedRffReader::new_reader(named_file, true);
        let mut cnt = 0;
        data_index.iter().for_each(|(_key, data_meta)| {
            let record = reader.read_serialized_data(data_meta).unwrap();
            println!("{}", String::from_utf8(record).unwrap());
            cnt += 1;
        });
    }
}
