// Write some text to the file

use std::{
    cell::RefCell,
    fs::OpenOptions,
    io::Write,
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    time::Instant,
};

use clap::Parser;
use io_uring::{IoUring, opcode, types};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    pub out_path: String,
    #[arg(long = "mode", default_value = "vanilla")]
    pub mode: String,
}

/*
  Round 1...
MB per second: 675.72MB/s
  Round 2...
MB per second: 758.82MB/s
  Round 3...
MB per second: 751.05MB/s
  Round 4...
MB per second: 613.35MB/s
  Round 5...
MB per second: 749.83MB/s

*/
fn vanilla_file_write(cli: &Cli) {
    let data_size = 1024 * 1024 * 1024 * 10; // 2 GB
    let data = vec![0_u8; data_size];
    // Open a file in write mode, creating it if it doesn't exist
    let mut file = std::fs::File::create(&cli.out_path).expect("Unable to create file");

    let mut start = 0;
    let buf_size = 4 * 1024 * 1024; // 1 MB buffer size
    let instant = Instant::now();
    while start < data.len() {
        // Write data to the file
        let end = std::cmp::min(start + buf_size, data.len());
        file.write_all(&data[start..end])
            .expect("Unable to write data");
        start = end;
    }
    file.sync_all().unwrap();
    let elapsed = instant.elapsed().as_secs_f64();
    let bytes_per_sec = data_size as f64 / elapsed;
    let mb_per_sec = bytes_per_sec / (1024.0 * 1024.0);
    println!("MB per second: {:.2}MB/s", mb_per_sec); // 568MB/s . 4M block 640MB/s
}

/// 分配 O_DIRECT 需要的对齐缓冲区
fn aligned_alloc(size: usize) -> Vec<u8> {
    use std::ptr;
    let align = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
    // println!("page_size:{}", align)
    let mut ptr: *mut u8 = ptr::null_mut();
    unsafe {
        let ret = libc::posix_memalign(&mut ptr as *mut _ as *mut _, align, size);
        if ret != 0 {
            panic!("posix_memalign failed");
        }
        assert_eq!(ptr as usize % align, 0, "allocate is not aligned");
        Vec::from_raw_parts(ptr, size, size)
    }
}

/*
  Round 1...
MB per second: 3025.75MB/s
  Round 2...
MB per second: 3095.02MB/s
  Round 3...
MB per second: 3076.54MB/s
  Round 4...
MB per second: 3132.42MB/s
  Round 5...
MB per second: 3027.08MB/s
*/
fn file_write_dio(cli: &Cli) {
    let data_size = 1024 * 1024 * 1024 * 20; // 2 GB
    let data = aligned_alloc(data_size);
    // Open a file in write mode, creating it if it doesn't exist
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_DIRECT) // Use O_DIRECT for direct I/O
        .open(&cli.out_path)
        .expect("Unable to create file");

    let mut start = 0;
    let buf_size = 4 * 1024 * 1024; // 1 MB buffer size
    let instant = Instant::now();
    while start < data.len() {
        // Write data to the file
        let end = std::cmp::min(start + buf_size, data.len());
        file.write_all(&data[start..end])
            .expect("Unable to write data");
        start = end;
    }
    file.sync_all().unwrap();
    let elapsed = instant.elapsed().as_secs_f64();
    let bytes_per_sec = data_size as f64 / elapsed;
    let mb_per_sec = bytes_per_sec / (1024.0 * 1024.0);
    println!("MB per second: {:.2}MB/s", mb_per_sec); // 4M block 1.5GB/s
}

#[derive(Debug, Clone)]
struct AlignedBuffer {
    buffer: Vec<u8>,
    data_size: usize,
}
impl AlignedBuffer {
    fn new(buf_size: usize) -> Self {
        let buffer = aligned_alloc(buf_size);
        Self {
            buffer,
            data_size: 0,
        }
    }

    fn fill_buffer(&mut self, data: &[u8]) -> usize {
        let remaining = self.buffer.len() - self.data_size;
        let to_cpy = remaining.min(data.len());
        self.buffer[self.data_size..self.data_size + to_cpy].copy_from_slice(&data[..to_cpy]);
        self.data_size += to_cpy;
        data.len() - to_cpy
    }

    fn clear_buf(&mut self) -> &mut Self {
        self.data_size = 0;
        self
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        let ptr = self.buffer.as_mut_ptr();
        assert!(ptr as usize % 4096 == 0);
        ptr
    }
}

struct FixedSizeStack {
    stack: Vec<usize>,
    cur_top: usize,
}

impl FixedSizeStack {
    fn new(size: usize) -> Self {
        Self {
            stack: Vec::with_capacity(size),
            cur_top: 0,
        }
    }

    fn fill_stack(&mut self, data: &[usize]) {
        for &item in data {
            if self.cur_top < self.stack.capacity() {
                self.stack.push(item);
                self.cur_top += 1;
            } else {
                panic!("Stack overflow");
            }
        }
    }

    fn push(&mut self, item: usize) {
        if self.cur_top < self.stack.capacity() {
            self.stack[self.cur_top] = item;
            self.cur_top += 1;
        } else {
            panic!("Stack overflow");
        }
    }

    fn pop(&mut self) -> Option<usize> {
        if self.cur_top > 0 {
            self.cur_top -= 1;
            Some(self.stack[self.cur_top])
        } else {
            None
        }
    }
}

/*
  Round 1...
seq_cnt:5120, cqe_cnt:5120
MB per second: 3149.60MB/s
  Round 2...
seq_cnt:5120, cqe_cnt:5120
MB per second: 3292.42MB/s
  Round 3...
seq_cnt:5120, cqe_cnt:5120
MB per second: 3208.95MB/s
  Round 4...
seq_cnt:5120, cqe_cnt:5120
MB per second: 2979.34MB/s
  Round 5...
seq_cnt:5120, cqe_cnt:5120
MB per second: 2523.44MB/s
*/
fn file_write_uring1(cli: &Cli) {
    let data_size = 1024 * 1024 * 1024 * 20; // 2 GB
    let mut data = aligned_alloc(data_size);
    data.iter_mut().enumerate().for_each(|(idx, v)| {
        if (idx + 1) % 2 == 1 {
            *v = 'A' as u8
        } else {
            *v = '\n' as u8
        }
    });
    assert_eq!(data.len(), data_size);

    // Open a file in write mode, creating it if it doesn't exist
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_DIRECT) // Use O_DIRECT for direct I/O
        .open(&cli.out_path)
        .expect("Unable to create file");

    let mut start = 0;
    let buf_size = 4 * 1024 * 1024; // 1 MB buffer size
    let instant = Instant::now();

    let mut valid_idx_queue = FixedSizeStack::new(8);
    valid_idx_queue.fill_stack(&vec![7, 6, 5, 4, 3, 2, 1, 0]);

    let io_depth = 8;
    let rio_buffers = (0..io_depth)
        .into_iter()
        .map(|_| RefCell::new(AlignedBuffer::new(buf_size)))
        .collect::<Vec<_>>();
    let mut ring = IoUring::new(io_depth as u32).expect("Failed to create IoUring");

    // init completions
    let mut sqe_cnt = 0;
    let mut cqe_cnt = 0;
    let mut remaining_io = 0;

    while start < data.len() {
        // Write data to the file
        let end = std::cmp::min(start + buf_size, data.len());
        if let Some(valid_idx) = valid_idx_queue.pop() {
            let remain = rio_buffers[valid_idx]
                .borrow_mut()
                .clear_buf()
                .fill_buffer(&data[start..end]);
            assert_eq!(remain, 0);
            let write_event = opcode::Write::new(
                types::Fd(file.as_raw_fd()),
                rio_buffers[valid_idx].borrow_mut().as_mut_ptr(),
                (end - start) as u32,
            )
            .offset(start as u64)
            .build()
            .user_data(valid_idx as u64);

            unsafe {
                ring.submission()
                    .push(&write_event)
                    .expect("Failed to push write event");
            }
            remaining_io += 1;
            start = end;
            sqe_cnt += 1;
        } else {
            ring.submit_and_wait(1).unwrap();
            let cqe = ring.completion().next().expect("No completion event");
            let buf_idx = cqe.user_data();
            valid_idx_queue.push(buf_idx as usize);
            cqe_cnt += 1;
            remaining_io -= 1;
        }
    }
    ring.submit_and_wait(remaining_io).unwrap();
    while let Some(_cqe) = ring.completion().next() {
        cqe_cnt += 1;
    }
    println!("seq_cnt:{}, cqe_cnt:{}", sqe_cnt, cqe_cnt);

    file.sync_all().expect("Failed to sync file");

    drop(file);

    let elapsed = instant.elapsed().as_secs_f64();
    let bytes_per_sec = start as f64 / elapsed;
    let mb_per_sec = bytes_per_sec / (1024.0 * 1024.0);
    println!("MB per second: {:.2}MB/s", mb_per_sec); // 4M block 5.7GB/s
}

/*
  Round 1...
MB per second: 2947.43MB/s
  Round 2...
MB per second: 3089.93MB/s
  Round 3...
MB per second: 2808.95MB/s
  Round 4...
MB per second: 2407.47MB/s
  Round 5...
MB per second: 3028.88MB/s

*/
fn file_write_uring2(cli: &Cli) {
    let data_size = 1024 * 1024 * 1024 * 20; // 2 GB
    let mut data = aligned_alloc(data_size);
    data.iter_mut().for_each(|v| *v = 'A' as u8);

    // Open a file in write mode, creating it if it doesn't exist
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_DIRECT) // Use O_DIRECT for direct I/O
        .open(&cli.out_path)
        .expect("Unable to create file");
    let io_depth = 8;

    let mut start = 0;
    let buf_size = 4 * 1024 * 1024; // 1 MB buffer size
    let instant = Instant::now();
    let real_buffer = (0..io_depth)
        .into_iter()
        .map(|_| RefCell::new(AlignedBuffer::new(buf_size)))
        .collect::<Vec<_>>();

    let mut valid_idx_queue = FixedSizeStack::new(8);
    valid_idx_queue.fill_stack(&vec![7, 6, 5, 4, 3, 2, 1, 0]);

    let rio_buffers = real_buffer
        .iter()
        .map(|buf| libc::iovec {
            iov_base: buf.borrow_mut().as_mut_ptr() as *mut _,
            iov_len: buf_size,
        })
        .collect::<Vec<_>>();

    let mut ring = IoUring::new(io_depth as u32).expect("Failed to create IoUring");

    // init completions
    while start < data.len() {
        // Write data to the file
        let end = std::cmp::min(start + buf_size, data.len());
        if let Some(valid_idx) = valid_idx_queue.pop() {
            real_buffer[valid_idx]
                .borrow_mut()
                .clear_buf()
                .fill_buffer(&data[start..end]);
            let write_event = opcode::Writev::new(
                types::Fd(file.as_raw_fd()),
                rio_buffers[valid_idx..valid_idx + 1].as_ptr(), // Get the pointer to the first element
                // (&rio_buffers[valid_idx]) as *const _,
                1,
            )
            .offset(start as u64)
            .build()
            .user_data(valid_idx as u64);

            unsafe {
                ring.submission()
                    .push(&write_event)
                    .expect("Failed to push write event");
            }
            start = end;
        } else {
            ring.submit_and_wait(1).unwrap();
            let cqe = ring.completion().next().expect("No completion event");
            valid_idx_queue.push(cqe.user_data() as usize);
        }
    }
    ring.submit_and_wait(io_depth).unwrap();
    while let Some(cqe) = ring.completion().next() {
        let valid_idx = cqe.user_data() as usize;
        real_buffer[valid_idx].borrow_mut().data_size = 0; // Clear the buffer after use
    }
    file.sync_all().expect("Failed to sync file");
    drop(file);

    let elapsed = instant.elapsed().as_secs_f64();
    let bytes_per_sec = data_size as f64 / elapsed;
    let mb_per_sec = bytes_per_sec / (1024.0 * 1024.0);
    println!("MB per second: {:.2}MB/s", mb_per_sec); // 4M block ？？？
}

fn file_write_uring3(cli: &Cli) {
    let data_size = 1024 * 1024 * 1024 * 20; // 2 GB
    let mut data = aligned_alloc(data_size);
    data.iter_mut().enumerate().for_each(|(idx, v)| {
        if (idx + 1) % 2 == 1 {
            *v = 'A' as u8;
        } else {
            *v = '\n' as u8;
        }
    });

    // Open a file in write mode, creating it if it doesn't exist
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_DIRECT) // Use O_DIRECT for direct I/O
        .open(&cli.out_path)
        .expect("Unable to create file");
    let io_depth = 8;

    let mut start = 0;
    let buf_size = 4 * 1024 * 1024; // 1 MB buffer size
    let instant = Instant::now();
    let real_buffer = (0..io_depth)
        .into_iter()
        .map(|_| RefCell::new(AlignedBuffer::new(buf_size)))
        .collect::<Vec<_>>();

    let mut valid_idx_queue = FixedSizeStack::new(8);
    valid_idx_queue.fill_stack(&vec![7, 6, 5, 4, 3, 2, 1, 0]);

    let rio_buffers = real_buffer
        .iter()
        .map(|buf| libc::iovec {
            iov_base: buf.borrow_mut().as_mut_ptr() as *mut _,
            iov_len: buf_size,
        })
        .collect::<Vec<_>>();

    let mut ring = IoUring::new(io_depth as u32).expect("Failed to create IoUring");

    unsafe {
        ring.submitter()
            .register_buffers(rio_buffers.as_slice())
            .unwrap();
        ring.submitter()
            .register_files(&[file.as_raw_fd()])
            .unwrap();
    }

    // init completions
    while start < data.len() {
        // Write data to the file
        let end = std::cmp::min(start + buf_size, data.len());
        if let Some(valid_idx) = valid_idx_queue.pop() {
            let mut buffer = real_buffer[valid_idx].borrow_mut();
            if buffer.data_size == 0 {
                buffer.clear_buf().fill_buffer(&data[start..end]);
            }
            let write_event = opcode::WriteFixed::new(
                types::Fixed(0),
                buffer.as_mut_ptr(),
                buf_size as u32,
                valid_idx as u16,
            )
            .offset(start as u64)
            .build()
            .user_data(valid_idx as u64);

            unsafe {
                ring.submission()
                    .push(&write_event)
                    .expect("Failed to push write event");
            }
            start = end;
        } else {
            ring.submit_and_wait(1).unwrap();
            let cqe = ring.completion().next().expect("No completion event");
            if cqe.result() < 0 {
                panic!("cqe error: {}", cqe.result());
            }
            valid_idx_queue.push(cqe.user_data() as usize);
        }
    }
    ring.submit_and_wait(io_depth).unwrap();
    while let Some(cqe) = ring.completion().next() {
        if cqe.result() < 0 {
            panic!("cqe error: {}", cqe.result());
        }

        let valid_idx = cqe.user_data() as usize;
        real_buffer[valid_idx].borrow_mut().data_size = 0; // Clear the buffer after use
    }
    file.sync_all().expect("Failed to sync file");
    drop(file);

    let elapsed = instant.elapsed().as_secs_f64();
    let bytes_per_sec = data_size as f64 / elapsed;
    let mb_per_sec = bytes_per_sec / (1024.0 * 1024.0);
    println!("MB per second: {:.2}MB/s", mb_per_sec); // 4M block ？？？
}

fn main() {
    let cli = Cli::parse();
    match cli.mode.as_str() {
        "vanilla" => vanilla_file_write(&cli),
        "dio" => file_write_dio(&cli),
        "uring1" => file_write_uring1(&cli),
        "uring2" => file_write_uring2(&cli),
        "uring3" => file_write_uring3(&cli),
        _ => panic!("Unknown mode: {}", cli.mode),
    }
}
