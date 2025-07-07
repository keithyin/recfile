use std::{
    cell::RefCell,
    fs,
    io::{Read, Write},
    os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    path, process,
    time::Instant,
};

use io_uring::{cqueue, opcode, types, IoUring};
use recfile::util::{
    aligned_alloc,
    buffer::{AlignedVecU8, Buffer},
    get_page_size,
    stack::FixedSizeStack,
};

fn clear_cache() {
    process::Command::new("sh")
        .arg("-c")
        .arg("sync")
        .status()
        .expect("Failed to sync");

    process::Command::new("sh")
        .arg("-c")
        .arg("echo 3 > /proc/sys/vm/drop_caches")
        .status()
        .expect("Failed to clear cache");
}

fn buffered_io(fname: &str, data: &[u8], chunk_size: usize) {
    let mut file = fs::File::create(fname).expect("Failed to create file");

    for idx in 0..(data.len() / chunk_size) {
        let start = idx * chunk_size;
        let end = start + chunk_size;
        file.write_all(&data[start..end])
            .expect("Failed to write data");
    }
    file.sync_all().expect("Failed to sync file");
}

fn direct_io(fname: &str, data: &[u8], chunk_size: usize) {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .custom_flags(libc::O_DIRECT)
        .open(fname)
        .expect("Failed to open file for direct IO");

    for idx in 0..(data.len() / chunk_size) {
        let start = idx * chunk_size;
        let end = start + chunk_size;
        file.write_all(&data[start..end])
            .expect("Failed to write data");
    }
    file.sync_all().expect("Failed to sync file");
}

fn io_uring(fname: &str, data: &[u8], chunk_size: usize) {
    let file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_DIRECT)
        .open(fname)
        .expect("Unable to create file");

    let mut valid_idx_queue = FixedSizeStack::new(8);
    valid_idx_queue = valid_idx_queue.fill_stack();

    let io_depth = 8;
    let rio_buffers = (0..io_depth)
        .into_iter()
        .map(|_| RefCell::new(Buffer::new(chunk_size, get_page_size())))
        .collect::<Vec<_>>();
    let mut ring = IoUring::new(io_depth as u32).expect("Failed to create IoUring");

    // init completions
    let mut remaining_io = 0;

    let mut start = 0;

    while start < data.len() {
        // Write data to the file
        let end = std::cmp::min(start + chunk_size, data.len());
        assert_eq!(end - start, chunk_size);

        if let Some(valid_idx) = valid_idx_queue.pop() {
            let remain = rio_buffers[valid_idx]
                .borrow_mut()
                .clear()
                .push(&data[start..end]);
            assert_eq!(remain, 0);
            let write_event = opcode::Write::new(
                types::Fd(file.as_raw_fd()),
                rio_buffers[valid_idx].borrow_mut().as_mut_ptr(),
                chunk_size as u32,
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
        } else {
            ring.submit_and_wait(1).unwrap();
            let cqe = ring.completion().next().expect("No completion event");
            let buf_idx = cqe.user_data();
            valid_idx_queue.push(buf_idx as usize);
            remaining_io -= 1;
        }
    }
    ring.submit_and_wait(remaining_io).unwrap();
    while let Some(_cqe) = ring.completion().next() {
        assert!(_cqe.result() > 0);
    }

    file.sync_all().expect("Failed to sync file");

    drop(file);
}

fn io_uring2(fname: &str, data: &[u8], chunk_size: usize) {
    let file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_DIRECT)
        .open(fname)
        .expect("Unable to create file");

    let mut valid_idx_queue = FixedSizeStack::new(8);
    valid_idx_queue = valid_idx_queue.fill_stack();

    let io_depth = 8;
    let real_buffers = (0..io_depth)
        .into_iter()
        .map(|_| RefCell::new(Buffer::new(chunk_size, get_page_size())))
        .collect::<Vec<_>>();

    let rio_buffers = real_buffers
        .iter()
        .map(|buf| libc::iovec {
            iov_base: buf.borrow_mut().as_mut_ptr() as *mut _,
            iov_len: chunk_size,
        })
        .collect::<Vec<_>>();
    let mut ring = IoUring::new(io_depth as u32).expect("Failed to create IoUring");

    // init completions
    let mut remaining_io = 0;

    unsafe {
        ring.submitter()
            .register_buffers(rio_buffers.as_slice())
            .unwrap();
        ring.submitter()
            .register_files(&[file.as_raw_fd()])
            .unwrap();
    }

    let mut start = 0;

    while start < data.len() {
        // Write data to the file
        let end = std::cmp::min(start + chunk_size, data.len());
        if let Some(valid_idx) = valid_idx_queue.pop() {
            let mut buffer = real_buffers[valid_idx].borrow_mut();
            buffer.clear().push(&data[start..end]);
            let write_event = opcode::WriteFixed::new(
                types::Fixed(0),
                buffer.as_mut_ptr(),
                chunk_size as u32,
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
            remaining_io += 1;
        } else {
            ring.submit_and_wait(1).unwrap();
            let cqe = ring.completion().next().expect("No completion event");
            if cqe.result() < 0 {
                panic!("cqe result : {}", cqe.result());
            }
            remaining_io -= 1;
            valid_idx_queue.push(cqe.user_data() as usize);
        }
    }
    ring.submit_and_wait(remaining_io).unwrap();
    while let Some(cqe) = ring.completion().next() {
        if cqe.result() < 0 {
            panic!("cqe error: {}", cqe.result());
        }
    }
    file.sync_all().expect("Failed to sync file");
    drop(file);
}

fn io_uring3(fname: &str, data: &[u8], chunk_size: usize) {
    let file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_DIRECT)
        .open(fname)
        .expect("Unable to create file");

    let mut valid_idx_queue = FixedSizeStack::new(8);
    valid_idx_queue = valid_idx_queue.fill_stack();

    let io_depth = 8;
    let real_buffers = (0..io_depth)
        .into_iter()
        .map(|_| RefCell::new(Buffer::new(chunk_size, get_page_size())))
        .collect::<Vec<_>>();

    let rio_buffers = real_buffers
        .iter()
        .map(|buf| libc::iovec {
            iov_base: buf.borrow_mut().as_mut_ptr() as *mut _,
            iov_len: chunk_size,
        })
        .collect::<Vec<_>>();
    // let mut ring = IoUring::new(io_depth as u32).expect("Failed to create IoUring");
    let mut ring = IoUring::builder()
        .setup_sqpoll(1000)
        .build(io_depth as u32)
        .expect("Failed to create IoUring");

    // init completions
    let mut remaining_io = 0;

    unsafe {
        ring.submitter()
            .register_buffers(rio_buffers.as_slice())
            .unwrap();
        ring.submitter()
            .register_files(&[file.as_raw_fd()])
            .unwrap();
    }

    let mut start = 0;

    while start < data.len() {
        // Write data to the file
        let end = std::cmp::min(start + chunk_size, data.len());
        if let Some(valid_idx) = valid_idx_queue.pop() {
            let mut buffer = real_buffers[valid_idx].borrow_mut();
            buffer.clear().push(&data[start..end]);
            let write_event = opcode::WriteFixed::new(
                types::Fixed(0),
                buffer.as_mut_ptr(),
                chunk_size as u32,
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
            remaining_io += 1;
        } else {
            ring.submit_and_wait(1).unwrap();
            let cqe: cqueue::Entry = ring.completion().next().expect("No completion event");
            if cqe.result() < 0 {
                panic!("cqe result : {}", cqe.result());
            }
            remaining_io -= 1;
            valid_idx_queue.push(cqe.user_data() as usize);
        }
    }
    ring.submit_and_wait(remaining_io).unwrap();
    while let Some(cqe) = ring.completion().next() {
        if cqe.result() < 0 {
            panic!("cqe error: {}", cqe.result());
        }
    }
    file.sync_all().expect("Failed to sync file");
    drop(file);
}

fn prepare_data() -> Vec<u8> {
    let mut file = fs::File::open("/dev/random").expect("open /dev/random failed");
    let data_size = 1024 * 1024 * 1024 * 10; // 10GB

    let mut data = aligned_alloc(data_size, get_page_size());
    file.read_exact(&mut data).expect("read_exact failed");
    data
}

fn remove_file(fname: &str) {
    let fpath = path::Path::new(fname);
    if fpath.exists() {
        fs::remove_file(fname).expect("Failed to remove file");
    }
}

fn benchmarking_core(
    tag: &str,
    fname: &str,
    data: &[u8],
    chunk_size: usize,
    op: fn(&str, &[u8], usize),
) {
    let mut speeds = vec![0.0; 20];

    for i in 0..20 {
        remove_file(fname);
        clear_cache();
        let instant = Instant::now();
        op(fname, data, chunk_size);

        let elapsed = instant.elapsed();
        let bytes_per_sec = data.len() as f64 / elapsed.as_secs_f64();
        let mb_per_sec = bytes_per_sec / (1024.0 * 1024.0);
        speeds[i] = mb_per_sec;
    }

    speeds.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "{}, mean:{}MB/s, median:{}MB/s, min:{}MB/s, max:{}MB/s",
        tag,
        speeds.iter().sum::<f64>() / speeds.len() as f64,
        (speeds[speeds.len() / 2] + speeds[speeds.len() / 2 - 1]) / 2.0,
        speeds[0],
        speeds[speeds.len() - 1]
    );
}

fn benckmark(fname: &str) {
    let data = prepare_data();
    let chunk_size = 1024 * 1024; // 1MB

    // benchmarking_core("buffered_io", fname, &data, chunk_size, buffered_io);
    benchmarking_core("direct_io", fname, &data, chunk_size, direct_io);
    benchmarking_core("io_uring", fname, &data, chunk_size, io_uring);
    benchmarking_core("io_uring2", fname, &data, chunk_size, io_uring2);
    benchmarking_core("io_uring3", fname, &data, chunk_size, io_uring3);
}

fn main() {
    let fname = "/data/io_benckmarking.data";
    benckmark(fname);
    remove_file(fname);
}
