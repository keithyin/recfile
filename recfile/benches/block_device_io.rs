use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
};

use criterion;
use recfile::util::{aligned_alloc, get_page_size};
use tempfile;

fn bench_block_device_io(c: &mut criterion::Criterion) {
    let mut file = fs::File::open("/dev/random").expect("open /dev/random failed");
    let data_size = 1024 * 1024 * 1024 * 10;

    let mut data = aligned_alloc(data_size, get_page_size());
    file.read_exact(&mut data).expect("read_exact failed");

    let mut group = c.benchmark_group("throughput");
    group.throughput(criterion::Throughput::Bytes(data_size as u64));

    let buffer_size = 1024 * 1024;

    group.bench_function("buffered_io", |b| {
        b.iter(|| {
            let mut tmp_file = tempfile::tempfile().expect("create tempfile failed");
            for idx in 0..(data_size / buffer_size) {
                let start = idx * buffer_size;
                let end = start + buffer_size;
                tmp_file
                    .write_all(&data[start..end])
                    .expect("write_all failed");
            }
            tmp_file.sync_all().expect("sync_all failed");
        })
    });

    group.bench_function("direct_io", |b| {
        b.iter(|| {
            let tmp_file = tempfile::NamedTempFile::new().expect("create NamedTempFile failed");
            let tmp_file_path = tmp_file.path();
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(tmp_file_path)
                .expect("failed to open NamedTempFile");

            for idx in 0..(data_size / buffer_size) {
                let start = idx * buffer_size;
                let end = start + buffer_size;
                file.write_all(&data[start..end]).expect("write_all failed");
            }

            file.sync_all().expect("sync_all failed");

        });
    });
}

criterion::criterion_group!(benches, bench_block_device_io);
criterion::criterion_main!(benches);
