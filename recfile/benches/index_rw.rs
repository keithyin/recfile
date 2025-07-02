use criterion;

fn bench_index_reader(c: &mut criterion::Criterion) {
    c.bench_function("index_read", |b| {
        b.iter(|| {
            // Simulate reading an index
            std::thread::sleep(std::time::Duration::from_millis(100));
        });
    });
}

criterion::criterion_group!(benches, bench_index_reader);
criterion::criterion_main!(benches);
