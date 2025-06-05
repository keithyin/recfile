use std::time;

fn read(data: &Vec<usize>) {
    let instant = time::Instant::now();
    let mut res = 0_usize;
    let num_loop = 16 * 1024 * 1024 * 1024 / 8 / data.len();
    (0..num_loop).into_iter().for_each(|_| {
        // res += data.iter().copied().fold(0_usize, |acc, value| {
        //     acc + unsafe { std::ptr::read_volatile(&value as *const _) }
        // });
        data.iter().for_each(|v| unsafe {
            res += std::ptr::read_volatile(v as *const _);
        });
    });

    let elapsed = instant.elapsed().as_secs_f64();
    let bytes = (data.len() * 8 * num_loop) as f64;
    let data_kb = data.len() * 8 / 1024;
    let speed = bytes / elapsed / 1024.0 / 1024.0 / 1024.0;
    println!("read: data_kb:{data_kb}, num_loop:{num_loop}, speed:{speed:.3}GB/s");
    std::hint::black_box(res);
}

fn cache_read_speed(data_size: usize) {
    let data = (0..data_size).into_iter().collect::<Vec<_>>();
    read(&data);
    // println!("res:{res}");
}

fn write(data: &mut Vec<usize>) {
    let instant = time::Instant::now();
    let num_loop = 16 * 1024 * 1024 * 1024 / 8 / data.len();
    (0..num_loop).into_iter().for_each(|_| {
        // res += data.iter().copied().fold(0_usize, |acc, value| {
        //     acc + unsafe { std::ptr::read_volatile(&value as *const _) }
        // });
        data.iter_mut().enumerate().for_each(|(idx, v)| unsafe {
            std::ptr::write_volatile(v as *mut usize, idx);
        });
    });

    let elapsed = instant.elapsed().as_secs_f64();
    let bytes = (data.len() * 8 * num_loop) as f64;
    let data_kb = data.len() * 8 / 1024;
    let speed = bytes / elapsed / 1024.0 / 1024.0 / 1024.0;
    println!("write: data_kb:{data_kb}, num_loop:{num_loop}, speed:{speed:.3}GB/s");
}

fn cache_write_speed(data_size: usize) {
    let mut data = (0..data_size).into_iter().collect::<Vec<_>>();
    write(&mut data);
    // println!("res:{res}");
}

fn main() {
    for data_size in [
        2 * 1024,
        2 * 1024,
        3 * 1024,
        4 * 1024, // L1d 32KB
        5 * 1024,
        20 * 1024,
        50 * 1024,
        60 * 1024,
        64 * 1024,     // L2 512 k
        2 * 64 * 1024, // 2 * 512k
        3 * 64 * 1024, // 3 * 512k
        1024 * 1024,   // 8M (L3 16M)
        1500 * 1024,
        2 * 1024 * 1024, // 16M
        3 * 1024 * 1024, // 24M
        4 * 1024 * 1024, // 24M
        5 * 1024 * 1024, // 40M
    ] {
        cache_read_speed(data_size);
        cache_write_speed(data_size);
    }

    println!("Hello, world!");
}
