use bincode::config::Configuration;

pub mod v1;
pub mod v2;

pub fn get_bincode_cfg() -> Configuration {
    bincode::config::standard()
        .with_little_endian()
        .with_variable_int_encoding()
}

pub fn aligned_alloc(size: usize, page_size: usize) -> Vec<u8> {
    use std::ptr;
    let mut ptr: *mut u8 = ptr::null_mut();
    unsafe {
        let ret = libc::posix_memalign(&mut ptr as *mut _ as *mut _, page_size, size);
        if ret != 0 {
            panic!("posix_memalign failed");
        }
        Vec::from_raw_parts(ptr, size, size)
    }
}
