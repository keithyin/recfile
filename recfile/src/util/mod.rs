pub mod buffer;
pub mod stack;

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

pub const fn get_page_size() -> usize {
    // unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
    4096
}

pub fn get_buffer_size() -> usize {
    4 * 1024 * 1024
}
