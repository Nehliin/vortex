use std::{
    io,
    os::{fd::AsRawFd, unix::fs::MetadataExt},
    path::Path,
    ptr,
};

pub struct MmapFile {
    addr: ptr::NonNull<libc::c_void>,
    len: usize,
}

impl MmapFile {
    pub fn create(path: impl AsRef<Path>, size: usize) -> io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(path)?;

        let current_size = file.metadata()?.size();
        if current_size < size as u64 {
            unsafe {
                let res = libc::fallocate(
                    file.as_raw_fd(),
                    0,
                    current_size as i64,
                    size as i64 - current_size as i64,
                );
                if res != 0 {
                    panic!("Failed to fallocate");
                }
            }
        }

        assert_eq!(file.metadata()?.size(), size as _);
        unsafe {
            match libc::mmap(
                ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED_VALIDATE | libc::MAP_POPULATE,
                file.as_raw_fd(),
                0,
            ) {
                libc::MAP_FAILED => Err(io::Error::last_os_error()),
                addr => {
                    // here, `mmap` will never return null
                    let addr = ptr::NonNull::new_unchecked(addr);
                    Ok(Self { addr, len: size })
                }
            }
        }
    }

    // Deref instead?
    pub fn get_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.addr.as_ptr() as _, self.len) }
    }
}

impl Drop for MmapFile {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.addr.as_ptr(), self.len);
        }
    }
}
