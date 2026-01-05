use std::{
    io,
    os::{
        fd::{AsRawFd, OwnedFd, RawFd},
        unix::fs::MetadataExt,
    },
    path::Path,
};

#[derive(Debug)]
pub struct File {
    file: OwnedFd,
    len: usize,
}

// SAFETY: You can't write to the file witout &mut access to the file except when writing through the ptr.
// that is only exposed to this module which does it via the WritablePieceFileViews that are
// themselves unsafe and require that the views do not overlap and are XOR to any readable views
unsafe impl Sync for File {}

// SAFETY: There is nothing inherit to the MmapFile struct that can't be sent to other threads
// safely
unsafe impl Send for File {}

impl File {
    pub fn create(path: impl AsRef<Path>, size: usize) -> io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(path)?;

        // let page_size = match unsafe { libc::sysconf(libc::_SC_PAGESIZE) } {
        //     -1 => panic!("Failed to get page size"),
        //     size => size as usize,
        // };

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
        Ok(Self {
            file: file.into(),
            len: size,
        })
        // unsafe {
        //     match libc::mmap(
        //         ptr::null_mut(),
        //         size,
        //         libc::PROT_READ | libc::PROT_WRITE,
        //         libc::MAP_SHARED_VALIDATE | libc::MAP_POPULATE,
        //         file.as_raw_fd(),
        //         0,
        //     ) {
        //         libc::MAP_FAILED => Err(io::Error::last_os_error()),
        //         addr => {
        //             // here, `mmap` will never return null
        //             let addr = ptr::NonNull::new_unchecked(addr);
        //             Ok(Self {
        //                 addr,
        //                 len: size,
        //                 page_size,
        //             })
        //         }
        //     }
        // }
    }

    pub fn as_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    // pub fn sync(&self, start: usize, end: usize) -> io::Result<()> {
    //     if end <= start || end > self.len {
    //         return Err(std::io::ErrorKind::InvalidInput.into());
    //     }
    //     let unaligned_addr = self.addr.as_ptr() as usize + start;
    //     // Addr is already page aligned
    //     let page_aligned_ptr = if start == 0 {
    //         self.addr
    //     } else {
    //         self.addr.map_addr(|addr| {
    //             let offset = addr.get() + start;
    //             // addr is guaranteed to be non zero and something + nonzero is still nonzero
    //             unsafe {
    //                 debug_assert!(offset.next_multiple_of(self.page_size) - self.page_size > 0);
    //                 NonZero::new_unchecked(offset.next_multiple_of(self.page_size) - self.page_size)
    //             }
    //         })
    //     };
    //     // Take alignment shift into account when specifiying length to msync
    //     let align_diff = unaligned_addr - page_aligned_ptr.as_ptr() as usize;
    //     let length = (end - start) + align_diff;
    //     // TODO: consider MS_INVALIDATE, needed for streaming stuff
    //     let ret = unsafe { libc::msync(page_aligned_ptr.as_ptr() as _, length, libc::MS_SYNC) };
    //     if ret < 0 {
    //         let err_code = unsafe { libc::__errno_location().read() };
    //         Err(std::io::Error::from_raw_os_error(err_code))
    //     } else {
    //         Ok(())
    //     }
    // }
}
