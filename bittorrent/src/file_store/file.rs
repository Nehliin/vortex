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

impl File {
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
        Ok(Self {
            file: file.into(),
            len: size,
        })
    }

    pub fn as_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }
}
