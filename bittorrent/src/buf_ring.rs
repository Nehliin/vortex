use core::panic;
use std::{
    io,
    ops::{Deref, DerefMut},
    ptr,
    sync::atomic::AtomicU16,
};

use io_uring::{Submitter, types};

/// An anonymous region of memory mapped using `mmap(2)`, not backed by a file
/// but that is guaranteed to be page-aligned and zero-filled.
#[derive(Debug)]
pub struct AnonymousMmap {
    addr: ptr::NonNull<libc::c_void>,
    len: usize,
}

unsafe impl Send for AnonymousMmap {}
unsafe impl Sync for AnonymousMmap {}

impl AnonymousMmap {
    /// Allocate `len` bytes that are page aligned and zero-filled.
    pub fn new(len: usize) -> io::Result<AnonymousMmap> {
        unsafe {
            match libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_PRIVATE | libc::MAP_POPULATE,
                -1,
                0,
            ) {
                libc::MAP_FAILED => Err(io::Error::last_os_error()),
                addr => {
                    // here, `mmap` will never return null
                    let addr = ptr::NonNull::new_unchecked(addr);
                    Ok(AnonymousMmap { addr, len })
                }
            }
        }
    }
}

impl Deref for AnonymousMmap {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { core::slice::from_raw_parts(self.addr.as_ptr().cast(), self.len) }
    }
}

impl DerefMut for AnonymousMmap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { core::slice::from_raw_parts_mut(self.addr.as_ptr().cast(), self.len) }
    }
}

impl Drop for AnonymousMmap {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.addr.as_ptr(), self.len);
        }
    }
}

pub type Bgid = u16;
pub type Bid = u16;

// Largest power-of-two capacity accepted by buffer-ring registration.
pub(crate) const MAX_BUFFER_RING_ENTRIES: usize = 32_768;

pub struct BufferRing {
    bgid: Bgid,
    ring_memory: AnonymousMmap,
    // individual buf len
    buf_len: usize,
    // How many buffers?
    entries: usize,
    // Backing buffer memory
    buffer_memory: AnonymousMmap,
    is_registerd: bool,
}

impl BufferRing {
    pub(crate) fn allocation_sizes(entries: usize, buf_len: usize) -> io::Result<(usize, usize)> {
        if !entries.is_power_of_two() || entries > MAX_BUFFER_RING_ENTRIES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "read_buffer_pool_size must be a power of two from 1 to {MAX_BUFFER_RING_ENTRIES}"
                ),
            ));
        }
        if buf_len == 0 || buf_len > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "network_read_buffer_size must be between 1 and {} bytes",
                    u32::MAX
                ),
            ));
        }
        let overflow = || {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Buffer-ring allocation size overflow",
            )
        };
        let descriptors = entries
            .checked_mul(std::mem::size_of::<types::BufRingEntry>())
            .ok_or_else(overflow)?;
        let payload = entries.checked_mul(buf_len).ok_or_else(overflow)?;
        Ok((descriptors, payload))
    }

    pub fn new(bgid: Bgid, entries: usize, buf_len: usize) -> io::Result<Self> {
        let (ring_mem_size, payload_size) = Self::allocation_sizes(entries, buf_len)?;
        let ring_memory = AnonymousMmap::new(ring_mem_size)?;
        let buffer_memory = AnonymousMmap::new(payload_size)?;

        Ok(Self {
            bgid,
            ring_memory,
            buf_len,
            entries,
            buffer_memory,
            is_registerd: false,
        })
    }

    pub fn bgid(&self) -> Bgid {
        self.bgid
    }

    pub fn get(&self, bid: Bid) -> &[u8] {
        unsafe {
            let mem_start = self
                .buffer_memory
                .addr
                .add(bid as usize * self.buf_len)
                .as_ptr();
            std::slice::from_raw_parts(mem_start as *const _, self.buf_len)
        }
    }

    /// Fill the buffer of `bid` with `data`, mimicking a recv completing into it
    #[cfg(test)]
    pub fn fill(&mut self, bid: Bid, data: &[u8]) {
        let start = bid as usize * self.buf_len;
        self.buffer_memory[start..start + data.len()].copy_from_slice(data);
    }

    pub fn register(&mut self, submitter: &Submitter<'_>) -> io::Result<()> {
        // Safety: The ring, represented by the ring_start and the ring_entries remains valid until
        // it is unregistered. The backing store is an AnonymousMmap which remains valid until it
        // is dropped which in this case, is when Self is dropped.
        unsafe {
            submitter.register_buf_ring_with_flags(
                self.ring_memory.addr.as_ptr() as _,
                self.entries as u16,
                self.bgid,
                0,
            )
        }
        .inspect_err(|error| {
            log::debug!(
                "Buffer-ring registration failed for group {}: {error}",
                self.bgid
            );
        })?;

        let mask = (self.entries - 1) as u16;
        unsafe {
            // Add the buffers after the registration
            for bid in 0..self.entries {
                let entry = self.ring_memory.addr.as_ptr() as *mut types::BufRingEntry;
                let entry = entry.add((bid as u16 & mask) as usize);
                let entry = &mut *entry;
                entry.set_addr(
                    self.buffer_memory.addr.as_ptr() as u64 + (bid * self.buf_len) as u64,
                );
                entry.set_len(self.buf_len as u32);
                entry.set_bid(bid as u16);
            }
            let tail = types::BufRingEntry::tail(
                self.ring_memory.addr.as_ptr() as *const types::BufRingEntry
            ) as *const AtomicU16;
            (*tail).store(self.entries as u16, std::sync::atomic::Ordering::Release);
        }
        self.is_registerd = true;

        Ok(())
    }

    pub fn return_bid(&mut self, bid: Bid) {
        unsafe {
            // find entry
            let tail_ptr = types::BufRingEntry::tail(
                self.ring_memory.addr.as_ptr() as *const types::BufRingEntry
            ) as *const AtomicU16;
            let tail = (*tail_ptr).load(std::sync::atomic::Ordering::Acquire);
            let ring_idx = tail & (self.entries - 1) as u16;

            // update entry
            let entry = self.ring_memory.addr.as_ptr() as *mut types::BufRingEntry;
            let entry = entry.add(ring_idx as usize);
            let entry = &mut *entry;
            entry.set_addr(
                self.buffer_memory.addr.as_ptr() as u64 + (bid as usize * self.buf_len) as u64,
            );
            entry.set_len(self.buf_len as u32);
            entry.set_bid(bid);

            (*tail_ptr).store(tail.wrapping_add(1), std::sync::atomic::Ordering::Release)
        };
    }

    pub fn unregister(&mut self, submitter: &Submitter<'_>) -> io::Result<()> {
        submitter.unregister_buf_ring(self.bgid())?;
        self.is_registerd = false;
        Ok(())
    }
}

impl Drop for BufferRing {
    fn drop(&mut self) {
        if self.is_registerd && !std::thread::panicking() {
            panic!("Must unregister before dropping");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BufferRing;

    #[test]
    fn invalid_buffer_ring_sizes_return_errors_without_allocating() {
        for (entries, length) in [(0, 16), (3, 16), (65536, 16), (16, 0), (16, usize::MAX)] {
            let error = BufferRing::new(1, entries, length)
                .err()
                .expect("invalid buffer-ring sizes must be rejected");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        }
    }
}
