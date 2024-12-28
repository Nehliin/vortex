use core::panic;
use std::{io, ptr, sync::atomic::AtomicU16};

use io_uring::{types, Submitter};

/// An anonymous region of memory mapped using `mmap(2)`, not backed by a file
/// but that is guaranteed to be page-aligned and zero-filled.
struct AnonymousMmap {
    addr: ptr::NonNull<libc::c_void>,
    len: usize,
}

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

impl Drop for AnonymousMmap {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.addr.as_ptr(), self.len);
        }
    }
}

pub type Bgid = u16;
pub type Bid = u16;

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
    pub fn new(bgid: Bgid, entries: usize, buf_len: usize) -> io::Result<Self> {
        if entries & (entries - 1) != 0 || buf_len == 0 || entries == 0 {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }

        let ring_mem_size = entries * std::mem::size_of::<types::BufRingEntry>();
        let ring_memory = AnonymousMmap::new(ring_mem_size)?;
        let buffer_memory = AnonymousMmap::new(entries * buf_len)?;

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
            let mem_start = self.buffer_memory.addr.add(bid as usize * self.buf_len).as_ptr();
            std::slice::from_raw_parts(mem_start as *const _, self.buf_len)
        }
    }

    pub fn register(&mut self, submitter: &Submitter<'_>) -> io::Result<()> {
        // Safety: The ring, represented by the ring_start and the ring_entries remains valid until
        // it is unregistered. The backing store is an AnonymousMmap which remains valid until it
        // is dropped which in this case, is when Self is dropped.
        let res = unsafe {
            submitter.register_buf_ring(
                self.ring_memory.addr.as_ptr() as _,
                self.entries as u16,
                self.bgid,
            )
        };

        if let Err(e) = res {
            match e.raw_os_error() {
                Some(libc::EINVAL) => {
                    // using buf_ring requires kernel 5.19 or greater.
                    return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("buf_ring.register returned {}, most likely indicating this kernel is not 5.19+", e),
                            ));
                }
                Some(libc::EEXIST) => {
                    // Registering a duplicate bgid is not allowed. There is an `unregister`
                    // operations that can remove the first, but care must be taken that there
                    // are no outstanding operations that will still return a buffer from that
                    // one.
                    return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "buf_ring.register returned `{}`, indicating the attempted buffer group id {} was already registered",
                            e,
                            self.bgid),
                        ));
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "buf_ring.register returned `{}` for group id {}",
                            e, self.bgid
                        ),
                    ));
                }
            }
        };

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

        res
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
       if self.is_registerd {
            panic!("Must unregister before dropping");
        } 
    }
}
