use std::io;

use slab::Slab;

use crate::buf_ring::AnonymousMmap;

pub struct BufferPool {
    free: Vec<usize>,
    buffer_size: usize,
    allocated_buffers: Slab<AnonymousMmap>,
}

pub struct Buffer<'a> {
    index: usize,
    inner: &'a mut [u8],
    cursor: usize,
}

impl Buffer<'_> {
    #[inline]
    pub fn index(&self) -> usize {
        self.index
    }

    pub fn get_writable_slice(&mut self, len: usize) -> io::Result<&mut [u8]> {
        if len >= self.inner.len() + self.cursor {
            return Err(io::ErrorKind::ResourceBusy.into());
        }
        let result = &mut self.inner[self.cursor..self.cursor + len];
        self.cursor += len;
        Ok(result)
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.inner[..self.cursor]
    }
}

impl BufferPool {
    pub fn new(entries: usize, buf_size: usize) -> Self {
        Self {
            free: Vec::with_capacity(entries),
            buffer_size: buf_size,
            allocated_buffers: Slab::with_capacity(entries),
        }
    }
    pub fn get_buffer(&mut self) -> Buffer<'_> {
        match self.free.pop() {
            Some(free_idx) => Buffer {
                index: free_idx,
                inner: &mut self.allocated_buffers[free_idx],
                cursor: 0,
            },
            None => {
                let buf = AnonymousMmap::new(self.buffer_size).expect("memory to be available");
                let buf_entry = self.allocated_buffers.vacant_entry();
                let buf_index = buf_entry.key();
                Buffer {
                    index: buf_index,
                    inner: buf_entry.insert(buf),
                    cursor: 0,
                }
            }
        }
    }

    pub fn return_buffer(&mut self, index: usize) {
        self.free.push(index);
    }
}
