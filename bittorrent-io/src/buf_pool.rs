use slab::Slab;

pub struct BufferPool {
    free: Vec<usize>,
    buffer_size: usize,
    allocated_buffers: Slab<Box<[u8]>>,
}

impl BufferPool {
    pub fn new(entries: usize, buf_size: usize) -> Self {
        Self {
            free: Vec::with_capacity(entries),
            buffer_size: buf_size,
            allocated_buffers: Slab::with_capacity(entries),
        }
    }
    pub fn get_buffer(&mut self) -> (usize, &mut [u8]) {
        match self.free.pop() {
            Some(free_idx) => (free_idx, &mut self.allocated_buffers[free_idx]),
            None => {
                let buf = vec![0u8; self.buffer_size].into_boxed_slice();
                let buf_entry = self.allocated_buffers.vacant_entry();
                let buf_index = buf_entry.key();
                (buf_index, buf_entry.insert(buf))
            }
        }
    }

    pub fn return_buffer(&mut self, index: usize) {
        self.free.push(index);
    }
}
