use bitvec::vec::BitVec;
use bytes::BufMut;

use crate::buf_ring::AnonymousMmap;

pub struct BufferPool {
    free: BitVec,
    buffer_size: usize,
    pool: Vec<Option<AnonymousMmap>>,
}

#[derive(Debug)]
pub struct Buffer {
    index: usize,
    inner: AnonymousMmap,
    cursor: usize,
    #[cfg(feature = "metrics")]
    time_taken: std::time::Instant,
}

impl Buffer {
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.inner[..self.cursor]
    }
}

unsafe impl BufMut for Buffer {
    fn remaining_mut(&self) -> usize {
        self.inner.len() - self.cursor
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.cursor += cnt
    }

    fn chunk_mut(&mut self) -> &mut bytes::buf::UninitSlice {
        bytes::buf::UninitSlice::new(&mut self.inner[self.cursor..])
    }
}

impl BufferPool {
    pub fn new(entries: usize, buf_size: usize) -> Self {
        let mut pool = Vec::with_capacity(entries);
        for _ in 0..entries {
            pool.push(Some(
                AnonymousMmap::new(buf_size).expect("memory to be available"),
            ));
        }
        Self {
            free: BitVec::repeat(true, entries),
            buffer_size: buf_size,
            pool,
        }
    }

    pub fn get_buffer(&mut self) -> Buffer {
        if let Some(free_index) = self.free.first_one() {
            self.free.set(free_index, false);
            Buffer {
                index: free_index,
                inner: self.pool[free_index]
                    .take()
                    .expect("Free list out of sync with buffer pool"),
                cursor: 0,
                #[cfg(feature = "metrics")]
                time_taken: std::time::Instant::now(),
            }
        } else {
            // resize
            let pool_size = self.pool.len();
            let new_size = (pool_size + 1).next_power_of_two();
            self.pool.resize_with(new_size, || {
                Some(AnonymousMmap::new(self.buffer_size).expect("memory to be available"))
            });
            self.free.resize(new_size, true);
            self.get_buffer()
        }
    }

    pub fn free_buffers(&self) -> usize {
        self.free.count_ones()
    }

    pub fn total_buffers(&self) -> usize {
        self.pool.len()
    }

    pub fn return_buffer(&mut self, buffer: Buffer) {
        #[cfg(feature = "metrics")]
        {
            use metrics::histogram;
            let histogram = histogram!("buffer_lifetime_ms");
            histogram.record(buffer.time_taken.elapsed().as_millis() as u32);
        }
        self.free.set(buffer.index, true);
        self.pool[buffer.index] = Some(buffer.inner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn test_buffer_index() {
        let mut pool = BufferPool::new(2, 1024);
        let buffer = pool.get_buffer();
        let index = buffer.index;

        // First buffer should have index 0
        assert_eq!(index, 0);
    }

    #[test]
    fn test_buffer_as_slice_initially_empty() {
        let mut pool = BufferPool::new(1, 1024);
        let buffer = pool.get_buffer();

        // Initial slice should be empty (cursor at 0)
        assert_eq!(buffer.as_slice().len(), 0);
    }

    #[test]
    fn test_buffer_remaining_mut() {
        let mut pool = BufferPool::new(1, 1024);
        let buffer = pool.get_buffer();

        // Should have full capacity available
        assert_eq!(buffer.remaining_mut(), 1024);
    }

    #[test]
    fn test_buffer_write_and_advance() {
        let mut pool = BufferPool::new(1, 1024);
        let mut buffer = pool.get_buffer();

        // Write 100 bytes
        buffer.put_slice(&[42u8; 100]);

        // as_slice should now return 100 bytes
        assert_eq!(buffer.as_slice().len(), 100);
        assert_eq!(buffer.remaining_mut(), 924);
    }

    #[test]
    fn test_buffer_multiple_writes() {
        let mut pool = BufferPool::new(1, 1024);
        let mut buffer = pool.get_buffer();

        // Write first 50 bytes
        buffer.put_slice(&[1u8; 50]);
        assert_eq!(buffer.as_slice().len(), 50);

        // Write another 50 bytes
        buffer.put_slice(&[2u8; 50]);
        assert_eq!(buffer.as_slice().len(), 100);

        // Verify remaining capacity
        assert_eq!(buffer.remaining_mut(), 924);
    }

    #[test]
    fn test_buffer_writes_persist() {
        let mut pool = BufferPool::new(1, 1024);
        let mut buffer = pool.get_buffer();

        // Write specific data
        buffer.put_slice(&[1, 2, 3, 4]);
        buffer.put_slice(&[5, 6, 7, 8]);

        // Verify as_slice contains both writes
        let data = buffer.as_slice();
        assert_eq!(data, &[1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn test_buffer_exact_capacity() {
        let mut pool = BufferPool::new(1, 100);
        let mut buffer = pool.get_buffer();

        // Write exact buffer size
        buffer.put_slice(&[0u8; 100]);
        assert_eq!(buffer.as_slice().len(), 100);
        assert_eq!(buffer.remaining_mut(), 0);
    }

    #[test]
    fn test_buffer_chunk_mut() {
        let mut pool = BufferPool::new(1, 1024);
        let mut buffer = pool.get_buffer();

        // Write some data first
        buffer.put_slice(&[1, 2, 3, 4]);

        // Get mutable chunk for remaining space
        let chunk = buffer.chunk_mut();
        assert_eq!(chunk.len(), 1020);
    }

    #[test]
    fn test_buffer_pool_reuse() {
        let mut pool = BufferPool::new(2, 1024);

        // Get first buffer
        let buffer1 = pool.get_buffer();
        let index1 = buffer1.index;

        // Return it to the pool
        pool.return_buffer(buffer1);

        // Get another buffer - should reuse the returned one
        let buffer2 = pool.get_buffer();
        assert_eq!(buffer2.index, index1);
    }

    #[test]
    fn test_buffer_pool_multiple_buffers() {
        let mut pool = BufferPool::new(3, 512);

        // Get buffers one at a time and collect their indices
        let idx1 = pool.get_buffer().index;
        let idx2 = pool.get_buffer().index;
        let idx3 = pool.get_buffer().index;

        // All buffers should have different indices
        assert_ne!(idx1, idx2);
        assert_ne!(idx1, idx3);
        assert_ne!(idx2, idx3);
    }

    #[test]
    fn test_buffer_pool_pre_allocation() {
        let mut pool = BufferPool::new(2, 256);

        // Pool pre-allocates all buffers
        assert_eq!(pool.pool.len(), 2);

        // Getting buffers doesn't change pool size (until we exceed capacity)
        let _buffer1 = pool.get_buffer();
        assert_eq!(pool.pool.len(), 2);

        let _buffer2 = pool.get_buffer();
        assert_eq!(pool.pool.len(), 2);
    }

    #[test]
    fn test_buffer_zero_length_write() {
        let mut pool = BufferPool::new(1, 1024);
        let mut buffer = pool.get_buffer();

        // Write zero bytes
        buffer.put_slice(&[]);
        assert_eq!(buffer.as_slice().len(), 0);
    }

    #[test]
    fn test_buffer_interleaved_operations() {
        let mut pool = BufferPool::new(1, 1024);
        let mut buffer = pool.get_buffer();

        // Write, check as_slice, repeat
        buffer.put_slice(&[0u8; 10]);
        assert_eq!(buffer.as_slice().len(), 10);

        buffer.put_slice(&[0u8; 20]);
        assert_eq!(buffer.as_slice().len(), 30);

        buffer.put_slice(&[0u8; 5]);
        assert_eq!(buffer.as_slice().len(), 35);
    }

    #[test]
    fn test_buffer_pool_growth() {
        let mut pool = BufferPool::new(2, 256);

        // Initial size is 2
        assert_eq!(pool.pool.len(), 2);

        // Get all pre-allocated buffers
        let buf1 = pool.get_buffer();
        let buf2 = pool.get_buffer();

        // Getting a third buffer should trigger growth
        let buf3 = pool.get_buffer();

        // Pool should grow to next power of 2 (from 2 to 4)
        assert_eq!(pool.pool.len(), 4);

        // All buffers should have different indices
        assert_ne!(buf1.index, buf2.index);
        assert_ne!(buf1.index, buf3.index);
        assert_ne!(buf2.index, buf3.index);
    }

    #[test]
    fn test_buffer_pool_multiple_growth_cycles() {
        let mut pool = BufferPool::new(1, 64);

        let mut buffers = Vec::new();

        // First buffer - pool size 1
        buffers.push(pool.get_buffer());
        assert_eq!(pool.pool.len(), 1);

        // Second buffer - should grow to 2
        buffers.push(pool.get_buffer());
        assert_eq!(pool.pool.len(), 2);

        // Third buffer - should grow to 4
        buffers.push(pool.get_buffer());
        assert_eq!(pool.pool.len(), 4);

        // Fourth buffer - still 4
        buffers.push(pool.get_buffer());
        assert_eq!(pool.pool.len(), 4);

        // Fifth buffer - should grow to 8
        buffers.push(pool.get_buffer());
        assert_eq!(pool.pool.len(), 8);
    }

    #[test]
    fn test_buffer_return_resets_cursor() {
        let mut pool = BufferPool::new(1, 1024);

        // Get buffer and write to it
        let mut buffer = pool.get_buffer();
        let index = buffer.index;
        buffer.put_slice(&[0u8; 100]);
        assert_eq!(buffer.as_slice().len(), 100);

        // Return buffer
        pool.return_buffer(buffer);

        // Get the same buffer again
        let buffer2 = pool.get_buffer();
        assert_eq!(buffer2.index, index);

        // Cursor should be reset to 0
        assert_eq!(buffer2.as_slice().len(), 0);
    }

    #[test]
    fn test_buffer_pool_return_and_reuse_multiple() {
        let mut pool = BufferPool::new(2, 512);

        // Get two buffers
        let buf1 = pool.get_buffer();
        let buf2 = pool.get_buffer();
        let idx1 = buf1.index;
        let idx2 = buf2.index;

        // Return both
        pool.return_buffer(buf1);
        pool.return_buffer(buf2);

        // Get two more - should reuse the same indices
        let buf3 = pool.get_buffer();
        let buf4 = pool.get_buffer();

        // Should get the same indices (order may vary)
        let new_indices = [buf3.index, buf4.index];
        assert!(new_indices.contains(&idx1));
        assert!(new_indices.contains(&idx2));
    }

    #[test]
    fn test_buffer_pool_partial_return() {
        let mut pool = BufferPool::new(3, 256);

        // Get three buffers
        let buf1 = pool.get_buffer();
        let buf2 = pool.get_buffer();
        let buf3 = pool.get_buffer();
        let idx2 = buf2.index;

        // Return only the middle one
        pool.return_buffer(buf2);

        // Get a new buffer - should reuse buf2's index
        let buf4 = pool.get_buffer();
        assert_eq!(buf4.index, idx2);

        // Still holding buf1, buf3, buf4 - pool shouldn't grow yet
        assert_eq!(pool.pool.len(), 3);

        // Clean up
        pool.return_buffer(buf1);
        pool.return_buffer(buf3);
        pool.return_buffer(buf4);
    }

    #[test]
    fn test_buffer_data_written_readable() {
        let mut pool = BufferPool::new(1, 1024);
        let mut buffer = pool.get_buffer();

        // Write sequential data
        for i in 0..10u8 {
            buffer.put_u8(i * 10);
        }

        // Verify all data is readable
        let data = buffer.as_slice();
        assert_eq!(data.len(), 10);
        for (i, item) in data.iter().enumerate().take(10) {
            assert_eq!(*item, i as u8 * 10);
        }
    }

    #[test]
    fn test_buffer_pool_metrics_after_growth() {
        let mut pool = BufferPool::new(2, 128);

        assert_eq!(pool.free_buffers(), 2);
        assert_eq!(pool.total_buffers(), 2);

        // Get all buffers
        let buf1 = pool.get_buffer();
        let buf2 = pool.get_buffer();

        assert_eq!(pool.free_buffers(), 0);
        assert_eq!(pool.total_buffers(), 2);

        // Get one more to trigger growth
        let buf3 = pool.get_buffer();

        // Pool grew to 4, and we have 3 buffers out
        assert_eq!(pool.free_buffers(), 1);
        assert_eq!(pool.total_buffers(), 4);

        // Return all buffers
        pool.return_buffer(buf1);
        pool.return_buffer(buf2);
        pool.return_buffer(buf3);

        assert_eq!(pool.free_buffers(), 4);
        assert_eq!(pool.total_buffers(), 4);
    }
}
