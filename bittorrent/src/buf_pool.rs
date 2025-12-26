use std::io;

use bitvec::vec::BitVec;

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
    pub fn index(&self) -> usize {
        self.index
    }

    pub fn get_writable_slice(&mut self, len: usize) -> io::Result<&mut [u8]> {
        if self.cursor + len > self.inner.len() {
            return Err(io::ErrorKind::StorageFull.into());
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

    #[cfg(feature = "metrics")]
    pub fn free_buffers(&self) -> usize {
        self.free.count_ones()
    }

    #[cfg(feature = "metrics")]
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

    #[test]
    fn test_buffer_index() {
        let mut pool = BufferPool::new(2, 1024);
        let buffer = pool.get_buffer();
        let index = buffer.index();

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
    fn test_buffer_get_writable_slice() {
        let mut pool = BufferPool::new(1, 1024);
        let mut buffer = pool.get_buffer();

        // Get a writable slice of 100 bytes
        let slice = buffer.get_writable_slice(100).expect("should succeed");
        assert_eq!(slice.len(), 100);

        // as_slice should now return 100 bytes
        assert_eq!(buffer.as_slice().len(), 100);
    }

    #[test]
    fn test_buffer_get_writable_slice_advances_cursor() {
        let mut pool = BufferPool::new(1, 1024);
        let mut buffer = pool.get_buffer();

        // Get first slice
        let slice1 = buffer.get_writable_slice(50).expect("should succeed");
        assert_eq!(slice1.len(), 50);

        // Get second slice
        let slice2 = buffer.get_writable_slice(50).expect("should succeed");
        assert_eq!(slice2.len(), 50);

        // as_slice should now return 100 bytes total
        assert_eq!(buffer.as_slice().len(), 100);
    }

    #[test]
    fn test_buffer_get_writable_slice_writes_persist() {
        let mut pool = BufferPool::new(1, 1024);
        let mut buffer = pool.get_buffer();

        // Write to first slice
        {
            let slice = buffer.get_writable_slice(4).expect("should succeed");
            slice.copy_from_slice(&[1, 2, 3, 4]);
        }

        // Write to second slice
        {
            let slice = buffer.get_writable_slice(4).expect("should succeed");
            slice.copy_from_slice(&[5, 6, 7, 8]);
        }

        // Verify as_slice contains both writes
        let data = buffer.as_slice();
        assert_eq!(data, &[1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn test_buffer_get_writable_slice_exact_capacity() {
        let mut pool = BufferPool::new(1, 100);
        let mut buffer = pool.get_buffer();

        // Request exact buffer size
        let slice = buffer.get_writable_slice(100).expect("should succeed");
        assert_eq!(slice.len(), 100);
        assert_eq!(buffer.as_slice().len(), 100);
    }

    #[test]
    fn test_buffer_get_writable_slice_exceeds_capacity() {
        let mut pool = BufferPool::new(1, 100);
        let mut buffer = pool.get_buffer();

        // Request more than buffer size
        let result = buffer.get_writable_slice(101);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::StorageFull);
    }

    #[test]
    fn test_buffer_get_writable_slice_exceeds_remaining_capacity() {
        let mut pool = BufferPool::new(1, 100);
        let mut buffer = pool.get_buffer();

        // Use 60 bytes
        buffer.get_writable_slice(60).expect("should succeed");

        // Try to get 50 more bytes (would exceed capacity)
        let result = buffer.get_writable_slice(50);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::StorageFull);
    }

    #[test]
    fn test_buffer_get_writable_slice_fits_remaining_capacity() {
        let mut pool = BufferPool::new(1, 100);
        let mut buffer = pool.get_buffer();

        // Use 60 bytes
        buffer.get_writable_slice(60).expect("should succeed");

        // Get exactly the remaining 40 bytes
        let slice = buffer.get_writable_slice(40).expect("should succeed");
        assert_eq!(slice.len(), 40);
        assert_eq!(buffer.as_slice().len(), 100);
    }

    #[test]
    fn test_buffer_pool_reuse() {
        let mut pool = BufferPool::new(2, 1024);

        // Get first buffer
        let buffer1 = pool.get_buffer();
        let index1 = buffer1.index();

        // Return it to the pool
        unsafe {
            pool.return_buffer(index1);
        }

        // Get another buffer - should reuse the returned one
        let buffer2 = pool.get_buffer();
        assert_eq!(buffer2.index(), index1);
    }

    #[test]
    fn test_buffer_pool_multiple_buffers() {
        let mut pool = BufferPool::new(3, 512);

        // Get buffers one at a time and collect their indices
        let idx1 = pool.get_buffer().index();
        let idx2 = pool.get_buffer().index();
        let idx3 = pool.get_buffer().index();

        // All buffers should have different indices
        assert_ne!(idx1, idx2);
        assert_ne!(idx1, idx3);
        assert_ne!(idx2, idx3);
    }

    #[test]
    fn test_buffer_pool_allocation_on_demand() {
        let mut pool = BufferPool::new(2, 256);

        // Pool starts with no allocated buffers
        assert_eq!(pool.pool.len(), 0);

        // First get_buffer allocates
        let _buffer1 = pool.get_buffer();
        assert_eq!(pool.pool.len(), 1);

        // Second get_buffer allocates
        let _buffer2 = pool.get_buffer();
        assert_eq!(pool.pool.len(), 2);
    }

    #[test]
    fn test_buffer_zero_length_slice() {
        let mut pool = BufferPool::new(1, 1024);
        let mut buffer = pool.get_buffer();

        // Get a zero-length slice
        let slice = buffer.get_writable_slice(0).expect("should succeed");
        assert_eq!(slice.len(), 0);
        assert_eq!(buffer.as_slice().len(), 0);
    }

    #[test]
    fn test_buffer_interleaved_operations() {
        let mut pool = BufferPool::new(1, 1024);
        let mut buffer = pool.get_buffer();

        // Get slice, check as_slice, repeat
        buffer.get_writable_slice(10).expect("should succeed");
        assert_eq!(buffer.as_slice().len(), 10);

        buffer.get_writable_slice(20).expect("should succeed");
        assert_eq!(buffer.as_slice().len(), 30);

        buffer.get_writable_slice(5).expect("should succeed");
        assert_eq!(buffer.as_slice().len(), 35);
    }
}
