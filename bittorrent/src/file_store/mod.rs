use std::{
    io,
    os::fd::RawFd,
    path::{Path, PathBuf},
    rc::Rc,
};

use file::File;
use lava_torrent::torrent::v1::Torrent;

use crate::{buf_pool::Buffer, event_loop::ConnectionId};

mod file;

#[derive(Debug)]
struct TorrentFile {
    // Index of the piece this file starts in
    start_piece: i32,
    // Offset within the start piece
    start_offset: i32,
    // Index of the piece this file ends in. If it hits a piece boundary it will be the next piece
    end_piece: i32,
    // Offset within the end piece
    end_offset: i32,
    // File handle
    file_handle: file::File,
}

#[derive(Debug, Clone, Copy)]
pub enum DiskOpType {
    Write,
    Read {
        // Connection that triggered the read
        connection_idx: ConnectionId,
        // Offset in the piece we want to read
        // this is only used to propagate the information
        // to the event so the correct subpiece can be returned
        piece_offset: i32,
    },
}

#[derive(Debug)]
pub struct DiskOp {
    pub fd: RawFd,
    pub piece_idx: i32,
    pub file_offset: usize,
    pub buffer_offset: usize,
    // write/read length
    pub operation_len: usize,
    pub op_type: DiskOpType,
    pub buffer: Rc<Buffer>,
}

// TODO: consider tracking readable/writable views
#[derive(Debug)]
pub struct FileStore {
    avg_piece_size: u64,
    files: Vec<TorrentFile>,
}

impl FileStore {
    pub fn new(root: impl AsRef<Path>, torrent_info: &Torrent) -> io::Result<Self> {
        fn new_impl(root: &Path, torrent_info: &Torrent) -> io::Result<FileStore> {
            let mut result = Vec::new();
            let mut start_piece = 0;
            let mut start_offset = 0;

            let piece_length = torrent_info.piece_length as u64;

            // why would you make me do this lava torrent?
            let files = torrent_info.files.clone().unwrap_or_else(|| {
                vec![lava_torrent::torrent::v1::File {
                    length: torrent_info.length,
                    path: PathBuf::from(torrent_info.name.clone()),
                    extra_fields: None,
                }]
            });

            let torrent_directory = root.join(&torrent_info.name);
            // Create new directory to store all files
            if let Err(err) = std::fs::create_dir_all(&torrent_directory) {
                // Do not error if they folders already exists
                if err.kind() != io::ErrorKind::AlreadyExists {
                    return Err(err);
                }
            }

            for torrent_file in files {
                let num_pieces = (torrent_file.length as u64 + start_offset as u64) / piece_length;
                let offset = (torrent_file.length as u64 + start_offset as u64) % piece_length;
                let file_path = torrent_directory.as_path().join(torrent_file.path);
                if let Some(parent_dir) = file_path.parent()
                    && let Err(err) = std::fs::create_dir_all(parent_dir)
                {
                    // Do not error if they folders already exists
                    if err.kind() != io::ErrorKind::AlreadyExists {
                        return Err(err);
                    }
                }
                let file_handle = File::create(&file_path, torrent_file.length as usize)?;

                let torrent_file = TorrentFile {
                    start_piece,
                    start_offset,
                    end_piece: start_piece + num_pieces as i32,
                    end_offset: offset as i32,
                    file_handle,
                };
                start_piece = torrent_file.end_piece;
                start_offset = torrent_file.end_offset;
                result.push(torrent_file)
            }
            Ok(FileStore {
                files: result,
                avg_piece_size: piece_length,
            })
        }
        new_impl(root.as_ref(), torrent_info)
    }

    pub fn queue_piece_disk_operation(
        &self,
        index: i32,
        data: Buffer,
        piece_len: usize,
        op_type: DiskOpType,
        // This is provided here as an argument to make State
        // which contains FileStore Send. That makes the lib easier to
        // use when writing applications
        pending_disk_operations: &mut Vec<DiskOp>,
    ) {
        let files = self
            .files
            .iter()
            .filter(|file| file.start_piece <= index && index <= file.end_piece);
        let mut piece_cursor: usize = 0;
        let buffer = Rc::new(data);
        for file in files {
            // Where in the _file_ does the piece start
            let file_index = (index - file.start_piece) as i64;
            // The offset might be negative here if the piece starts before the file
            let file_offset = file_index * self.avg_piece_size as i64 - file.start_offset as i64;
            // Where should the writing/reading start from (taking into account the already r/w parts)
            // NOTE: the r/w head should never be negative since we loop across all files in order
            // that are part of the piece. We should thus have already r/w the relevant parts
            // of the piece from the previous files to ensure we start at minimum on 0
            let current_file_cursor = file_offset + piece_cursor as i64;
            assert!(current_file_cursor >= 0);
            let current_file_cursor = current_file_cursor as usize;
            // if we are past this file, move on to the next
            if current_file_cursor >= file.file_handle.len() {
                continue;
            }
            let max_possible_operation_length =
                (file.file_handle.len() - current_file_cursor).min(piece_len - piece_cursor);
            pending_disk_operations.push(DiskOp {
                fd: file.file_handle.as_fd(),
                piece_idx: index,
                file_offset: current_file_cursor,
                buffer_offset: piece_cursor,
                operation_len: max_possible_operation_length,
                op_type,
                buffer: buffer.clone(),
            });
            piece_cursor += max_possible_operation_length;
            // Break early if we've written the entire piece
            if piece_cursor >= piece_len {
                break;
            }
        }
        // Must have written all data
        assert_eq!(piece_cursor, piece_len);
    }

    /// Synchronously check if a piece exists and has the correct hash.
    /// This is used during initialization to determine which pieces are already complete.
    /// Returns true if the piece data matches the expected hash.
    pub fn check_piece_hash_sync(
        &self,
        piece_index: i32,
        expected_hash: &[u8],
    ) -> io::Result<bool> {
        use sha1::Digest;

        let mut hasher = sha1::Sha1::new();
        let mut total_read = 0;

        let files = self
            .files
            .iter()
            .filter(|file| file.start_piece <= piece_index && piece_index <= file.end_piece);

        for file in files {
            // Where in the _file_ does the piece start
            let file_index = (piece_index - file.start_piece) as i64;
            // The offset might be negative here if the piece starts before the file
            let file_offset = file_index * self.avg_piece_size as i64 - file.start_offset as i64;
            let piece_offset_in_file = file_offset + total_read as i64;

            // we should always have read the files in order so that
            // total read ensures this offset to be greater than 0
            assert!(piece_offset_in_file >= 0);
            let piece_offset_in_file = piece_offset_in_file as u64;

            let to_read = if piece_index == file.end_piece {
                // Either the piece ends within this file, then we should read
                // to that piece offset - how much we've already read
                // (end_offset is the offset relative to the entire piece)
                file.end_offset as usize - total_read
            } else {
                // Or the piece continues past this file (or ends exactly at the end of this file),
                // in that case we read the remainder of the piece up the maximum of the file length
                (self.avg_piece_size as usize - total_read).min(file.file_handle.len())
            };

            // Nothing to read, the file ends at a piece boundary
            if to_read == 0 {
                continue;
            }

            let fd = file.file_handle.as_fd();
            let mut buffer = vec![0u8; to_read];
            let mut bytes_read = 0;

            // pread might not read all bytes in one call, so loop until we get everything
            while bytes_read < to_read {
                let result = unsafe {
                    libc::pread(
                        fd,
                        buffer.as_mut_ptr().add(bytes_read) as *mut libc::c_void,
                        to_read - bytes_read,
                        (piece_offset_in_file + bytes_read as u64) as libc::off_t,
                    )
                };

                if result < 0 {
                    return Err(io::Error::last_os_error());
                } else if result == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "unexpected EOF while reading file",
                    ));
                }

                bytes_read += result as usize;
            }

            hasher.update(&buffer);
            total_read += to_read;
        }

        Ok(hasher.finalize().as_slice() == expected_hash)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::BufMut;
    use lava_torrent::torrent::v1::TorrentBuilder;
    use rand::Rng;
    use slotmap::Key;
    use std::os::fd::FromRawFd;

    use super::*;

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(path: &str) -> Self {
            let path = format!("/tmp/{path}");
            std::fs::create_dir_all(&path).unwrap();
            let path: PathBuf = path.into();
            Self {
                path: std::fs::canonicalize(path).unwrap(),
            }
        }

        fn add_file(&self, file_path: &str, data: &[u8]) {
            let file_path = self.path.as_path().join(file_path);
            if let Some(parent) = file_path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(file_path, data).unwrap();
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.path).unwrap();
        }
    }

    fn create_buffer_with_data(data: &[u8]) -> Buffer {
        use crate::buf_pool::BufferPool;
        let mut pool = BufferPool::new(1, data.len());
        let mut buffer = pool.get_buffer();
        buffer.raw_mut_slice()[..data.len()].copy_from_slice(data);
        unsafe { buffer.advance_mut(data.len()) };
        buffer
    }

    fn verify_disk_operations(
        file_store: &FileStore,
        piece_index: i32,
        piece_data: &[u8],
        op_type: DiskOpType,
    ) -> Vec<DiskOp> {
        let mut pending_ops = Vec::new();
        let buffer = create_buffer_with_data(piece_data);

        file_store.queue_piece_disk_operation(
            piece_index,
            buffer,
            piece_data.len(),
            op_type,
            &mut pending_ops,
        );

        // Verify basic properties
        assert!(
            !pending_ops.is_empty(),
            "Should create at least one disk operation"
        );

        // Verify total operation length matches piece length
        let total_op_len: usize = pending_ops.iter().map(|op| op.operation_len).sum();
        assert_eq!(
            total_op_len,
            piece_data.len(),
            "Total operation length should equal piece length"
        );

        // Verify buffer offsets are contiguous and cover the entire buffer
        let mut expected_buffer_offset = 0;
        for op in &pending_ops {
            assert_eq!(
                op.buffer_offset, expected_buffer_offset,
                "Buffer offsets should be contiguous"
            );
            assert_eq!(
                op.piece_idx, piece_index,
                "All ops should have correct piece index"
            );
            matches!(op.op_type, ref expected_type if std::mem::discriminant(expected_type) == std::mem::discriminant(&op_type));
            expected_buffer_offset += op.operation_len;
        }

        // Verify file offsets are valid and within bounds
        // Group operations by file descriptor to check contiguity per file
        let mut ops_by_fd: HashMap<RawFd, Vec<&DiskOp>> = HashMap::new();
        for op in &pending_ops {
            ops_by_fd.entry(op.fd).or_default().push(op);
        }

        for (fd, ops) in ops_by_fd {
            // Find the corresponding file in file_store
            let file = file_store
                .files
                .iter()
                .find(|f| f.file_handle.as_fd() == fd)
                .expect("DiskOp should reference a valid file");

            // Verify each operation is within file bounds
            for op in &ops {
                assert!(
                    op.file_offset < file.file_handle.len(),
                    "File offset {} should be within file length {}",
                    op.file_offset,
                    file.file_handle.len()
                );
                assert!(
                    op.file_offset + op.operation_len <= file.file_handle.len(),
                    "Operation end ({}) should be within file length {}",
                    op.file_offset + op.operation_len,
                    file.file_handle.len()
                );
            }

            // Verify file offsets within same file are contiguous
            if ops.len() > 1 {
                let mut sorted_ops = ops.clone();
                sorted_ops.sort_by_key(|op| op.file_offset);
                for window in sorted_ops.windows(2) {
                    let (prev, curr) = (window[0], window[1]);
                    assert_eq!(
                        prev.file_offset + prev.operation_len,
                        curr.file_offset,
                        "File offsets should be contiguous within the same file"
                    );
                }
            }
        }

        pending_ops
    }

    // Execute disk operations manually for testing
    fn execute_disk_ops(disk_ops: &[DiskOp]) {
        use std::os::unix::fs::FileExt;
        for op in disk_ops {
            let file = unsafe { std::fs::File::from_raw_fd(op.fd) };
            let data_slice =
                &op.buffer.raw_slice()[op.buffer_offset..op.buffer_offset + op.operation_len];

            match op.op_type {
                DiskOpType::Write => {
                    file.write_all_at(data_slice, op.file_offset as u64)
                        .expect("Write should succeed");
                }
                DiskOpType::Read { .. } => {
                    // For reads, we'd need to write into the buffer, but we're mainly testing write logic
                    // So we'll skip actual read execution in tests
                }
            }
            // Don't drop the file - it would close the fd which we don't own
            std::mem::forget(file);
        }
    }

    fn test_multifile(
        torrent_name: &str,
        piece_len: usize,
        _subpiece_size: usize,
        file_data: HashMap<String, Vec<u8>>,
    ) {
        let torrent_tmp_dir = TempDir::new(&format!("{torrent_name}_torrent"));
        let download_tmp_dir = TempDir::new(&format!("{torrent_name}_download_dir"));
        file_data.iter().for_each(|(path, data)| {
            torrent_tmp_dir.add_file(path, data);
        });

        let torrent_info = TorrentBuilder::new(&torrent_tmp_dir.path, piece_len as i64)
            .set_name(torrent_name.to_string())
            .build()
            .unwrap();

        // why would you make me do this lava torrent?
        let files = torrent_info.files.clone().unwrap_or_else(|| {
            vec![lava_torrent::torrent::v1::File {
                length: torrent_info.length,
                path: PathBuf::from(torrent_info.name.clone()),
                extra_fields: None,
            }]
        });

        let mut all_data: Vec<_> = files
            .iter()
            .flat_map(|file| file_data.get(file.path.to_str().unwrap()).unwrap())
            .copied()
            .collect();
        let download_tmp_dir_path = download_tmp_dir.path.clone();
        let file_store = FileStore::new(&download_tmp_dir_path, &torrent_info).unwrap();

        // Test Write operations
        let mut write_data = all_data.clone();
        for (index, _piece_hash) in torrent_info.pieces.iter().enumerate() {
            let current_piece_len = piece_len.min(write_data.len());
            let (piece, remainder) = write_data.split_at(current_piece_len);
            let piece = piece.to_vec();

            // Verify write operations are created correctly
            let disk_ops =
                verify_disk_operations(&file_store, index as i32, &piece, DiskOpType::Write);

            // Execute the write operations
            execute_disk_ops(&disk_ops);

            write_data = remainder.to_vec();
        }
        assert!(write_data.is_empty());

        // Verify file contents match expected data
        for file in files.iter() {
            let path = file.path.to_str().unwrap();
            let written_data =
                std::fs::read(download_tmp_dir.path.join(&torrent_info.name).join(path)).unwrap();
            let expected_data = file_data.get(path).unwrap();
            assert_eq!(
                written_data.len(),
                expected_data.len(),
                "File {} has wrong length",
                path
            );
            assert_eq!(
                &written_data, expected_data,
                "File {} has wrong content",
                path
            );
        }

        // Test Read operations
        for (index, _piece_hash) in torrent_info.pieces.iter().enumerate() {
            let current_piece_len = piece_len.min(all_data.len());
            let (piece, remainder) = all_data.split_at(current_piece_len);
            let piece = piece.to_vec();

            // Verify read operations are created correctly
            verify_disk_operations(
                &file_store,
                index as i32,
                &piece,
                DiskOpType::Read {
                    connection_idx: ConnectionId::null(),
                    piece_offset: 0,
                },
            );

            all_data = remainder.to_vec();
        }
        assert!(all_data.is_empty());
    }

    #[test]
    fn basic_multifile_alinged() {
        let files: HashMap<String, Vec<u8>> = (0..10)
            .map(|i| {
                let data = vec![i; 1024];
                (format!("test/file_{i}.txt"), data)
            })
            .collect();
        test_multifile("basic_multifile_alinged", 256, 32, files)
    }

    #[test]
    fn small_multifile_misalinged() {
        let files: HashMap<String, Vec<u8>> = [
            ("f1.txt".to_owned(), vec![1_u8; 64]),
            ("f2.txt".to_owned(), vec![2_u8; 100]),
            ("f3.txt".to_owned(), vec![3_u8; 50]),
            ("f4.txt".to_owned(), vec![4_u8; 20]),
            ("f5.txt".to_owned(), vec![5_u8; 10]),
            ("f6.txt".to_owned(), vec![6_u8; 268]),
        ]
        .into_iter()
        .collect();
        test_multifile("small_multifile_misalinged", 256, 32, files)
    }

    #[test]
    fn small_multifile_misalinged_files_and_subpiece() {
        let files: HashMap<String, Vec<u8>> = [
            ("f1.txt".to_owned(), vec![1_u8; 64]),
            ("f2.txt".to_owned(), vec![2_u8; 100]),
            ("f3.txt".to_owned(), vec![3_u8; 50]),
            ("f4.txt".to_owned(), vec![4_u8; 20]),
            ("f5.txt".to_owned(), vec![5_u8; 10]),
            ("f6.txt".to_owned(), vec![6_u8; 268]),
        ]
        .into_iter()
        .collect();
        test_multifile(
            "small_multifile_misalinged_files_and_subpiece",
            256,
            35,
            files,
        )
    }

    #[test]
    fn multifile_not_multiple_of_piece_size() {
        let files: HashMap<String, Vec<u8>> = [
            ("f1.txt".to_owned(), vec![1_u8; 64]),
            ("f2.txt".to_owned(), vec![2_u8; 256]),
            ("f3.txt".to_owned(), vec![3_u8; 50]),
            ("f4.txt".to_owned(), vec![4_u8; 20]),
            ("f5.txt".to_owned(), vec![5_u8; 10]),
            ("f6.txt".to_owned(), vec![6_u8; 300]),
        ]
        .into_iter()
        .collect();
        test_multifile("multifile_not_multiple_of_piece_size", 256, 32, files)
    }

    #[test]
    fn multifile_misalinged_v2() {
        let files: HashMap<String, Vec<u8>> = [
            ("f1.txt".to_owned(), vec![1_u8; 84]),
            ("f2.txt".to_owned(), vec![2_u8; 114]),
            ("f3.txt".to_owned(), vec![3_u8; 134]),
            ("f4.txt".to_owned(), vec![4_u8; 24]),
        ]
        .into_iter()
        .collect();
        test_multifile("multifile_misalinged_v2", 64, 8, files)
    }

    #[test]
    fn multifile_misalinged_v3() {
        let files: HashMap<String, Vec<u8>> = [
            ("f1.txt".to_owned(), vec![1_u8; 84]),
            ("f2.txt".to_owned(), vec![2_u8; 114]),
            ("f3.txt".to_owned(), vec![3_u8; 134]),
            ("f4.txt".to_owned(), vec![4_u8; 24]),
        ]
        .into_iter()
        .collect();
        test_multifile("multifile_misalinged_v3", 64, 8, files)
    }

    #[test]
    fn multifile_misalinged() {
        let files: HashMap<String, Vec<u8>> = (0..10)
            .map(|i| {
                let data = vec![i; 800];
                (format!("test/file_{i}.txt"), data)
            })
            .collect();
        test_multifile("multifile_misalinged", 256, 64, files);
    }

    #[test]
    fn basic_single_file_aligned() {
        let data: Vec<u8> = (0..)
            .map(|_| rand::rng().random::<u8>())
            .take(1024)
            .collect();
        let files: HashMap<String, Vec<u8>> =
            [("test_single.txt".to_owned(), data)].into_iter().collect();
        test_multifile("basic_single_file_aligned", 256, 8, files);
    }

    #[test]
    fn basic_single_file_aligned_unaligned_subpiece() {
        let data: Vec<u8> = (0..)
            .map(|_| rand::rng().random::<u8>())
            .take(1024)
            .collect();
        let files: HashMap<String, Vec<u8>> =
            [("test_single.txt".to_owned(), data)].into_iter().collect();
        test_multifile(
            "basic_single_file_aligned_unaligned_subpiece",
            256,
            10,
            files,
        );
    }

    #[test]
    fn single_file_misaligned() {
        let data: Vec<u8> = (0..)
            .map(|_| rand::rng().random::<u8>())
            .take(1354)
            .collect();
        let files: HashMap<String, Vec<u8>> =
            [("test_single.txt".to_owned(), data)].into_iter().collect();
        test_multifile("single_file_misaligned", 256, 32, files);
    }

    #[test]
    fn single_file_misaligned_v2() {
        let piece_size = 256;
        let subpiece_size = 32;
        let length = 282;
        let subpieces = length / subpiece_size;
        let mut data: Vec<u8> = (0..subpieces)
            .flat_map(|i| vec![i as u8; subpiece_size])
            .collect();
        data.append(&mut vec![subpieces as u8; length % subpiece_size]);
        assert_eq!(data.len(), length);
        let files: HashMap<String, Vec<u8>> =
            [("test_single.txt".to_owned(), data)].into_iter().collect();
        test_multifile(
            "single_file_misaligned_v2",
            piece_size,
            subpiece_size,
            files,
        );
    }

    #[test]
    fn disk_operations_for_all_valid_piece_indices() {
        // custom root to avoid conflict with concurrently running tests
        let root_dir = TempDir::new("disk_operations_indices");
        let download_tmp_dir = TempDir::new("disk_operations_indices_download");
        let file_name = "test/root/test_single.txt";
        let file_size = 10000;
        root_dir.add_file(file_name, &vec![1; file_size]);
        let piece_len = 256;

        let torrent_info = TorrentBuilder::new(&root_dir.path, piece_len as i64)
            .build()
            .unwrap();

        let file_store = FileStore::new(&download_tmp_dir.path, &torrent_info).unwrap();

        // Test that all valid piece indices work correctly
        let num_pieces = file_size.div_ceil(piece_len);
        for piece_idx in 0..num_pieces as i32 {
            let current_piece_len = if piece_idx == num_pieces as i32 - 1 {
                let remainder = file_size % piece_len;
                if remainder == 0 { piece_len } else { remainder }
            } else {
                piece_len
            };
            let piece_data = vec![1_u8; current_piece_len];

            // Should create valid disk operations
            let disk_ops =
                verify_disk_operations(&file_store, piece_idx, &piece_data, DiskOpType::Write);
            assert!(
                !disk_ops.is_empty(),
                "Should create disk operations for valid piece index {}",
                piece_idx
            );
        }
    }
}
