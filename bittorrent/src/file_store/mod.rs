use std::{
    io,
    path::{Path, PathBuf},
};

use file::MmapFile;
use lava_torrent::torrent::v1::Torrent;
use sha1::Digest;
use smallvec::SmallVec;

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
    file_handle: file::MmapFile,
}

/// View of all the files the Piece overlaps with
pub struct WritablePieceFileView<'a> {
    index: i32,
    avg_piece_size: u64,
    files: SmallVec<&'a TorrentFile, 5>,
}

impl<'a> WritablePieceFileView<'a> {
    /// Writes the subpiece to disk.
    pub fn write_subpiece(&mut self, subpiece_offset: usize, data: &[u8]) {
        let mut subpiece_written: usize = 0;
        for file in self.files.iter_mut() {
            // Where in the _file_ does the piece start
            let file_index = (self.index - file.start_piece) as i64;
            // The offset might be negative here if the piece starts before the file
            let file_offset = file_index * self.avg_piece_size as i64 - file.start_offset as i64;
            // Where in the _file_ does the subpiece start
            let subpiece_offset = file_offset + subpiece_offset as i64;

            // Where should the writing start from (taking into account the already written parts)
            // NOTE: the write head should never be negative since we loop across all files in order
            // that are part of the piece. We should thus have already written the relevant parts
            // of the subpiece from the previous files to ensure we start at minimum on 0
            let current_write_head = subpiece_offset + subpiece_written as i64;
            assert!(current_write_head >= 0);
            let current_write_head = current_write_head as usize;
            // if we are past this file, move on to the next
            if current_write_head >= file.file_handle.len() {
                continue;
            }

            let max_possible_write =
                (file.file_handle.len() - current_write_head).min(data.len() - subpiece_written);
            unsafe {
                std::ptr::copy_nonoverlapping(
                    data.as_ptr().add(subpiece_written),
                    file.file_handle.ptr().add(current_write_head).as_ptr() as _,
                    max_possible_write,
                );
            }
            subpiece_written += max_possible_write;
            // Break early if we've written the entire subpiece
            if subpiece_written >= data.len() {
                break;
            }
        }
        // Must have written all data
        assert_eq!(subpiece_written, data.len());
    }

    pub fn into_readable(self) -> ReadablePieceFileView<'a> {
        ReadablePieceFileView {
            index: self.index,
            avg_piece_size: self.avg_piece_size,
            files: self.files,
        }
    }
}

pub struct ReadablePieceFileView<'a> {
    pub index: i32,
    avg_piece_size: u64,
    files: SmallVec<&'a TorrentFile, 5>,
}

impl ReadablePieceFileView<'_> {
    /// Calculate the offset of the subpiece relative to the file start
    #[inline]
    fn subpiece_offset(&self, subpiece_offset: usize, file: &TorrentFile) -> i64 {
        // Where in the _file_ does the piece start
        let file_index = (self.index - file.start_piece) as i64;
        // The offset might be negative here if the piece starts before the file
        let file_offset = file_index * self.avg_piece_size as i64 - file.start_offset as i64;
        // Where in the _file_ does the subpiece start
        file_offset + subpiece_offset as i64
    }

    pub fn read_subpiece(&self, subpiece_offset: usize, buffer: &mut [u8]) {
        let mut subpiece_read: usize = 0;
        for file in self.files.iter() {
            // Where in the _file_ does the subpiece start
            let subpiece_offset = self.subpiece_offset(subpiece_offset, file);
            // Where should the reading start from (taking into account the already read parts)
            // NOTE: the read head should never be negative since we loop across all files in order
            // that are part of the piece. We should thus have already read the relevant pieces
            // from the previous files to ensure we start at minimum on 0
            let current_read_head = subpiece_offset + subpiece_read as i64;
            assert!(current_read_head >= 0);
            let current_read_head = current_read_head as usize;
            // if we are past this file, move on to the next
            if current_read_head >= file.file_handle.len() {
                continue;
            }
            // what is the maximum that can be read
            let max_possible_read =
                (file.file_handle.len() - current_read_head).min(buffer.len() - subpiece_read);
            let file_buffer = file.file_handle.get();
            buffer[subpiece_read..subpiece_read + max_possible_read].copy_from_slice(
                &file_buffer[current_read_head..current_read_head + max_possible_read],
            );
            subpiece_read += max_possible_read;
            // Break early if the subpiece has been completely read
            if subpiece_read >= buffer.len() {
                break;
            }
        }
        // Must have read all data
        assert_eq!(subpiece_read, buffer.len());
    }

    /// Sync the relevant file pieces to disk and compare against expected hash
    pub fn sync_and_check_hash(&self, expected_piece_hash: &[u8]) -> io::Result<bool> {
        let mut hasher = sha1::Sha1::new();
        let mut total_read = 0;
        for file in self.files.iter() {
            // Where does the piece start relative to the file
            let piece_start_offset = self.subpiece_offset(0, file);
            let file_offset = piece_start_offset + total_read as i64;
            // we should always have read the files in order so that
            // total read ensures this offset to be greater than 0
            assert!(file_offset >= 0);
            let to_read = if self.index == file.end_piece {
                // Either the piece ends within this file, then we should read
                // to that piece offset - how much we've already read
                // (end_offset is the offset relative to the entire the piece)
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
            let file_data = file.file_handle.get();
            let file_data_start = file_offset as usize;
            let file_data_end = file_offset as usize + to_read;
            file.file_handle.sync(file_data_start, file_data_end)?;
            let relevant_piece_data: &[u8] = &file_data[file_data_start..file_data_end];
            hasher.update(relevant_piece_data);
            total_read += to_read;
        }
        Ok(hasher.finalize().as_slice() == expected_piece_hash)
    }
}

// TODO: Consider getting rid of default impl
#[derive(Debug)]
pub struct FileStore {
    piece_length: u64,
    files: Vec<TorrentFile>,
}

impl FileStore {
    pub fn dummy() -> Self {
        Self {
            piece_length: 0,
            files: Default::default(),
        }
    }

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

            let torrent_directory = PathBuf::from(root).join(&torrent_info.name);
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
                if let Some(parent_dir) = file_path.parent() {
                    if let Err(err) = std::fs::create_dir_all(parent_dir) {
                        // Do not error if they folders already exists
                        if err.kind() != io::ErrorKind::AlreadyExists {
                            return Err(err);
                        }
                    }
                }
                let file = MmapFile::create(&file_path, torrent_file.length as usize)?;

                let torrent_file = TorrentFile {
                    start_piece,
                    start_offset,
                    end_piece: start_piece + num_pieces as i32,
                    end_offset: offset as i32,
                    file,
                };
                start_piece = torrent_file.end_piece;
                start_offset = torrent_file.end_offset;
                result.push(torrent_file)
            }
            Ok(FileStore {
                files: result,
                piece_length,
            })
        }
        new_impl(root.as_ref(), torrent_info)
    }

    pub fn write_piece(&mut self, index: i32, piece_data: &[u8]) -> io::Result<()> {
        let files = self
            .files
            .iter_mut()
            .filter(|file| file.start_piece <= index && index <= file.end_piece);
        for file in files {
            let file_offset = file.offset(index, self.piece_length);
            let data_start_offset = if index == file.start_piece {
                file.start_offset as usize
            } else {
                0
            };
            let relevant_piece_data = if index == file.end_piece {
                &piece_data[data_start_offset..file.end_offset as usize]
            } else {
                &piece_data[data_start_offset..]
            };
            let file_data = file.file.get_mut();

            unsafe {
                std::ptr::copy_nonoverlapping(
                    relevant_piece_data.as_ptr(),
                    file_data.as_mut_ptr().add(file_offset as usize),
                    relevant_piece_data.len(),
                )
            }
        }
        Ok(())
    }

    // invariant here is that the piece has been completed before, it's not up
    // to the store to control that.
    pub fn read_piece(&self, index: i32) -> io::Result<Vec<u8>> {
        let files = self
            .files
            .iter()
            .filter(|file| file.start_piece <= index && index <= file.end_piece);
        // Total read is used instead of explicitly keeping track of piece lenght per
        // index. One could use the same calulation as is done in piece_selector but
        // didn't want to duplicate that logic here.
        let mut total_read = 0;
        let mut piece_data: Vec<u8> = vec![0; self.piece_length as usize];
        for file in files {
            let file_offset = file.offset(index, self.piece_length);
            let data_start_offset = if index == file.start_piece {
                file.start_offset as usize
            } else {
                0
            };
            let relevant_piece_data = if index == file.end_piece {
                &mut piece_data[data_start_offset..file.end_offset as usize]
            } else {
                &mut piece_data[data_start_offset..]
            };
            let file_data = file.file.get();
            unsafe {
                std::ptr::copy_nonoverlapping(
                    file_data.as_ptr().add(file_offset as usize),
                    relevant_piece_data.as_mut_ptr(),
                    relevant_piece_data.len(),
                )
            }
            total_read += relevant_piece_data.len();
        }
        piece_data.truncate(total_read);
        if piece_data.is_empty() {
            Err(io::ErrorKind::InvalidInput.into())
        } else {
            Ok(piece_data)
        }
    }

    pub fn sync(self) -> io::Result<()> {
        for torrent_file in self.files {
            torrent_file.file.sync()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use lava_torrent::torrent::v1::TorrentBuilder;
    use rand::Rng;
    use sha1::{Digest, Sha1};

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

    fn read_file_by_piece(root: impl AsRef<Path>, torrent_info: &Torrent) {
        let store = FileStore::new(root, torrent_info).unwrap();
        for (index, piece_hash) in torrent_info.pieces.iter().enumerate() {
            let piece_data = store.read_piece(index as i32).unwrap();
            let mut hasher = Sha1::new();
            hasher.update(piece_data);
            let actual_hash = hasher.finalize();
            assert_eq!(actual_hash.as_slice(), piece_hash);
        }
    }

    fn test_multifile(torrent_name: &str, piece_len: usize, file_data: HashMap<String, Vec<u8>>) {
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
        let mut file_store = FileStore::new(&download_tmp_dir_path, &torrent_info).unwrap();

        for (index, _) in torrent_info.pieces.iter().enumerate() {
            let (piece, remainder) = all_data.split_at(piece_len.min(all_data.len()));
            file_store.write_piece(index as i32, piece).unwrap();
            all_data = remainder.to_vec();
        }
        assert!(all_data.is_empty());
        // Sync so they can be opened up later on
        // and should ensure everything is synced properly
        for file in file_store.files {
            file.file.sync().unwrap();
        }
        read_file_by_piece(&download_tmp_dir_path, &torrent_info);

        for file in files.iter() {
            let path = file.path.to_str().unwrap();
            let written_data =
                std::fs::read(download_tmp_dir.path.join(&torrent_info.name).join(path)).unwrap();
            let data = file_data.get(path).unwrap();
            assert_eq!(written_data.len(), data.len());
            assert_eq!(&written_data, data);
        }
    }

    #[test]
    fn basic_multifile_alinged() {
        let files: HashMap<String, Vec<u8>> = (0..10)
            .map(|i| {
                let data = vec![i; 1024];
                (format!("test/file_{i}.txt"), data)
            })
            .collect();
        test_multifile("basic_multifile_alinged", 256, files)
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
        test_multifile("small_multifile_misalinged", 256, files)
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
        test_multifile("multifile_not_multiple_of_piece_size", 256, files)
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
        test_multifile("multifile_misalinged_v2", 64, files)
    }

    #[test]
    fn multifile_misalinged() {
        let files: HashMap<String, Vec<u8>> = (0..10)
            .map(|i| {
                let data = vec![i; 800];
                (format!("test/file_{i}.txt"), data)
            })
            .collect();
        test_multifile("multifile_misalinged", 256, files);
    }

    #[test]
    fn basic_single_file_aligned() {
        let data: Vec<u8> = (0..)
            .map(|_| rand::thread_rng().gen::<u8>())
            .take(1024)
            .collect();
        let files: HashMap<String, Vec<u8>> =
            [("test_single.txt".to_owned(), data)].into_iter().collect();
        test_multifile("basic_single_file_aligned", 256, files);
    }

    #[test]
    fn single_file_misaligned() {
        let data: Vec<u8> = (0..)
            .map(|_| rand::thread_rng().gen::<u8>())
            .take(1354)
            .collect();
        let files: HashMap<String, Vec<u8>> =
            [("test_single.txt".to_owned(), data)].into_iter().collect();
        test_multifile("single_file_misaligned", 256, files);
    }

    #[test]
    fn errors_on_invalid_piece_index() {
        // custom root to avoid conflict with concurrently running tests
        let root_dir = TempDir::new("errors_on_invalid_piece_index");
        let download_tmp_dir = TempDir::new("errors_on_invalid_piece_index_download_dir");
        let file_name = "test/root/test_single.txt";
        root_dir.add_file(file_name, &vec![1; 10000]);
        let piece_len = 256;

        let torrent_info = TorrentBuilder::new(&root_dir.path, piece_len as i64)
            .build()
            .unwrap();

        let file_store = FileStore::new(&download_tmp_dir.path, &torrent_info).unwrap();
        // 500 is out of bounds
        assert!(file_store.read_piece(500).is_err());
    }
}
