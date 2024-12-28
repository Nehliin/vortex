use std::{
    io,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use file::MmapFile;
use lava_torrent::torrent::v1::Torrent;

mod file;

#[derive(Debug)]
struct TorrentFile {
    // Index of the piece this file starts in
    start_piece: i32,
    // Offset within the start piece
    start_offset: i32,
    // Index of the piece this file ends in
    end_piece: i32,
    // Offset within the end piece
    end_offset: i32,
    // File handle
    file: file::MmapFile,
}

impl TorrentFile {
    /// Caluclates the byte offset within the file for a given piece index and
    /// piece length
    #[inline]
    fn offset(&self, index: i32, piece_length: u64) -> u64 {
        // Convert the piece index to index relative to file start
        let file_index = (index - self.start_piece) as u64;
        let mut file_offset = file_index * piece_length;
        if file_index > 0 {
            // if this isn't the first file index the offset needs
            // to be removed
            file_offset -= self.start_offset as u64;
        }
        file_offset
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

            for torrent_file in files {
                let num_pieces = (torrent_file.length as u64 + start_offset as u64) / piece_length;
                let offset = (torrent_file.length as u64 + start_offset as u64) % piece_length;

                let mut path_buf = PathBuf::from(root);
                path_buf.push(&torrent_file.path);
                if let Some(parent_dir) = path_buf.parent() {
                    // TODO: consider caching already created directories
                    std::fs::create_dir_all(parent_dir)?;
                }
                let file = MmapFile::create(&path_buf, torrent_file.length as usize)?;

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
            return Err(io::ErrorKind::InvalidInput.into());
        } else {
            Ok(piece_data)
        }
    }

    pub fn close(self) -> io::Result<()> {
        for torrent_file in self.files {
            torrent_file.file.close()?;
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
            std::fs::create_dir_all(path).unwrap();
            let path: PathBuf = path.into();
            assert!(!path.has_root());
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

    fn test_multifile(folder_prefix: &str, piece_len: usize, file_data: HashMap<String, Vec<u8>>) {
        let torrent_tmp_dir = TempDir::new(&format!("{folder_prefix}_torrent"));
        let download_tmp_dir = TempDir::new(&format!("{folder_prefix}_download_dir"));
        file_data.iter().for_each(|(path, data)| {
            torrent_tmp_dir.add_file(path, data);
        });

        let torrent_info = TorrentBuilder::new(&torrent_tmp_dir.path, piece_len as i64)
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
        // Close so they can be opened up later on
        // and should ensure everything is synced properly
        for file in file_store.files {
            file.file.close().unwrap();
        }
        read_file_by_piece(&download_tmp_dir_path, &torrent_info);

        for file in files.iter() {
            let path = file.path.to_str().unwrap();
            let written_data = std::fs::read(download_tmp_dir.path.join(path)).unwrap();
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
