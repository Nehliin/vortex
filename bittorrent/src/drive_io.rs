use std::path::{Path, PathBuf};

use tokio_uring::{
    buf::BoundedBuf,
    fs::{File, OpenOptions},
};

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
    file: File,
}

// TODO: Consider getting rid of default impl
#[derive(Debug, Default)]
pub struct FileStore {
    piece_length: u64,
    files: Vec<TorrentFile>,
}

impl FileStore {
    pub async fn new(
        root: impl AsRef<Path>,
        torrent_info: &bip_metainfo::Info,
    ) -> anyhow::Result<Self> {
        async fn new_impl(
            root: &Path,
            torrent_info: &bip_metainfo::Info,
        ) -> anyhow::Result<FileStore> {
            let mut result = Vec::new();
            let mut start_piece = 0;
            let mut start_offset = 0;

            let piece_length = torrent_info.piece_length();

            for torrent_file in torrent_info.files() {
                let num_pieces = (torrent_file.length() + start_offset as u64) / piece_length;
                let offset = (torrent_file.length() + start_offset as u64) % piece_length;

                let mut path_buf = PathBuf::from(root);
                path_buf.push(torrent_file.path());
                if let Some(parent_dir) = path_buf.parent() {
                    // TODO: consider caching already created directories
                    tokio_uring::fs::create_dir_all(parent_dir).await?;
                }
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(&path_buf)
                    .await?;

                let metadata = file.statx().await?;
                // Only fallocate if necessary so existing data isn't overwritten
                if metadata.stx_size != torrent_file.length() {
                    // Default mode is described in the man pages as:
                    // allocates the disk space within the range specified by offset and
                    // len. The file size (as reported by stat(2)) will be changed if
                    // offset+len is greater than the file size.  Any subregion within
                    // the range specified by offset and len that did not contain data
                    // before the call will be initialized to zero.
                    const DEFAULT_FALLOCATE_MODE: i32 = 0;

                    file.fallocate(0, torrent_file.length(), DEFAULT_FALLOCATE_MODE)
                        .await?;
                }

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
        new_impl(root.as_ref(), torrent_info).await
    }

    pub async fn write_piece(&self, index: i32, mut piece_data: Vec<u8>) -> anyhow::Result<()> {
        let files = self
            .files
            .iter()
            .filter(|file| file.start_piece <= index && index <= file.end_piece);
        for file in files {
            // Convert the piece index to index relative to file start
            let file_index = (index - file.start_piece) as u64;
            let file_offset = file_index * self.piece_length
                // if this isn't the first file index the offset needs 
                // to be removed
                - if file_index > 0 {
                    file.start_offset as u64
                } else {
                    0
                };

            let data_start_offset = if index == file.start_piece {
                file.start_offset as usize
            } else {
                0
            };
            let file_data = if index == file.end_piece {
                piece_data.slice(data_start_offset..file.end_offset as usize)
            } else {
                piece_data.slice(data_start_offset..)
            };
            let (res, buf) = file.file.write_all_at(file_data, file_offset).await;
            piece_data = buf.into_inner();
            res?;
        }
        Ok(())
    }

    pub async fn close(self) -> anyhow::Result<()> {
        for torrent_file in self.files {
            torrent_file.file.close().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bip_metainfo::{DirectAccessor, FileAccessor, Metainfo, MetainfoBuilder, PieceLength};
    use rand::Rng;

    use super::*;

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(path: &str) -> Self {
            std::fs::create_dir_all(path).unwrap();
            Self { path: path.into() }
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
            assert!(!self.path.has_root());
            std::fs::remove_dir_all(&self.path).unwrap();
        }
    }

    fn test_multifile(folder_prefix: &str, piece_len: usize, file_data: HashMap<String, Vec<u8>>) {
        let torrent_tmp_dir = TempDir::new(&format!("{folder_prefix}_torrent"));
        let download_tmp_dir = TempDir::new(&format!("{folder_prefix}_download_dir"));
        file_data.iter().for_each(|(path, data)| {
            torrent_tmp_dir.add_file(path, data);
        });

        let builder = MetainfoBuilder::new().set_piece_length(PieceLength::Custom(piece_len));

        let accessor = FileAccessor::new(&torrent_tmp_dir.path).unwrap();

        let bytes = builder.build(4, accessor, |_progress| {}).unwrap();
        let torrent_info = Metainfo::from_bytes(bytes).unwrap();
        let info_clone = torrent_info.clone();

        let mut all_data: Vec<_> = torrent_info
            .info()
            .files()
            .flat_map(|file| file_data.get(file.path().to_str().unwrap()).unwrap())
            .copied()
            .collect();
        let download_tmp_dir_path = download_tmp_dir.path.clone();
        tokio_uring::start(async move {
            let file_store = FileStore::new(&download_tmp_dir_path, torrent_info.info())
                .await
                .unwrap();

            for (index, _) in torrent_info.info().pieces().enumerate() {
                let (piece, remainder) = all_data.split_at(piece_len.min(all_data.len()));
                file_store
                    .write_piece(index as i32, piece.to_vec())
                    .await
                    .unwrap();
                all_data = remainder.to_vec();
            }
            assert!(all_data.is_empty());
            // Close so they can be opened up later on
            // and should ensure everything is synced properly
            for file in file_store.files {
                file.file.close().await.unwrap();
            }
        });

        for file in info_clone.info().files() {
            let path = file.path().to_str().unwrap();
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
            ("f1.txt".to_owned(), vec![1_u8; 120]),
            ("f2.txt".to_owned(), vec![2_u8; 150]),
            ("f3.txt".to_owned(), vec![3_u8; 170]),
            ("f4.txt".to_owned(), vec![4_u8; 60]),
        ]
        .into_iter()
        .collect();
        test_multifile("multifile_misalinged_v2", 100, files)
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
        // custom root to avoid conflict with concurrently running tests
        let root_dir = "basic_single_file_aligned/test/root";
        let file_name = "test_single.txt";
        let piece_len = 256;
        let builder = MetainfoBuilder::new().set_piece_length(PieceLength::Custom(piece_len));
        let data: Vec<u8> = (0..)
            .map(|_| rand::thread_rng().gen::<u8>())
            .take(1024)
            .collect();
        let accessor = DirectAccessor::new(file_name, &data);

        let bytes = builder.build(1, accessor, |_progress| {}).unwrap();
        let torrent_info = Metainfo::from_bytes(bytes).unwrap();

        let mut data_clone = data.clone();
        tokio_uring::start(async move {
            let file_store = FileStore::new(root_dir, torrent_info.info()).await.unwrap();

            for (index, _) in torrent_info.info().pieces().enumerate() {
                let (piece, remainder) = data_clone.split_at(piece_len.min(data_clone.len()));
                file_store
                    .write_piece(index as i32, piece.to_vec())
                    .await
                    .unwrap();
                data_clone = remainder.to_vec();
            }
            assert!(data_clone.is_empty());
            // Close so they can be opened up later on
            // and should ensure everything is synced properly
            for file in file_store.files {
                file.file.close().await.unwrap();
            }
        });

        let written_data = std::fs::read(format!("{root_dir}/{file_name}")).unwrap();
        assert_eq!(written_data, data);
        std::fs::remove_dir_all("basic_single_file_aligned").unwrap();
    }

    #[test]
    fn single_file_misaligned() {
        // custom root to avoid conflict with concurrently running tests
        let root_dir = "single_file_misaligned/test/root";
        let file_name = "test_single.txt";
        let piece_len = 256;
        let builder = MetainfoBuilder::new().set_piece_length(PieceLength::Custom(piece_len));
        let data: Vec<u8> = (0..)
            .map(|_| rand::thread_rng().gen::<u8>())
            .take(1354)
            .collect();
        let accessor = DirectAccessor::new(file_name, &data);

        let bytes = builder.build(1, accessor, |_progress| {}).unwrap();
        let torrent_info = Metainfo::from_bytes(bytes).unwrap();

        let mut data_clone = data.clone();
        tokio_uring::start(async move {
            let file_store = FileStore::new(root_dir, torrent_info.info()).await.unwrap();

            for (index, _) in torrent_info.info().pieces().enumerate() {
                let (piece, remainder) = data_clone.split_at(piece_len.min(data_clone.len()));
                file_store
                    .write_piece(index as i32, piece.to_vec())
                    .await
                    .unwrap();
                data_clone = remainder.to_vec();
            }
            assert!(data_clone.is_empty());
            // Close so they can be opened up later on
            // and should ensure everything is synced properly
            for file in file_store.files {
                file.file.close().await.unwrap();
            }
        });

        let written_data = std::fs::read(format!("{root_dir}/{file_name}")).unwrap();
        assert_eq!(written_data, data);
        std::fs::remove_dir_all("single_file_misaligned").unwrap();
    }
}
