use std::{
    path::{Path, PathBuf},
    rc::Rc,
};

use anyhow::Context;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
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

pub struct FileStore {
    piece_length: u64,
    files: Vec<TorrentFile>,
}

impl FileStore {
    // TODO take AsRef<Path> instead
    pub async fn new(root: &Path, torrent_info: &bip_metainfo::Info) -> anyhow::Result<Self> {
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

            // Default mode is described in the man pages as:
            // allocates the disk space within the range specified by offset and
            // len. The file size (as reported by stat(2)) will be changed if
            // offset+len is greater than the file size.  Any subregion within
            // the range specified by offset and len that did not contain data
            // before the call will be initialized to zero.
            const DEFAULT_FALLOCATE_MODE: i32 = 0;

            file.fallocate(0, torrent_file.length(), DEFAULT_FALLOCATE_MODE)
                .await?;

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

    pub async fn close(&mut self) {
        let files = std::mem::take(&mut self.files);
        for torrent_file in files {
            torrent_file.file.close().await.unwrap();
        }
    }
}

