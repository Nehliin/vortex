use std::{path::PathBuf, rc::Rc};

use anyhow::Context;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_uring::fs::File;

#[derive(Debug)]
pub enum DiskJob {
    Write { offset: u64, bytes: Vec<u8> },
    //Read { offset: u64, buffer: Vec<u8>,  },
    Close,
}

pub struct FileHandle {
    job_sender: UnboundedSender<DiskJob>,
}

impl FileHandle {
    pub fn new(path: PathBuf) -> Self {
        let job_sender = start_disk_io_thread(path);
        Self { job_sender }
    }

    #[inline]
    pub fn write(&self, offset: u64, bytes: Vec<u8>) -> anyhow::Result<()> {
        self.job_sender
            .send(DiskJob::Write { offset, bytes })
            .context("Disk io thread have shutdown")
    }

    #[inline]
    pub fn close(&self) -> anyhow::Result<()> {
        self.job_sender
            .send(DiskJob::Close)
            .context("Disk io thread have shutdown")
    }
}

fn start_disk_io_thread(path: PathBuf) -> UnboundedSender<DiskJob> {
    let (job_tx, mut job_rc): (_, UnboundedReceiver<DiskJob>) =
        tokio::sync::mpsc::unbounded_channel();
    std::thread::spawn(move || {
        tokio_uring::start(async move {
            let file = Rc::new(File::create(&path).await.unwrap());
            while let Some(job) = job_rc.recv().await {
                match job {
                    DiskJob::Write { offset, bytes } => {
                        let file_clone = file.clone();
                        tokio_uring::spawn(async move {
                            let (result, _buf) = file_clone.write_all_at(bytes, offset).await;
                            if let Err(err) = result {
                                // TODO: This will silently corrupt the file and is currently
                                // not recoverable
                                panic!("Failed to write piece: {err}");
                            }
                        });
                    }
                    DiskJob::Close => {
                        file.sync_all().await.unwrap();
                        break;
                    }
                }
            }
            // TODO closing it here?
        });
    });
    job_tx
}
