use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    time::Instant,
};

use vortex_bittorrent::{connect_to, TorrentState};

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        //        .target(env_logger::Target::Pipe(Box::new(log_file)))
        .init();
    let torrent = lava_torrent::torrent::v1::Torrent::read_from_file(
        "bittorrent/assets/test-file-1.torrent",
    )
    .unwrap();
    let torrent = TorrentState::new(torrent);
    //let handle = std::thread::spawn(|| {
    //   let listener = TcpListener::bind("127.0.0.1:3456").unwrap();
    //  let (stream, _) = listener.accept().unwrap();
    //});
    //std::net::TcpStream::connect("172.17.0.2:51413").unwrap();

    let download_time = Instant::now();
    connect_to("172.17.0.2:51413".parse().unwrap(), torrent);
    let elapsed = download_time.elapsed();
    log::info!("Download complete in: {}s", elapsed.as_secs());
    let expected = std::fs::read("bittorrent/assets/test-file-1").unwrap();
    let actual = std::fs::read("bittorrent/downloaded/test-file-1").unwrap();
    assert_eq!(actual, expected);
    //handle.join().unwrap();
}
