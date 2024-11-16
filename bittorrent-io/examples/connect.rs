use std::{
    io::Write,
    net::{TcpListener, TcpStream},
};

use bittorrent_io::{connect_to, TorrentState};

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        //        .target(env_logger::Target::Pipe(Box::new(log_file)))
        .init();
    let torrent = lava_torrent::torrent::v1::Torrent::read_from_file(
        "/home/popuser/vortex/bittorrent/assets/test-file-1.torrent",
    )
    .unwrap();
    let torrent = TorrentState::new(torrent);
    //let handle = std::thread::spawn(|| {
    //   let listener = TcpListener::bind("127.0.0.1:3456").unwrap();
    //  let (stream, _) = listener.accept().unwrap();
    //});
    //std::net::TcpStream::connect("172.17.0.2:51413").unwrap();

    connect_to("172.17.0.2:51413".parse().unwrap(), torrent);
    //handle.join().unwrap();
}
