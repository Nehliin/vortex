use std::time::Instant;

use vortex_bittorrent::{generate_peer_id, Torrent};

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Trace)
        //        .target(env_logger::Target::Pipe(Box::new(log_file)))
        .init();
    let torrent =
        lava_torrent::torrent::v1::Torrent::read_from_file("bittorrent/assets/test-file-1.torrent")
            .unwrap();
    let our_id = generate_peer_id();
    let torrent = Torrent::new(torrent, our_id);
    //let handle = std::thread::spawn(|| {
    //   let listener = TcpListener::bind("127.0.0.1:3456").unwrap();
    //  let (stream, _) = listener.accept().unwrap();
    //});
    //std::net::TcpStream::connect("172.17.0.2:51413").unwrap();

    let download_time = Instant::now();
    let (tx, rc) = std::sync::mpsc::channel();
    tx.send("172.17.0.2:51413".parse().unwrap()).unwrap();
    torrent.start(rc).unwrap();
    let elapsed = download_time.elapsed();
    log::info!("Download complete in: {}s", elapsed.as_secs());
    let expected = std::fs::read("bittorrent/assets/test-file-1").unwrap();
    let actual = std::fs::read("bittorrent/downloaded/test-file-1").unwrap();
    assert_eq!(actual, expected);
    //handle.join().unwrap();
}
