use std::{io::Write, net::TcpStream};

use bittorrent_io::setup_listener;

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        //        .target(env_logger::Target::Pipe(Box::new(log_file)))
        .init();
    let handle = std::thread::spawn(|| {
        setup_listener();
    });
    std::thread::sleep_ms(500);
    let mut stream = TcpStream::connect("127.0.0.1:3456").unwrap();
    stream.write_all(b"Hello from stream!!").unwrap();
    std::thread::sleep_ms(500);
    stream.write_all(b"HERE AGAIN!!").unwrap();
    stream.write_all(b"WOOOO").unwrap();
    // 36 + 10 * 300
    for _ in 0..300 {
        stream.write_all(b"SOMETHING\n").unwrap();
    }
    handle.join().unwrap();
}
