use std::{io::Write, net::TcpStream};

use bittorrent_io::setup_listener;

fn main() {
    let handle = std::thread::spawn(|| {
        setup_listener();
    });
    std::thread::sleep_ms(500);
    let mut stream = TcpStream::connect("127.0.0.1:3456").unwrap();
    stream.write_all(b"Hello from stream!!").unwrap();
    handle.join().unwrap();
}
