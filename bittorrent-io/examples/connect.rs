use std::{
    io::Write,
    net::{TcpListener, TcpStream},
};

use bittorrent_io::connect_to;

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        //        .target(env_logger::Target::Pipe(Box::new(log_file)))
        .init();
    let handle = std::thread::spawn(|| {
        let listener = TcpListener::bind("127.0.0.1:3456").unwrap();
        let (stream, _) = listener.accept().unwrap();
    });
    connect_to("127.0.0.1:3456".parse().unwrap());
    handle.join().unwrap();
}
