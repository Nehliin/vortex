use std::time::Duration;

use utp_socket::utp_socket::UtpSocket;

#[test]
#[ignore = "Flakey on ci"]
fn end_to_end() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Trace)
        .is_test(true)
        .try_init();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            tokio_uring::start(async move {
                let socket = UtpSocket::bind("127.0.0.1:1337".parse().unwrap())
                    .await
                    .unwrap();
                println!("Bound sender");
                let stream = tokio::time::timeout(
                    Duration::from_secs(3),
                    socket.connect("127.0.0.1:1338".parse().unwrap()),
                )
                .await
                .unwrap()
                .unwrap();

                stream.write(b"Hello world!".to_vec()).await.unwrap();
            });
        });
        scope.spawn(|| {
            tokio_uring::start(async move {
                let socket = UtpSocket::bind("127.0.0.1:1338".parse().unwrap())
                    .await
                    .unwrap();
                println!("Bound Receiver");
                let stream = socket.accept().await.unwrap();
                let mut buf = vec![0u8; 256];
                let read = stream.read(&mut buf).await;
                assert_eq!(&buf[..read], b"Hello world!");
            });
        });
    });
}
