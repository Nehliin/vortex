mod dht;
mod krpc;
mod node;
mod routing_table;

#[test]
fn test() {
    let mut log_builder = env_logger::builder();
    log_builder.filter_level(log::LevelFilter::Debug).init();
    tokio_uring::start(async {
        let dht = dht::Dht::new("0.0.0.0:1337".parse().unwrap())
            .await
            .unwrap();
        dht.start().await.unwrap();
    });
}
