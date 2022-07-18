use bytes::{BufMut, Bytes};
use magnet_url::Magnet;
use rand::prelude::*;
use serde_bytes::ByteBuf;
use serde_derive::Deserialize;
use sha1::{Digest, Sha1};

#[derive(Debug, Deserialize)]
struct Node(String, i64);

#[derive(Debug, Deserialize)]
struct File {
    path: Vec<String>,
    length: i64,
    #[serde(default)]
    md5sum: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Info {
    name: String,
    pieces: ByteBuf,
    #[serde(rename = "piece length")]
    piece_length: i64,
    #[serde(default)]
    md5sum: Option<String>,
    #[serde(default)]
    length: Option<i64>,
    #[serde(default)]
    files: Option<Vec<File>>,
    #[serde(default)]
    private: Option<u8>,
    #[serde(default)]
    path: Option<Vec<String>>,
    #[serde(default)]
    #[serde(rename = "root hash")]
    root_hash: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Torrent {
    info: Info,
    #[serde(default)]
    announce: Option<String>,
    #[serde(default)]
    nodes: Option<Vec<Node>>,
    #[serde(default)]
    encoding: Option<String>,
    #[serde(default)]
    httpseeds: Option<Vec<String>>,
    #[serde(default)]
    #[serde(rename = "announce-list")]
    announce_list: Option<Vec<Vec<String>>>,
    #[serde(default)]
    #[serde(rename = "creation date")]
    creation_date: Option<i64>,
    #[serde(rename = "comment")]
    comment: Option<String>,
    #[serde(default)]
    #[serde(rename = "created by")]
    created_by: Option<String>,
}

fn render_torrent(torrent: &Torrent) {
    println!("name:\t\t{}", torrent.info.name);
    println!("announce:\t{:?}", torrent.announce);
    println!("nodes:\t\t{:?}", torrent.nodes);
    if let Some(al) = &torrent.announce_list {
        for a in al {
            println!("announce list:\t{}", a[0]);
        }
    }
    println!("httpseeds:\t{:?}", torrent.httpseeds);
    println!("creation date:\t{:?}", torrent.creation_date);
    println!("comment:\t{:?}", torrent.comment);
    println!("created by:\t{:?}", torrent.created_by);
    println!("encoding:\t{:?}", torrent.encoding);
    println!("piece length:\t{:?}", torrent.info.piece_length);
    println!("private:\t{:?}", torrent.info.private);
    println!("root hash:\t{:?}", torrent.info.root_hash);
    println!("md5sum:\t\t{:?}", torrent.info.md5sum);
    println!("path:\t\t{:?}", torrent.info.path);
    if let Some(files) = &torrent.info.files {
        for f in files {
            println!("file path:\t{:?}", f.path);
            println!("file length:\t{}", f.length);
            println!("file md5sum:\t{:?}", f.md5sum);
        }
    }
}
// https://www.bittorrent.org/beps/bep_0015.html
struct TrackerPacket {
    protocol_id: i64,
    action: i32,
    transaction_id: i32,
}

impl TrackerPacket {
    fn new(action: i32) -> Self {
        TrackerPacket {
            protocol_id: 0x41727101980, // magic constant
            action,
            transaction_id: rand::random::<i32>(),
        }
    }

    fn to_bytes(self) -> bytes::Bytes {
        let mut bytes = bytes::BytesMut::new();
        bytes.reserve(16);
        bytes.put_i64(self.protocol_id);
        bytes.put_i32(self.action);
        bytes.put_i32(self.transaction_id);
        bytes.freeze()
    }
}

#[derive(Debug)]
struct TrackerResponse {
    action: i32,
    transaction_id: i32,
    connection_id: i64,
}

impl From<Bytes> for TrackerResponse {
    fn from(mut bytes: Bytes) -> Self {
        use bytes::Buf;
        let action = bytes.get_i32();
        let transaction_id = bytes.get_i32();
        let connection_id = bytes.get_i64();
        TrackerResponse {
            action,
            transaction_id,
            connection_id,
        }
    }
}

#[derive(Debug)]
struct AnnounceRequest {
    connection_id: i64,
    action: i32,
    transaction_id: i32,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    downloaded: i64,
    left: i64,
    uploaded: i64,
    event: i32,
    ip_addr: i32,
    key: i32,
    num_want: i32,
    port: i32,
}

impl AnnounceRequest {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = bytes::BytesMut::new();
        bytes.put_i64(self.connection_id);
        bytes.put_i32(self.action);
        bytes.put_i32(self.transaction_id);
        bytes.put_slice(&self.info_hash);
        bytes.put_slice(&self.peer_id);
        bytes.put_i64(self.downloaded);
        bytes.put_i64(self.left);
        bytes.put_i64(self.uploaded);
        bytes.put_i64(self.uploaded);
        bytes.freeze()
    }
}

fn generate_node_id() -> [u8; 20] {
    let id = rand::random::<[u8; 20]>();
    let mut hasher = Sha1::new();
    hasher.update(id);
    hasher.finalize().into()
}

fn main() {
    /*let test: Torrent = serde_bencode::de::from_bytes(include_bytes!("../test.torrent")).unwrap();
    render_torrent(&test);

    let connect = TrackerPacket::new(0);

    tokio_uring::start(async move {
        let socket = tokio_uring::net::UdpSocket::bind("0.0.0.0:1337".parse().unwrap())
            .await
            .unwrap();

        let tracker_addr = test
            .announce_list
            .unwrap()
            .iter()
            .find(|announce| announce[0].starts_with("udp"))
            .unwrap()[0]
            .clone();
        dbg!(&tracker_addr);

        let addr_v2 = "93.158.213.92:1337".parse().unwrap();
        let trans_id = connect.transaction_id;
        socket.send_to(connect.to_bytes(), addr_v2).await;
        let buf = vec![0; 16];
        while let (Ok((size, recv_addr)), bytes) = socket.recv_from(buf.clone()).await {
            if recv_addr == addr_v2 {
                println!("Got data!");
                if size == 0 {
                    continue;
                }
                let bytes: Bytes = dbg!(bytes.into());
                let response = TrackerResponse::from(bytes);
                assert!(response.transaction_id == trans_id);
                println!("connected!");
                dbg!(response);
            }
        }
    });*/

    let magent_url = Magnet::new("magnet:?xt=urn:btih:VIJHHSNY6CICT7FIBXMBIIVNCHV4UIDA").unwrap();
    let node_id = generate_node_id();

    dbg!(magent_url);
}
