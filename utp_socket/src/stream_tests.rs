use crate::{
    utp_packet::{get_microseconds, Packet, PacketHeader, PacketType, HEADER_SIZE},
    utp_stream::{ConnectionState, UtpStream},
};
use bytes::Bytes;
use std::{rc::Rc, time::Duration};
use tokio::sync::mpsc::Receiver;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tokio_uring::net::UdpSocket;

async fn setup_connected_stream(
    initial_stream_window: u32,
) -> (Rc<UdpSocket>, UtpStream, Receiver<Packet>) {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Trace)
        .is_test(true)
        .try_init();
    let socket = Rc::new(
        UdpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    // Needs to be randomized to avoid confilicting inbetween tests
    // not perfect though
    let port: u16 = rand::random();
    let addr = format!("127.0.0.1:{port}").parse().unwrap();
    let stream = UtpStream::new(1, addr, Rc::downgrade(&socket));
    let (pkt_tx, mut pkt_rc) = tokio::sync::mpsc::channel(256);
    let (ready_tx, ready_rc) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        tokio_uring::start(async move {
            let socket = UdpSocket::bind(addr).await.unwrap();
            let _ = ready_tx.send(());
            loop {
                let buf = vec![0; 1024];
                let (result, buf) = socket.recv_from(buf).await;
                let (recv, _) = result.unwrap();
                let packet_header = PacketHeader::try_from(&buf[..recv]).unwrap();

                let packet = Packet {
                    header: packet_header,
                    data: Bytes::copy_from_slice(&buf[HEADER_SIZE as usize..recv]),
                };

                pkt_tx.send(packet).await.unwrap();
            }
        })
    });
    // Wait for the other socket to have been bound
    ready_rc.await.unwrap();
    // Needed so spawned task starts
    assert_eq!(stream.state().conn_id_recv + 1, stream.state().conn_id_send);
    let response_ack_nr = rand::random();
    stream.send_syn().await.unwrap();
    {
        let syn_pkt = pkt_rc.recv().await.unwrap();
        assert!(syn_pkt.data.is_empty());
        assert_eq!(syn_pkt.header.ack_nr, 0);
        assert_eq!(syn_pkt.header.packet_type, PacketType::Syn);
        assert_eq!(syn_pkt.header.timestamp_difference_microseconds, 0);
        assert!(syn_pkt.header.wnd_size > 0);

        let header = PacketHeader {
            seq_nr: response_ack_nr,
            ack_nr: syn_pkt.header.seq_nr,
            conn_id: syn_pkt.header.conn_id,
            packet_type: PacketType::State,
            timestamp_microseconds: get_microseconds() as u32,
            timestamp_difference_microseconds: get_microseconds() as u32
                - syn_pkt.header.timestamp_microseconds,
            wnd_size: initial_stream_window,
            extension: 0,
        };
        stream
            .process_incoming(Packet {
                header,
                data: Bytes::new(),
            })
            .await
            .unwrap();
    }
    assert_eq!(stream.state().connection_state, ConnectionState::Connected);
    assert_eq!(
        stream.state().their_advertised_window,
        initial_stream_window
    );
    assert_eq!(stream.state().ack_nr, response_ack_nr);
    assert!(stream.state().outgoing_buffer.is_empty());
    assert!(stream.state().incoming_buffer.is_empty());
    (socket, stream, pkt_rc)
}

#[test]
fn does_shutdown() {
    tokio_uring::start(async move {
        let socket = Rc::new(
            UdpSocket::bind("0.0.0.0:1336".parse().unwrap())
                .await
                .unwrap(),
        );
        let _stream = UtpStream::new(1, "0.0.0.0:1337".parse().unwrap(), Rc::downgrade(&socket));
        tokio::time::sleep(Duration::from_millis(400)).await;
    });
}

#[test]
fn connect_basic() {
    tokio_uring::start(async move {
        let socket = Rc::new(
            UdpSocket::bind("127.0.0.1:1336".parse().unwrap())
                .await
                .unwrap(),
        );
        let stream = UtpStream::new(1, "127.0.0.1:1337".parse().unwrap(), Rc::downgrade(&socket));
        let stream_clone = stream.clone();
        let response_ack_nr = rand::random();
        let handle = tokio_uring::spawn(async move {
            let socket = UdpSocket::bind("127.0.0.1:1337".parse().unwrap())
                .await
                .unwrap();
            let buf = vec![0; 1024];
            let (result, buf) = socket.recv_from(buf).await;
            let (recv, _) = result.unwrap();
            let packet_header = PacketHeader::try_from(&buf[..recv]).unwrap();

            let packet = Packet {
                header: packet_header,
                data: Bytes::copy_from_slice(&buf[HEADER_SIZE as usize..recv]),
            };

            // No data is sent
            assert!(packet.data.is_empty());
            assert_eq!(packet.header.ack_nr, 0);
            assert_eq!(packet.header.packet_type, PacketType::Syn);
            assert_eq!(packet.header.timestamp_difference_microseconds, 0);
            assert!(packet.header.wnd_size > 0);

            let header = PacketHeader {
                seq_nr: response_ack_nr,
                ack_nr: packet.header.seq_nr,
                conn_id: packet.header.conn_id,
                packet_type: PacketType::State,
                timestamp_microseconds: get_microseconds() as u32,
                timestamp_difference_microseconds: get_microseconds() as u32
                    - packet.header.timestamp_microseconds,
                wnd_size: 123,
                extension: 0,
            };
            stream_clone
                .process_incoming(Packet {
                    header,
                    data: Bytes::new(),
                })
                .await
                .unwrap();
        });
        tokio::task::yield_now().await;
        assert_eq!(stream.state().conn_id_recv + 1, stream.state().conn_id_send);
        stream.connect().await.unwrap();
        assert_eq!(stream.state().connection_state, ConnectionState::Connected);
        assert_eq!(stream.state().their_advertised_window, 123);
        assert_eq!(stream.state().ack_nr, response_ack_nr);
        assert!(stream.state().outgoing_buffer.is_empty());
        assert!(stream.state().incoming_buffer.is_empty());
        handle.await.unwrap();
    });
}

#[test]
fn basic_acking() {
    tokio_uring::start(async move {
        let (_socket, stream, pkt_rc) = setup_connected_stream(123).await;
        // The id used to send data back to the stream after SYN-ACK
        let conn_id_send = stream.state().conn_id_recv;
        let rc_seq_nr = stream.state().ack_nr;
        let old_seq_nr = stream.state().seq_nr;

        // Connected -----------------------------------
        stream.write(vec![1; 50]).await.unwrap();
        assert_eq!(stream.state().outgoing_buffer.len(), 1);
        assert_eq!(stream.state().seq_nr, old_seq_nr + 1);

        let mut pkt_stream = ReceiverStream::new(pkt_rc);
        let pkt = pkt_stream.next().await.unwrap();
        assert_eq!(pkt.header.seq_nr, old_seq_nr + 1);
        assert_eq!(pkt.data, vec![1; 50]);

        stream
            .process_incoming(Packet {
                header: PacketHeader {
                    seq_nr: rc_seq_nr,
                    ack_nr: pkt.header.seq_nr,
                    conn_id: conn_id_send,
                    packet_type: PacketType::State,
                    timestamp_microseconds: get_microseconds() as u32,
                    timestamp_difference_microseconds: get_microseconds() as u32
                        - pkt.header.timestamp_microseconds,
                    wnd_size: 123,
                    extension: 0,
                },
                data: Bytes::new(),
            })
            .await
            .unwrap();

        assert_eq!(stream.state().outgoing_buffer.len(), 0);
    });
}

const LOREM_IPSUM: &[u8] = br#"
      Lorem ipsum dolor sit amet, consectetur adipiscing elit, 
      sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. 
      Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi
      ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in
      voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat
      cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum"#;

#[test]
fn respects_stream_window_size() {
    tokio_uring::start(async move {
        let (_socket, stream, pkt_rc) = setup_connected_stream().await;
        // The id used to send data back to the stream after SYN-ACK
        let conn_id_send = stream.state().conn_id_recv;
        let rc_seq_nr = stream.state().ack_nr;
        let old_seq_nr = stream.state().seq_nr;

        // Connected -----------------------------------
        stream.write(LOREM_IPSUM.to_vec()).await.unwrap();
        assert_eq!(stream.state().outgoing_buffer.len(), 1);
        assert_eq!(stream.state().seq_nr, old_seq_nr + 1);
        stream.write(vec![1; 50]).await.unwrap();
        assert_eq!(stream.state().outgoing_buffer.len(), 2);
        assert_eq!(stream.state().seq_nr, old_seq_nr + 2);
        stream.write(vec![2; 1000]).await.unwrap();
        assert_eq!(stream.state().outgoing_buffer.len(), 3);
        assert_eq!(stream.state().seq_nr, old_seq_nr + 3);

        let mut pkt_stream = ReceiverStream::new(pkt_rc);
        let pkt = pkt_stream.next().await.unwrap();
        assert_eq!(pkt.header.seq_nr, old_seq_nr + 2);
        assert_eq!(pkt.data, vec![1; 50]);

        // Window size is increased
        stream
            .process_incoming(Packet {
                header: PacketHeader {
                    seq_nr: rc_seq_nr,
                    ack_nr: pkt.header.seq_nr,
                    conn_id: conn_id_send,
                    packet_type: PacketType::State,
                    timestamp_microseconds: get_microseconds() as u32,
                    timestamp_difference_microseconds: get_microseconds() as u32
                        - pkt.header.timestamp_microseconds,
                    wnd_size: 500,
                    extension: 0,
                },
                data: Bytes::new(),
            })
            .await
            .unwrap();
        // TODO fix after ack test
        let pkt = pkt_stream.next().await.unwrap();
        assert_eq!(pkt.header.seq_nr, old_seq_nr + 1);
        assert_eq!(pkt.data, LOREM_IPSUM.to_vec());
    });
}
