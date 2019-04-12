use bytes::{Bytes, BytesMut};
use futures::{future, prelude::*, sync::mpsc};
use std::{io, iter, net::SocketAddr};
use tokio::{codec::{LengthDelimitedCodec, Framed}, net::{TcpListener, TcpStream}, runtime::Runtime};
use yamux::{Config, Connection, Mode};

fn roundtrip(addr: SocketAddr, nstreams: u64, nmessages: usize, data: Bytes) {
    let rt = Runtime::new().expect("runtime");
    let e1 = rt.executor();
    let e2 = rt.executor();

    let server = TcpListener::bind(&addr).expect("tcp bind").incoming()
        .into_future()
        .then(move |sock| {
            let sock = sock.expect("sock ok").0.expect("some sock");
            Connection::new(e1, sock, Config::default(), Mode::Server).expect("connection")
                .map_err(|()| io::Error::new(io::ErrorKind::Other, "connection closed"))
                .for_each(|stream| {
                    let (sink, stream) = Framed::new(stream, LengthDelimitedCodec::new()).split();
                    stream.map(BytesMut::freeze).forward(sink).map(|_| ())
                })
        })
        .map_err(|e| panic!("server error: {}", e));

    let client = TcpStream::connect(&addr)
        .from_err()
        .and_then(move |sock| {
            let (tx, rx) = mpsc::unbounded();
            let c = Connection::new(e2.clone(), sock, Config::default(), Mode::Client).expect("connection");
            for _ in 0 .. nstreams {
                let t = tx.clone();
                let d = data.clone();
                let f = c.open_stream().and_then(move |stream| {
                    let stream = stream.expect("not eof");
                    let (sink, stream) = Framed::new(stream, LengthDelimitedCodec::new()).split();
                    // a) send and receive `nmessages` one-by-one
                    // futures::stream::iter_ok(iter::repeat(d).take(nmessages))
                    //     .fold((sink, stream, 0usize), |(sink, stream, acc), d|
                    //         sink.send(d).and_then(move |mut sink|
                    //             stream.into_future().map_err(|(e,_)| e)
                    //                 .map(move |(d, s)| (sink, s, acc + 1))))
                    //     .and_then(|(mut sink, _stream, n)|
                    //         future::poll_fn(move || sink.close())
                    //             .map(move |()| n))
                    //     .map(move |n| t.unbounded_send(n).expect("send to channel"))
                    //     .from_err()

                    // b) send `nmessages` and then receive `nmessages`
                    sink.with_flat_map(move |d| futures::stream::iter_ok(iter::repeat(d).take(nmessages)))
                        .send(d.clone())
                        .and_then(|mut sink| future::poll_fn(move || sink.close()))
                        .and_then(move |()| stream.fold(0usize, |acc, _| future::ok::<_, io::Error>(acc + 1)))
                        .map(move |n| t.unbounded_send(n).expect("send to channel"))
                        .from_err()
                });
                e2.spawn(f.map_err(|e| panic!("client error: {}", e)));
            }
            rx.take(nstreams)
                .map_err(|()| panic!("channel interrupted"))
                .fold(0, |acc, n| Ok::<_, yamux::Error>(acc + n))
                .and_then(move |n| {
                    assert_eq!(n, nstreams as usize * nmessages);
                    std::mem::drop(c);
                    Ok::<_, yamux::Error>(())
                })
        })
        .map_err(|e| panic!("client error: {}", e));

    rt.block_on_all(server.join(client).map(|_| ())).expect("runtime")
}

#[test]
fn concurrent_streams() {
    env_logger::init();
    let data = std::iter::repeat(0x42u8).take(100 * 1024).collect::<Vec<_>>().into();
    roundtrip("127.0.0.1:9000".parse().expect("valid address"), 1000, 10, data)
}
