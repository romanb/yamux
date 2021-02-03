// Copyright (c) 2018-2019 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

use crate::{Config, Connection, ConnectionError, Mode, Control, connection::State};
use crate::WindowUpdateMode;
use futures::{future, prelude::*};
use futures::io::AsyncReadExt;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use rand::Rng;
use std::{fmt::Debug, io, net::{Ipv4Addr, SocketAddr, SocketAddrV4}};
use tokio::{net::{TcpStream, TcpListener}, runtime::Runtime, task};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};
use futures::channel::mpsc::{unbounded, UnboundedSender, UnboundedReceiver};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::pin::Pin;
use futures::future::join;
use futures::task::Spawn;
use futures::executor::block_on;

#[test]
fn prop_config_send_recv_single() {
    fn prop(mut msgs: Vec<Msg>, cfg1: TestConfig, cfg2: TestConfig) -> TestResult {
        let rt = Runtime::new().unwrap();
        msgs.insert(0, Msg(vec![1u8; crate::DEFAULT_CREDIT as usize]));
        rt.block_on(async move {
            let num_requests = msgs.len();
            let iter = msgs.into_iter().map(|m| m.0);

            let (listener, address) = bind().await.expect("bind");

            let server = async {
                let socket = listener.accept().await.expect("accept").0.compat();
                let connection = Connection::new(socket, cfg1.0, Mode::Server);
                repeat_echo(connection).await.expect("repeat_echo")
            };

            let client = async {
                let socket = TcpStream::connect(address).await.expect("connect").compat();
                let connection = Connection::new(socket, cfg2.0, Mode::Client);
                let control = connection.control();
                task::spawn(crate::into_stream(connection).for_each(|_| future::ready(())));
                send_recv_single(control, iter.clone()).await.expect("send_recv")
            };

            let result = futures::join!(server, client).1;
            TestResult::from_bool(result.len() == num_requests && result.into_iter().eq(iter))
        })
    }
    QuickCheck::new().tests(10).quickcheck(prop as fn(_, _, _) -> _)
}

#[test]
fn prop_config_send_recv_multi() {
    fn prop(mut msgs: Vec<Msg>, cfg1: TestConfig, cfg2: TestConfig) -> TestResult {
        let rt = Runtime::new().unwrap();
        msgs.insert(0, Msg(vec![1u8; crate::DEFAULT_CREDIT as usize]));
        rt.block_on(async move {
            let num_requests = msgs.len();
            let iter = msgs.into_iter().map(|m| m.0);

            let (listener, address) = bind().await.expect("bind");

            let server = async {
                let socket = listener.accept().await.expect("accept").0.compat();
                let connection = Connection::new(socket, cfg1.0, Mode::Server);
                repeat_echo(connection).await.expect("repeat_echo")
            };

            let client = async {
                let socket = TcpStream::connect(address).await.expect("connect").compat();
                let connection = Connection::new(socket, cfg2.0, Mode::Client);
                let control = connection.control();
                task::spawn(crate::into_stream(connection).for_each(|_| future::ready(())));
                send_recv(control, iter.clone()).await.expect("send_recv")
            };

            let result = futures::join!(server, client).1;
            TestResult::from_bool(result.len() == num_requests && result.into_iter().eq(iter))
        })
    }
    QuickCheck::new().tests(10).quickcheck(prop as fn(_, _, _) -> _)
}

#[test]
fn prop_send_recv() {
    fn prop(msgs: Vec<Msg>) -> TestResult {
        if msgs.is_empty() {
            return TestResult::discard()
        }
        let rt = Runtime::new().unwrap();
        rt.block_on(async move {
            let num_requests = msgs.len();
            let iter = msgs.into_iter().map(|m| m.0);

            let (listener, address) = bind().await.expect("bind");

            let server = async {
                let socket = listener.accept().await.expect("accept").0.compat();
                let connection = Connection::new(socket, Config::default(), Mode::Server);
                repeat_echo(connection).await.expect("repeat_echo")
            };

            let client = async {
                let socket = TcpStream::connect(address).await.expect("connect").compat();
                let connection = Connection::new(socket, Config::default(), Mode::Client);
                let control = connection.control();
                task::spawn(crate::into_stream(connection).for_each(|_| future::ready(())));
                send_recv(control, iter.clone()).await.expect("send_recv")
            };

            let result = futures::join!(server, client).1;
            TestResult::from_bool(result.len() == num_requests && result.into_iter().eq(iter))
        })
    }
    QuickCheck::new().tests(1).quickcheck(prop as fn(_) -> _)
}

#[test]
fn prop_max_streams() {
    fn prop(n: usize) -> bool {
        let max_streams = n % 100;
        let mut cfg = Config::default();
        cfg.set_max_num_streams(max_streams);

        let rt = Runtime::new().unwrap();
        rt.block_on(async move {
            let (listener, address) = bind().await.expect("bind");

            let cfg_s = cfg.clone();
            let server = async move {
                let socket = listener.accept().await.expect("accept").0.compat();
                let connection = Connection::new(socket, cfg_s, Mode::Server);
                repeat_echo(connection).await
            };

            task::spawn(server);

            let socket = TcpStream::connect(address).await.expect("connect").compat();
            let connection = Connection::new(socket, cfg, Mode::Client);
            let mut control = connection.control();
            task::spawn(crate::into_stream(connection).for_each(|_| future::ready(())));
            let mut v = Vec::new();
            for _ in 0 .. max_streams {
                v.push(control.open_stream().await.expect("open_stream"))
            }
            if let Err(ConnectionError::TooManyStreams) = control.open_stream().await {
                true
            } else {
                false
            }
        })
    }
    QuickCheck::new().tests(7).quickcheck(prop as fn(_) -> _)
}

#[test]
fn prop_send_recv_half_closed() {
    fn prop(msg: Msg) {
        let msg_len = msg.0.len();
        let rt = Runtime::new().unwrap();
        rt.block_on(async move {
            let (listener, address) = bind().await.expect("bind");

            // Server should be able to write on a stream shutdown by the client.
            let server = async {
                let socket = listener.accept().await.expect("accept").0.compat();
                let mut connection = Connection::new(socket, Config::default(), Mode::Server);
                let mut stream = connection.next_stream().await
                    .expect("S: next_stream")
                    .expect("S: some stream");
                task::spawn(crate::into_stream(connection).for_each(|_| future::ready(())));
                let mut buf = vec![0; msg_len];
                stream.read_exact(&mut buf).await.expect("S: read_exact");
                stream.write_all(&buf).await.expect("S: send");
                stream.close().await.expect("S: close")
            };

            // Client should be able to read after shutting down the stream.
            let client = async {
                let socket = TcpStream::connect(address).await.expect("connect").compat();
                let connection = Connection::new(socket, Config::default(), Mode::Client);
                let mut control = connection.control();
                task::spawn(crate::into_stream(connection).for_each(|_| future::ready(())));
                let mut stream = control.open_stream().await.expect("C: open_stream");
                stream.write_all(&msg.0).await.expect("C: send");
                stream.close().await.expect("C: close");
                assert_eq!(State::SendClosed, stream.state());
                let mut buf = vec![0; msg_len];
                stream.read_exact(&mut buf).await.expect("C: read_exact");
                assert_eq!(buf, msg.0);
                assert_eq!(Some(0), stream.read(&mut buf).await.ok());
                assert_eq!(State::Closed, stream.state());
            };

            futures::join!(server, client);
        })
    }
    QuickCheck::new().tests(7).quickcheck(prop as fn(_))
}

#[derive(Clone, Debug)]
struct Msg(Vec<u8>);

impl Arbitrary for Msg {
    fn arbitrary<G: Gen>(g: &mut G) -> Msg {
        let n: usize = g.gen_range(1, g.size() + 1);
        let mut v = vec![0; n];
        g.fill(&mut v[..]);
        Msg(v)
    }
}

#[derive(Clone, Debug)]
struct TestConfig(Config);

impl Arbitrary for TestConfig {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        let mut c = Config::default();
        c.set_window_update_mode(if g.gen() {
            WindowUpdateMode::OnRead
        } else {
            WindowUpdateMode::OnReceive
        });
        c.set_read_after_close(g.gen());
        c.set_receive_window(g.gen_range(256 * 1024, 1024 * 1024));
        TestConfig(c)
    }
}

async fn bind() -> io::Result<(TcpListener, SocketAddr)> {
    let i = Ipv4Addr::new(127, 0, 0, 1);
    let s = SocketAddr::V4(SocketAddrV4::new(i, 0));
    let l = TcpListener::bind(&s).await?;
    let a = l.local_addr()?;
    Ok((l, a))
}

/// For each incoming stream of `c` echo back to the sender.
async fn repeat_echo(c: Connection<Compat<TcpStream>>) -> Result<(), ConnectionError> {
    let c = crate::into_stream(c);
    c.try_for_each_concurrent(None, |mut stream| async move {
        {
            let (mut r, mut w) = futures::io::AsyncReadExt::split(&mut stream);
            futures::io::copy(&mut r, &mut w).await?;
        }
        stream.close().await?;
        Ok(())
    })
    .await
}

/// For each message in `iter`, open a new stream, send the message and
/// collect the response. The sequence of responses will be returned.
async fn send_recv<I>(mut control: Control, iter: I) -> Result<Vec<Vec<u8>>, ConnectionError>
where
    I: Iterator<Item = Vec<u8>>
{
    let mut result = Vec::new();

    for msg in iter {
        let stream = control.open_stream().await?;
        log::debug!("C: new stream: {}", stream);
        let id = stream.id();
        let len = msg.len();
        let (mut reader, mut writer) = AsyncReadExt::split(stream);
        let write_fut = async {
            writer.write_all(&msg).await.unwrap();
            log::debug!("C: {}: sent {} bytes", id, len);
            writer.close().await.unwrap();
        };
        let mut data = Vec::new();
        let read_fut = async {
            reader.read_to_end(&mut data).await.unwrap();
            log::debug!("C: {}: received {} bytes", id, data.len());
        };
        futures::future::join(write_fut, read_fut).await;
        result.push(data);
    }

    log::debug!("C: closing connection");
    control.close().await?;
    Ok(result)
}

/// Open a stream, send all messages and collect the responses. The
/// sequence of responses will be returned.
async fn send_recv_single<I>(mut control: Control, iter: I) -> Result<Vec<Vec<u8>>, ConnectionError>
where
    I: Iterator<Item = Vec<u8>>
{
    let stream = control.open_stream().await?;
    log::debug!("C: new stream: {}", stream);
    let id = stream.id();
    let (mut reader, mut writer) = AsyncReadExt::split(stream);
    let mut result = Vec::new();
    for msg in iter {
        let len = msg.len();
        let write_fut = async {
            writer.write_all(&msg).await.unwrap();
            log::debug!("C: {}: sent {} bytes", id, len);
        };
        let mut data = vec![0; msg.len()];
        let read_fut = async {
            reader.read_exact(&mut data).await.unwrap();
            log::debug!("C: {}: received {} bytes", id, data.len());
        };
        futures::future::join(write_fut, read_fut).await;
        result.push(data)
    }
    writer.close().await?;
    log::debug!("C: closing connection");
    control.close().await?;
    Ok(result)
}

#[test]
fn deadlock_low_capacity_link() {
    use futures::task::SpawnExt;

    let _ = env_logger::try_init();

    let mut pool = futures::executor::LocalPool::new();
    // Message is 10x smaller than link capacity.
    let msg = vec![1u8; 1024];
    // On my machine stalls with > 12.
    let num_streams = 100;
    let (tx, rx) = unbounded();

    let (server_endpoint, client_endpoint) = bounded_capacity::Endpoint::new(10 * 1024);

    // Create and spawn a server that echoes every message back to the client.
    let server = Connection::new(server_endpoint, Config::default(), Mode::Server);
    pool.spawner().spawn_obj(async move {
        crate::into_stream(server)
            .try_for_each_concurrent(None, |mut stream| async move {
                {
                    let (mut r, mut w) = futures::io::AsyncReadExt::split(&mut stream);
                    futures::io::copy(&mut r, &mut w).await?;
                }
                log::debug!("S: stream {} done.", stream.id());
                stream.close().await?;
                Ok(())
            })
            .await
            .expect("server works")
    }.boxed().into()).unwrap();

    // Create and spawn a client.
    let client = Connection::new(client_endpoint, Config::default(), Mode::Client);
    let ctrl = client.control();
    pool.spawner()
        .spawn_obj(crate::into_stream(client).for_each(|r| async { r.unwrap(); }).boxed().into())
        .unwrap();

    // Handles to tasks on which to wait for completion.
    let mut handles = Vec::new();

    // Create `num_streams` streams, sending and receiving `msg` on each.
    for _ in 0..num_streams {
        let msg = msg.clone();
        let mut ctrl = ctrl.clone();
        let tx = tx.clone();
        handles.push(pool.spawner().spawn_with_handle(async move {
            let stream = ctrl.open_stream().await.unwrap();
            let (mut reader, mut writer) = AsyncReadExt::split(stream);
            let mut b = vec![0; msg.len()];
            let _ = join(
                writer.write_all(msg.as_ref()).map_err(|e| panic!(e)),
                reader.read_exact(&mut b[..]).map_err(|e| panic!(e)),
            ).await;
            let mut stream = reader.reunite(writer).unwrap();
            stream.close().await.unwrap();
            log::debug!("C: Stream {} done.", stream.id());
            tx.unbounded_send(b.len()).unwrap();
        }.boxed()).unwrap());
    };

    // Wait for completion.
    for h in handles {
        pool.run_until(h);
    }

    drop(pool);
    drop(tx);

    // Expect each of the `num_streams` tasks to finish, reporting the amount of bytes they send and
    // received.
    let n = block_on(rx.fold(0, |acc, n| future::ready(acc + n)));
    assert_eq!(n, num_streams * msg.len());
}

mod bounded_capacity {
    use super::*;
    use futures::ready;
    use std::io::{Error, ErrorKind, Result};

    pub struct Endpoint {
        sender: UnboundedSender<Vec<u8>>,
        receiver: UnboundedReceiver<Vec<u8>>,
        next_item: Option<Vec<u8>>,

        shared_send: Arc<Mutex<Shared>>,
        shared_receive: Arc<Mutex<Shared>>,

        capacity: usize,
    }

    #[derive(Default)]
    struct Shared {
        size: usize,
        waker_write: Option<Waker>,
    }

    impl Endpoint {
        pub fn new(capacity: usize) -> (Endpoint, Endpoint) {
            let (a_to_b_sender, a_to_b_receiver) = unbounded();
            let (b_to_a_sender, b_to_a_receiver) = unbounded();

            let a_to_b_shared = Arc::new(Mutex::new(Default::default()));
            let b_to_a_shared = Arc::new(Mutex::new(Default::default()));

            let a = Endpoint {
                sender: a_to_b_sender,
                receiver: b_to_a_receiver,
                next_item: None,
                shared_send: a_to_b_shared.clone(),
                shared_receive: b_to_a_shared.clone(),
                capacity,
            };

            let b = Endpoint {
                sender: b_to_a_sender,
                receiver: a_to_b_receiver,
                next_item: None,
                shared_send: b_to_a_shared,
                shared_receive: a_to_b_shared,
                capacity,
            };

            (a, b)

        }
    }

    impl AsyncRead for Endpoint {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<Result<usize>> {
            let item = match self.next_item.as_mut() {
                Some(item) => item,
                None => match ready!(self.receiver.poll_next_unpin(cx)) {
                    Some(item) => {
                        self.next_item = Some(item);
                        self.next_item.as_mut().unwrap()
                    }
                    None => {
                        return Poll::Ready(Ok(0));
                    }
                },
            };

            let n = std::cmp::min(buf.len(), item.len());

            buf[0..n].copy_from_slice(&item[0..n]);

            if n < item.len() {
                *item = item.split_off(n);
            } else {
                self.next_item.take().unwrap();
            }

            let mut shared = self.shared_receive.lock().unwrap();
            if let Some(waker) = shared.waker_write.take() {
                waker.wake();
            }

            debug_assert!(shared.size >= n);
            shared.size -= n;

            Poll::Ready(Ok(n))
        }
    }

    impl AsyncWrite for Endpoint {
        fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
            let mut shared = self.shared_send.lock().unwrap();
            let n = std::cmp::min(self.capacity - shared.size, buf.len());
            if n == 0 {
                shared.waker_write = Some(cx.waker().clone());
                return Poll::Pending;
            }

            self.sender
                .unbounded_send(buf[0..n].to_vec())
                .map_err(|e| Error::new(ErrorKind::ConnectionAborted, e))?;

            shared.size += n;

            Poll::Ready(Ok(n))
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
            ready!(self.sender.poll_flush_unpin(cx)).unwrap();
            Poll::Ready(Ok(()))
        }

        fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
            ready!(self.sender.poll_close_unpin(cx)).unwrap();
            Poll::Ready(Ok(()))
        }
    }
}
