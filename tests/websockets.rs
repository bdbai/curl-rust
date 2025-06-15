#![cfg(feature = "websockets_7_86_0")]

use std::error::Error;
use std::io::{self, Cursor, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::windows::io::AsRawSocket;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use curl::easy::websockets::{WebsocketFrameType, WebsocketOptions};
use curl::easy::Easy;
use curl::multi::Multi;
use curl_sys::{curl_socket_t, fd_set};
use tungstenite::protocol::Role;
use tungstenite::Message;

#[derive(Debug, Clone, PartialEq, Eq)]
enum WebSocketEvents {
    ErrorOccurred(String),
    Accepted,
    MsgReceived(Message),
    Closed,
}

fn server_handle_download(
    stream: TcpStream,
    tx: &mut Sender<WebSocketEvents>,
) -> Result<(), Box<dyn Error>> {
    let mut websocket = tungstenite::accept(stream)?;
    tx.send(WebSocketEvents::Accepted).ok();

    // Make sure the client is properly blocking until data is ready
    std::thread::sleep(std::time::Duration::from_millis(10));

    websocket.send(Message::text("banner"))?;
    websocket.send(Message::binary(b"123".to_vec()))?;
    websocket.send(Message::Ping(Default::default()))?;
    websocket.send(Message::Close(None))?;
    tx.send(WebSocketEvents::Closed)?;
    Ok(())
}
fn server_handle_upload(
    stream: TcpStream,
    tx: &mut Sender<WebSocketEvents>,
) -> Result<(), Box<dyn Error>> {
    let mut websocket = tungstenite::accept(stream)?;
    tx.send(WebSocketEvents::Accepted).ok();

    // Make sure the client is properly blocking until data is ready
    std::thread::sleep(std::time::Duration::from_millis(10));
    eprintln!("sending banner");
    websocket.send(Message::text("banner"))?;
    std::thread::sleep(std::time::Duration::from_millis(10));
    eprintln!("sending binary");
    websocket.send(Message::binary(b"123".to_vec()))?;
    std::thread::sleep(std::time::Duration::from_millis(10));
    eprintln!("sending ping");
    websocket.send(Message::Ping(Default::default()))?;
    std::thread::sleep(std::time::Duration::from_millis(10));
    loop {
        eprintln!("waiting for messages");
        let read_res = websocket.read()?;
        tx.send(WebSocketEvents::MsgReceived(read_res.clone())).ok();
        if read_res.is_close() {
            eprintln!("sending close message");
            websocket.send(Message::Close(None)).ok();
            break;
        }
    }
    tx.send(WebSocketEvents::Closed).ok();
    Ok(())
}

fn setup_server(
    handler: fn(TcpStream, &mut Sender<WebSocketEvents>) -> Result<(), Box<dyn Error>>,
) -> (SocketAddr, Receiver<WebSocketEvents>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind TCP listener");
    let listen_addr = listener.local_addr().unwrap();
    let (mut tx, rx) = std::sync::mpsc::channel();

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(e) = handler(stream, &mut tx) {
                        eprintln!("Error handling connection: {}", e);
                        tx.send(WebSocketEvents::ErrorOccurred(e.to_string())).ok();
                    }
                }
                Err(e) => eprintln!("Error accepting connection: {}", e),
            }
        }
    });

    (listen_addr, rx)
}

#[test]
fn test_blocking_websocket_download() {
    let (addr, _rx) = setup_server(server_handle_download);

    let mut easy = curl::easy::Easy::new();
    easy.url(&format!("ws://{}", addr)).unwrap();
    let buf = Arc::new(Mutex::new(vec![]));
    easy.write_function({
        let buf = Arc::clone(&buf);
        move |data| {
            let mut buf = buf.lock().unwrap();
            buf.extend_from_slice(data);
            buf.push(b'\n');

            Ok(data.len())
        }
    })
    .expect("Failed to set write function");
    easy.perform().ok(); // Received error when closed: [56] Recv failure: Connection was aborted

    drop(easy);
    assert_eq!(*buf.lock().unwrap(), b"banner\n123\n"[..]);
}

#[test]
fn test_blocking_websocket_download_raw_mode() {
    let (addr, _rx) = setup_server(server_handle_download);

    let addr = format!("ws://{}", addr);
    let mut ws = tungstenite::WebSocket::from_raw_socket(
        CurlRawWsWrapper::connect(&addr),
        Role::Client,
        None,
    );
    ws.get_mut().wait_all();

    let msg = ws.read().expect("Failed to read from WebSocket");
    assert_eq!(msg, Message::Text("banner".into()));

    let msg = ws.read().expect("Failed to read from WebSocket");
    assert_eq!(msg, Message::Binary(b"123"[..].into()));

    let msg = ws.read().expect("Failed to read from WebSocket");
    assert_eq!(msg, Message::Ping(Default::default()));

    let msg = ws.read().expect("Failed to read from WebSocket");
    assert!(msg.is_close());

    let e = ws.read().unwrap_err();
    assert!(matches!(e, tungstenite::Error::ConnectionClosed));
}

#[test]
fn test_nonblocking_websocket_twoway() {
    let (addr, rx) = setup_server(server_handle_upload);
    // TODO: assert rx

    let addr = format!("ws://{}", addr);
    let mut easy = Easy::new();
    easy.url(&addr).expect("Failed to set URL");
    easy.websocket_connect_only()
        .expect("Failed to set WebSocket connect only");

        curl_socket_t
    let mut multi = Multi::new();
    let mut easy_handle = multi.add(easy).expect("Failed to add easy handle to multi");

    #[derive(Debug)]
    enum State {
        WaitingForConnect,
        Connected,
        AwaitingBinary,
        AwaitingPing,
        AwaitingClose,
    }

    let mut running = 1;
    let mut state = State::WaitingForConnect;
    'multi_loop: loop {
        if running == 0 {
            panic!("No more running handles?");
        }
        let mut read = fd_set {
            fd_count: 0,
            fd_array: [0; 64],
        };
        let mut write = fd_set {
            fd_count: 0,
            fd_array: [0; 64],
        };
        multi
            .fdset2(Some(&mut read), Some(&mut write), None)
            .expect("Failed to set fdset2");
        dbg!((read.fd_count, write.fd_count));
        multi
            .wait(&mut [][..], Duration::from_secs(1))
            .expect("Failed to wait for multi");

        running = multi.perform().expect("Failed to perform multi");
        let mut res = None;
        multi.messages(|msg| {
            res = msg.result_for(&easy_handle);
        });
        match dbg!(res) {
            None => continue,
            Some(Err(e)) => panic!("Error in multi messages: {}", e),
            Some(Ok(())) if matches!(state, State::WaitingForConnect) => {
                state = State::Connected;
                // let easy = multi
                //     .remove(easy_handle)
                //     .expect("Failed to remove easy handle");
                // easy_handle = multi.add(easy).expect("Failed to re-add easy handle");
            }
            _ => {}
        }

        loop {
            println!("State: {state:?}");
            let mut read_buf = [0; 256];
            let res = easy_handle.websocket_recv(&mut read_buf);
            let (size, meta) = match dbg!(res) {
                Ok((size, meta)) => (size, meta),
                Err(e) if e.is_again() => {
                    continue 'multi_loop;
                }
                Err(e) => panic!("Failed to receive WebSocket message: {}", e),
            };
            match state {
                State::Connected => {
                    assert_eq!(meta.frame_type(), WebsocketFrameType::Text);
                    assert_eq!(size, 6);
                    assert_eq!(&read_buf[..size], b"banner");
                    state = State::AwaitingBinary;
                }
                State::AwaitingBinary => {
                    assert_eq!(meta.frame_type(), WebsocketFrameType::Binary);
                    assert_eq!(size, 3);
                    assert_eq!(&read_buf[..size], b"123");
                    state = State::AwaitingPing;
                }
                State::AwaitingPing => {
                    assert_eq!(meta.frame_type(), WebsocketFrameType::Ping);
                    assert_eq!(size, 0);
                    state = State::AwaitingClose;
                }
                State::AwaitingClose => {
                    assert_eq!(meta.frame_type(), WebsocketFrameType::Close);
                    assert_eq!(size, 0);
                    state = State::WaitingForConnect;
                }
                _ => {}
            };
        }
    }
}

struct CurlRawWsWrapper {
    easy: Easy,
    read_buffer: Arc<Mutex<Cursor<Vec<u8>>>>,
}

impl CurlRawWsWrapper {
    pub fn connect(addr: &str) -> Self {
        let mut easy = Easy::new();
        let read_buffer = Arc::new(Mutex::new(Cursor::new(Vec::new())));
        easy.url(addr).expect("Failed to set URL");
        easy.websocket_options(WebsocketOptions::new().raw_mode())
            .expect("Failed to set raw mode");
        easy.write_function({
            let read_buffer = Arc::clone(&read_buffer);
            move |data| {
                let mut read_buffer = read_buffer.lock().unwrap();
                read_buffer
                    .write(data)
                    .expect("Failed to write to read buffer");
                Ok(data.len())
            }
        })
        .expect("Failed to set write function");

        CurlRawWsWrapper { easy, read_buffer }
    }

    pub fn wait_all(&mut self) {
        self.easy.perform().ok();
        self.read_buffer.lock().unwrap().set_position(0);
    }
}

impl Read for CurlRawWsWrapper {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut read_buffer = self.read_buffer.lock().unwrap();
        read_buffer.read(buf)
    }
}

impl Write for CurlRawWsWrapper {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
