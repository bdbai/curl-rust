#![cfg(feature = "websockets_7_86_0")]

use std::error::Error;
use std::io::{self, Cursor, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};

use curl::easy::websockets::{WebsocketFrameType, WebsocketOptions};
use curl::easy::Easy;
use tungstenite::protocol::Role;
use tungstenite::Message;

#[derive(Debug, Clone, PartialEq, Eq)]
enum WebSocketEvents {
    ErrorOccurred(String),
    Accepted,
    MsgReceived(Message),
    Closed,
}

fn server_handle_inner(
    stream: TcpStream,
    tx: &mut Sender<WebSocketEvents>,
) -> Result<(), Box<dyn Error>> {
    let mut websocket = tungstenite::accept(stream)?;
    tx.send(WebSocketEvents::Accepted).ok();

    websocket.send(Message::text("banner"))?;
    websocket
        .send(Message::binary(b"123".to_vec()))
        .map_err(|e| e.to_string())?;
    loop {
        let read_res = websocket.read()?;
        tx.send(WebSocketEvents::MsgReceived(read_res.clone())).ok();
        if read_res.is_close() {
            websocket.send(Message::Close(None)).ok();
            break;
        }
    }
    tx.send(WebSocketEvents::Closed).ok();
    Ok(())
}

fn setup_server() -> (SocketAddr, Receiver<WebSocketEvents>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind TCP listener");
    let listen_addr = listener.local_addr().unwrap();
    let (mut tx, rx) = std::sync::mpsc::channel();

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(e) = server_handle_inner(stream, &mut tx) {
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
fn test_blocking_websocket() {
    let (addr, rx) = setup_server();

    let mut easy = curl::easy::Easy::new();
    easy.url(&format!("ws://{}", addr)).unwrap();
    easy.websocket_connect_only()
        .expect("Failed to set websocket connect only");
    easy.perform().expect("Failed to perform easy request");

    assert_eq!(rx.recv().unwrap(), WebSocketEvents::Accepted);

    let mut buf = vec![0; 16];
    let (read_len, meta) = easy
        .websocket_recv(&mut buf)
        .expect("Failed to receive banner text");
    assert!(meta.is_text());
    assert_eq!(buf[0..read_len], b"banner"[..]);
    assert_eq!(meta.bytes_left(), 0);
    assert_eq!(meta.length(), read_len);
    assert_eq!(meta.offset(), 0);

    let (read_len, meta) = easy
        .websocket_recv(&mut buf)
        .expect("Failed to receive binary data");
    assert!(meta.is_binary());
    assert_eq!(buf[0..read_len], b"123"[..]);

    easy.websocket_send(b"ciallo", WebsocketFrameType::Text, None, false)
        .expect("Failed to send text message");
    assert_eq!(
        rx.recv().unwrap(),
        WebSocketEvents::MsgReceived(Message::text("ciallo"))
    );

    easy.websocket_send(b"\xbe\xef", WebsocketFrameType::Binary, None, false)
        .expect("Failed to send binary message");
    assert_eq!(
        rx.recv().unwrap(),
        WebSocketEvents::MsgReceived(Message::binary(b"\xbe\xef".to_vec()))
    );

    easy.websocket_send(b"", WebsocketFrameType::Close, None, false)
        .expect("Failed to send close message");
    assert_eq!(
        rx.recv().unwrap(),
        WebSocketEvents::MsgReceived(Message::Close(None))
    );
    drop(easy);
    assert_eq!(rx.recv().unwrap(), WebSocketEvents::Closed);
}

#[test]
fn test_blocking_websocket_raw_mode() {
    let (addr, rx) = setup_server();

    let addr = format!("ws://{}", addr);
    let mut ws = tungstenite::WebSocket::from_raw_socket(
        CurlRawWsWrapper::connect(&addr),
        Role::Client,
        None,
    );

    assert_eq!(rx.recv().unwrap(), WebSocketEvents::Accepted);

    let msg = ws.read().expect("Failed to receive banner text");
    assert_eq!(msg, Message::text("banner"));

    let msg = ws.read().expect("Failed to receive binary data");
    assert_eq!(msg, Message::binary(b"123".to_vec()));

    ws.send(Message::text("ciallo"))
        .expect("Failed to send text message");
    assert_eq!(
        rx.recv().unwrap(),
        WebSocketEvents::MsgReceived(Message::text("ciallo"))
    );

    ws.send(Message::binary(b"\xbe\xef".to_vec()))
        .expect("Failed to send binary message");
    assert_eq!(
        rx.recv().unwrap(),
        WebSocketEvents::MsgReceived(Message::binary(b"\xbe\xef".to_vec()))
    );

    ws.send(Message::Close(None))
        .expect("Failed to send close message");
    assert_eq!(
        rx.recv().unwrap(),
        WebSocketEvents::MsgReceived(Message::Close(None))
    );
    drop(ws);
    assert_eq!(rx.recv().unwrap(), WebSocketEvents::Closed);
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
        easy.websocket_connect_only()
            .expect("Failed to set websocket connect only");
        easy.perform().expect("Failed to perform easy request");

        CurlRawWsWrapper { easy, read_buffer }
    }
}

impl Read for CurlRawWsWrapper {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.easy
            .recv(buf)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
        // self.easy
        //     .websocket_recv(buf)
        //     .map(|(len, _)| len)
        //     .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()));
        // let mut read_buffer = self.read_buffer.lock().unwrap();
        // read_buffer.read(buf)
    }
}

impl Write for CurlRawWsWrapper {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.easy
            .send(buf)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
        // self.easy
        //     .websocket_send(buf, WebsocketFrameType::Binary, Some(0), false)
        //     .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        // Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
