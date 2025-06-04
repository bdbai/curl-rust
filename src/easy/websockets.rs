//! Types for Websocket support.

use std::ffi::{c_uint, c_ulong};

/// Metadata of a received WebSocket frame.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct WebsocketFrameMetadata {
    raw: curl_sys::curl_ws_frame,
}

/// Frame messsage type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WebsocketFrameType {
    /// Only used when [`WebsocketOptions::raw_mode()`] when sending a frame.
    Raw,
    /// This is a message with text data. Note that this makes a difference to
    /// WebSocket but libcurl itself does not make any verification of the
    /// content or precautions that you actually receive valid UTF-8 content.
    Text,
    /// This is a message with binary data.
    Binary,
    /// This is a ping message. It may contain up to 125 bytes of payload text.
    /// libcurl does not verify that the payload is valid UTF-8.
    ///
    /// Upon receiving a ping message, libcurl automatically responds with a
    /// pong message unless the [`WebsocketOptions::raw_mode()`] option
    // /// or [`WebsocketOptions::no_auto_pong()`] option
    /// is set.
    Ping,
    /// This is a pong message. It may contain up to 125 bytes of payload text.
    /// libcurl does not verify that the payload is valid UTF-8.
    Pong,
    /// This is a close message. No more data follows.
    ///
    /// It may contain a 2-byte unsigned integer in network byte order that
    /// indicates the close reason and may additionally contain up to 123 bytes
    /// of further textual payload for a total of at most 125 bytes. libcurl
    /// does not verify that the textual description is valid UTF-8.
    Close,
}

/// Options to specify WebSocket behaviors.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WebsocketOptions {
    bitmask: c_ulong,
}

const FRAME_TYPE_MASK_MAPPING: [(c_uint, WebsocketFrameType); 5] = [
    (curl_sys::CURLWS_TEXT, WebsocketFrameType::Text),
    (curl_sys::CURLWS_BINARY, WebsocketFrameType::Binary),
    (curl_sys::CURLWS_PING, WebsocketFrameType::Ping),
    (curl_sys::CURLWS_PONG, WebsocketFrameType::Pong),
    (curl_sys::CURLWS_CLOSE, WebsocketFrameType::Close),
];

impl WebsocketFrameMetadata {
    pub(super) fn from_raw(frame: &curl_sys::curl_ws_frame) -> &Self {
        // Safety: `WebsocketFrameMetadata` has repr(transparent) over `curl_sys::curl_ws_frame`,
        // guranteeing the same memory layout.
        unsafe { std::mem::transmute(frame) }
    }

    /// Can only occur in conjunction with frame type
    /// [`WebsocketFrameType::Text`] or [`WebsocketFrameType::Binary`].
    ///
    /// This is not the final fragment of the message, it implies that there is
    /// another fragment coming as part of the same message. The application
    /// must reassemble the fragments to receive the complete message.
    ///
    /// Only a single fragmented message can be transmitted at a time, but it
    /// may be interrupted by [`WebsocketFrameType::Ping`],
    /// [`WebsocketFrameType::Pong`] or [`WebsocketFrameType::Close`] frames.
    #[inline]
    pub fn is_cont(&self) -> bool {
        self.raw.flags as u32 & curl_sys::CURLWS_CONT != 0
    }

    /// Get the message type of the frame.
    #[inline]
    pub fn frame_type(&self) -> WebsocketFrameType {
        if self.raw.flags == 0 {
            return WebsocketFrameType::Raw;
        }
        for (mask, frame_type) in FRAME_TYPE_MASK_MAPPING {
            if self.raw.flags as u32 & mask != 0 {
                return frame_type;
            }
        }
        unreachable!("Unknown websocket frame type: {:b}", self.raw.flags)
    }

    /// Check if the frame is a text message.
    #[inline]
    pub fn is_text(&self) -> bool {
        self.frame_type() == WebsocketFrameType::Text
    }

    /// Check if the frame is a binary message.
    #[inline]
    pub fn is_binary(&self) -> bool {
        self.frame_type() == WebsocketFrameType::Binary
    }

    /// Check if the frame is a ping message.
    #[inline]
    pub fn is_ping(&self) -> bool {
        self.frame_type() == WebsocketFrameType::Ping
    }

    /// Check if the frame is a pong message.
    #[inline]
    pub fn is_pong(&self) -> bool {
        self.frame_type() == WebsocketFrameType::Pong
    }

    /// Check if the frame is a close message.
    #[inline]
    pub fn is_close(&self) -> bool {
        self.frame_type() == WebsocketFrameType::Close
    }

    /// When this chunk is a continuation of frame data already delivered, this
    /// is the offset into the final frame data where this piece belongs to.
    #[inline]
    pub fn offset(&self) -> curl_sys::curl_off_t {
        self.raw.offset
    }

    /// If this is not a complete fragment, this property informs about how
    /// many additional bytes are expected to arrive before this fragment is
    /// complete.
    #[inline]
    pub fn bytes_left(&self) -> curl_sys::curl_off_t {
        self.raw.bytesleft
    }

    /// The length of the current data chunk.
    #[inline]
    pub fn length(&self) -> usize {
        self.raw.len
    }
}

impl WebsocketFrameType {
    pub(super) fn to_flag(self) -> c_uint {
        match self {
            WebsocketFrameType::Raw => 0,
            WebsocketFrameType::Text => curl_sys::CURLWS_TEXT,
            WebsocketFrameType::Binary => curl_sys::CURLWS_BINARY,
            WebsocketFrameType::Ping => curl_sys::CURLWS_PING,
            WebsocketFrameType::Pong => curl_sys::CURLWS_PONG,
            WebsocketFrameType::Close => curl_sys::CURLWS_CLOSE,
        }
    }
}

impl WebsocketOptions {
    #[must_use]
    /// Create a [`WebsocketOptions`] with no options set.
    pub fn new() -> Self {
        Self { bitmask: 0 }
    }

    pub(super) fn to_bitmask(self) -> c_ulong {
        self.bitmask
    }

    /// Tell libcurl to pass on the data from the network without parsing it,
    /// leaving that entirely to the application.
    ///
    /// This mode is intended for applications that already have a WebSocket
    /// parser/engine and want to switch over to use libcurl for enabling
    /// WebSocket, and keep parts of the existing software architecture.
    ///
    /// In raw mode, libcurl does not handle pings or any other frame for the
    /// application.
    pub fn raw_mode(&self) -> Self {
        Self {
            bitmask: self.bitmask | curl_sys::CURLWS_RAW_MODE,
        }
    }

    // /// Disable the automatic reply to PING messages. This means users must
    // /// send a PONG message with `websocket_send`.
    // ///
    // /// This feature is added with version 8.14.0.
    // pub fn no_auto_pong(&self) -> Self {
    //     Self {
    //         bitmask: self.bitmask | curl_sys::CURLWS_NOAUTOPONG,
    //     }
    // }
}
