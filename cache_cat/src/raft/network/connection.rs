use crate::raft::network::redis_server::RespCodec;
use std::io::IoSlice;
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, RawSocket};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::{
    TlsStream, client::TlsStream as TlsClientStream, server::TlsStream as TlsServerStream,
};

pub trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncReadWrite for T {}

#[allow(clippy::large_enum_variant)]
pub enum Connection {
    Tcp(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl Connection {
    #[inline]
    pub fn as_stream(&self) -> &TcpStream {
        match self {
            Self::Tcp(x) => x,
            Self::Tls(x) => x.get_ref().0,
        }
    }
}

impl AsyncRead for Connection {
    #[inline]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(x) => Pin::new(x).poll_read(cx, buf),
            Self::Tls(x) => Pin::new(x).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Connection {
    #[inline]
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(x) => Pin::new(x).poll_write(cx, buf),
            Self::Tls(x) => Pin::new(x).poll_write(cx, buf),
        }
    }

    #[inline]
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(x) => Pin::new(x).poll_write_vectored(cx, bufs),
            Self::Tls(x) => Pin::new(x).poll_write_vectored(cx, bufs),
        }
    }

    #[inline]
    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Tcp(x) => x.is_write_vectored(),
            Self::Tls(x) => x.is_write_vectored(),
        }
    }

    #[inline]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(x) => Pin::new(x).poll_flush(cx),
            Self::Tls(x) => Pin::new(x).poll_flush(cx),
        }
    }

    #[inline]
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(x) => Pin::new(x).poll_shutdown(cx),
            Self::Tls(x) => Pin::new(x).poll_shutdown(cx),
        }
    }
}

#[cfg(windows)]
impl AsRawSocket for Connection {
    #[inline]
    fn as_raw_socket(&self) -> RawSocket {
        match self {
            Self::Tcp(x) => x.as_raw_socket(),
            Self::Tls(x) => x.as_raw_socket(),
        }
    }
}

#[cfg(unix)]
impl AsRawFd for Connection {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        match self {
            Self::Tcp(x) => x.as_raw_fd(),
            Self::Tls(x) => x.as_raw_fd(),
        }
    }
}

impl From<TcpStream> for Connection {
    #[inline]
    fn from(value: TcpStream) -> Self {
        Self::Tcp(value)
    }
}

impl From<TlsStream<TcpStream>> for Connection {
    #[inline]
    fn from(value: TlsStream<TcpStream>) -> Self {
        Self::Tls(value)
    }
}

impl From<TlsServerStream<TcpStream>> for Connection {
    #[inline]
    fn from(value: TlsServerStream<TcpStream>) -> Self {
        Self::Tls(TlsStream::Server(value))
    }
}

impl From<TlsClientStream<TcpStream>> for Connection {
    #[inline]
    fn from(value: TlsClientStream<TcpStream>) -> Self {
        Self::Tls(TlsStream::Client(value))
    }
}
