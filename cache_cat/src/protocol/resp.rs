use bytes::{Buf, Bytes, BytesMut};

use crate::raft::types::core::response_value::Value;

pub enum Parser {
    String {
        /// full size with `mode`, `line`, `eof`.
        len: usize,
    },

    Error {
        /// full size with `mode`, `line`, `eof`.
        len: usize,
    },

    Integer {
        /// full size with `mode`, `line`, `eof`.
        len: usize,

        // the value of integer
        value: i64,
    },

    Bytes {
        /// full size with `length-line`, `bytes`, `eof`.
        len: usize,

        /// `Some((pos, length))`
        bytes: Option<(usize, usize)>,
    },

    Array {
        /// full size with `mode`, `data`, `eof`.
        len: usize,

        /// `Some((pos, elements))`
        value: Option<(usize, Vec<Parser>)>,
    },
}

impl Parser {
    /// get the length of full parsed element,
    /// with `mode`, `data`, `eof` and so on.
    #[inline]
    pub const fn len(&self) -> usize {
        match self {
            Parser::String { len }
            | Parser::Error { len }
            | Parser::Integer { len, .. }
            | Parser::Bytes { len, .. }
            | Parser::Array { len, .. } => *len,
        }
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn take_from_bytes_stream(buffer: &mut BytesMut) -> Option<Value> {
        // check has `mode` (1 byte) and `eof` (2 bytes)
        if buffer.len() < 3 {
            return None;
        }

        // parse the metadata of element
        let meta = Self::parse_meta(buffer)?;
        let len = meta.len();
        // split the buffer data
        let buffer = buffer.split_to(len).freeze();
        // take the value
        Some(meta.take(buffer))
    }

    fn take(self, mut buffer: Bytes) -> Value {
        match self {
            Parser::String { len } => {
                Value::SimpleString(String::from_utf8_lossy(&buffer[1..len - 2]).into_owned())
            }

            Parser::Error { len } => {
                Value::Error(String::from_utf8_lossy(&buffer[1..len - 2]).into_owned())
            }

            Parser::Integer { value, .. } => Value::Integer(value),

            Parser::Bytes { bytes: None, .. } => Value::BulkString(None),

            Parser::Bytes {
                bytes: Some((pos, len)),
                ..
            } => {
                // split `length` line
                buffer.advance(pos);

                // split off `eof`
                Value::BulkString(Some(buffer.split_off(len)))
            }

            Parser::Array { value: None, .. } => Value::Array(None),

            Parser::Array {
                value: Some((pos, elements)),
                ..
            } => {
                // split `count` line
                buffer.advance(pos);

                // take values
                let elements = elements
                    .into_iter()
                    .map(move |element| {
                        let len = element.len();
                        let buffer = buffer.split_to(len);
                        element.take(buffer)
                    })
                    .collect::<Vec<_>>();

                Value::Array(Some(elements))
            }
        }
    }

    fn parse_meta(buffer: &[u8]) -> Option<Parser> {
        // check has `mode` (1 byte) and `eof` (2 bytes)
        if buffer.len() < 3 {
            return None;
        }

        let mode = buffer[0];

        match mode {
            b'+' => Self::parse_simple_string(buffer),
            b'-' => Self::parse_error(buffer),
            b':' => Self::parse_integer(buffer),
            b'$' => Self::parse_bulk_string(buffer),
            b'*' => Self::parse_array(buffer),
            _ => None,
        }
    }

    /// Read `line` from buffer, start with `mode`,
    /// end with `eof` ( `\r\n` ), return `Some((line, len))`.
    /// Returned `len` includes full size of `mode`, `line`, `eof`.
    ///
    /// ```text
    ///  1 byte       n bytes       2 bytes
    /// ┌──────┬────────────────────┬─────┐
    /// │ mode │       line         │ eof │
    /// └──────┴────────────────────┴─────┘
    /// ```
    #[inline]
    fn read_line(buffer: &[u8]) -> Option<(&[u8], usize)> {
        let index = Self::find_line(&buffer[1..])?;

        Some((&buffer[1..index + 1], index + 3))
    }

    /// find the index of `eof` ( `\r\n` ),
    /// also the length of `line`
    #[inline]
    fn find_line(buffer: &[u8]) -> Option<usize> {
        for (index, window) in buffer.array_windows::<2>().enumerate() {
            if window == b"\r\n" {
                return Some(index);
            }
        }

        None
    }

    /// Parse simple string from buffer using `line`.
    fn parse_simple_string(buffer: &[u8]) -> Option<Parser> {
        let (_, len) = Self::read_line(buffer)?;

        Some(Parser::String { len })
    }

    /// Parse error string from buffer using `line`.
    fn parse_error(buffer: &[u8]) -> Option<Parser> {
        let (_, len) = Self::read_line(buffer)?;

        Some(Parser::Error { len })
    }

    /// Return `Some((value, len))`.
    /// Returned `len` includes full size of `mode`, `line`, `eof`.
    #[inline]
    fn read_i64(buffer: &[u8]) -> Option<(i64, usize)> {
        let (line, len) = Self::read_line(buffer)?;
        let value = str::from_utf8(line).ok()?.parse::<i64>().ok()?;
        Some((value, len))
    }

    fn parse_integer(buffer: &[u8]) -> Option<Parser> {
        let (value, len) = Self::read_i64(buffer)?;

        Some(Parser::Integer { len, value })
    }

    /// Parse `bytes` form buffer.
    ///
    ///  ```text
    ///  1 byte   n bytes   2 bytes   `len` bytes   2 bytes
    /// ┌──────┬──────────┬────────┬──────────────┬────────┐
    /// │ mode │   len    │  eof   │    bytes     │  eof   │
    /// └──────┴──────────┴────────┴──────────────┴────────┘
    /// ```
    fn parse_bulk_string(buffer: &[u8]) -> Option<Parser> {
        let (len, pos) = Self::read_i64(buffer)?;

        let len = match len {
            -1 => {
                return Some(Parser::Bytes {
                    len: pos,
                    bytes: None,
                });
            }

            // TODO: Handle the Error
            ..0 => return None,

            len => len as usize,
        };

        let full = pos + len + 2;
        if full > buffer.len() {
            // the data is not completed
            return None;
        }

        Some(Parser::Bytes {
            len: full,
            bytes: Some((pos, len)),
        })
    }

    /// Parse `array` form buffer.
    ///
    /// ```text
    ///  1 byte   n bytes   2 bytes   1 byte  ...  2 bytes
    /// ┌──────┬──────────┬────────┬────────┬─────┬─────┐
    /// │ mode │  count   │  eof   │  mode  │ ... │ eof │
    /// └──────┴──────────┴────────┴────────┴─────┴─────┘
    /// ```
    fn parse_array(buffer: &[u8]) -> Option<Parser> {
        let (count, pos) = Self::read_i64(buffer)?;

        let count = match count {
            -1 => {
                return Some(Parser::Array {
                    len: pos,

                    value: None,
                });
            }

            // TODO: Handle the Error
            ..0 => return None,

            count => count as usize,
        };

        let mut elements = Vec::with_capacity(count);

        let mut full = pos;
        for _ in 0..count {
            let meta = Self::parse_meta(&buffer[full..])?;
            let len = meta.len();
            full += len;
            elements.push(meta);
        }

        Some(Parser::Array {
            len: full,
            value: Some((pos, elements)),
        })
    }
}
