//! Length-prefixed frame: [wire_id: u8][kind: u8][len: u32 BE][payload]

use crate::error::{Error, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_FRAME: u32 = 16 * 1024 * 1024;
pub const HEADER_LEN: usize = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    Message = 0x01,
    Stream = 0x02,
}

impl FrameKind {
    pub fn from_u8(b: u8) -> Result<Self> {
        match b {
            0x01 => Ok(Self::Message),
            0x02 => Ok(Self::Stream),
            _ => Err(Error::BadPayload("unknown frame kind")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub wire_id: u8,
    pub kind: FrameKind,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn message(wire_id: u8, payload: Vec<u8>) -> Self {
        Self {
            wire_id,
            kind: FrameKind::Message,
            payload,
        }
    }

    pub fn stream_open(wire_id: u8) -> Self {
        Self {
            wire_id,
            kind: FrameKind::Stream,
            payload: Vec::new(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + self.payload.len());
        out.push(self.wire_id);
        out.push(self.kind as u8);
        out.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < HEADER_LEN {
            return Err(Error::Truncated("header"));
        }
        let wire_id = buf[0];
        let kind = FrameKind::from_u8(buf[1])?;
        let len = u32::from_be_bytes([buf[2], buf[3], buf[4], buf[5]]);
        if len > MAX_FRAME {
            return Err(Error::FrameTooLarge(len));
        }
        let total = HEADER_LEN + len as usize;
        if buf.len() < total {
            return Err(Error::Truncated("payload"));
        }
        Ok((
            Self {
                wire_id,
                kind,
                payload: buf[HEADER_LEN..total].to_vec(),
            },
            total,
        ))
    }

    pub async fn write_to<W: AsyncWrite + Unpin>(&self, w: &mut W) -> Result<()> {
        w.write_u8(self.wire_id)
            .await
            .map_err(|_| Error::Truncated("wire_id"))?;
        w.write_u8(self.kind as u8)
            .await
            .map_err(|_| Error::Truncated("kind"))?;
        w.write_u32(self.payload.len() as u32)
            .await
            .map_err(|_| Error::Truncated("len"))?;
        w.write_all(&self.payload)
            .await
            .map_err(|_| Error::Truncated("payload"))?;
        Ok(())
    }

    pub async fn read_from<R: AsyncRead + Unpin>(r: &mut R) -> Result<Self> {
        let wire_id = r.read_u8().await.map_err(|_| Error::Truncated("wire_id"))?;
        let kind = r.read_u8().await.map_err(|_| Error::Truncated("kind"))?;
        let kind = FrameKind::from_u8(kind)?;
        let len = r.read_u32().await.map_err(|_| Error::Truncated("len"))?;
        if len > MAX_FRAME {
            return Err(Error::FrameTooLarge(len));
        }
        let mut payload = vec![0u8; len as usize];
        r.read_exact(&mut payload)
            .await
            .map_err(|_| Error::Truncated("payload"))?;
        Ok(Self {
            wire_id,
            kind,
            payload,
        })
    }
}
