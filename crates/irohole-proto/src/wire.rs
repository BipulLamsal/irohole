//! Canonical wire addresses.

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum WireId {
    Tcp = 0x01,
    Input = 0x02,
    Screen = 0x03,
    Shell = 0x04,
}

impl WireId {
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn from_u8(b: u8) -> Result<Self> {
        match b {
            0x01 => Ok(Self::Tcp),
            0x02 => Ok(Self::Input),
            0x03 => Ok(Self::Screen),
            0x04 => Ok(Self::Shell),
            _ => Err(Error::UnknownWireId(b)),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Input => "input",
            Self::Screen => "screen",
            Self::Shell => "shell",
        }
    }
}

impl From<WireId> for u8 {
    fn from(id: WireId) -> u8 {
        id.as_u8()
    }
}
