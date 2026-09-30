use core::fmt;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Underlying transport closed or short read.
    Truncated(&'static str),
    /// length exceeds limit or is otherwise invalid.
    FrameTooLarge(u32),
    /// No plugin registered for this wire id.
    UnknownWireId(u8),
    /// Payload failed to decode.
    BadPayload(&'static str),
    /// Plugin level decision
    Denied,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated(value) => write!(f, "truncated frame at {value}"),
            Self::FrameTooLarge(n) => write!(f, "frame too large: {n}"),
            Self::UnknownWireId(b) => write!(f, "unknown wire id: {b:#04x}"),
            Self::BadPayload(value) => write!(f, "bad payload: {value}"),
            Self::Denied => write!(f, "plugin denied"),
        }
    }
}

impl core::error::Error for Error {}

pub type Result<T> = core::result::Result<T, Error>;

impl From<postcard::Error> for Error {
    fn from(_: postcard::Error) -> Self {
        Self::BadPayload("postcard decode failed")
    }
}
