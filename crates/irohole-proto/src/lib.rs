pub mod error;
pub mod frame;
pub mod wire;

pub use error::{Error, Result};
pub use frame::{Frame, FrameKind, HEADER_LEN, MAX_FRAME};
pub use wire::WireId;

pub struct Ctx {
    pub peer: String,
}

pub trait IoStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}

impl<T> IoStream for T where T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}

#[async_trait::async_trait]
pub trait Plugin: Send + Sync + 'static {
    fn id(&self) -> &'static str;
    fn wire_id(&self) -> WireId;

    /// message handler are used when we need to just connect or reply within a single connection
    async fn on_message(&self, ctx: &Ctx, payload: Vec<u8>) -> Result<Option<Vec<u8>>>;

    /// mechanism to own the bi-directional stream for consistent connection  
    async fn on_stream(&self, _ctx: &Ctx, _io: Box<dyn IoStream>) -> Result<()> {
        Err(Error::Denied)
    }
}
