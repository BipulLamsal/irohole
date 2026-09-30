use async_trait::async_trait;
use irohole_proto::{Ctx, Error, IoStream, Plugin, Result, WireId};
use std::net::SocketAddr;

pub struct TcpTunnelPlugin {
    target: SocketAddr,
}

impl TcpTunnelPlugin {
    pub fn new(target: SocketAddr) -> Self {
        Self { target }
    }
}

#[async_trait]
impl Plugin for TcpTunnelPlugin {
    fn id(&self) -> &'static str {
        WireId::Tcp.name()
    }
    fn wire_id(&self) -> WireId {
        WireId::Tcp
    }
    async fn on_message(&self, _ctx: &Ctx, _payload: Vec<u8>) -> Result<Option<Vec<u8>>> {
        Err(Error::Denied)
    }
    async fn on_stream(&self, _ctx: &Ctx, mut io: Box<dyn IoStream>) -> Result<()> {
        let mut tcp = tokio::net::TcpStream::connect(self.target)
            .await
            .map_err(|_| Error::Truncated("tcp dial"))?;
        tokio::io::copy_bidirectional(&mut tcp, &mut io)
            .await
            .map_err(|_| Error::Truncated("tcp splice"))?;
        Ok(())
    }
}
