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

/// expose a local port or dial one over P2P.
#[derive(clap::Args, Debug)]
pub struct TcpCommand {
    #[command(subcommand)]
    pub action: TcpAction,
}

#[derive(clap::Subcommand, Debug)]
pub enum TcpAction {
    /// Host the target over P2P and print a ticket.
    Serve(TcpServeArgs),
    /// Dial a ticket and expose the host locally.
    Connect(TcpConnectArgs),
}

#[derive(clap::Args, Debug)]
pub struct TcpServeArgs {
    /// Local address of the TCP service to expose. Default: (127.0.0.1:8080)
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub target: String,
}

#[derive(clap::Args, Debug)]
pub struct TcpConnectArgs {
    /// Ticket printed by the host.
    #[arg(long)]
    pub ticket: String,
    /// Local address to listen on. Default: 127.0.0.1:9090.
    #[arg(long, default_value = "127.0.0.1:9090")]
    pub listen: String,
}

impl TcpCommand {
    pub async fn run(self) -> anyhow::Result<()> {
        match self.action {
            TcpAction::Serve(a) => {
                let target: SocketAddr = a.target.parse()?;
                let mut reg = irohole_core::Registry::new();
                reg.register(TcpTunnelPlugin::new(target))?;
                irohole_core::with_ctrl_c(irohole_core::serve(reg)).await
            }
            TcpAction::Connect(a) => {
                let listen: SocketAddr = a.listen.parse()?;
                irohole_core::with_ctrl_c(irohole_core::connect(&a.ticket, listen)).await
            }
        }
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
