use std::net::SocketAddrV4;

use anyhow::anyhow;
use tokio::net::TcpStream;

use crate::daemon::ipc::{handle_ipc_connection, IpcMessageType, SocketAddr};

pub struct Connect {
    node: String,
    name: String,
    ip: [u8; 4],
    port: u16,
}

impl Connect {
    pub fn new(name: String, node: String, ip: Option<[u8; 4]>, port: u16) -> Self {
        Self {
            node,
            name,
            ip: ip.unwrap_or([127, 0, 0, 1]), // localhost by default
            port,
        }
    }
    pub async fn start(&self) -> anyhow::Result<()> {
        let addr = SocketAddr(self.ip, self.port);
        let test = self.test_local_service(addr).await;
        if test.is_ok() {
            return Err(anyhow!("Port already running cannot served on {}", addr));
        }
        handle_ipc_connection(
            self.name.clone(),
            addr.into(),
            IpcMessageType::Connect,
            Some(self.node.to_string()),
        )
        .await?;
        Ok(())
    }
    /// Test if the target service is responding
    async fn test_local_service(&self, socket: SocketAddr) -> anyhow::Result<()> {
        TcpStream::connect::<SocketAddrV4>(socket.into())
            .await
            .map_err(|_| anyhow::anyhow!("Port: {:?} not responding!", socket))?;
        Ok(())
    }
}
