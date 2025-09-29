use crate::daemon::ipc::{handle_ipc_connection, IpcMessageType, SocketAddr};

pub struct Service {
    name: String,
    ip: [u8; 4],
    port: u16,
}

impl Service {
    pub fn new(name: String, ip: Option<[u8; 4]>, port: u16) -> Self {
        Self {
            name,
            ip: ip.unwrap_or([127, 0, 0, 1]), // localhost by default
            port,
        }
    }

    /// Starts the proxy server and returns the proxy port
    pub async fn start(&self) -> anyhow::Result<()> {
        let addr = SocketAddr(self.ip, self.port);
        let service_name = self.name.clone();
        handle_ipc_connection(service_name, addr.into(), IpcMessageType::Serve, None).await?;
        Ok(())
    }
}
