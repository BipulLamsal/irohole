use crate::daemon::ipc::{IpcMessageType, SocketAddr, handle_ipc_connection, send_stop_command};

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

        println!("🎉 Tunnel ready!");
        println!("   Tunnel address served: http://localhost:{}", self.port);
        println!("   Press Ctrl+C to stop serving");

        let name = service_name.clone();

        tokio::select! {
            result = handle_ipc_connection(service_name, addr.into(), IpcMessageType::Serve, None) => {
                result?;
            }

            _ = tokio::signal::ctrl_c() => {
                if let Err(e) = send_stop_command(name).await {
                    eprintln!("Failed to stop tunnel: {:?}", e);
                }
            }
        }
        Ok(())
    }
}
