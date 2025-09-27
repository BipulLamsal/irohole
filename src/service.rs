use std::net::{Ipv4Addr, SocketAddrV4};
use tokio::io::BufReader;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};

use tracing::{debug, error, info, warn};

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
    pub async fn start(&self) -> anyhow::Result<u16> {
        let socket = SocketAddrV4::new(
            Ipv4Addr::new(self.ip[0], self.ip[1], self.ip[2], self.ip[3]),
            self.port,
        );

        self.test_local_service(socket).await?;

        let proxy_port = self.find_available_port().await?;
        let target_socket = socket;
        let service_name = self.name.clone();
        tokio::spawn(async move { Self::run_proxy_server(target_socket, proxy_port).await });

        info!(proxy_port, "Proxy ready for '{}'", self.name);
        Ok(proxy_port)
    }

    /// Test if the target service is responding
    async fn test_local_service(&self, socket: SocketAddrV4) -> anyhow::Result<()> {
        TcpStream::connect(socket).await.map_err(|error| {
            error!(%socket, %error, "Local service not responding");
            anyhow::anyhow!(
                "Local service on {} not responding: {}. Start your service first!",
                socket,
                error
            )
        })?;
        Ok(())
    }

    /// Find an available port for the proxy
    async fn find_available_port(&self) -> anyhow::Result<u16> {
        for port in 9000..9999 {
            if TcpListener::bind(("127.0.0.1", port)).await.is_ok() {
                return Ok(port);
            }
        }
        anyhow::bail!("No available ports for proxy")
    }

    /// Run the actual proxy server
    async fn run_proxy_server(target_socket: SocketAddrV4, proxy_port: u16) -> anyhow::Result<()> {
        let listener = TcpListener::bind(("127.0.0.1", proxy_port)).await?;

        info!(%target_socket, proxy_port, "Listening for proxy connections");

        while let Ok((client_stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                if let Err(e) = Self::handle_proxy_connection(target_socket, client_stream).await {
                    warn!(%e, "Connection handler failed");
                }
            });
        }
        Ok(())
    }

    /// Handle a single proxy connection
    async fn handle_proxy_connection(
        target_socket: SocketAddrV4,
        mut client_stream: TcpStream,
    ) -> anyhow::Result<()> {
        let mut buf = [0u8; 1024];
        let n = client_stream.peek(&mut buf).await?;

        let first_line = std::str::from_utf8(&buf[..n])?
            .lines()
            .next()
            .unwrap_or_default();

        if n > 0 {
            let parts: Vec<&str> = first_line.trim_end().split_whitespace().collect();
            if parts.len() >= 2 {
                let method = parts[0];
                let path = parts[1];
                info!(%method, %path, "Incoming HTTP request");
            }
        }
        let mut target_stream = TcpStream::connect(target_socket).await?;
        // Copy data bidirectionally between client and target
        match copy_bidirectional(&mut client_stream, &mut target_stream).await {
            Ok((bytes_to_target, bytes_from_target)) => {
                info!(
                    bytes_to_target,
                    bytes_from_target, "Proxy connection finished"
                );
            }
            Err(e) => {
                error!(%e, "Connection closed with error");
            }
        }
        Ok(())
    }
}
