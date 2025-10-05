#![allow(unreachable_code)]
use anyhow::Context;
use ipc::IpcMessage;
use iroh::endpoint::RecvStream;
use iroh::endpoint::SendStream;
use iroh::Endpoint;
use iroh::NodeAddr;
use iroh::PublicKey;
use rand::Rng;
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::Context as Ctx;
use std::task::Poll;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::ReadBuf;
use tokio::net::unix::OwnedWriteHalf;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::net::UnixListener;
use tokio::net::UnixStream;
use tokio::sync::oneshot;
use tokio::sync::Mutex;

use crate::daemon::ipc::IpcMessageType;
use crate::daemon::ipc::SocketAddr;

pub mod ipc;

type Tunnels = Arc<Mutex<HashMap<u16, Tunnel>>>;

pub struct Daemon {
    endpoint: Endpoint,
    socket: PathBuf,
    tunnels: Tunnels,
}
pub struct Tunnel {
    name: String,
    addr: SocketAddr,
    tunnel_type: IpcMessageType,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl Daemon {
    pub async fn new() -> anyhow::Result<Self> {
        let socket = std::env::temp_dir().join("irohole.sock");
        if socket.exists() {
            fs::remove_file(&socket)?;
        }
        Ok(Self {
            endpoint: Endpoint::builder()
                .discovery_n0()
                .discovery_local_network()
                .alpns(vec![b"irohole/1".to_vec()])
                .bind()
                .await?,
            socket,
            tunnels: Arc::new(Mutex::new(HashMap::new())),
        })
    }
    pub async fn run(&self) -> anyhow::Result<()> {
        println!("🚀 IroHole daemon started");
        let listener = UnixListener::bind(self.socket.clone())?;
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let ep = self.endpoint.clone();
                    let tunnels = self.tunnels.clone();
                    println!("✨ New Client Attached");
                    tokio::spawn(async move {
                        if let Err(e) = handle_client(stream, ep, tunnels).await {
                            eprintln!("🚨 IPC client error: {:?}", e);
                        }
                    });
                }
                Err(err) => eprintln!("🚨 Error sending message {:?}", err),
            }
        }
        Ok(())
    }
}

async fn handle_client(
    stream: UnixStream,
    endpoint: Endpoint,
    tunnels: Tunnels,
) -> anyhow::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    let first_message = lines.next_line().await?;

    if let Some(line) = first_message {
        let command = line.parse::<IpcMessage>()?;
        let ep = endpoint.clone();

        match command {
            IpcMessage::Serve { name, addr } => {
                // here we serve our stream info the p2p iroh protocol
                let ticket = {
                    let tunnels_map = tunnels.lock().await;
                    let name_exists = tunnels_map.values().any(|t| t.name == name);
                    if name_exists {
                        writer
                            .write_all(format!("Tunnel '{}' already exists\n", name).as_bytes())
                            .await?;
                        return Ok(());
                    }

                    loop {
                        let ticket = generate_id();
                        if !tunnels_map.contains_key(&ticket) {
                            break ticket; // found unique ticket, exit loop
                        }
                    }
                };
                let (shutdown_tx, shutdown_rx) = oneshot::channel();
                let tunnel = Tunnel {
                    name: name.clone(),
                    addr,
                    tunnel_type: IpcMessageType::Serve,
                    shutdown_tx,
                };
                {
                    let mut tunnels_map = tunnels.lock().await;
                    tunnels_map.insert(ticket, tunnel);
                }

                handle_proxy_connection(ep, addr, shutdown_rx, writer).await?;
            }

            IpcMessage::Connect { name, addr, node } => {
                connect_remote_peer(endpoint, addr, node.clone()).await?;
                let response = IpcMessage::Data {
                    name,
                    message: format!("connecting to {}", node),
                };
                writer.write_all(response.to_string().as_bytes()).await?;
            }

            IpcMessage::Stop { name } => {
                let mut tunnels_map = tunnels.lock().await;
                let tunnel_to_stop = tunnels_map
                    .iter()
                    .find(|(_, tunnel)| tunnel.name == name)
                    .map(|(key, _)| *key);
                if let Some(ticket_id) = tunnel_to_stop {
                    if let Some(tunnel) = tunnels_map.remove(&ticket_id) {
                        if tunnel.shutdown_tx.send(()).is_err() {
                            writer
                                .write_all(
                                    format!("Unable tol Stop the  tunnel: {}\n", name).as_bytes(),
                                )
                                .await?;
                        }
                        writer
                            .write_all(format!("Stopped tunnel: {}\n", name).as_bytes())
                            .await?;
                    }
                } else {
                    writer.write_all(b"Tunnel not found\n").await?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}
/// serve connection to the remote network
async fn handle_proxy_connection(
    endpoint: Endpoint,
    addr: SocketAddr,
    mut shutdown_rx: oneshot::Receiver<()>,
    mut writer: OwnedWriteHalf,
) -> anyhow::Result<()> {
    // tx and rx for writer events
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();

    writer
        .write_all(format!("Tunnel active: {}, waiting for connections...\n", addr).as_bytes())
        .await?;

    loop {
        tokio::select! {
            result = endpoint.accept() => {
                let incoming = result.context("No incoming connections")?;
                let remote_addr = incoming.remote_address();

                writer
                    .write_all(format!("New connection from {}\n", remote_addr).as_bytes())
                    .await?;

                let event_tx = event_tx.clone();

                tokio::spawn(async move {
                    if let Ok(iroh_conn) = incoming.await {
                        match iroh_conn.accept_bi().await {
                            Ok((send_stream, recv_stream)) => {
                                if let Ok(mut tcp_stream) =
                                    TcpStream::connect::<SocketAddrV4>(addr.into()).await
                                {
                                    let mut iroh_stream = IrohBiStream {
                                        recv: recv_stream,
                                        send: send_stream,
                                    };

                                    let mut peek_buf = [0u8; 512];
                                    if let Ok(n) = tcp_stream.peek(&mut peek_buf).await && n > 0 {
                                        let line = String::from_utf8_lossy(&peek_buf[..n]);
                                        if let Some(first_line) = line.lines().next() {
                                            let parts: Vec<&str> = first_line.split_whitespace().collect();
                                            if parts.len() >= 2
                                                && (parts[0] == "GET"
                                                    || parts[0] == "POST"
                                                    || parts[0] == "PUT"
                                                    || parts[0] == "DELETE")
                                            {
                                                let _ = event_tx.send(format!("{} {}\n", parts[0], parts[1]));
                                            }
                                        }
                                    }

                                    match tokio::io::copy_bidirectional(&mut tcp_stream, &mut iroh_stream).await {
                                        Ok((to_server, to_client)) => {
                                            let _ = event_tx.send(format!(
                                                "Connection closed: ↑{}B ↓{}B\n",
                                                to_server, to_client
                                            ));
                                        }
                                        Err(e) => {
                                            let _ = event_tx.send(format!("Proxy error: {:?}\n", e));
                                        }
                                    }
                                } else {
                                    let _ = event_tx.send("Failed to connect to local server\n".to_string());
                                }
                            }
                            Err(e) => {
                                let _ = event_tx.send(format!("Failed to accept stream: {:?}\n", e));
                            }
                        }
                    }
                });
            }

            Some(msg) = event_rx.recv() => {
                writer.write_all(msg.as_bytes()).await?;
            }

            // shutdown signal killing the loops and will drop the result
            _ = &mut shutdown_rx => {
                writer.write_all(b"Tunnel shutting down...\n").await?;
                break;
            }
        }
    }

    Ok(())
}

/// connect to the remote peer
async fn connect_remote_peer(
    endpoint: Endpoint,
    target_socket: SocketAddr,
    node_addr: String,
) -> anyhow::Result<()> {
    let pk = PublicKey::from_str(node_addr.as_str())?;
    let addr = NodeAddr::new(pk);
    let quic_connection = endpoint.connect(addr, b"irohole/1").await?;
    let listener = TcpListener::bind::<SocketAddrV4>(target_socket.into()).await?;
    loop {
        let (mut local_stream, _addr) = listener.accept().await?;
        let quic_connection = quic_connection.clone();
        tokio::spawn(async move {
            match quic_connection.open_bi().await {
                Ok((send, recv)) => {
                    let mut iroh_stream = IrohBiStream { recv, send };
                    if let Err(e) =
                        tokio::io::copy_bidirectional(&mut local_stream, &mut iroh_stream).await
                    {
                        eprintln!("Proxy error: {:?}", e);
                    }
                }
                Err(e) => eprintln!("Failed to open Iroh stream: {:?}", e),
            }
        });
    }

    Ok(())
}

struct IrohBiStream {
    recv: RecvStream,
    send: SendStream,
}

impl AsyncRead for IrohBiStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Ctx<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.recv).poll_read(cx, buf)
    }
}

impl AsyncWrite for IrohBiStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let res = Pin::new(&mut self.send).poll_write(cx, buf);
        res.map_err(|e| std::io::Error::other(e.to_string()))
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Ctx<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.send).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Ctx<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.send).poll_shutdown(cx)
    }
}

fn generate_id() -> u16 {
    let mut rng = rand::rng();
    rng.random::<u16>()
}
