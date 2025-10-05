#![allow(unreachable_code)]

use anyhow::Context;
use ipc::IpcMessage;
use iroh::endpoint::RecvStream;
use iroh::endpoint::SendStream;
use iroh::Endpoint;
use iroh::NodeAddr;
use iroh::PublicKey;
use std::fs;
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::task::Context as Ctx;
use std::task::Poll;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::ReadBuf;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::net::UnixListener;
use tokio::net::UnixStream;

use crate::daemon::ipc::SocketAddr;

pub mod ipc;

pub struct Daemon {
    endpoint: Endpoint,
    socket: PathBuf,
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
        })
    }
    pub async fn run(&self) -> anyhow::Result<()> {
        println!("🚀 IroHole daemon started");
        // println!("✨ Node ID: {}", self.endpoint.node_id());
        let listener = UnixListener::bind(self.socket.clone())?;
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let ep = self.endpoint.clone();
                    println!("✨ New Client Attached");
                    tokio::spawn(async move {
                        if let Err(e) = handle_client(stream, ep).await {
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

async fn handle_client(stream: UnixStream, endpoint: Endpoint) -> anyhow::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    let first_message = lines.next_line().await?;

    if let Some(line) = first_message {
        let command = line.parse::<IpcMessage>()?;
        let ep = endpoint.clone();

        match command {
            IpcMessage::Serve { name, addr } => {
                // here we serve our stream info the p2p iroh protocol
                handle_proxy_connection(ep, addr).await?;
                let response = IpcMessage::Data {
                    name,
                    message: format!("registered at {}", addr),
                };
                writer.write_all(response.to_string().as_bytes()).await?;
            }

            IpcMessage::Connect { name, addr, node } => {
                connect_remote_peer(endpoint, addr, node.clone()).await?;
                let response = IpcMessage::Data {
                    name,
                    message: format!("connecting to {}", node),
                };
                writer.write_all(response.to_string().as_bytes()).await?;
            }
            _ => {}
        }
    }
    Ok(())
}
/// serve connection to the remote network
async fn handle_proxy_connection(endpoint: Endpoint, addr: SocketAddr) -> anyhow::Result<()> {
    loop {
        let incoming = endpoint.accept().await.context("No incoming connections")?;
        tokio::spawn({
            async move {
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
                                if let Err(e) =
                                    tokio::io::copy_bidirectional(&mut tcp_stream, &mut iroh_stream)
                                        .await
                                {
                                    eprintln!("Proxy error: {:?}", e);
                                }
                            } else {
                                eprintln!("Failed to connect to local server");
                            }
                        }
                        Err(e) => eprintln!("Failed to accept Iroh stream: {:?}", e),
                    }
                }
            }
        });
    }
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

// fn generate_id() -> ConnectionId {
//     let mut rng = rand::rng();
//     rng.random::<ConnectionId>()
// }
