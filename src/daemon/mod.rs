#![allow(unreachable_code)]

use anyhow::Context;
use ipc::IpcMessage;
use iroh::endpoint::RecvStream;
use iroh::endpoint::SendStream;
use iroh::Endpoint;
use std::fs;
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Context as Ctx;
use std::task::Poll;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::ReadBuf;
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
        println!("🚀 IroHole daemon starting...");
        println!("✨ Node ID: {}", self.endpoint.node_id());
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

            IpcMessage::Connect {
                name,
                addr: _,
                node,
            } => {
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
async fn handle_proxy_connection(
    endpoint: Endpoint,
    target_socket: SocketAddr,
) -> anyhow::Result<()> {
    let mut tcp_stream = TcpStream::connect::<SocketAddrV4>(target_socket.into()).await?;
    let iroh_conn = endpoint
        .accept()
        .await
        .context("No Incoming Connections")?
        .await?;
    let (send_stream, recv_stream) = iroh_conn.accept_bi().await?;
    let mut iroh_stream = IrohBiStream {
        recv: recv_stream,
        send: send_stream,
    };
    tokio::io::copy_bidirectional(&mut tcp_stream, &mut iroh_stream).await?;
    // tokio::io::copy(&mut tcp_reader, &mut send_stream).await?;
    // tokio::io::copy(&mut recv_stream, &mut tcp_writer).await?;
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
