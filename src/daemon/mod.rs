#![allow(unreachable_code)]
use anyhow::Context;
use ipc::IpcMessage;
use iroh::Endpoint;
use iroh::NodeAddr;
use iroh::PublicKey;
use iroh::endpoint::RecvStream;
use iroh::endpoint::SendStream;
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
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::net::UnixListener;
use tokio::net::UnixStream;
use tokio::net::unix::OwnedWriteHalf;
use tokio::sync::Mutex;
use tokio::sync::oneshot;

use crate::daemon::ipc::IPC_SOCKET;
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
    ticket: String,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

pub enum EventMessage {
    Data(String),
    Error(String),
}

impl Daemon {
    pub async fn new() -> anyhow::Result<Self> {
        let socket = std::env::temp_dir().join(IPC_SOCKET);
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
                        let msg = IpcMessage::Data {
                            message: format!("Tunnel '{}' already exists", name),
                        };
                        writer.write_all(msg.to_string().as_bytes()).await?;
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
                let encoded_ticket = format!("{}@{}", ticket, endpoint.node_id());
                let tunnel = Tunnel {
                    name: name.clone(),
                    addr,
                    ticket: encoded_ticket.clone(),
                    tunnel_type: IpcMessageType::Serve,
                    shutdown_tx,
                };
                {
                    let mut tunnels_map = tunnels.lock().await;
                    tunnels_map.insert(ticket, tunnel);
                }
                let data = IpcMessage::Data {
                    message: format!("Share this ticket : {}", encoded_ticket),
                };
                writer.write_all(data.to_string().as_bytes()).await?;

                handle_proxy_connection(ep, addr, tunnels.clone(), shutdown_rx, writer).await?;
            }

            IpcMessage::Connect { name, addr, ticket } => {
                let mut ticket_iter = ticket.split("@");
                let map_key: u16 = ticket_iter
                    .next()
                    .and_then(|s| s.parse::<u16>().ok())
                    .unwrap_or(0);

                let node_id: String = ticket_iter.next().unwrap_or("").to_string();
                let (shutdown_tx, shutdown_rx) = oneshot::channel();

                let conn_tunnel = Tunnel {
                    name: name.clone(),
                    addr,
                    ticket: ticket.clone(),
                    tunnel_type: IpcMessageType::Connect,
                    shutdown_tx,
                };
                tunnels.lock().await.insert(map_key, conn_tunnel);
                connect_remote_peer(endpoint, addr, node_id, map_key, shutdown_rx, writer).await?;
                // let response = IpcMessage::Data {
                //     name,
                //     message: format!("connecting to {}", ticket),
                // };
                // writer.write_all(response.to_string().as_bytes()).await?;
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
                            let error = IpcMessage::Error {
                                message: format!("Unable tol Stop the  tunnel: {}", name),
                            };

                            writer.write_all(error.to_string().as_bytes()).await?;
                        }
                        let data = IpcMessage::Data {
                            message: format!("Stopped tunnel: {}", name),
                        };
                        writer.write_all(data.to_string().as_bytes()).await?;
                    }
                } else {
                    let error = IpcMessage::Error {
                        message: "Tunnel not found".to_string(),
                    };
                    writer.write_all(error.to_string().as_bytes()).await?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

pub async fn handle_proxy_connection(
    endpoint: Endpoint,
    addr: SocketAddr,
    tunnels: Tunnels,
    mut shutdown_rx: oneshot::Receiver<()>,
    mut writer: OwnedWriteHalf,
) -> anyhow::Result<()> {
    // tx and rx for writer events
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<EventMessage>();

    let data = IpcMessage::Data {
        message: format!("Tunnel active: {}, waiting for connections...", addr),
    };
    writer.write_all(data.to_string().as_bytes()).await?;

    loop {
        tokio::select! {
            // Accept a new incoming Iroh connection
            result = endpoint.accept() => {
                let incoming = result.context("No incoming connections")?;
                let remote_addr = incoming.remote_address();

                let data = IpcMessage::Data {
                    message: format!("New connection from {}", remote_addr),
                };
                writer.write_all(data.to_string().as_bytes()).await?;

                let event_tx = event_tx.clone();
                let addr_clone = addr.clone();
                let tunnels = tunnels.clone();

                tokio::spawn(async move {
                    if let Ok(iroh_conn) = incoming.await {
                        loop {
                            match iroh_conn.accept_bi().await {
                                Ok((send_stream, mut recv_stream)) => {
                                    let event_tx = event_tx.clone();
                                    let addr_clone = addr_clone.clone();

                                    let mut key_buf = [0u8; 2];
                                 if recv_stream.read_exact(&mut key_buf).await.is_err(){
                                    return;
                                 }
                                // extract which port bad boi wants to listen to

                                 let map_key = u16::from_be_bytes(key_buf);
                                 let tunnels_map = tunnels.lock().await;
                                 let tunnel = tunnels_map.get(&map_key);
                                 if tunnel.is_none(){
                                return;
                             }
                                let port = tunnel.unwrap().addr;

                                drop(tunnels_map);

                                    tokio::spawn(async move {
                                        // connect to the local TCP server
                                        if let Ok(mut tcp_stream) =
                                            TcpStream::connect::<SocketAddrV4>(port.into()).await
                                        {
                                            let mut iroh_stream = IrohBiStream {
                                                recv: recv_stream,
                                                send: send_stream,
                                            };

                                            // Bidirectional pipe
                                            match tokio::io::copy_bidirectional(
                                                &mut tcp_stream,
                                                &mut iroh_stream,
                                            ).await {
                                                Ok((to_server, to_client)) => {
                                                    let _ = event_tx.send(EventMessage::Data(format!(
                                                        "Connection closed: ↑{}B ↓{}B",
                                                        to_server, to_client
                                                    )));
                                                }
                                                Err(e) => {
                                                    let _ = event_tx.send(EventMessage::Error(format!(
                                                        "Proxy error: {:?}",
                                                        e
                                                    )));
                                                }
                                            }
                                        } else {
                                            let _ = event_tx.send(EventMessage::Error(
                                                "Failed to connect to local server".to_string(),
                                            ));
                                        }
                                    });
                                }
                                Err(e) => {
                                    // connection closed or errored; exit loop
                                    let _ = event_tx.send(EventMessage::Error(format!(
                                        "Failed to accept bi-stream: {:?}",
                                        e
                                    )));
                                    break;
                                }
                            }
                        }
                    } else {
                        let _ = event_tx.send(EventMessage::Error(
                            "Failed to await incoming connection".to_string(),
                        ));
                    }
                });
            }

            // Send messages from event queue to writer
            Some(msg) = event_rx.recv() => {
                let event: IpcMessage = match msg {
                    EventMessage::Data(v) => IpcMessage::Data { message: v },
                    EventMessage::Error(v) => IpcMessage::Error { message: v },
                };
                writer.write_all(event.to_string().as_bytes()).await?;
            }

            // Shutdown signal
            _ = &mut shutdown_rx => {
                let msg = IpcMessage::Data {
                    message: "Tunnel shutting down...".to_string(),
                };
                writer.write_all(msg.to_string().as_bytes()).await?;
                break;
            }
        }
    }

    Ok(())
}

// /// serve connection to the remote network
// async fn handle_proxy_connection(
//     endpoint: Endpoint,
//     addr: SocketAddr,
//     tunnels: Tunnels,
//     mut shutdown_rx: oneshot::Receiver<()>,
//     mut writer: OwnedWriteHalf,
// ) -> anyhow::Result<()> {
//     // tx and rx for writer events
//     let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<EventMessage>();
//     let data = IpcMessage::Data {
//         message: format!("Tunnel active: {}, waiting for connections...", addr),
//     };
//     writer.write_all(data.to_string().as_bytes()).await?;
//
//     loop {
//         tokio::select! {
//             result = endpoint.accept() => {
//                 let incoming = result.context("No incoming connections")?;
//                 let remote_addr = incoming.remote_address();
//
//                 let data = IpcMessage::Data {
//                         message: format!("New connection from {}", remote_addr),
//                 };
//                 writer.write_all(data.to_string().as_bytes()).await?;
//
//                 let event_tx = event_tx.clone();
//                 // let tunnels_clone = tunnels.clone();
//
//                 tokio::spawn(async move {
//                     if let Ok(iroh_conn) = incoming.await {
//                         match iroh_conn.accept_bi().await {
//                             Ok((send_stream,  recv_stream)) =>
//                             {
//                                 // let mut key_buf = [0u8; 2];
//                                 //
//                                 // if recv_stream.read_exact(&mut key_buf).await.is_err(){
//                                 //     return;
//                                 // }
//                                 // extract which port bad boi wants to listen to
//
//                                 // let map_key = u16::from_be_bytes(key_buf);
//                                 // let tunnels_map = tunnels_clone.lock().await;
//                                 // let tunnel = tunnels_map.get(&map_key);
//                                 // if tunnel.is_none(){
//                                     // return;
//                                 // }
//                                 // let port = tunnel.unwrap().addr;
//
//                                 // drop(tunnels_map);
//
//                                 if let Ok(mut tcp_stream) =
//                                     TcpStream::connect::<SocketAddrV4>(addr.into()).await
//                                 {
//                                     let mut iroh_stream = IrohBiStream {
//                                         recv: recv_stream,
//                                         send: send_stream,
//                                     };
//
//                                     // let mut peek_buf = [0u8; 512];
//                                     // if let Ok(n) = tcp_stream.peek(&mut peek_buf).await && n > 0 {
//                                     //     let line = String::from_utf8_lossy(&peek_buf[..n]);
//                                     //     if let Some(first_line) = line.lines().next() {
//                                     //         let parts: Vec<&str> = first_line.split_whitespace().collect();
//                                     //         if parts.len() >= 2
//                                     //             && (parts[0] == "GET"
//                                     //                 || parts[0] == "POST"
//                                     //                 || parts[0] == "PUT"
//                                     //                 || parts[0] == "DELETE")
//                                     //         {
//                                     //             let _ = event_tx.send(EventMessage::Data( format!("{} {}", parts[0], parts[1])));
//                                     //         }
//                                     //     }
//                                     // }
//
//                                     match tokio::io::copy_bidirectional(&mut tcp_stream, &mut iroh_stream).await {
//                                         Ok((to_server, to_client)) => {
//                                             let _ = event_tx.send(EventMessage::Data(format!(
//                                                 "Connection closed: ↑{}B ↓{}B",
//                                                 to_server, to_client
//                                             )));
//                                         }
//                                         Err(e) => {
//                                             let _ = event_tx.send(EventMessage::Data(format!("Proxy error: {:?}", e)));
//                                         }
//                                     }
//                                 } else {
//                                     let _ = event_tx.send(EventMessage::Data("Failed to connect to local server".to_string()));
//                                 }
//                             }
//                             Err(e) => {
//                                 let _ = event_tx.send(EventMessage::Data(format!("Failed to accept stream: {:?}", e)));
//                             }
//                         }
//                     }
//                 });
//             }
//
//             Some(msg) = event_rx.recv() => {
//                 let event : IpcMessage = match msg {
//                     EventMessage::Data(v) => {
//                         IpcMessage::Data { message: v }
//                     }
//                     EventMessage::Error(v) => {
//                          IpcMessage::Error { message: v }
//
//                     }
//                 };
//                 writer.write_all(event.to_string().as_bytes()).await?;
//             }
//
//             // shutdown signal killing the loops and will drop the result
//             _ = &mut shutdown_rx => {
//                 let msg = IpcMessage::Data{message : "Tunnel shutting down...".to_string()};
//                 writer.write_all(msg.to_string().as_bytes()).await?;
//                 break;
//             }
//         }
//     }
//
//     Ok(())
// }

/// connect to the remote peer
async fn connect_remote_peer(
    endpoint: Endpoint,
    target_socket: SocketAddr,
    node_addr: String,
    map_key: u16,
    mut shutdown_rx: oneshot::Receiver<()>,
    mut writer: OwnedWriteHalf,
) -> anyhow::Result<()> {
    let pk = PublicKey::from_str(node_addr.as_str())?;
    let addr = NodeAddr::new(pk);
    let quic_connection = endpoint.connect(addr, b"irohole/1").await?;
    let listener = TcpListener::bind::<SocketAddrV4>(target_socket.into()).await?;

    // tx and rx for writer events
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<EventMessage>();
    let data = IpcMessage::Data {
        message: format!("Connected! Listening on {}", target_socket),
    };

    writer.write_all(data.to_string().as_bytes()).await?;

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (mut local_stream, client_addr) = result?;
                let quic_connection = quic_connection.clone();
                let data = IpcMessage::Data {
                    message: format!("New connection from {}", client_addr),
                };

                writer.write_all(data.to_string().as_bytes()).await?;

                let event_tx = event_tx.clone();

                tokio::spawn(async move {
                    match quic_connection.open_bi().await {
                        Ok(( mut send, recv)) => {
                            // initial as the tunnel_key
                            if let Err(e) = send.write_all(&map_key.to_be_bytes()).await {
                                let _ = event_tx.send(EventMessage::Data(format!("Failed to send map_key: {}", e)));
                                return;
                            }

                            let mut iroh_stream = IrohBiStream { recv, send };
                            if let Err(e) = tokio::io::copy_bidirectional(&mut local_stream, &mut iroh_stream).await {
                                let _ = event_tx.send(EventMessage::Error(format!("Proxy Tunnel Error Unable to copy the stream, {}", e)));
                            }
                        }
                        Err(e) => {let _ = event_tx.send(EventMessage::Data(format!("Unable to Connect via Iroh, {}", e)));}
                    }
                });
            }

            Some(msg) = event_rx.recv() => {
                let event : IpcMessage = match msg {
                    EventMessage::Data(v) => {
                        IpcMessage::Data { message: v }
                    }
                    EventMessage::Error(v) => {
                         IpcMessage::Error { message: v }

                    }
                };
                writer.write_all(event.to_string().as_bytes()).await?;
            }


            _ = &mut shutdown_rx => {
                let msg = IpcMessage::Data{message : "Tunnel shutting down...".to_string()};
                writer.write_all(msg.to_string().as_bytes()).await?;
                break;
            }
        }
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
