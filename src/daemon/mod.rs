use ipc::IpcServer;
use iroh::{Endpoint, protocol::Router};
use rand::Rng;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::Interest;
use tokio::net::UnixListener;
use tokio::net::UnixStream;
use tunnelmanager::TunnelManager;

mod ipc;
mod tunnelmanager;

type ConnectionId = u64;

struct Daemon {
    endpoint: Endpoint,
    socket: PathBuf,
    tunnels: TunnelManager,
}

impl Daemon {
    pub async fn new() -> anyhow::Result<Self> {
        let socket = std::env::temp_dir().join("irohole.sock");
        Ok(Self {
            endpoint: Endpoint::builder()
                .discovery_n0()
                .discovery_local_network()
                .alpns(vec![b"irohole/1".to_vec()])
                .bind()
                .await?,
            socket,
            tunnels: TunnelManager::new(),
        })
    }
    pub async fn run(&self) -> anyhow::Result<()> {
        println!("🚀 IroHole daemon starting...");
        println!("✨ Node ID: {}", self.endpoint.node_id());
        let listener = UnixListener::bind(self.socket.clone())?;
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    println!("✨ New Client Attached");
                    tokio::spawn(async move {
                        if let Err(e) = handle_client(stream).await {
                            eprintln!("🚨 IPC client error: {:?}", e);
                        }
                    });
                }
                Err(err) => eprintln!("🚨 Error sending message {:?}", err),
            }
        }
        // tokio::signal::ctrl_c().await?;
        // println!("🎯 Exiting the service");
        Ok(())
    }
}

async fn handle_client(mut stream: UnixStream) -> anyhow::Result<()> {
    let (reader, mut writer) = stream.split();
    let mut lines = BufReader::new(reader).lines();
    while let Some(line) = lines.next_line().await? {
        let command = line.trim();
        let response = match command {
            "serve" => "ok: service registered",
            "connect" => "ok: tunnel connected",
            _ => "error: unknown command",
        };
        writer.write_all(response.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }

    println!("Client disconnected");
    Ok(())
}

fn generate_id() -> ConnectionId {
    let mut rng = rand::rng();
    rng.random::<ConnectionId>()
}
