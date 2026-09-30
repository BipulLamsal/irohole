use std::str::FromStr;

use anyhow::Context;
use iroh::{Endpoint, EndpointId, endpoint::presets};
use tokio::net::TcpListener;

use irohole_proto::{Ctx, Frame, WireId};

use crate::Registry;

pub const ALPN: &[u8] = b"irohole/1";

pub async fn with_ctrl_c(
    fut: impl std::future::Future<Output = anyhow::Result<()>>,
) -> anyhow::Result<()> {
    tokio::select! {
        r = fut => r,
        _ = tokio::signal::ctrl_c() => {
            println!("Shutting down.");
            Ok(())
        }
    }
}

pub async fn create_endpoint() -> anyhow::Result<Endpoint> {
    let ep = Endpoint::builder(presets::N0)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await?;
    Ok(ep)
}

pub async fn serve(registry: Registry) -> anyhow::Result<()> {
    let ep = create_endpoint().await?;
    let ticket = ep.id().to_string();
    println!("Share this ticket: {ticket}");
    println!(
        "Hosting {} plugin(s). Ctrl+C to stop.",
        registry.ids().len()
    );

    loop {
        let incoming = ep.accept().await.context("endpoint closed")?;
        let registry = registry.clone();

        tokio::spawn(async move {
            let conn = match incoming.await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("incoming failed: {e:?}");
                    return;
                }
            };

            let peer = conn.remote_id().to_string();
            let ctx = Ctx { peer };

            loop {
                /* QUIC muliplexing connection accept */
                let (mut send, mut recv) = match conn.accept_bi().await {
                    Ok(s) => s,
                    Err(_) => break,
                };

                let registry = registry.clone();
                let ctx_peer = ctx.peer.clone();

                tokio::spawn(async move {
                    let ctx = Ctx { peer: ctx_peer };

                    match Frame::read_from(&mut recv).await {
                        Ok(frame) => {
                            let wire = match WireId::from_u8(frame.wire_id) {
                                Ok(w) => w,
                                Err(e) => {
                                    eprintln!("bad wire_id: {e}");
                                    return;
                                }
                            };

                            match registry.get(wire) {
                                Some(plugin) => match frame.kind {
                                    irohole_proto::FrameKind::Message => {
                                        match plugin.on_message(&ctx, frame.payload).await {
                                            Ok(Some(reply)) => {
                                                let out = Frame::message(wire.as_u8(), reply);

                                                if let Err(e) = out.write_to(&mut send).await {
                                                    eprintln!("reply write failed: {e}");
                                                }
                                            }
                                            Ok(None) => {}
                                            Err(e) => {
                                                eprintln!("plugin {} error: {e}", plugin.id())
                                            }
                                        }
                                    }
                                    irohole_proto::FrameKind::Stream => {
                                        let io: Box<dyn irohole_proto::IoStream> =
                                            Box::new(IrohStream { recv, send });
                                        if let Err(e) = plugin.on_stream(&ctx, io).await {
                                            eprintln!("plugin {} stream error: {e}", plugin.id());
                                        }
                                    }
                                },
                                None => eprintln!("no plugin for wire {wire:?}"),
                            }
                        }
                        Err(e) => eprintln!("frame error: {e}"),
                    }
                });
            }
        });
    }
}

pub async fn connect(ticket: &str, local_addr: std::net::SocketAddr) -> anyhow::Result<()> {
    let ep = create_endpoint().await?;
    let peer = EndpointId::from_str(ticket.trim())?;
    let conn = ep.connect(peer, ALPN).await?;
    let listener = TcpListener::bind(local_addr).await?;
    println!("Connected! Listening on {local_addr}");

    loop {
        let (mut local, _) = listener.accept().await?;
        let conn = conn.clone();
        tokio::spawn(async move {
            let (mut send, recv) = match conn.open_bi().await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("open_bi failed: {e:?}");
                    return;
                }
            };
            // First frame header addresses the Tcp plugin; payload streams.
            let head = Frame::stream_open(WireId::Tcp.as_u8());
            if head.write_to(&mut send).await.is_err() {
                return;
            }

            let mut io = IrohStream { recv, send };
            let _ = tokio::io::copy_bidirectional(&mut local, &mut io).await;
        });
    }
}

struct IrohStream {
    recv: iroh::endpoint::RecvStream,
    send: iroh::endpoint::SendStream,
}

impl tokio::io::AsyncRead for IrohStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.recv).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for IrohStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.send)
            .poll_write(cx, buf)
            .map_err(|e| std::io::Error::other(e.to_string()))
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.send).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.send).poll_shutdown(cx)
    }
}
