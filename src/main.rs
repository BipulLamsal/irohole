#![allow(dead_code)]
use clap::Parser;
use irohole::cli::Cli;
use irohole::cli::CliSub;
use irohole::service::Service;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing()?;

    match cli.subcommand {
        CliSub::Daemon() => todo!(),
        CliSub::Serve(args) => {
            let service = Service::new(args.name, None, args.port);
            service.start().await?;
            match service.start().await {
                Ok(proxy_port) => {
                    println!("🎉 Proxy ready!");
                    println!("   Target: http://localhost:{}", args.port);
                    println!("   Proxy: http://localhost:{}", proxy_port);
                    println!("   Press Ctrl+C to stop");
                    tokio::signal::ctrl_c().await?;
                    println!("👋 Shutting down proxy");
                }
                Err(e) => {
                    eprintln!("❌ Failed to start proxy: {}", e);
                }
            }
        }
        CliSub::Connect(args) => todo!(),
    }

    Ok(())
}

fn init_tracing() -> anyhow::Result<()> {
    use tracing_subscriber::prelude::*;
    let fmt_layer = tracing_subscriber::fmt::Layer::default()
        .with_ansi(true)
        .with_target(false);
    let subscriber = tracing_subscriber::registry().with(fmt_layer);
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}
