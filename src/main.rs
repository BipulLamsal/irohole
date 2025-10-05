#![allow(dead_code)]
use clap::Parser;
use irohole::cli::Cli;
use irohole::cli::CliSub;
use irohole::connect::Connect;
use irohole::daemon::Daemon;
use irohole::service::Service;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // init_tracing()?;

    match cli.subcommand {
        CliSub::Daemon() => {
            let daemon = Daemon::new().await?;
            daemon.run().await?;
        }
        CliSub::Serve(args) => {
            let service = Service::new(args.name, None, args.port);
            service.start().await?;
        }
        CliSub::Connect(args) => {
            let connect = Connect::new(args.name, args.node, None, args.port);
            connect.start().await?;
        }
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
