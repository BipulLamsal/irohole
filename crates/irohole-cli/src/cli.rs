use clap::{Parser, Subcommand};

macro_rules! define_plugins {
    ($( $plugin:ident($cmd:ty) ),* $(,)?) => {
        #[derive(Subcommand, Debug)]
        pub enum Plugin {
            $($plugin($cmd),)*
        }

        impl Plugin {
            pub async fn run(self) -> anyhow::Result<()> {
                match self {
                    $(Self::$plugin(c) => c.run().await,)*
                }
            }
        }
    };
}

define_plugins! {
    Tcp(irohole_plugin_tcp::TcpCommand),
}

#[derive(Parser, Debug)]
#[command(about = "irohole cli")]
pub struct Cli {
    #[command(subcommand)]
    pub plugin: Plugin,
}
