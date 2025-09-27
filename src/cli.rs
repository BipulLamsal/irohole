use clap::error::{Error, ErrorKind};
use clap::{ArgMatches, Args as _, Command, FromArgMatches, Parser, Subcommand};
/// Serve the your resources to peer's
#[derive(Parser, Debug)]
pub struct ServeArgs {
    /// Name of the service to open port for 
    #[arg(short,long)]
    name: String,
    /// Port on which service is served 
    #[arg(long)]
    port:u16,
    #[arg(long, default_value = "http")]
    protocol: String,
}
/// Connect to your peer's resouce
#[derive(Parser, Debug)]
pub struct ConnectArgs {
    #[arg(long)]
    target: String,
    #[arg(long)]
    local_port: u16,
}

#[derive(Debug)]
pub enum CliSub {
    /// Daemon service for irohole 
    Daemon(),
    /// Serve the your resources to peer's 
    Serve(ServeArgs),
    /// Connect to your peer's resouce 
    Connect(ConnectArgs)
}


impl FromArgMatches for CliSub {
    fn from_arg_matches(matches: &ArgMatches) -> Result<Self, Error> {
        match matches.subcommand() {
            Some(("daemon", _args)) => Ok(Self::Daemon()),
            Some(("serve", args)) => Ok(Self::Serve(ServeArgs::from_arg_matches(args)?)),
            Some(("connect", args)) => Ok(Self::Connect(ConnectArgs::from_arg_matches(args)?)),
            Some((_, _)) => Err(Error::raw(
                ErrorKind::InvalidSubcommand,
                "Valid subcommands are `daemon`,`serve` and `connect`",
            )),
            None => Err(Error::raw(
                ErrorKind::MissingSubcommand,
                "Valid subcommands are `daemon`,`serve` and `connect`",
            )),
        }
    }
    fn update_from_arg_matches(&mut self, matches: &ArgMatches) -> Result<(), Error> {
        match matches.subcommand() {
            Some(("daemon", _args)) => *self = Self::Daemon(),
            Some(("serve", args)) => *self = Self::Serve(ServeArgs::from_arg_matches(args)?),
            Some(("connect", args)) => *self = Self::Connect(ConnectArgs::from_arg_matches(args)?),
            Some((_, _)) => {
                return Err(Error::raw(
                    ErrorKind::InvalidSubcommand,
                    "Valid subcommands are `daemon`,`serve` and `connect`",
                ))
            }
            None => (),
        };
        Ok(())
    }
}

impl Subcommand for CliSub {
    fn augment_subcommands(cmd: Command) -> Command {
        cmd.subcommand(Command::new("daemon"))
            .subcommand(ServeArgs::augment_args(Command::new("serve")))
           .subcommand(ConnectArgs::augment_args(Command::new("connect")))
            .subcommand_required(true)
    }
    fn augment_subcommands_for_update(cmd: Command) -> Command {
        cmd.subcommand(Command::new("daemon"))
            .subcommand(ServeArgs::augment_args(Command::new("serve")))
           .subcommand(ConnectArgs::augment_args(Command::new("connect")))
            .subcommand_required(true)
    }
    fn has_subcommand(name: &str) -> bool {
        matches!(name, "daemon" | "connect" | "serve")
    }
}

#[derive(Parser, Debug)]
pub struct Cli {
    #[command(subcommand)]
    pub subcommand: CliSub,
}

