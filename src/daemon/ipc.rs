use anyhow::bail;
use std::fmt;
use std::net::SocketAddrV4;
use std::str::FromStr;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::UnixStream;

#[derive(Debug)]
pub enum IpcMessageType {
    Serve,
    Connect,
}

#[derive(Debug)]
pub enum IpcMessage {
    Serve {
        name: String,
        addr: SocketAddr,
    },
    Data {
        name: String,
        message: String,
    },
    Connect {
        name: String,
        node: String,
        addr: SocketAddr,
    },
    Error {
        name: String,
        message: String,
    },
}

#[derive(Debug)]
pub enum IpcError {
    InvalidFormat,
    InvalidAddr(String),
    UnknownCommand(String),
}

impl std::error::Error for IpcError {}

#[derive(Debug, Clone, Copy)]
pub struct SocketAddr(pub [u8; 4], pub u16);

impl fmt::Display for SocketAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let octets = &self.0;
        write!(
            f,
            "{}.{}.{}.{}:{}",
            octets[0], octets[1], octets[2], octets[3], self.1
        )
    }
}

#[derive(Debug)]
pub enum SocketAddrParseError {
    InvalidFormat,
    InvalidIp(std::num::ParseIntError),
    InvalidPort(std::num::ParseIntError),
}

impl fmt::Display for SocketAddrParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SocketAddrParseError::InvalidFormat => write!(f, "Invalid socket address format"),
            SocketAddrParseError::InvalidIp(e) => write!(f, "Invalid IP octet: {}", e),
            SocketAddrParseError::InvalidPort(e) => write!(f, "Invalid port: {}", e),
        }
    }
}

impl FromStr for SocketAddr {
    type Err = SocketAddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(':');
        let ip_part = parts.next().ok_or(SocketAddrParseError::InvalidFormat)?;
        let port_part = parts.next().ok_or(SocketAddrParseError::InvalidFormat)?;

        let port: u16 = port_part
            .parse()
            .map_err(SocketAddrParseError::InvalidPort)?;

        let octets: Vec<u8> = ip_part
            .split('.')
            .map(|x| x.parse().map_err(SocketAddrParseError::InvalidIp))
            .collect::<Result<Vec<_>, _>>()?;

        if octets.len() != 4 {
            return Err(SocketAddrParseError::InvalidFormat);
        }

        Ok(SocketAddr(
            [octets[0], octets[1], octets[2], octets[3]],
            port,
        ))
    }
}
impl From<SocketAddrV4> for SocketAddr {
    fn from(addr: SocketAddrV4) -> Self {
        SocketAddr(addr.ip().octets(), addr.port())
    }
}

impl From<SocketAddr> for SocketAddrV4 {
    fn from(wrapper: SocketAddr) -> Self {
        SocketAddrV4::new(wrapper.0.into(), wrapper.1)
    }
}

impl FromStr for IpcMessage {
    type Err = IpcError;

    fn from_str(line: &str) -> Result<Self, Self::Err> {
        let line = line.trim_end();
        let mut parts = line.splitn(4, ':');

        let command = parts.next().ok_or(IpcError::InvalidFormat)?;
        let name = parts.next().ok_or(IpcError::InvalidFormat)?;
        let node = parts.next().ok_or(IpcError::InvalidFormat)?;
        let rest = parts.next().ok_or(IpcError::InvalidFormat)?;
        match command {
            "serve" => {
                let addr = parse_addr(rest)?;
                Ok(IpcMessage::Serve {
                    name: name.to_string(),
                    addr,
                })
            }
            "data" => Ok(IpcMessage::Data {
                name: name.to_string(),
                message: rest.to_string(),
            }),
            "connect" => {
                let addr = parse_addr(rest)?;
                Ok(IpcMessage::Connect {
                    node: node.to_string(),
                    name: name.to_string(),
                    addr,
                })
            }
            "error" => Ok(IpcMessage::Error {
                name: name.to_string(),
                message: rest.to_string(),
            }),
            other => Err(IpcError::UnknownCommand(other.to_string())),
        }
    }
}

impl fmt::Display for IpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IpcError::UnknownCommand(value) => {
                writeln!(f, "Coudn't parse IPC command:{}", value)
            }
            IpcError::InvalidFormat => {
                writeln!(f, "Invalid IPC format")
            }
            IpcError::InvalidAddr(value) => {
                writeln!(f, "Invalid IPC socket address:{}", value)
            }
        }
    }
}

impl fmt::Display for IpcMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IpcMessage::Serve { name, addr } => {
                writeln!(f, "serve:{}::{}", name, addr)
            }
            IpcMessage::Data { name, message } => {
                writeln!(f, "data:{}::{}", name, message)
            }
            IpcMessage::Connect { name, addr, node } => {
                writeln!(f, "connect:{}:{}:{}", name, node, addr)
            }
            IpcMessage::Error { name, message } => {
                writeln!(f, "error:{}::{}", name, message)
            }
        }
    }
}

fn parse_addr(s: &str) -> Result<SocketAddr, IpcError> {
    let mut parts = s.split(':');
    let ip_str = parts.next().ok_or(IpcError::InvalidFormat)?;
    let port_str = parts.next().ok_or(IpcError::InvalidFormat)?;

    let port: u16 = port_str
        .parse()
        .map_err(|_| IpcError::InvalidAddr(s.to_string()))?;
    let octets: Vec<u8> = ip_str
        .split('.')
        .map(|p| p.parse::<u8>())
        .collect::<Result<_, _>>()
        .map_err(|_| IpcError::InvalidAddr(s.to_string()))?;

    if octets.len() != 4 {
        return Err(IpcError::InvalidAddr(s.to_string()));
    }

    Ok(SocketAddr(
        [octets[0], octets[1], octets[2], octets[3]],
        port,
    ))
}

/// Sends the IPC serve request and receives responses  
pub async fn handle_ipc_connection(
    name: String,
    target_socket: SocketAddrV4,
    message_type: IpcMessageType,
    node: Option<String>,
) -> anyhow::Result<()> {
    let socket = std::env::temp_dir().join("irohole.sock");
    let stream = UnixStream::connect(&socket).await?;
    let (stream_reader, mut stream_writer) = stream.into_split();

    let addr: SocketAddr = target_socket.into();
    let request = match message_type {
        IpcMessageType::Serve => IpcMessage::Serve { name, addr },
        IpcMessageType::Connect => {
            if let Some(n) = node {
                IpcMessage::Connect {
                    name: name.to_string(),
                    node: n.to_string(),
                    addr,
                }
            } else {
                bail!("Node Id is required")
            }
        }
    };

    stream_writer
        .write_all(request.to_string().as_bytes())
        .await?;

    let mut lines = BufReader::new(stream_reader).lines();
    while let Some(line) = lines.next_line().await? {
        match line.parse::<IpcMessage>() {
            Ok(msg) => println!("{}", msg),
            Err(err) => eprintln!("Error: {:?}", err),
        }
    }

    Ok(())
}
