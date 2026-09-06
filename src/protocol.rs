pub const HASH_WIDTH_IN_BYTES: usize = 32;

use crate::config::ServiceType;
use anyhow::{bail, Context, Result};
use bytes::{Bytes, BytesMut};
use lazy_static::lazy_static;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::trace;

type ProtocolVersion = u8;
const _PROTO_V0: u8 = 0u8;
const _PROTO_V1: u8 = 1u8;
const _PROTO_V2: u8 = 2u8; // user-level auth, client-side port registration, directory
const _PROTO_V3: u8 = 3u8; // exposed ports are configured on the server
const PROTO_V4: u8 = 4u8; // RegisterAck carries the nginx domain, client can disable ports

pub const CURRENT_PROTO_VERSION: ProtocolVersion = PROTO_V4;

/// Upper bound of a variable-length frame
const MAX_FRAME: u32 = 1 << 20;

pub type Digest = [u8; HASH_WIDTH_IN_BYTES];

#[derive(Deserialize, Serialize, Debug)]
pub enum Hello {
    ControlChannelHello(ProtocolVersion, Digest), // sha256sum(user name) or a nonce
    DataChannelHello(ProtocolVersion, Digest),    // nonce provided by CreateDataChannel
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Auth(pub Digest);

#[derive(Deserialize, Serialize, Debug)]
pub enum Ack {
    Ok,
    UserNotExist,
    AuthFailed,
}

impl std::fmt::Display for Ack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Ack::Ok => "Ok",
                Ack::UserNotExist => "User not exist",
                Ack::AuthFailed => "Incorrect key",
            }
        )
    }
}

/// Sent by the server after `Ack::Ok`, once the user's configured ports got
/// remote ports allocated. `Ok` carries the nginx domain (if configured) and
/// is followed by a `ControlChannelCmd::Directory`
#[derive(Deserialize, Serialize, Debug)]
pub enum RegisterAck {
    Ok(Option<String>),
    Err(String),
}

/// The only client -> server frame after registration
#[derive(Deserialize, Serialize, Debug)]
pub enum ClientCmd {
    /// Local ports the client stopped exposing; the server marks them offline
    Disabled(Vec<u16>),
}

/// One entry of the global mapping directory
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct Mapping {
    pub user: String,
    pub proto: ServiceType,
    pub local_port: u16,
    pub remote_port: u16,
    pub online: bool,
}

impl Mapping {
    /// nginx host name of a TCP mapping: `<local_port>-<user>.<domain>`
    pub fn host(&self, domain: &str) -> String {
        format!("{}-{}.{}", self.local_port, self.user, domain)
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub enum ControlChannelCmd {
    CreateDataChannel,
    HeartBeat,
    Directory(Vec<Mapping>),
}

/// The u16 is the local port the client should forward to
#[derive(Deserialize, Serialize, Debug)]
pub enum DataChannelCmd {
    StartForwardTcp(u16),
    StartForwardUdp(u16),
}

type UdpPacketLen = u16; // `u16` should be enough for any practical UDP traffic on the Internet
#[derive(Deserialize, Serialize, Debug)]
struct UdpHeader {
    from: SocketAddr,
    len: UdpPacketLen,
}

#[derive(Debug)]
pub struct UdpTraffic {
    pub from: SocketAddr,
    pub data: Bytes,
}

impl UdpTraffic {
    pub async fn write<T: AsyncWrite + Unpin>(&self, writer: &mut T) -> Result<()> {
        let hdr = UdpHeader {
            from: self.from,
            len: self.data.len() as UdpPacketLen,
        };

        let v = bincode::serialize(&hdr).unwrap();

        trace!("Write {:?} of length {}", hdr, v.len());
        writer.write_u8(v.len() as u8).await?;
        writer.write_all(&v).await?;

        writer.write_all(&self.data).await?;

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn write_slice<T: AsyncWrite + Unpin>(
        writer: &mut T,
        from: SocketAddr,
        data: &[u8],
    ) -> Result<()> {
        let hdr = UdpHeader {
            from,
            len: data.len() as UdpPacketLen,
        };

        let v = bincode::serialize(&hdr).unwrap();

        trace!("Write {:?} of length {}", hdr, v.len());
        writer.write_u8(v.len() as u8).await?;
        writer.write_all(&v).await?;

        writer.write_all(data).await?;

        Ok(())
    }

    pub async fn read<T: AsyncRead + Unpin>(reader: &mut T, hdr_len: u8) -> Result<UdpTraffic> {
        let mut buf = vec![0; hdr_len as usize];
        reader
            .read_exact(&mut buf)
            .await
            .with_context(|| "Failed to read udp header")?;

        let hdr: UdpHeader =
            bincode::deserialize(&buf).with_context(|| "Failed to deserialize UdpHeader")?;

        trace!("hdr {:?}", hdr);

        let mut data = BytesMut::new();
        data.resize(hdr.len as usize, 0);
        reader.read_exact(&mut data).await?;

        Ok(UdpTraffic {
            from: hdr.from,
            data: data.freeze(),
        })
    }
}

pub fn digest(data: &[u8]) -> Digest {
    use sha2::{Digest, Sha256};
    let d = Sha256::new().chain_update(data).finalize();
    d.into()
}

struct PacketLength {
    hello: usize,
    ack: usize,
    auth: usize,
}

impl PacketLength {
    pub fn new() -> PacketLength {
        let username = "default";
        let d = digest(username.as_bytes());
        let hello = bincode::serialized_size(&Hello::ControlChannelHello(CURRENT_PROTO_VERSION, d))
            .unwrap() as usize;
        let ack = Ack::Ok;
        let ack = bincode::serialized_size(&ack).unwrap() as usize;

        let auth = bincode::serialized_size(&Auth(d)).unwrap() as usize;
        PacketLength { hello, ack, auth }
    }
}

lazy_static! {
    static ref PACKET_LEN: PacketLength = PacketLength::new();
}

pub async fn read_hello<T: AsyncRead + AsyncWrite + Unpin>(conn: &mut T) -> Result<Hello> {
    let mut buf = vec![0u8; PACKET_LEN.hello];
    conn.read_exact(&mut buf)
        .await
        .with_context(|| "Failed to read hello")?;
    let hello = bincode::deserialize(&buf).with_context(|| "Failed to deserialize hello")?;

    match hello {
        Hello::ControlChannelHello(v, _) | Hello::DataChannelHello(v, _) => {
            if v != CURRENT_PROTO_VERSION {
                bail!(
                    "Protocol version mismatched. Expected {}, got {}. Please update `rathole`.",
                    CURRENT_PROTO_VERSION,
                    v
                );
            }
        }
    }

    Ok(hello)
}

pub async fn read_auth<T: AsyncRead + AsyncWrite + Unpin>(conn: &mut T) -> Result<Auth> {
    let mut buf = vec![0u8; PACKET_LEN.auth];
    conn.read_exact(&mut buf)
        .await
        .with_context(|| "Failed to read auth")?;
    bincode::deserialize(&buf).with_context(|| "Failed to deserialize auth")
}

pub async fn read_ack<T: AsyncRead + AsyncWrite + Unpin>(conn: &mut T) -> Result<Ack> {
    let mut bytes = vec![0u8; PACKET_LEN.ack];
    conn.read_exact(&mut bytes)
        .await
        .with_context(|| "Failed to read ack")?;
    bincode::deserialize(&bytes).with_context(|| "Failed to deserialize ack")
}

/// Write a length-prefixed (u32 BE) bincode frame and flush
pub async fn write_frame<T: Serialize, W: AsyncWrite + Unpin>(w: &mut W, v: &T) -> Result<()> {
    let body = bincode::serialize(v).with_context(|| "Failed to serialize frame")?;
    if body.len() as u64 > MAX_FRAME as u64 {
        bail!("Frame too large: {} bytes", body.len());
    }
    w.write_u32(body.len() as u32).await?;
    w.write_all(&body).await?;
    w.flush().await?;
    Ok(())
}

/// Read a length-prefixed (u32 BE) bincode frame
pub async fn read_frame<T: DeserializeOwned, R: AsyncRead + Unpin>(r: &mut R) -> Result<T> {
    let len = r
        .read_u32()
        .await
        .with_context(|| "Failed to read frame length")?;
    if len > MAX_FRAME {
        bail!("Frame too large: {} bytes", len);
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf)
        .await
        .with_context(|| "Failed to read frame")?;
    bincode::deserialize(&buf).with_context(|| "Failed to deserialize frame")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_frame_roundtrip() {
        let (mut a, mut b) = tokio::io::duplex(4096);
        let cmd = ControlChannelCmd::Directory(vec![Mapping {
            user: "alice".into(),
            proto: ServiceType::Tcp,
            local_port: 3000,
            remote_port: 20000,
            online: true,
        }]);
        write_frame(&mut a, &cmd).await.unwrap();
        let got: ControlChannelCmd = read_frame(&mut b).await.unwrap();
        match got {
            ControlChannelCmd::Directory(d) => assert_eq!(d[0].remote_port, 20000),
            _ => panic!("wrong frame"),
        }
    }
}
