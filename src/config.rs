use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::ops::{Deref, RangeInclusive};
use std::path::{Path, PathBuf};
use tokio::fs;
use url::Url;

use crate::transport::{DEFAULT_KEEPALIVE_INTERVAL, DEFAULT_KEEPALIVE_SECS, DEFAULT_NODELAY};

/// Application-layer heartbeat interval in secs
const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 30;
const DEFAULT_HEARTBEAT_TIMEOUT_SECS: u64 = 40;

/// Client
const DEFAULT_CLIENT_RETRY_INTERVAL_SECS: u64 = 1;

/// String with Debug implementation that emits "MASKED"
/// Used to mask sensitive strings when logging
#[derive(Serialize, Deserialize, Default, PartialEq, Eq, Clone)]
pub struct MaskedString(String);

impl Debug for MaskedString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.write_str("MASKED")
    }
}

impl Deref for MaskedString {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&str> for MaskedString {
    fn from(s: &str) -> MaskedString {
        MaskedString(String::from(s))
    }
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq, Default)]
pub enum TransportType {
    #[default]
    #[serde(rename = "tcp")]
    Tcp,
    #[serde(rename = "tls")]
    Tls,
    #[serde(rename = "noise")]
    Noise,
    #[serde(rename = "websocket")]
    Websocket,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum ServiceType {
    #[serde(rename = "tcp")]
    #[default]
    Tcp,
    #[serde(rename = "udp")]
    Udp,
}

impl std::fmt::Display for ServiceType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ServiceType::Tcp => "tcp",
            ServiceType::Udp => "udp",
        })
    }
}

/// Parse "3000" or "8000-8010" into an inclusive port range
pub fn parse_port_range(s: &str) -> Result<RangeInclusive<u16>> {
    let s = s.trim();
    let (lo, hi) = match s.split_once('-') {
        Some((a, b)) => (a.trim(), b.trim()),
        None => (s, s),
    };
    let lo: u16 = lo
        .parse()
        .with_context(|| format!("Invalid port `{}`", lo))?;
    let hi: u16 = hi
        .parse()
        .with_context(|| format!("Invalid port `{}`", hi))?;
    if lo == 0 || lo > hi {
        bail!("Invalid port range `{}`", s);
    }
    Ok(lo..=hi)
}

/// Expand a list of port specs into a sorted, deduplicated port list
fn expand_ports(specs: &[String]) -> Result<Vec<u16>> {
    let mut v: Vec<u16> = Vec::new();
    for s in specs {
        v.extend(parse_port_range(s)?);
    }
    v.sort_unstable();
    v.dedup();
    Ok(v)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    pub hostname: Option<String>,
    pub trusted_root: Option<String>,
    pub pkcs12: Option<String>,
    pub pkcs12_password: Option<MaskedString>,
}

fn default_noise_pattern() -> String {
    String::from("Noise_NK_25519_ChaChaPoly_BLAKE2s")
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NoiseConfig {
    #[serde(default = "default_noise_pattern")]
    pub pattern: String,
    pub local_private_key: Option<MaskedString>,
    pub remote_public_key: Option<String>,
    // TODO: Maybe psk can be added
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WebsocketConfig {
    pub tls: bool,
}

fn default_nodelay() -> bool {
    DEFAULT_NODELAY
}

fn default_keepalive_secs() -> u64 {
    DEFAULT_KEEPALIVE_SECS
}

fn default_keepalive_interval() -> u64 {
    DEFAULT_KEEPALIVE_INTERVAL
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TcpConfig {
    #[serde(default = "default_nodelay")]
    pub nodelay: bool,
    #[serde(default = "default_keepalive_secs")]
    pub keepalive_secs: u64,
    #[serde(default = "default_keepalive_interval")]
    pub keepalive_interval: u64,
    pub proxy: Option<Url>,
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            nodelay: default_nodelay(),
            keepalive_secs: default_keepalive_secs(),
            keepalive_interval: default_keepalive_interval(),
            proxy: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct TransportConfig {
    #[serde(rename = "type")]
    pub transport_type: TransportType,
    #[serde(default)]
    pub tcp: TcpConfig,
    pub tls: Option<TlsConfig>,
    pub noise: Option<NoiseConfig>,
    pub websocket: Option<WebsocketConfig>,
}

fn default_heartbeat_timeout() -> u64 {
    DEFAULT_HEARTBEAT_TIMEOUT_SECS
}

fn default_client_retry_interval() -> u64 {
    DEFAULT_CLIENT_RETRY_INTERVAL_SECS
}

fn default_alias_bind() -> String {
    String::from("127.0.0.1")
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
    pub remote_addr: String,
    pub user: String,
    pub key: MaskedString,
    /// Host on which alias listeners (`<alias_bind>:<remote_port>`) are opened.
    /// `""` disables aliases (e.g. when the server only exposes ports to nginx)
    #[serde(default = "default_alias_bind")]
    pub alias_bind: String,
    #[serde(default)]
    pub prefer_ipv6: bool,
    pub nodelay: Option<bool>,
    #[serde(default)]
    pub transport: TransportConfig,
    #[serde(default = "default_heartbeat_timeout")]
    pub heartbeat_timeout: u64,
    #[serde(default = "default_client_retry_interval")]
    pub retry_interval: u64,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            remote_addr: Default::default(),
            user: Default::default(),
            key: Default::default(),
            alias_bind: default_alias_bind(),
            prefer_ipv6: false,
            nodelay: None,
            transport: Default::default(),
            heartbeat_timeout: default_heartbeat_timeout(),
            retry_interval: default_client_retry_interval(),
        }
    }
}

fn default_heartbeat_interval() -> u64 {
    DEFAULT_HEARTBEAT_INTERVAL_SECS
}

fn default_alloc_file() -> PathBuf {
    PathBuf::from("allocations.toml")
}

/// Generate an nginx `map` file from the directory:
/// `<local_port>-<user>.<domain> <remote_port>;` per TCP mapping (one label, so a single-level wildcard certificate covers it)
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub struct NginxConfig {
    /// Relative to the config file
    pub map_file: PathBuf,
    pub domain: String,
    /// Shell command run after the file changed, e.g. "nginx -s reload"
    pub reload_cmd: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub bind_addr: String,
    /// Host the allocated remote ports listen on. Default: host of `bind_addr`
    pub expose_bind: Option<String>,
    pub nginx: Option<NginxConfig>,
    /// `[server.users.<name>]`
    #[serde(default)]
    pub users: HashMap<String, UserConfig>,
    /// Path of the persisted port allocation table, relative to the config file
    #[serde(default = "default_alloc_file")]
    pub alloc_file: PathBuf,
    pub nodelay: Option<bool>,
    #[serde(default)]
    pub transport: TransportConfig,
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: Default::default(),
            expose_bind: None,
            nginx: None,
            users: Default::default(),
            alloc_file: default_alloc_file(),
            nodelay: None,
            transport: Default::default(),
            heartbeat_interval: default_heartbeat_interval(),
        }
    }
}

/// `[server.users.<name>]`: a user, its exclusive remote port block and the
/// local ports of its client that get exposed
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct UserConfig {
    #[serde(skip)]
    pub name: String,
    pub key: MaskedString,
    /// e.g. "20000-20999"
    pub port_block: String,
    /// Local TCP ports of the client, e.g. `["3000", "8000-8010"]`
    #[serde(default)]
    pub tcp: Vec<String>,
    #[serde(default)]
    pub udp: Vec<String>,
    /// Parsed `port_block`, filled by `validate`
    #[serde(skip)]
    pub block: (u16, u16),
    /// Expanded `tcp` / `udp`, filled by `validate`
    #[serde(skip)]
    pub tcp_ports: Vec<u16>,
    #[serde(skip)]
    pub udp_ports: Vec<u16>,
}

fn validate_users(users: &mut HashMap<String, UserConfig>) -> Result<()> {
    for (name, u) in users.iter_mut() {
        u.name = name.clone();
        let r = parse_port_range(&u.port_block)
            .with_context(|| format!("Invalid port_block of user {}", name))?;
        u.block = (*r.start(), *r.end());
        u.tcp_ports =
            expand_ports(&u.tcp).with_context(|| format!("Invalid tcp of user {}", name))?;
        u.udp_ports =
            expand_ports(&u.udp).with_context(|| format!("Invalid udp of user {}", name))?;
        if u.tcp_ports.len() + u.udp_ports.len() > r.count() {
            bail!("User {} exposes more ports than its port_block holds", name);
        }
    }
    let mut blocks: Vec<&UserConfig> = users.values().collect();
    blocks.sort_by_key(|u| u.block);
    for w in blocks.windows(2) {
        if w[0].block.1 >= w[1].block.0 {
            bail!("port_block of user {} and {} overlap", w[0].name, w[1].name);
        }
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub server: Option<ServerConfig>,
    pub client: Option<ClientConfig>,
}

impl Config {
    /// Parse and validate a config. Paths are not resolved (see `from_file`)
    pub fn parse(s: &str) -> Result<Config> {
        let mut config: Config = toml::from_str(s).with_context(|| "Failed to parse the config")?;

        if let Some(server) = config.server.as_mut() {
            Config::validate_server_config(server)?;
        }

        if let Some(client) = config.client.as_mut() {
            Config::validate_client_config(client)?;
        }

        if config.server.is_none() && config.client.is_none() {
            Err(anyhow!("Neither of `[server]` or `[client]` is defined"))
        } else {
            Ok(config)
        }
    }

    fn validate_server_config(server: &mut ServerConfig) -> Result<()> {
        validate_users(&mut server.users)?;
        Config::validate_transport_config(&server.transport, true)?;
        Ok(())
    }

    fn validate_client_config(client: &mut ClientConfig) -> Result<()> {
        if client.user.is_empty() {
            bail!("`client.user` is not set");
        }
        Config::validate_transport_config(&client.transport, false)?;
        Ok(())
    }

    fn validate_transport_config(config: &TransportConfig, is_server: bool) -> Result<()> {
        config
            .tcp
            .proxy
            .as_ref()
            .map_or(Ok(()), |u| match u.scheme() {
                "socks5" => Ok(()),
                "http" => Ok(()),
                _ => Err(anyhow!(format!("Unknown proxy scheme: {}", u.scheme()))),
            })?;
        match config.transport_type {
            TransportType::Tcp => Ok(()),
            TransportType::Tls => {
                let tls_config = config
                    .tls
                    .as_ref()
                    .ok_or_else(|| anyhow!("Missing TLS configuration"))?;
                if is_server {
                    tls_config
                        .pkcs12
                        .as_ref()
                        .and(tls_config.pkcs12_password.as_ref())
                        .ok_or_else(|| anyhow!("Missing `pkcs12` or `pkcs12_password`"))?;
                }
                Ok(())
            }
            TransportType::Noise => {
                // The check is done in transport
                Ok(())
            }
            TransportType::Websocket => Ok(()),
        }
    }

    pub async fn from_file(path: &Path) -> Result<Config> {
        let s: String = fs::read_to_string(path)
            .await
            .with_context(|| format!("Failed to read the config {:?}", path))?;
        let mut config = Config::parse(&s).with_context(|| {
            "Configuration is invalid. Please refer to the configuration specification."
        })?;
        // Server-side file paths are relative to the config file
        if let (Some(server), Some(dir)) = (config.server.as_mut(), path.parent()) {
            let nginx_map = server.nginx.as_mut().map(|n| &mut n.map_file);
            for p in std::iter::once(&mut server.alloc_file).chain(nginx_map) {
                if p.is_relative() {
                    *p = dir.join(&*p);
                }
            }
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::PathBuf};

    use anyhow::Result;

    fn list_config_files<T: AsRef<Path>>(root: T) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                files.push(path);
            } else if path.is_dir() {
                files.append(&mut list_config_files(path)?);
            }
        }
        Ok(files)
    }

    fn get_all_example_config() -> Result<Vec<PathBuf>> {
        Ok(list_config_files("./examples")?
            .into_iter()
            .filter(|x| x.extension().map_or(false, |e| e == "toml"))
            .collect())
    }

    #[test]
    fn test_example_config() -> Result<()> {
        for p in get_all_example_config()? {
            let s = fs::read_to_string(&p)?;
            Config::parse(&s).with_context(|| format!("{:?}", p))?;
        }
        Ok(())
    }

    #[test]
    fn test_valid_config() -> Result<()> {
        let paths = list_config_files("tests/config_test/valid_config")?;
        for p in paths {
            let s = fs::read_to_string(p)?;
            Config::parse(&s)?;
        }
        Ok(())
    }

    #[test]
    fn test_invalid_config() -> Result<()> {
        let paths = list_config_files("tests/config_test/invalid_config")?;
        for p in paths {
            let s = fs::read_to_string(p)?;
            assert!(Config::parse(&s).is_err());
        }
        Ok(())
    }

    #[test]
    fn test_parse_port_range() {
        assert_eq!(parse_port_range("3000").unwrap(), 3000..=3000);
        assert_eq!(parse_port_range(" 8000-8010 ").unwrap(), 8000..=8010);
        assert!(parse_port_range("0").is_err());
        assert!(parse_port_range("9-1").is_err());
        assert!(parse_port_range("abc").is_err());
        assert!(parse_port_range("70000").is_err());
        assert_eq!(
            expand_ports(&["8001-8002".into(), "8000".into(), "8001".into()]).unwrap(),
            vec![8000, 8001, 8002]
        );
    }

    #[test]
    fn test_users_config() {
        let ok = Config::parse(
            r#"
            [server]
            bind_addr = "0.0.0.0:2333"
            [server.users.alice]
            key = "a"
            port_block = "20000-20999"
            tcp = ["3000", "8000-8001"]
            udp = ["5000"]
            [server.users.bob]
            key = "b"
            port_block = "21000"
            "#,
        )
        .unwrap()
        .server
        .unwrap()
        .users;
        assert_eq!(ok["alice"].name, "alice");
        assert_eq!(ok["alice"].block, (20000, 20999));
        assert_eq!(ok["alice"].tcp_ports, vec![3000, 8000, 8001]);
        assert_eq!(ok["alice"].udp_ports, vec![5000]);
        assert_eq!(ok["bob"].block, (21000, 21000));
        assert!(ok["bob"].tcp_ports.is_empty());

        let bad = |users: &str| {
            Config::parse(&format!(
                "[server]\nbind_addr = \"0.0.0.0:2333\"\n{}",
                users
            ))
            .is_err()
        };
        // overlapping blocks
        assert!(bad(r#"
            [server.users.alice]
            key = "a"
            port_block = "20000-20999"
            [server.users.bob]
            key = "b"
            port_block = "20999-21010"
            "#));
        // more ports than the block holds
        assert!(bad(r#"
            [server.users.alice]
            key = "a"
            port_block = "20000-20001"
            tcp = ["3000-3002"]
            "#));
        // bad port spec
        assert!(bad(r#"
            [server.users.alice]
            key = "a"
            port_block = "20000"
            tcp = ["9-1"]
            "#));
    }
}
