use crate::config::{ClientConfig, Config, ServiceType, TransportType};
use crate::helper::{host_port_pair, udp_connect};
use crate::protocol::Hello::{self, *};
use crate::protocol::{
    self, read_ack, read_frame, read_hello, write_frame, Ack, Auth, ClientCmd, ControlChannelCmd,
    DataChannelCmd, Mapping, RegisterAck, UdpTraffic, CURRENT_PROTO_VERSION, HASH_WIDTH_IN_BYTES,
};
use crate::transport::{AddrMaybeCached, SocketOpts, TcpTransport, Transport};
use anyhow::{anyhow, bail, Context, Result};
use backoff::backoff::Backoff;
use backoff::future::retry_notify;
use backoff::ExponentialBackoff;
use bytes::{Bytes, BytesMut};
use lazy_static::lazy_static;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{self, copy_bidirectional, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{broadcast, mpsc, oneshot, watch, RwLock};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{self, Duration, Instant};
use tracing::{debug, error, info, instrument, trace, warn, Instrument, Span};

#[cfg(feature = "noise")]
use crate::transport::NoiseTransport;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::transport::TlsTransport;
#[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
use crate::transport::WebsocketTransport;

use crate::constants::{run_control_chan_backoff, UDP_BUFFER_SIZE, UDP_SENDQ_SIZE, UDP_TIMEOUT};

/// How often the client checks which of its own local ports are listening
const PROBE_INTERVAL_SECS: u64 = 5;

/// What the client currently knows, for the CLI table and the desktop UI
#[derive(Clone, Default, Debug, Serialize)]
pub struct ClientState {
    pub connected: bool,
    pub user: String,
    pub alias_bind: String,
    /// Host part of `remote_addr`; remote ports are reachable at `<server_host>:<remote_port>`
    pub server_host: String,
    /// `server.nginx.domain`, if the server has one
    pub domain: Option<String>,
    pub directory: Vec<Mapping>,
    /// Own local TCP ports that accept connections right now
    pub listening: Vec<u16>,
    /// Own local ports turned off with `set_port_enabled`
    pub disabled: Vec<u16>,
}

// ponytail: process-wide state, one client per process. Survives hot-reload
// restarts on purpose (disabled ports stay disabled).
lazy_static! {
    static ref STATE: watch::Sender<ClientState> = watch::channel(ClientState::default()).0;
    static ref DISABLED: watch::Sender<Vec<u16>> = watch::channel(Vec::new()).0;
}

pub fn client_state() -> watch::Receiver<ClientState> {
    STATE.subscribe()
}

/// Turn exposing a local port on or off. Takes effect on the next directory push.
pub fn set_port_enabled(port: u16, enabled: bool) {
    DISABLED.send_modify(|v| {
        v.retain(|p| *p != port);
        if !enabled {
            v.push(port);
            v.sort_unstable();
        }
    });
}

fn update_state(f: impl FnOnce(&mut ClientState)) {
    STATE.send_modify(f);
}

// The entrypoint of running a client
pub async fn run_client(config: Config, shutdown_rx: broadcast::Receiver<bool>) -> Result<()> {
    let config = config.client.ok_or_else(|| {
        anyhow!(
        "Try to run as a client, but the configuration is missing. Please add the `[client]` block"
    )
    })?;

    match config.transport.transport_type {
        TransportType::Tcp => {
            let mut client = Client::<TcpTransport>::from(config).await?;
            client.run(shutdown_rx).await
        }
        TransportType::Tls => {
            #[cfg(any(feature = "native-tls", feature = "rustls"))]
            {
                let mut client = Client::<TlsTransport>::from(config).await?;
                client.run(shutdown_rx).await
            }
            #[cfg(not(any(feature = "native-tls", feature = "rustls")))]
            crate::helper::feature_neither_compile("native-tls", "rustls")
        }
        TransportType::Noise => {
            #[cfg(feature = "noise")]
            {
                let mut client = Client::<NoiseTransport>::from(config).await?;
                client.run(shutdown_rx).await
            }
            #[cfg(not(feature = "noise"))]
            crate::helper::feature_not_compile("noise")
        }
        TransportType::Websocket => {
            #[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
            {
                let mut client = Client::<WebsocketTransport>::from(config).await?;
                client.run(shutdown_rx).await
            }
            #[cfg(not(any(feature = "websocket-native-tls", feature = "websocket-rustls")))]
            crate::helper::feature_neither_compile("websocket-native-tls", "websocket-rustls")
        }
    }
}

type UserDigest = protocol::Digest;
type Nonce = protocol::Digest;

// Holds the state of a client
struct Client<T: Transport> {
    config: ClientConfig,
    transport: Arc<T>,
}

impl<T: 'static + Transport> Client<T> {
    // Create a Client from `[client]` config block
    async fn from(config: ClientConfig) -> Result<Client<T>> {
        let transport =
            Arc::new(T::new(&config.transport).with_context(|| "Failed to create the transport")?);
        Ok(Client { config, transport })
    }

    // The entrypoint of Client
    async fn run(&mut self, mut shutdown_rx: broadcast::Receiver<bool>) -> Result<()> {
        let handle = ControlChannelHandle::new(self.config.clone(), self.transport.clone());

        // Wait for the shutdown signal
        if let Err(err) = shutdown_rx.recv().await {
            error!("Unable to listen for shutdown signal: {}", err);
        }

        handle.shutdown();
        Ok(())
    }
}

struct RunDataChannelArgs<T: Transport> {
    session_key: Nonce,
    remote_addr: AddrMaybeCached,
    connector: Arc<T>,
    socket_opts: SocketOpts,
    prefer_ipv6: bool,
}

async fn do_data_channel_handshake<T: Transport>(
    args: Arc<RunDataChannelArgs<T>>,
) -> Result<T::Stream> {
    // Retry at least every 100ms, at most for 10 seconds
    let backoff = ExponentialBackoff {
        max_interval: Duration::from_millis(100),
        max_elapsed_time: Some(Duration::from_secs(10)),
        ..Default::default()
    };

    // Connect to remote_addr
    let mut conn: T::Stream = retry_notify(
        backoff,
        || async {
            args.connector
                .connect(&args.remote_addr)
                .await
                .with_context(|| format!("Failed to connect to {}", &args.remote_addr))
                .map_err(backoff::Error::transient)
        },
        |e, duration| {
            warn!("{:#}. Retry in {:?}", e, duration);
        },
    )
    .await?;

    T::hint(&conn, args.socket_opts);

    // Send nonce
    let v: &[u8; HASH_WIDTH_IN_BYTES] = args.session_key[..].try_into().unwrap();
    let hello = Hello::DataChannelHello(CURRENT_PROTO_VERSION, v.to_owned());
    conn.write_all(&bincode::serialize(&hello).unwrap()).await?;
    conn.flush().await?;

    Ok(conn)
}

async fn run_data_channel<T: Transport>(args: Arc<RunDataChannelArgs<T>>) -> Result<()> {
    // Do the handshake
    let mut conn = do_data_channel_handshake(args.clone()).await?;

    // Forward. The server tells us which local port this channel is for
    let cmd: DataChannelCmd = read_frame(&mut conn).await?;
    let port = match cmd {
        DataChannelCmd::StartForwardTcp(p) | DataChannelCmd::StartForwardUdp(p) => p,
    };
    if DISABLED.borrow().contains(&port) {
        debug!("Refusing data channel for disabled port {}", port);
        return Ok(());
    }
    match cmd {
        DataChannelCmd::StartForwardTcp(port) => {
            run_data_channel_for_tcp::<T>(conn, &format!("127.0.0.1:{}", port)).await?;
        }
        DataChannelCmd::StartForwardUdp(port) => {
            run_data_channel_for_udp::<T>(conn, &format!("127.0.0.1:{}", port), args.prefer_ipv6)
                .await?;
        }
    }
    Ok(())
}

// Simply copying back and forth for TCP
#[instrument(skip(conn))]
async fn run_data_channel_for_tcp<T: Transport>(
    mut conn: T::Stream,
    local_addr: &str,
) -> Result<()> {
    debug!("New data channel starts forwarding");

    let mut local = TcpStream::connect(local_addr)
        .await
        .with_context(|| format!("Failed to connect to {}", local_addr))?;
    let _ = copy_bidirectional(&mut conn, &mut local).await;
    Ok(())
}

// Things get a little tricker when it gets to UDP because it's connection-less.
// A UdpPortMap must be maintained for recent seen incoming address, giving them
// each a local port, which is associated with a socket. So just the sender
// to the socket will work fine for the map's value.
type UdpPortMap = Arc<RwLock<HashMap<SocketAddr, mpsc::Sender<Bytes>>>>;

#[instrument(skip(conn))]
async fn run_data_channel_for_udp<T: Transport>(
    conn: T::Stream,
    local_addr: &str,
    prefer_ipv6: bool,
) -> Result<()> {
    debug!("New data channel starts forwarding");

    let port_map: UdpPortMap = Arc::new(RwLock::new(HashMap::new()));

    // The channel stores UdpTraffic that needs to be sent to the server
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<UdpTraffic>(UDP_SENDQ_SIZE);

    // FIXME: https://github.com/tokio-rs/tls/issues/40
    // Maybe this is our concern
    let (mut rd, mut wr) = io::split(conn);

    // Keep sending items from the outbound channel to the server
    tokio::spawn(async move {
        while let Some(t) = outbound_rx.recv().await {
            trace!("outbound {:?}", t);
            if let Err(e) = t
                .write(&mut wr)
                .await
                .with_context(|| "Failed to forward UDP traffic to the server")
            {
                debug!("{:?}", e);
                break;
            }
        }
    });

    loop {
        // Read a packet from the server
        let hdr_len = rd.read_u8().await?;
        let packet = UdpTraffic::read(&mut rd, hdr_len)
            .await
            .with_context(|| "Failed to read UDPTraffic from the server")?;
        let m = port_map.read().await;

        if m.get(&packet.from).is_none() {
            // This packet is from a address we don't see for a while,
            // which is not in the UdpPortMap.
            // So set up a mapping (and a forwarder) for it

            // Drop the reader lock
            drop(m);

            // Grab the writer lock
            // This is the only thread that will try to grab the writer lock
            // So no need to worry about some other thread has already set up
            // the mapping between the gap of dropping the reader lock and
            // grabbing the writer lock
            let mut m = port_map.write().await;

            match udp_connect(local_addr, prefer_ipv6).await {
                Ok(s) => {
                    let (inbound_tx, inbound_rx) = mpsc::channel(UDP_SENDQ_SIZE);
                    m.insert(packet.from, inbound_tx);
                    tokio::spawn(run_udp_forwarder(
                        s,
                        inbound_rx,
                        outbound_tx.clone(),
                        packet.from,
                        port_map.clone(),
                    ));
                }
                Err(e) => {
                    error!("{:#}", e);
                }
            }
        }

        // Now there should be a udp forwarder that can receive the packet
        let m = port_map.read().await;
        if let Some(tx) = m.get(&packet.from) {
            let _ = tx.send(packet.data).await;
        }
    }
}

// Run a UdpSocket for the visitor `from`
#[instrument(skip_all, fields(from))]
async fn run_udp_forwarder(
    s: UdpSocket,
    mut inbound_rx: mpsc::Receiver<Bytes>,
    outbount_tx: mpsc::Sender<UdpTraffic>,
    from: SocketAddr,
    port_map: UdpPortMap,
) -> Result<()> {
    debug!("Forwarder created");
    let mut buf = BytesMut::new();
    buf.resize(UDP_BUFFER_SIZE, 0);

    loop {
        tokio::select! {
            // Receive from the server
            data = inbound_rx.recv() => {
                if let Some(data) = data {
                    s.send(&data).await?;
                } else {
                    break;
                }
            },

            // Receive from the service
            val = s.recv(&mut buf) => {
                let len = match val {
                    Ok(v) => v,
                    Err(_) => break
                };

                let t = UdpTraffic{
                    from,
                    data: Bytes::copy_from_slice(&buf[..len])
                };

                outbount_tx.send(t).await?;
            },

            // No traffic for the duration of UDP_TIMEOUT, clean up the state
            _ = time::sleep(Duration::from_secs(UDP_TIMEOUT)) => {
                break;
            }
        }
    }

    let mut port_map = port_map.write().await;
    port_map.remove(&from);

    debug!("Forwarder dropped");
    Ok(())
}

/// Status of a directory row as seen by this client
fn row_status(m: &Mapping, st: &ClientState) -> &'static str {
    if m.user != st.user {
        return if m.online { "online" } else { "offline" };
    }
    if st.disabled.contains(&m.local_port) {
        "disabled"
    } else if !m.online {
        "offline"
    } else if m.proto == ServiceType::Tcp && !st.listening.contains(&m.local_port) {
        "not listening"
    } else {
        "online"
    }
}

/// Render the directory as a table. Without `show_all` only ports that are
/// reachable (online, or own ports turned off) are listed.
pub fn render_directory(st: &ClientState, show_all: bool) -> String {
    let mut out = format!(
        "\n{:<12} {:<5} {:>6} {:>7} {:<13} {:<21}{}\n",
        "USER",
        "PROTO",
        "LOCAL",
        "REMOTE",
        "STATUS",
        "ALIAS",
        if st.domain.is_some() { " DOMAIN" } else { "" }
    );
    for m in &st.directory {
        let status = row_status(m, st);
        if !show_all && status != "online" && status != "disabled" {
            continue;
        }
        let alias = if m.online && m.proto == ServiceType::Tcp && m.user != st.user {
            format!("{}:{}", st.alias_bind, m.remote_port)
        } else {
            String::from("-")
        };
        let domain = match &st.domain {
            Some(d) if m.proto == ServiceType::Tcp => format!(" {}", m.host(d)),
            Some(_) => String::from(" -"),
            None => String::new(),
        };
        out += &format!(
            "{:<12} {:<5} {:>6} {:>7} {:<13} {:<21}{}\n",
            m.user, m.proto, m.local_port, m.remote_port, status, alias, domain
        );
    }
    out += "ALIAS: <alias_bind>:<remote_port> on this machine forwards to that user's service.";
    if st.domain.is_some() {
        out += " DOMAIN: nginx host name of the mapping.";
    }
    out += "\n";
    out
}

/// Own local TCP ports of `dir` that accept a connection right now
async fn probe_listening(dir: &[Mapping], me: &str) -> Vec<u16> {
    let mut v = Vec::new();
    for m in dir {
        if m.user == me && m.proto == ServiceType::Tcp && !v.contains(&m.local_port) {
            let addr = format!("127.0.0.1:{}", m.local_port);
            if let Ok(Ok(_)) =
                time::timeout(Duration::from_millis(200), TcpStream::connect(&addr)).await
            {
                v.push(m.local_port);
            }
        }
    }
    v
}

/// Alias listeners: `<alias_bind>:<remote_port>` on this machine forwarding to
/// `<server_host>:<remote_port>` for every online TCP mapping of other users.
struct Aliases {
    bind_host: String,
    server_host: String,
    me: String,
    tasks: HashMap<u16, JoinHandle<()>>,
}

/// Remote ports of other users' online TCP mappings that get an alias on this
/// machine. Ports that are one of our own exposed local ports are skipped: the
/// alias would shadow the local service (and our own data channels).
fn alias_ports(dir: &[Mapping], me: &str) -> HashSet<u16> {
    let mine: HashSet<u16> = dir
        .iter()
        .filter(|m| m.user == me)
        .map(|m| m.local_port)
        .collect();
    dir.iter()
        .filter(|m| m.online && m.proto == ServiceType::Tcp && m.user != me)
        .map(|m| m.remote_port)
        .filter(|p| !mine.contains(p))
        .collect()
}

impl Aliases {
    fn apply(&mut self, dir: &[Mapping]) {
        // `alias_bind = ""` turns aliases off
        let want = if self.bind_host.is_empty() {
            HashSet::new()
        } else {
            alias_ports(dir, &self.me)
        };

        self.tasks.retain(|port, h| {
            if want.contains(port) {
                true
            } else {
                info!("Closing alias :{}", port);
                h.abort();
                false
            }
        });

        for port in want {
            if let std::collections::hash_map::Entry::Vacant(e) = self.tasks.entry(port) {
                let bind = format!("{}:{}", self.bind_host, port);
                let target = format!("{}:{}", self.server_host, port);
                e.insert(tokio::spawn(
                    run_alias(bind, target).instrument(Span::current()),
                ));
            }
        }
    }
}

impl Drop for Aliases {
    fn drop(&mut self) {
        for h in self.tasks.values() {
            h.abort();
        }
    }
}

#[instrument(skip_all, fields(bind, target))]
async fn run_alias(bind: String, target: String) {
    // Keep retrying: the port may be briefly held by a previous alias (reload)
    // or by another process. This task is aborted when the mapping goes offline.
    let mut busy_warned = false;
    let l = loop {
        // A bind to a specific address succeeds on macOS even when a local
        // service listens on the wildcard, and then shadows it. Check first.
        if TcpStream::connect(&bind).await.is_ok() {
            if !busy_warned {
                warn!("Alias {} not opened: something already listens there", bind);
                busy_warned = true;
            }
            time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        match TcpListener::bind(&bind).await {
            Ok(l) => break l,
            Err(e) => {
                warn!("Failed to open alias {}: {}. Retry in 1s", bind, e);
                time::sleep(Duration::from_secs(1)).await;
            }
        }
    };
    info!("Alias {} -> {}", bind, target);

    // Dropping the JoinSet (when this task is aborted) aborts in-flight connections
    let mut set = JoinSet::new();
    loop {
        let accepted = tokio::select! {
            r = l.accept() => r,
            // Reap finished connections so the set doesn't grow forever
            Some(_) = set.join_next(), if !set.is_empty() => continue,
        };
        match accepted {
            Ok((mut visitor, _)) => {
                let target = target.clone();
                set.spawn(async move {
                    match TcpStream::connect(&target).await {
                        Ok(mut relay) => {
                            let _ = copy_bidirectional(&mut visitor, &mut relay).await;
                        }
                        Err(e) => warn!("Alias failed to connect to {}: {}", target, e),
                    }
                });
            }
            Err(e) => {
                warn!("Alias accept error: {}", e);
                time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

// Control channel, using T as the transport layer
struct ControlChannel<T: Transport> {
    digest: UserDigest,                 // SHA256 of the user name
    config: ClientConfig,               // `[client]` config block
    shutdown_rx: oneshot::Receiver<u8>, // Receives the shutdown signal
    transport: Arc<T>,                  // Wrapper around the transport layer
}

// Handle of a control channel
// Dropping it will also drop the actual control channel
struct ControlChannelHandle {
    shutdown_tx: oneshot::Sender<u8>,
}

impl<T: 'static + Transport> ControlChannel<T> {
    #[instrument(skip_all)]
    async fn run(&mut self) -> Result<()> {
        let mut remote_addr = AddrMaybeCached::new(&self.config.remote_addr);
        remote_addr.resolve().await?;

        let mut conn = self
            .transport
            .connect(&remote_addr)
            .await
            .with_context(|| format!("Failed to connect to {}", &self.config.remote_addr))?;
        T::hint(&conn, SocketOpts::for_control_channel());

        // Send hello
        debug!("Sending hello");
        let hello_send =
            Hello::ControlChannelHello(CURRENT_PROTO_VERSION, self.digest[..].try_into().unwrap());
        conn.write_all(&bincode::serialize(&hello_send).unwrap())
            .await?;
        conn.flush().await?;

        // Read hello
        debug!("Reading hello");
        let nonce = match read_hello(&mut conn).await? {
            ControlChannelHello(_, d) => d,
            _ => {
                bail!("Unexpected type of hello");
            }
        };

        // Send auth
        debug!("Sending auth");
        let mut concat = Vec::from(self.config.key.as_bytes());
        concat.extend_from_slice(&nonce);

        let session_key = protocol::digest(&concat);
        let auth = Auth(session_key);
        conn.write_all(&bincode::serialize(&auth).unwrap()).await?;
        conn.flush().await?;

        // Read ack
        debug!("Reading ack");
        match read_ack(&mut conn).await? {
            Ack::Ok => {}
            v => {
                return Err(anyhow!("{}", v))
                    .with_context(|| format!("Authentication failed: {}", self.config.user));
            }
        }

        // The server allocates remote ports for the ports configured for this user
        debug!("Waiting for registration");
        let domain = match read_frame(&mut conn).await? {
            RegisterAck::Ok(domain) => domain,
            RegisterAck::Err(e) => bail!("Registration rejected: {}", e),
        };

        // Channel ready
        info!("Control channel established");
        let me = self.config.user.clone();
        let alias_bind = self.config.alias_bind.clone();
        let (server_host, _) = host_port_pair(&self.config.remote_addr)?;
        update_state(|st| {
            *st = ClientState {
                connected: true,
                user: me.clone(),
                alias_bind: alias_bind.clone(),
                server_host: server_host.to_owned(),
                domain,
                disabled: DISABLED.borrow().clone(),
                ..Default::default()
            }
        });

        // Socket options for the data channel
        let socket_opts = SocketOpts::nodelay(self.config.nodelay);
        let data_ch_args = Arc::new(RunDataChannelArgs {
            session_key,
            remote_addr,
            connector: self.transport.clone(),
            socket_opts,
            prefer_ipv6: self.config.prefer_ipv6,
        });

        let mut aliases = Aliases {
            bind_host: alias_bind,
            server_host: server_host.to_owned(),
            me: me.clone(),
            tasks: HashMap::new(),
        };

        let (mut rd, mut wr) = io::split(conn);

        // Tell the server which ports are turned off, now and on every change
        let mut disabled_rx = DISABLED.subscribe();
        let disabled = disabled_rx.borrow_and_update().clone();
        if !disabled.is_empty() {
            write_frame(&mut wr, &ClientCmd::Disabled(disabled)).await?;
        }

        // Frames from the server are read by a task: `read_frame` is not
        // cancel-safe, so it can't sit in the `select!` below
        let (frame_tx, mut frame_rx) = mpsc::channel(8);
        let reader = tokio::spawn(async move {
            loop {
                let r = read_frame::<ControlChannelCmd, _>(&mut rd).await;
                let err = r.is_err();
                if frame_tx.send(r).await.is_err() || err {
                    break;
                }
            }
        });

        let mut probe = time::interval(Duration::from_secs(PROBE_INTERVAL_SECS));

        let result = loop {
            tokio::select! {
                val = frame_rx.recv() => {
                    let val = match val {
                        Some(Ok(v)) => v,
                        Some(Err(e)) => break Err(e),
                        None => break Err(anyhow!("Control channel closed")),
                    };
                    match val {
                        ControlChannelCmd::CreateDataChannel => {
                            debug!("Received CreateDataChannel");
                            let args = data_ch_args.clone();
                            tokio::spawn(async move {
                                if let Err(e) = run_data_channel(args).await.with_context(|| "Failed to run the data channel") {
                                    warn!("{:#}", e);
                                }
                            }.instrument(Span::current()));
                        },
                        ControlChannelCmd::HeartBeat => (),
                        ControlChannelCmd::Directory(dir) => {
                            let listening = probe_listening(&dir, &me).await;
                            aliases.apply(&dir);
                            update_state(|st| { st.directory = dir; st.listening = listening; });
                            println!("{}", render_directory(&STATE.borrow(), false));
                        }
                    }
                },
                _ = probe.tick() => {
                    let dir = STATE.borrow().directory.clone();
                    let listening = probe_listening(&dir, &me).await;
                    if listening != STATE.borrow().listening {
                        update_state(|st| st.listening = listening);
                        println!("{}", render_directory(&STATE.borrow(), false));
                    }
                },
                Ok(()) = disabled_rx.changed() => {
                    let disabled = disabled_rx.borrow_and_update().clone();
                    if let Err(e) = write_frame(&mut wr, &ClientCmd::Disabled(disabled.clone())).await {
                        break Err(e);
                    }
                    update_state(|st| st.disabled = disabled);
                },
                _ = time::sleep(Duration::from_secs(self.config.heartbeat_timeout)), if self.config.heartbeat_timeout != 0 => {
                    break Err(anyhow!("Heartbeat timed out"));
                }
                _ = &mut self.shutdown_rx => {
                    break Ok(());
                }
            }
        };

        reader.abort();
        update_state(|st| st.connected = false);
        result?;

        info!("Control channel shutdown");
        Ok(())
    }
}

impl ControlChannelHandle {
    #[instrument(name="handle", skip_all, fields(user = %config.user))]
    fn new<T: 'static + Transport>(
        config: ClientConfig,
        transport: Arc<T>,
    ) -> ControlChannelHandle {
        let digest = protocol::digest(config.user.as_bytes());

        info!("Starting {}", hex::encode(digest));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let mut retry_backoff = run_control_chan_backoff(config.retry_interval);

        let mut s = ControlChannel {
            digest,
            config,
            shutdown_rx,
            transport,
        };

        tokio::spawn(
            async move {
                let mut start = Instant::now();

                while let Err(err) = s
                    .run()
                    .await
                    .with_context(|| "Failed to run the control channel")
                {
                    if s.shutdown_rx.try_recv() != Err(oneshot::error::TryRecvError::Empty) {
                        break;
                    }

                    if start.elapsed() > Duration::from_secs(3) {
                        // The client runs for at least 3 secs and then disconnects
                        retry_backoff.reset();
                    }

                    if let Some(duration) = retry_backoff.next_backoff() {
                        error!("{:#}. Retry in {:?}...", err, duration);
                        time::sleep(duration).await;
                    } else {
                        // Should never reach
                        panic!("{:#}. Break", err);
                    }

                    start = Instant::now();
                }
            }
            .instrument(Span::current()),
        );

        ControlChannelHandle { shutdown_tx }
    }

    fn shutdown(self) {
        // A send failure shows that the actor has already shutdown.
        let _ = self.shutdown_tx.send(0u8);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alias_ports() {
        let m = |user: &str, local_port, remote_port| Mapping {
            user: user.into(),
            proto: ServiceType::Tcp,
            local_port,
            remote_port,
            online: true,
        };
        // feng's remote 5173 collides with coca's own local 5173: no alias
        let dir = [
            m("coca", 5173, 8100),
            m("feng", 5173, 5173),
            m("feng", 5174, 5174),
        ];
        let want = alias_ports(&dir, "coca");
        assert!(!want.contains(&5173));
        assert!(want.contains(&5174));
        assert!(!want.contains(&8100));
    }

    #[test]
    fn test_render_directory() {
        let m = |user: &str, local_port, remote_port, online| Mapping {
            user: user.into(),
            proto: ServiceType::Tcp,
            local_port,
            remote_port,
            online,
        };
        let st = ClientState {
            connected: true,
            user: "alice".into(),
            alias_bind: "127.0.0.1".into(),
            server_host: "srv".into(),
            domain: Some("example.com".into()),
            directory: vec![
                m("alice", 80, 20000, true),
                m("alice", 81, 20001, true),
                m("alice", 82, 20002, false),
                m("bob", 3000, 21000, true),
                m("bob", 3001, 21001, false),
            ],
            listening: vec![80, 82],
            disabled: vec![82],
        };
        let out = render_directory(&st, false);
        assert!(out.contains("80-alice.example.com"));
        assert!(out.contains("disabled"));
        assert!(out.contains("127.0.0.1:21000"));
        assert!(!out.contains("20001") && !out.contains("21001"));
        let all = render_directory(&st, true);
        assert!(all.contains("not listening") && all.contains("21001"));
    }
}
