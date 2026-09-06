use crate::config::{Config, NginxConfig, ServerConfig, ServiceType, TransportType, UserConfig};
use crate::constants::{listen_backoff, UDP_BUFFER_SIZE};
use crate::helper::{host_port_pair, retry_notify_with_deadline};
use crate::multi_map::MultiMap;
use crate::protocol::Hello::{ControlChannelHello, DataChannelHello};
use crate::protocol::{
    self, read_auth, read_frame, read_hello, write_frame, Ack, ClientCmd, ControlChannelCmd,
    DataChannelCmd, Hello, Mapping, RegisterAck, UdpTraffic, HASH_WIDTH_IN_BYTES,
};
use crate::registry::Registry;
use crate::transport::{SocketOpts, TcpTransport, Transport};
use anyhow::{anyhow, bail, Context, Result};
use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;

use rand::RngCore;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::io::{self, copy_bidirectional, AsyncReadExt, AsyncWriteExt, WriteHalf};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{broadcast, mpsc, watch, RwLock};
use tokio::time;
use tracing::{debug, error, info, info_span, instrument, warn, Instrument, Span};

#[cfg(feature = "noise")]
use crate::transport::NoiseTransport;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::transport::TlsTransport;
#[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
use crate::transport::WebsocketTransport;

type UserDigest = protocol::Digest; // SHA256 of a user name
type Nonce = protocol::Digest; // Also called `session_key`

const TCP_POOL_SIZE: usize = 8; // The number of cached data channels per user
const CHAN_SIZE: usize = 2048; // The capacity of various chans
const HANDSHAKE_TIMEOUT: u64 = 5; // Timeout for transport handshake

// The entrypoint of running a server
pub async fn run_server(config: Config, shutdown_rx: broadcast::Receiver<bool>) -> Result<()> {
    let config = match config.server {
            Some(config) => config,
            None => {
                return Err(anyhow!("Try to run as a server, but the configuration is missing. Please add the `[server]` block"))
            }
        };

    match config.transport.transport_type {
        TransportType::Tcp => {
            let mut server = Server::<TcpTransport>::from(config).await?;
            server.run(shutdown_rx).await?;
        }
        TransportType::Tls => {
            #[cfg(any(feature = "native-tls", feature = "rustls"))]
            {
                let mut server = Server::<TlsTransport>::from(config).await?;
                server.run(shutdown_rx).await?;
            }
            #[cfg(not(any(feature = "native-tls", feature = "rustls")))]
            crate::helper::feature_neither_compile("native-tls", "rustls")
        }
        TransportType::Noise => {
            #[cfg(feature = "noise")]
            {
                let mut server = Server::<NoiseTransport>::from(config).await?;
                server.run(shutdown_rx).await?;
            }
            #[cfg(not(feature = "noise"))]
            crate::helper::feature_not_compile("noise")
        }
        TransportType::Websocket => {
            #[cfg(any(feature = "websocket-native-tls", feature = "websocket-rustls"))]
            {
                let mut server = Server::<WebsocketTransport>::from(config).await?;
                server.run(shutdown_rx).await?;
            }
            #[cfg(not(any(feature = "websocket-native-tls", feature = "websocket-rustls")))]
            crate::helper::feature_neither_compile("websocket-native-tls", "websocket-rustls")
        }
    }

    Ok(())
}

// A hash map of ControlChannelHandles, indexed by UserDigest or Nonce
// See also MultiMap
type ControlChannelMap<T> = MultiMap<UserDigest, Nonce, ControlChannelHandle<T>>;
type Users = Arc<HashMap<UserDigest, UserConfig>>;
type SharedRegistry = Arc<Mutex<Registry>>;
type DirectoryRx = watch::Receiver<Arc<Vec<Mapping>>>;

// Server holds all states of running a server
struct Server<T: Transport> {
    // `[server]` config
    config: Arc<ServerConfig>,
    // `[server.users]`, indexed by UserDigest
    users: Users,
    // Collection of control channels
    control_channels: Arc<RwLock<ControlChannelMap<T>>>,
    // Port allocations, online state and the directory
    registry: SharedRegistry,
    // Wrapper around the transport layer
    transport: Arc<T>,
}

// Generate a hash map of users which is indexed by UserDigest
fn generate_user_hashmap(users: &HashMap<String, UserConfig>) -> HashMap<UserDigest, UserConfig> {
    users
        .iter()
        .map(|(name, u)| (protocol::digest(name.as_bytes()), u.clone()))
        .collect()
}

impl<T: 'static + Transport> Server<T> {
    // Create a server from `[server]`
    pub async fn from(config: ServerConfig) -> Result<Server<T>> {
        let config = Arc::new(config);
        info!("Loaded {} users", config.users.len());
        let users = Arc::new(generate_user_hashmap(&config.users));
        let registry = Arc::new(Mutex::new(Registry::load(config.alloc_file.clone())?));
        let control_channels = Arc::new(RwLock::new(ControlChannelMap::new()));
        let transport = Arc::new(T::new(&config.transport)?);
        Ok(Server {
            config,
            users,
            control_channels,
            registry,
            transport,
        })
    }

    // The entry point of Server
    pub async fn run(&mut self, mut shutdown_rx: broadcast::Receiver<bool>) -> Result<()> {
        // Listen at `server.bind_addr`
        let l = self
            .transport
            .bind(&self.config.bind_addr)
            .await
            .with_context(|| "Failed to listen at `server.bind_addr`")?;
        info!("Listening at {}", self.config.bind_addr);

        // Keep the nginx map in sync with the directory
        let nginx_task = self.config.nginx.clone().map(|cfg| {
            let rx = self.registry.lock().unwrap().subscribe();
            tokio::spawn(run_nginx_map(cfg, rx))
        });

        // Retry at least every 100ms
        let mut backoff = ExponentialBackoff {
            max_interval: Duration::from_millis(100),
            max_elapsed_time: None,
            ..Default::default()
        };

        // Wait for connections and shutdown signals
        loop {
            tokio::select! {
                // Wait for incoming control and data channels
                ret = self.transport.accept(&l) => {
                    match ret {
                        Err(err) => {
                            // Detects whether it's an IO error
                            if let Some(err) = err.downcast_ref::<io::Error>() {
                                // If it is an IO error, then it's possibly an
                                // EMFILE. So sleep for a while and retry
                                // TODO: Only sleep for EMFILE, ENFILE, ENOMEM, ENOBUFS
                                if let Some(d) = backoff.next_backoff() {
                                    error!("Failed to accept: {:#}. Retry in {:?}...", err, d);
                                    time::sleep(d).await;
                                } else {
                                    // This branch will never be executed according to the current retry policy
                                    error!("Too many retries. Aborting...");
                                    break;
                                }
                            }
                            // If it's not an IO error, then it comes from
                            // the transport layer, so just ignore it
                        }
                        Ok((conn, addr)) => {
                            backoff.reset();

                            // Do transport handshake with a timeout
                            match time::timeout(Duration::from_secs(HANDSHAKE_TIMEOUT), self.transport.handshake(conn)).await {
                                Ok(conn) => {
                                    match conn.with_context(|| "Failed to do transport handshake") {
                                        Ok(conn) => {
                                            let users = self.users.clone();
                                            let control_channels = self.control_channels.clone();
                                            let server_config = self.config.clone();
                                            let registry = self.registry.clone();
                                            tokio::spawn(async move {
                                                if let Err(err) = handle_connection(conn, users, control_channels, registry, server_config).await {
                                                    error!("{:#}", err);
                                                }
                                            }.instrument(info_span!("connection", %addr)));
                                        }, Err(e) => {
                                            error!("{:#}", e);
                                        }
                                    }
                                },
                                Err(e) => {
                                    error!("Transport handshake timeout: {}", e);
                                }
                            }
                        }
                    }
                },
                // Wait for the shutdown signal
                _ = shutdown_rx.recv() => {
                    info!("Shuting down gracefully...");
                    break;
                },
            }
        }

        if let Some(t) = nginx_task {
            t.abort();
        }

        info!("Shutdown");

        Ok(())
    }
}

/// Rewrite `cfg.map_file` (and run `reload_cmd`) whenever the TCP mappings change
async fn run_nginx_map(cfg: NginxConfig, mut rx: DirectoryRx) {
    loop {
        let dir = rx.borrow_and_update().clone();
        if let Err(e) = update_nginx_map(&cfg, &dir).await {
            error!("{:#}", e);
        }
        if rx.changed().await.is_err() {
            break;
        }
    }
}

fn render_nginx_map(dir: &[Mapping], domain: &str) -> String {
    dir.iter()
        .filter(|m| m.proto == ServiceType::Tcp)
        .map(|m| format!("{} {};\n", m.host(domain), m.remote_port))
        .collect()
}

async fn update_nginx_map(cfg: &NginxConfig, dir: &[Mapping]) -> Result<()> {
    let s = render_nginx_map(dir, &cfg.domain);
    if tokio::fs::read_to_string(&cfg.map_file)
        .await
        .ok()
        .as_deref()
        == Some(s.as_str())
    {
        return Ok(());
    }
    tokio::fs::write(&cfg.map_file, &s)
        .await
        .with_context(|| format!("Failed to write {:?}", cfg.map_file))?;
    info!("Wrote nginx map {:?}", cfg.map_file);
    if let Some(cmd) = &cfg.reload_cmd {
        let (sh, flag) = if cfg!(windows) {
            ("cmd", "/C")
        } else {
            ("sh", "-c")
        };
        let status = tokio::process::Command::new(sh)
            .arg(flag)
            .arg(cmd)
            .status()
            .await
            .with_context(|| format!("Failed to run `{}`", cmd))?;
        if !status.success() {
            bail!("`{}` exited with {}", cmd, status);
        }
        info!("Ran `{}`", cmd);
    }
    Ok(())
}

// Handle connections to `server.bind_addr`
async fn handle_connection<T: 'static + Transport>(
    mut conn: T::Stream,
    users: Users,
    control_channels: Arc<RwLock<ControlChannelMap<T>>>,
    registry: SharedRegistry,
    server_config: Arc<ServerConfig>,
) -> Result<()> {
    // Read hello
    let hello = read_hello(&mut conn).await?;
    match hello {
        ControlChannelHello(_, user_digest) => {
            do_control_channel_handshake(
                conn,
                users,
                control_channels,
                registry,
                user_digest,
                server_config,
            )
            .await?;
        }
        DataChannelHello(_, nonce) => {
            do_data_channel_handshake(conn, control_channels, nonce).await?;
        }
    }
    Ok(())
}

async fn do_control_channel_handshake<T: 'static + Transport>(
    mut conn: T::Stream,
    users: Users,
    control_channels: Arc<RwLock<ControlChannelMap<T>>>,
    registry: SharedRegistry,
    user_digest: UserDigest,
    server_config: Arc<ServerConfig>,
) -> Result<()> {
    info!("Try to handshake a control channel");

    T::hint(&conn, SocketOpts::for_control_channel());

    // Generate a nonce
    let mut nonce = vec![0u8; HASH_WIDTH_IN_BYTES];
    rand::thread_rng().fill_bytes(&mut nonce);

    // Send hello
    let hello_send = Hello::ControlChannelHello(
        protocol::CURRENT_PROTO_VERSION,
        nonce.clone().try_into().unwrap(),
    );
    conn.write_all(&bincode::serialize(&hello_send).unwrap())
        .await?;
    conn.flush().await?;

    // Lookup the user
    let user = match users.get(&user_digest) {
        Some(v) => v,
        None => {
            conn.write_all(&bincode::serialize(&Ack::UserNotExist).unwrap())
                .await?;
            bail!("No such a user {}", hex::encode(user_digest));
        }
    }
    .to_owned();

    // Calculate the checksum
    let mut concat = Vec::from(user.key.as_bytes());
    concat.append(&mut nonce);

    // Read auth
    let protocol::Auth(d) = read_auth(&mut conn).await?;

    // Validate
    let session_key = protocol::digest(&concat);
    if session_key != d {
        conn.write_all(&bincode::serialize(&Ack::AuthFailed).unwrap())
            .await?;
        debug!(
            "Expect {}, but got {}",
            hex::encode(session_key),
            hex::encode(d)
        );
        bail!("User {} failed the authentication", user.name);
    }

    // Send ack
    conn.write_all(&bincode::serialize(&Ack::Ok).unwrap())
        .await?;
    conn.flush().await?;

    // Allocate remote ports for the user's configured local ports
    let (mappings, dir_rx) = {
        let mut r = registry.lock().unwrap();
        (r.register(&user, session_key), r.subscribe())
    };
    let mappings = match mappings {
        Ok(m) => m,
        Err(e) => {
            write_frame(&mut conn, &RegisterAck::Err(format!("{:#}", e))).await?;
            return Err(e).with_context(|| format!("User {} failed to register", user.name));
        }
    };
    let domain = server_config.nginx.as_ref().map(|n| n.domain.clone());
    write_frame(&mut conn, &RegisterAck::Ok(domain)).await?;

    let mut h = control_channels.write().await;

    // If there's already a control channel for the user, then drop the old one.
    // Because a control channel doesn't report back when it's dead,
    // the handle in the map could be stall, dropping the old handle enables
    // the client to reconnect.
    if h.remove1(&user_digest).is_some() {
        warn!("Dropping previous control channel for user {}", user.name);
    }

    for m in &mappings {
        info!(user = %user.name, "{} 127.0.0.1:{} -> :{}", m.proto, m.local_port, m.remote_port);
    }
    info!(user = %user.name, "Control channel established");

    let bind_host = match &server_config.expose_bind {
        Some(h) => h.clone(),
        None => host_port_pair(&server_config.bind_addr)?.0.to_owned(),
    };
    let handle = ControlChannelHandle::new(
        conn,
        user.name.clone(),
        session_key,
        mappings,
        bind_host,
        server_config.nodelay,
        server_config.heartbeat_interval,
        registry,
        dir_rx,
        control_channels.clone(),
    );

    // Insert the new handle
    let _ = h.insert(user_digest, session_key, handle);

    Ok(())
}

async fn do_data_channel_handshake<T: 'static + Transport>(
    conn: T::Stream,
    control_channels: Arc<RwLock<ControlChannelMap<T>>>,
    nonce: Nonce,
) -> Result<()> {
    debug!("Try to handshake a data channel");

    // Validate
    let control_channels_guard = control_channels.read().await;
    match control_channels_guard.get2(&nonce) {
        Some(handle) => {
            T::hint(&conn, SocketOpts::nodelay(handle.nodelay));

            // Send the data channel to the corresponding control channel
            handle
                .data_ch_tx
                .send(conn)
                .await
                .with_context(|| "Data channel for a stale control channel")?;
        }
        None => {
            warn!("Data channel has incorrect nonce");
        }
    }
    Ok(())
}

// Data channels of a user are shared by all its listeners
type DataChannelRx<T> = Arc<tokio::sync::Mutex<mpsc::Receiver<<T as Transport>::Stream>>>;

pub struct ControlChannelHandle<T: Transport> {
    // Shutdown the control channel by dropping it
    _shutdown_tx: broadcast::Sender<bool>,
    // Kept here so `data_ch_req_rx` stays open for users without TCP listeners
    _data_ch_req_tx: mpsc::UnboundedSender<bool>,
    data_ch_tx: mpsc::Sender<T::Stream>,
    nodelay: Option<bool>,
}

impl<T> ControlChannelHandle<T>
where
    T: 'static + Transport,
{
    // Create a control channel handle, where the control channel handling task
    // and the listeners for all mappings of the user are created.
    #[allow(clippy::too_many_arguments)]
    #[instrument(name = "handle", skip_all, fields(user = %user))]
    fn new(
        conn: T::Stream,
        user: String,
        nonce: Nonce,
        mappings: Vec<Mapping>,
        bind_host: String,
        nodelay: Option<bool>,
        heartbeat_interval: u64,
        registry: SharedRegistry,
        dir_rx: DirectoryRx,
        control_channels: Arc<RwLock<ControlChannelMap<T>>>,
    ) -> ControlChannelHandle<T> {
        // Create a shutdown channel
        let (shutdown_tx, shutdown_rx) = broadcast::channel::<bool>(1);

        // Store data channels
        let (data_ch_tx, data_ch_rx) = mpsc::channel(CHAN_SIZE * 2);
        let data_ch_rx: DataChannelRx<T> = Arc::new(tokio::sync::Mutex::new(data_ch_rx));

        // Store data channel creation requests
        let (data_ch_req_tx, data_ch_req_rx) = mpsc::unbounded_channel();

        // Visitors of all TCP listeners
        let (visitor_tx, visitor_rx) = mpsc::channel(CHAN_SIZE);

        let mut pool_size = 0;
        let mut has_tcp = false;
        for m in mappings {
            let addr = format!("{}:{}", bind_host, m.remote_port);
            match m.proto {
                ServiceType::Tcp => {
                    has_tcp = true;
                    tcp_listen_and_send(
                        addr,
                        m.local_port,
                        visitor_tx.clone(),
                        data_ch_req_tx.clone(),
                        shutdown_tx.subscribe(),
                    );
                }
                ServiceType::Udp => {
                    pool_size += 1;
                    let data_ch_rx = data_ch_rx.clone();
                    let shutdown_rx = shutdown_tx.subscribe();
                    tokio::spawn(
                        async move {
                            if let Err(e) = run_udp_connection_pool::<T>(
                                addr,
                                m.local_port,
                                data_ch_rx,
                                shutdown_rx,
                            )
                            .await
                            .with_context(|| "Failed to run UDP connection pool")
                            {
                                error!("{:#}", e);
                            }
                        }
                        .instrument(Span::current()),
                    );
                }
            }
        }
        if has_tcp {
            pool_size += TCP_POOL_SIZE;
            let data_ch_rx = data_ch_rx.clone();
            let data_ch_req_tx = data_ch_req_tx.clone();
            tokio::spawn(
                async move {
                    if let Err(e) =
                        run_tcp_connection_pool::<T>(visitor_rx, data_ch_rx, data_ch_req_tx)
                            .await
                            .with_context(|| "Failed to run TCP connection pool")
                    {
                        error!("{:#}", e);
                    }
                }
                .instrument(Span::current()),
            );
        }

        // Cache some data channels for later use
        for _i in 0..pool_size {
            if let Err(e) = data_ch_req_tx.send(true) {
                error!("Failed to request data channel {}", e);
            };
        }

        // Frames from the client are read by a task: `read_frame` is not
        // cancel-safe, so it can't sit in the `select!` below
        let (rd, wr) = io::split(conn);
        let (client_tx, client_rx) = mpsc::channel(8);
        let reader = tokio::spawn(async move {
            let mut rd = rd;
            loop {
                let r = read_frame::<ClientCmd, _>(&mut rd).await;
                let err = r.is_err();
                if client_tx.send(r).await.is_err() || err {
                    break;
                }
            }
        });

        // Create the control channel
        let ch = ControlChannel::<T> {
            wr,
            client_rx,
            reader,
            shutdown_rx,
            data_ch_req_rx,
            heartbeat_interval,
            dir_rx,
            user,
            nonce,
            registry,
            // Weak: a strong reference would keep the map (and thus our own
            // shutdown sender) alive after the Server is dropped on reload
            control_channels: Arc::downgrade(&control_channels),
        };

        // Run the control channel
        tokio::spawn(
            async move {
                if let Err(err) = ch.run().await {
                    error!("{:#}", err);
                }
            }
            .instrument(Span::current()),
        );

        ControlChannelHandle {
            _shutdown_tx: shutdown_tx,
            _data_ch_req_tx: data_ch_req_tx,
            data_ch_tx,
            nodelay,
        }
    }
}

// Control channel, using T as the transport layer
struct ControlChannel<T: Transport> {
    wr: WriteHalf<T::Stream>, // The connection of control channel
    client_rx: mpsc::Receiver<Result<ClientCmd>>, // Frames sent by the client
    reader: tokio::task::JoinHandle<()>, // Holds the read half; aborted on exit so the socket closes
    shutdown_rx: broadcast::Receiver<bool>, // Receives the shutdown signal
    data_ch_req_rx: mpsc::UnboundedReceiver<bool>, // Receives visitor connections
    heartbeat_interval: u64,             // Application-layer heartbeat interval in secs
    dir_rx: DirectoryRx,                 // Directory updates to push
    user: String,
    nonce: Nonce,
    registry: SharedRegistry,
    control_channels: Weak<RwLock<ControlChannelMap<T>>>,
}

impl<T: Transport> ControlChannel<T> {
    async fn send(&mut self, cmd: &ControlChannelCmd) -> Result<()> {
        write_frame(&mut self.wr, cmd)
            .await
            .with_context(|| "Failed to write control cmds")
    }

    // Run a control channel
    #[instrument(skip_all)]
    async fn run(mut self) -> Result<()> {
        // Push the current directory first
        let dir = self.dir_rx.borrow_and_update().clone();
        self.send(&ControlChannelCmd::Directory(dir.as_ref().clone()))
            .await?;

        // Wait for data channel requests and the shutdown signal
        loop {
            tokio::select! {
                val = self.data_ch_req_rx.recv() => {
                    match val {
                        Some(_) => {
                            if let Err(e) = self.send(&ControlChannelCmd::CreateDataChannel).await {
                                error!("{:#}", e);
                                break;
                            }
                        }
                        None => {
                            break;
                        }
                    }
                },
                r = self.dir_rx.changed() => {
                    if r.is_err() {
                        break;
                    }
                    let dir = self.dir_rx.borrow_and_update().clone();
                    if let Err(e) = self.send(&ControlChannelCmd::Directory(dir.as_ref().clone())).await {
                        error!("{:#}", e);
                        break;
                    }
                },
                _ = time::sleep(Duration::from_secs(self.heartbeat_interval)), if self.heartbeat_interval != 0 => {
                    if let Err(e) = self.send(&ControlChannelCmd::HeartBeat).await {
                        error!("{:#}", e);
                        break;
                    }
                },
                val = self.client_rx.recv() => {
                    match val {
                        Some(Ok(ClientCmd::Disabled(ports))) => {
                            info!("Disabled ports {:?}", ports);
                            self.registry.lock().unwrap().set_disabled(&self.user, self.nonce, ports);
                        }
                        Some(Err(e)) => {
                            debug!("{:#}", e);
                            info!("Client disconnected");
                            break;
                        }
                        None => {
                            info!("Client disconnected");
                            break;
                        }
                    }
                },
                // Wait for the shutdown signal
                _ = self.shutdown_rx.recv() => {
                    break;
                }
            }
        }

        self.reader.abort();

        // Remove ourselves (only this session, keyed by nonce) so the listeners close
        if let Some(m) = self.control_channels.upgrade() {
            let _ = m.write().await.remove2(&self.nonce);
        }
        self.registry
            .lock()
            .unwrap()
            .unregister(&self.user, self.nonce);

        info!("Control channel shutdown");

        Ok(())
    }
}

fn tcp_listen_and_send(
    addr: String,
    local_port: u16,
    tx: mpsc::Sender<(TcpStream, u16)>,
    data_ch_req_tx: mpsc::UnboundedSender<bool>,
    mut shutdown_rx: broadcast::Receiver<bool>,
) {
    tokio::spawn(async move {
        let l = retry_notify_with_deadline(listen_backoff(),  || async {
            Ok(TcpListener::bind(&addr).await?)
        }, |e, duration| {
            error!("{:#}. Retry in {:?}", e, duration);
        }, &mut shutdown_rx).await
        .with_context(|| "Failed to listen for the service");

        let l: TcpListener = match l {
            Ok(v) => v,
            Err(e) => {
                error!("{:#}", e);
                return;
            }
        };

        info!("Listening at {}", &addr);

        // Retry at least every 1s
        let mut backoff = ExponentialBackoff {
            max_interval: Duration::from_secs(1),
            max_elapsed_time: None,
            ..Default::default()
        };

        // Wait for visitors and the shutdown signal
        loop {
            tokio::select! {
                val = l.accept() => {
                    match val {
                        Err(e) => {
                            // `l` is a TCP listener so this must be a IO error
                            // Possibly a EMFILE. So sleep for a while
                            error!("{}. Sleep for a while", e);
                            if let Some(d) = backoff.next_backoff() {
                                time::sleep(d).await;
                            } else {
                                // This branch will never be reached for current backoff policy
                                error!("Too many retries. Aborting...");
                                break;
                            }
                        }
                        Ok((incoming, addr)) => {
                            // For every visitor, request to create a data channel
                            if data_ch_req_tx.send(true).with_context(|| "Failed to send data chan create request").is_err() {
                                // An error indicates the control channel is broken
                                // So break the loop
                                break;
                            }

                            backoff.reset();

                            debug!("New visitor from {}", addr);

                            // Send the visitor to the connection pool
                            let _ = tx.send((incoming, local_port)).await;
                        }
                    }
                },
                _ = shutdown_rx.recv() => {
                    break;
                }
            }
        }

        info!("TCPListener shutdown");
    }.instrument(Span::current()));
}

#[instrument(skip_all)]
async fn run_tcp_connection_pool<T: Transport>(
    mut visitor_rx: mpsc::Receiver<(TcpStream, u16)>,
    data_ch_rx: DataChannelRx<T>,
    data_ch_req_tx: mpsc::UnboundedSender<bool>,
) -> Result<()> {
    'pool: while let Some((mut visitor, local_port)) = visitor_rx.recv().await {
        let cmd = DataChannelCmd::StartForwardTcp(local_port);
        loop {
            let ch = data_ch_rx.lock().await.recv().await;
            if let Some(mut ch) = ch {
                if write_frame(&mut ch, &cmd).await.is_ok() {
                    tokio::spawn(async move {
                        let _ = copy_bidirectional(&mut ch, &mut visitor).await;
                    });
                    break;
                } else {
                    // Current data channel is broken. Request for a new one
                    if data_ch_req_tx.send(true).is_err() {
                        break 'pool;
                    }
                }
            } else {
                break 'pool;
            }
        }
    }

    info!("Shutdown");
    Ok(())
}

#[instrument(skip_all, fields(local_port))]
async fn run_udp_connection_pool<T: Transport>(
    bind_addr: String,
    local_port: u16,
    data_ch_rx: DataChannelRx<T>,
    mut shutdown_rx: broadcast::Receiver<bool>,
) -> Result<()> {
    // TODO: Load balance

    let l = retry_notify_with_deadline(
        listen_backoff(),
        || async { Ok(UdpSocket::bind(&bind_addr).await?) },
        |e, duration| {
            warn!("{:#}. Retry in {:?}", e, duration);
        },
        &mut shutdown_rx,
    )
    .await
    .with_context(|| "Failed to listen for the service")?;

    info!("Listening at {}", &bind_addr);

    // Receive one data channel
    let mut conn = data_ch_rx
        .lock()
        .await
        .recv()
        .await
        .ok_or_else(|| anyhow!("No available data channels"))?;
    write_frame(&mut conn, &DataChannelCmd::StartForwardUdp(local_port)).await?;

    let mut buf = [0u8; UDP_BUFFER_SIZE];
    loop {
        tokio::select! {
            // Forward inbound traffic to the client
            val = l.recv_from(&mut buf) => {
                let (n, from) = val?;
                UdpTraffic::write_slice(&mut conn, from, &buf[..n]).await?;
            },

            // Forward outbound traffic from the client to the visitor
            hdr_len = conn.read_u8() => {
                let t = UdpTraffic::read(&mut conn, hdr_len?).await?;
                l.send_to(&t.data, t.from).await?;
            }

            _ = shutdown_rx.recv() => {
                break;
            }
        }
    }

    debug!("UDP pool dropped");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_nginx_map() {
        let m = |proto, local_port, remote_port| Mapping {
            user: "alice".into(),
            proto,
            local_port,
            remote_port,
            online: false,
        };
        let dir = [
            m(ServiceType::Tcp, 80, 20000),
            m(ServiceType::Udp, 53, 20001),
            m(ServiceType::Tcp, 8000, 20002),
        ];
        assert_eq!(
            render_nginx_map(&dir, "example.com"),
            "80-alice.example.com 20000;\n8000-alice.example.com 20002;\n"
        );
    }
}
