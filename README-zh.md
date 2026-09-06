# rathole

![rathole-logo](./docs/img/rathole-logo.png)

[English](README.md) | [简体中文](README-zh.md)

安全、稳定、高性能的内网穿透工具，用 Rust 语言编写。

本仓库是 [rathole](https://github.com/rapiz1/rathole) 的二次开发版本：把原来“每个服务一个 token”的模型改成了**用户模型**。服务端在 `server.toml` 里定义用户、密钥、独占的远端端口块以及要暴露的本地端口；客户端只需要 `服务器地址 + 用户名 + 密钥`，暴露什么一切以服务端为准，远端端口由服务端在该用户的端口块内自动、稳定地分配。所有客户端都能看到全网映射目录，并在本机自动开出别名端口访问其他人的服务。

<!-- TOC -->

- [rathole](#rathole)
  - [Features](#features)
  - [Quickstart](#quickstart)
  - [映射目录与别名](#映射目录与别名)
  - [Configuration](#configuration)
    - [server.users](#serverusers)
    - [nginx 子域名反代](#nginx-子域名反代)
    - [端口分配规则](#端口分配规则)
    - [热重载](#热重载)
    - [Logging](#logging)
    - [Tuning](#tuning)
  - [Build](#build)
  - [Benchmark](#benchmark)
  - [Development Status](#development-status)

<!-- /TOC -->

## Features

- **高性能** 具有更高的吞吐量，高并发下更稳定。见 [Benchmark](#benchmark)
- **低资源消耗** 内存占用远低于同类工具。[二进制文件最小](docs/build-guide.md)可以到 **~500KiB**，可以部署在嵌入式设备如路由器上。
- **用户级认证** 服务端 `[server.users]` 定义用户、密钥、独占的远端端口块和要暴露的本地端口；客户端只需 `remote_addr + user + key`。使用 Noise Protocol 可以简单地配置传输加密，而不需要自签证书。同时也支持 TLS。
- **稳定端口分配** 服务端用 `tcp = ["3000", "8000-8010"]` 指定该用户要暴露的本地端口，在端口块内分配远端端口并持久化到 `allocations.toml`，重启、重连后映射不变。
- **映射目录与别名** 认证后服务端下发全网映射目录（谁的哪个本地端口映射到哪个远端端口、是否在线）。客户端打印表格，并在本机 `127.0.0.1:<远端端口>` 开出别名监听转发到服务器；对端下线即关闭。
- **热重载** 配置文件修改后自动重启实例，客户端自动重连。

## Quickstart

假设你有一台公网服务器 `myserver.com`，家里 NAT 后面有一台 NAS，想把它的 ssh 暴露出去。

1. 在公网服务器上

创建 `server.toml`：

```toml
# server.toml
[server]
bind_addr = "0.0.0.0:2333" # 服务端监听客户端连接的端口

[server.users.alice]
key = "use_a_secret_that_only_you_know" # 客户端认证密钥
port_block = "20000-20999"              # alice 独占的远端端口块，各用户不能重叠
tcp = ["22"]                            # 要暴露的 alice 本机端口，支持 "端口" 或 "起-止" 范围
```

然后运行：

```bash
./rathole server.toml
```

2. 在 NAT 后面的主机（你的 NAS）上

创建 `client.toml`：

```toml
# client.toml
[client]
remote_addr = "myserver.com:2333" # 服务器地址，端口必须与 `server.bind_addr` 一致
user = "alice"
key = "use_a_secret_that_only_you_know"
```

客户端不用声明任何端口，暴露哪些端口以服务端配置为准。

然后运行：

```bash
./rathole client.toml
```

3. 客户端连上服务器后会打印映射表，例如：

```
USER         PROTO  LOCAL  REMOTE STATUS   ALIAS
alice        tcp       22   20000 online   -
```

任何到 `myserver.com:20000` 的流量都会被转发到 NAS 的 `22` 端口，所以你可以 `ssh -p 20000 myserver.com` 登录 NAS。分配结果会写进服务端的 `allocations.toml`，下次 `22` 仍然映射到 `20000`。

更多示例见 [examples](./examples)：

| 目录 | 说明 |
|---|---|
| `examples/minimal` | 最小配置，含一个只看目录、用别名的 `client_bob.toml` |
| `examples/dev_ports` | 客户端开放 `5173-5200`，服务端原样映射到 `5173-5200` |
| `examples/udp` | 暴露 UDP 端口（`udp = [...]`） |
| `examples/tls` / `examples/noise_nk` | TLS / Noise 加密传输 |
| `examples/nginx` | 用 nginx 按子域名反代到各映射端口 |
| `examples/use_proxy` | 客户端经 socks5/http 代理连接服务器 |
| `examples/systemd` | Linux 上作为后台服务运行 |

## 映射目录与别名

每个客户端认证后都会收到全网映射目录，并在目录变化（有人上线、下线）时收到推送。客户端把目录打印成表格，同时为**其他用户**的每个在线 TCP 映射在本机开一个别名监听 `<alias_bind>:<远端端口>`（默认 `127.0.0.1`），把连接转发到 `服务器:<远端端口>`。

也就是说，bob 的机器上执行 `ssh -p 20000 127.0.0.1` 就能登录 alice 的 NAS，不需要记服务器地址。alice 下线时，bob 的表格立刻变成 `offline`，别名端口随之关闭。

```
USER         PROTO  LOCAL  REMOTE STATUS   ALIAS
alice        tcp       22   20000 online   127.0.0.1:20000
alice        tcp     8080   20001 offline  -
bob          tcp     3000   21000 online   -
```

注意：

- 别名只对 TCP 生效，UDP 映射不开别名。
- 自己的映射不开别名（走服务器回环没有意义）。
- 别名端口号与远端端口号相同，若客户端与服务端跑在同一台机器上会端口冲突，此时可以把 `alias_bind` 改成 `::1` 或其他本机地址。

## Configuration

如果只有一个 `[server]` 或 `[client]` 块存在，`rathole` 会根据配置文件自动决定运行模式。两个块也可以放在一个文件里，然后用 `rathole --server config.toml` / `rathole --client config.toml` 显式指定。

关于如何配置 Noise Protocol 和 TLS 来进行加密传输，参见 [Transport](./docs/transport.md)。

下面是完整的配置格式。

```toml
[client]
remote_addr = "example.com:2333" # 必填。服务器地址
user = "alice"                   # 必填。必须存在于服务端的 [server.users]
key = "alice_secret"             # 必填。用户密钥
alias_bind = "127.0.0.1"         # 可选。别名监听的本机地址。默认 "127.0.0.1"
prefer_ipv6 = false              # 可选。本地 UDP 套接字优先使用 IPv6。默认 false
nodelay = true                   # 可选。数据通道的 TCP_NODELAY。默认不修改
heartbeat_timeout = 40           # 可选。0 关闭应用层心跳检测，必须大于 server.heartbeat_interval。默认 40 秒
retry_interval = 1               # 可选。重连服务器的最大间隔。默认 1 秒

[client.transport] # 整块可选。指定传输层
type = "tcp" # 可选。可选值：["tcp", "tls", "noise", "websocket"]。默认 "tcp"

[client.transport.tcp] # 可选。同样影响 noise 和 tls
proxy = "socks5://user:passwd@127.0.0.1:1080" # 可选。连接服务器使用的代理，支持 http 和 socks5
nodelay = true          # 可选。控制通道的 TCP_NODELAY。默认 true
keepalive_secs = 20     # 可选。tcp(7) 的 tcp_keepalive_time。默认 20 秒
keepalive_interval = 8  # 可选。tcp(7) 的 tcp_keepalive_intvl。默认 8 秒

[client.transport.tls] # type 为 "tls" 时必填
trusted_root = "ca.pem"  # 必填。签发服务器证书的 CA 证书
hostname = "example.com" # 可选。校验证书用的主机名，缺省回退到 client.remote_addr

[client.transport.noise] # Noise 协议，见 docs/transport.md
pattern = "Noise_NK_25519_ChaChaPoly_BLAKE2s" # 可选。默认如上
local_private_key = "key_encoded_in_base64"   # 可选
remote_public_key = "key_encoded_in_base64"   # 可选

[client.transport.websocket] # type 为 "websocket" 时必填
tls = true # 为 true 时使用 client.transport.tls 的设置

[server]
bind_addr = "0.0.0.0:2333"      # 必填。监听客户端连接的地址。分配出去的远端端口也绑定在同一个地址上
expose_bind = "127.0.0.1"       # 可选。分配出去的远端端口监听的地址。默认与 bind_addr 同一主机
alloc_file = "allocations.toml" # 可选。远端端口分配表，相对本文件。默认 "allocations.toml"
nodelay = true                  # 可选。数据通道的 TCP_NODELAY。默认不修改
heartbeat_interval = 30         # 可选。应用层心跳间隔，0 关闭。默认 30 秒

[server.users.alice] # 每个用户一个表，表名即客户端的 user
key = "alice_secret"        # 必填。客户端用它认证
port_block = "20000-20999"  # 必填。远端端口块，"端口" 或 "起-止"。各用户的端口块不能重叠
tcp = ["22", "8000-8010"]   # 可选。要暴露的客户端本地 TCP 端口，每项为 "端口" 或 "起-止"。客户端连接到 127.0.0.1:<端口>
udp = ["5353"]              # 可选。同上，UDP

[server.nginx] # 可选。自动维护一份 nginx map 文件，见下文「nginx 子域名反代」
map_file = "rathole.map"       # 必填。相对本文件
domain = "example.com"         # 必填。生成的域名后缀
reload_cmd = "nginx -s reload" # 可选。文件变化后执行的命令

[server.transport] # 同 [client.transport]
type = "tcp"

[server.transport.tcp] # 同客户端
nodelay = true
keepalive_secs = 20
keepalive_interval = 8

[server.transport.tls] # type 为 "tls" 时必填
pkcs12 = "identify.pfx"     # 必填。服务器证书和私钥的 pkcs12 文件
pkcs12_password = "password" # 必填。pkcs12 文件密码

[server.transport.noise] # 同 [client.transport.noise]
pattern = "Noise_NK_25519_ChaChaPoly_BLAKE2s"
local_private_key = "key_encoded_in_base64"
remote_public_key = "key_encoded_in_base64"

[server.transport.websocket] # type 为 "websocket" 时必填
tls = true
```

### server.users

```toml
[server.users.alice]
key = "alice_secret"       # 必填。客户端用它认证
port_block = "20000-20999" # 必填。远端端口块，"端口" 或 "起-止"。各用户的端口块不能重叠
tcp = ["22"]               # 可选。要暴露的 alice 本机 TCP 端口
udp = []                   # 可选。UDP 同理

[server.users.bob]         # 不暴露任何端口，只看目录、用别名
key = "bob_secret"
port_block = "21000-21999"
```

暴露的端口数不能超过端口块大小，否则配置校验失败。

### nginx 子域名反代

配置 `[server.nginx]` 后，服务端会在映射目录变化时重写 `map_file`，每个 TCP 映射一行：

```
80-alice.example.com 20000;
8000-alice.example.com 20001;
```

nginx 把它 `include` 进一个 `map $host $rathole_port` 块，再用 `proxy_pass http://127.0.0.1:$rathole_port` 反代，泛域名 `*.example.com` 解析到服务器即可通过 `https://80-alice.example.com` 访问 alice 本机的 80 端口。完整片段见 `examples/nginx/nginx.conf`。

配合 `expose_bind = "127.0.0.1"` 可以让映射端口只在本机监听，外部流量必须经过 nginx。代价是其他客户端的别名功能无法再直连这些端口，因为别名转发的目标就是 `服务器:<远端端口>`。

只对 HTTP / WebSocket 有效；ssh 等原始 TCP 没有 Host 头，仍需按端口直连。

域名只占一级（`80-alice`），是为了落在 `*.example.com` 这类单级泛域名证书的覆盖范围内；Cloudflare 免费的 Universal SSL 不覆盖 `80.alice.example.com` 这种两级子域名。

### 端口分配规则

- 分配键是 `(用户, 协议, 本地端口)`，值是远端端口，全部保存在 `alloc_file` 里。
- 客户端认证后，服务端按该用户配置的本地端口升序处理：已有分配且仍在端口块内就沿用；否则取端口块内该用户最小的空闲端口。
- TCP 和 UDP 共用同一个端口块的号段，避免同一个号码同时给两个协议。
- 端口块用完时服务端拒绝注册，客户端会打印错误并按 `retry_interval` 重试（正常情况下配置校验已经拦住了这种情况）。
- 端口块与暴露范围完全一致时（如 `examples/dev_ports`），结果就是原样映射。

### 热重载

- 修改 `server.toml` / `client.toml` 会整体重启实例；服务端重启后所有客户端自动重连重注册，端口分配从 `alloc_file` 恢复，映射不变。

### Logging

`rathole` 使用环境变量控制日志级别，支持 `info`, `warn`, `error`, `debug`, `trace`：

```shell
RUST_LOG=error ./rathole config.toml
```

如果 `RUST_LOG` 不存在，默认的日志级别是 `info`。

### Tuning

rathole 默认启用 TCP_NODELAY。这能够减少延迟并使交互式应用受益，比如 RDP、Minecraft 服务器，但会减少一些带宽。如果带宽更重要，可以通过 `nodelay = false` 关闭。

## Build

```sh
cargo build --release                                   # 本机
cargo build --release --target x86_64-apple-darwin      # macOS x86_64（在 Apple Silicon 上）
cross build --release --target x86_64-unknown-linux-musl # Linux，需要 docker 和 cargo install cross
cross build --release --target x86_64-pc-windows-gnu     # Windows
```

更多平台与最小化二进制见 [build-guide](docs/build-guide.md)。

## Benchmark

rathole 的延迟与 [frp](https://github.com/fatedier/frp) 相近，在高并发情况下表现更好，能提供更大的带宽，内存占用更少。细节见 [Benchmark](./docs/benchmark.md)。

![http_throughput](./docs/img/http_throughput.svg)
![tcp_bitrate](./docs/img/tcp_bitrate.svg)
![udp_bitrate](./docs/img/udp_bitrate.svg)
![mem](./docs/img/mem-graph.png)

## Development Status

- [x] 用户级认证与端口块
- [x] 稳定端口分配（持久化）
- [x] 映射目录推送与本机别名
- [x] 配置热重载
- [x] TLS / Noise / WebSocket 传输
- [x] UDP
- [ ] TUN、Web 管理界面、多租户（不在计划内）
