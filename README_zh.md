# frp (性能与优化增强版)

[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

[README](README.md) | [中文文档](README_zh.md)

本项目是基于 [fatedier/frp](https://github.com/fatedier/frp)（基于 `v0.71.1`）的个人优化与增强分支，专注于网络性能优化、低延迟 UDP 隧道及协议增强。配置格式与选项与官方 frp 保持一致，现有的配置文件可直接使用。

---

## 目录

- [核心特性与功能增强](#核心特性与功能增强)
- [快速入门](#快速入门)
  - [1. 服务端 (`frps`)](#1-服务端-frps)
  - [2. 客户端 (`frpc`)](#2-客户端-frpc)
- [增强特性与配置示例](#增强特性与配置示例)
  - [1. Linux 透明代理与真实客户端 IP 保持 (`tproxy`)](#1-linux-透明代理与真实客户端-ip-保持-tproxy)
  - [2. RFC 9221 QUIC 数据报中继 (`quicDatagrams`)](#2-rfc-9221-quic-数据报中继-quicdatagrams)
  - [3. UDP 每包 PROXY Protocol 模式 (`per-packet`)](#3-udp-每包-proxy-protocol-模式-per-packet)
  - [4. Wire Protocol v2 与硬件级 AEAD 加密](#4-wire-protocol-v2-与硬件级-aead-加密)
  - [5. 服务端插件两阶段准入确认 (`NewProxyResult`)](#5-服务端插件两阶段准入确认-newproxyresult)
  - [6. 独立监控与可观测性端口 (`metricsServer`)](#6-独立监控与可观测性端口-metricsserver)
- [高并发与稳定性加固](#高并发与稳定性加固)
- [文档索引](#文档索引)
- [配置兼容性](#配置兼容性)
- [开源协议](#开源协议)

---

## 核心特性与功能增强

* **真实客户端 IP 伪装（Linux TPROXY）：** 借助 Linux 内核 `IP_TRANSPARENT` 特性，使后端真实服务直接看到来访客户端的真实源 IP 与源端口。无需后端应用支持或解析应用层 PROXY Protocol 协议头。原生具备本地端口冲突自动回退机制（保留客户端真实 IP，使用临时端口）。
* **RFC 9221 QUIC 数据报中继：** 支持将 UDP 数据包作为不可靠、乱序到达的独立 QUIC Datagram 进行转发，彻底消除队头阻塞（Head-of-Line Blocking）。具备严格通道隔离模式（`quicDatagramStrict = true`），超大包直接丢弃而不退化为可靠流，杜绝流与数据报交叉乱序，有效保护 **WireGuard**、**IPsec** 与 **DTLS** 的抗重放滑动窗口。
* **UDP 每数据包 PROXY Protocol 模式：** UDP 代理除了传统的首包模式外，支持 `transport.proxyProtocolMode = "per-packet"`，完美兼容无状态四层负载均衡器（如 HAProxy `mode udp` 或 NGINX Stream）。
* **Wire Protocol v2 与硬件加速 AEAD：** 控制通道支持临时 X25519 ECDH 密钥协商与基于 HKDF-SHA256 的握手转录哈希绑定，通信帧采用硬件加速的 AES-256-GCM 或 XChaCha20-Poly1305 AEAD 加密。可配置 `transport.requireKeyShare = true` 强制开启前向安全。
* **零内存分配低延迟优化：** 客户端与服务端连接默认开启 `TCP_NODELAY`，彻底消除 40ms Nagle 算法造成的延迟停顿；v2 消息派发采用预构建构造表代替反射；优化计时器分配，减少 GC 压力。
* **网络超时抗抖动（Jitter-Immune Deadlines）：** 将原有的半窗口（$T/2$）保活超时刷新阈值修正为四分之一窗口（$3/4\,T$），从机制上消除因调度抖动或 Go GC 暂停导致的误断开。
* **无阻塞 UDP 队列设计：** UDP/SUDP 读写工作通道采用 1024 包饱和丢弃缓冲队列，严格符合 RFC 768 UDP 协议语义；代理停止时同步关闭活动 UDP 连接，立即释放内核临时端口与读阻塞。
* **两阶段服务端插件准入机制：** 新增 `NewProxyResult` 插件生命周期操作，配合不可变 128 位 `attempt_id` 事务关联；异步 `CloseProxy` 通知队列容量扩容至 2048，并引入指数退避重试与严格请求超时。
* **代码级嵌入与系统 CA 支持：** 提供 `ServiceOptions.OnFirstLoginSuccess` 钩子用于在代理注册前对客户端会话进行鉴权校验；支持在所有 TLS 传输协议（包括 QUIC 和 WSS）中继承系统根证书库并与自定义 CA 结合；Unix 域套接字错误日志脱敏，防止泄露本地路径。

---

## 快速入门

### 1. 服务端 (`frps`)

创建 `frps.toml`：

```toml
bindPort = 7000
vhostHTTPPort = 80
vhostHTTPSPort = 443

auth.method = "token"
auth.token = "your_secure_cluster_token"

# 启用 TCP 多路复用
transport.tcpMux = true

# 可选：启用 Wire Protocol v2 并强制密钥协商
transport.wireProtocol = "v2"
transport.requireKeyShare = true
```

启动服务端：
```bash
./frps -c frps.toml
```

### 2. 客户端 (`frpc`)

创建 `frpc.toml`：

```toml
serverAddr = "x.x.x.x"
serverPort = 7000

auth.method = "token"
auth.token = "your_secure_cluster_token"

transport.tcpMux = true
transport.wireProtocol = "v2"
transport.requireKeyShare = true

# 标准 TCP 代理示例
[[proxies]]
name = "ssh-internal"
type = "tcp"
localIP = "127.0.0.1"
localPort = 22
remotePort = 6000
```

启动客户端：
```bash
./frpc -c frpc.toml
```

---

## 企业级功能与配置示例

### 1. Linux 透明代理与真实客户端 IP 保持 (`tproxy`)

无需应用层协议支持，使后端应用直接获取客户端真实源 IP：

```toml
# frpc.toml
[[proxies]]
name = "web-tproxy"
type = "tcp"
localIP = "127.0.0.1"
localPort = 8080
remotePort = 80
transport.proxyProtocol = "tproxy"
transport.socketMark = 1 # Linux fwmark 策略路由标记，默认值为 1
```

**Linux 主机网络配置：**
```bash
# 赋予 frpc 网络管理能力（非 root 运行支持）
sudo setcap cap_net_admin,cap_net_raw+ep $(which frpc)

# 开启内核转发与本地路由
sudo sysctl -w net.ipv4.ip_forward=1
sudo sysctl -w net.ipv4.conf.all.route_localnet=1

# 添加策略路由规则，将标记流量路由至本地环回
sudo ip rule add fwmark 1 lookup 100
sudo ip route add local 0.0.0.0/0 dev lo table 100
```
详细指南请参阅 [透明代理配置文档 (doc/transparent_proxying.md)](doc/transparent_proxying.md)。

---

### 2. RFC 9221 QUIC 数据报中继 (`quicDatagrams`)

适用于游戏、VoIP、DNS 及 VPN 穿透等极度敏感于延迟的场景：

```toml
# frpc.toml
serverAddr = "x.x.x.x"
serverPort = 7000
transport.protocol = "quic"

# 低延迟 DNS UDP 代理
[[proxies]]
name = "dns-udp"
type = "udp"
localIP = "127.0.0.1"
localPort = 53
remotePort = 5353
quicDatagrams = true

# WireGuard / DTLS 严格通道隔离模式（杜绝流/数据报交叉导致重放窗口失效）
[[proxies]]
name = "wireguard"
type = "udp"
localIP = "127.0.0.1"
localPort = 51820
remotePort = 51820
quicDatagrams = true
quicDatagramStrict = true
```
详细指南请参阅 [QUIC 数据报中继文档 (doc/quic_datagrams.md)](doc/quic_datagrams.md)。

---

### 3. UDP 每包 PROXY Protocol 模式 (`per-packet`)

适配需要每包解析 PROXY Protocol 头的无状态 UDP 负载均衡集群：

```toml
# frpc.toml
[[proxies]]
name = "udp-lb"
type = "udp"
localIP = "127.0.0.1"
localPort = 5000
remotePort = 5000
transport.proxyProtocol = "v2"
transport.proxyProtocolMode = "per-packet"
```

---

### 4. Wire Protocol v2 与硬件级 AEAD 加密

为控制信道提供现代密码学保护：

```toml
# 同时在 frps.toml 与 frpc.toml 中配置
[transport]
wireProtocol = "v2"
requireKeyShare = true
```
详细设计请参阅 [Wire Protocol v2 说明文档 (doc/wire_protocol_v2.md)](doc/wire_protocol_v2.md)。

---

### 5. 服务端插件两阶段准入确认 (`NewProxyResult`)

支持通过服务端 HTTP 插件实现准入事务联动：
- `NewProxy`: 插件准备外部路由或配额。
- `NewProxyResult`: 携带不可变 `attempt_id` 与准入状态 `admitted`，返回拒绝将撤回代理。
- `CloseProxy`: 扩容至 2048 容量的非阻塞 FIFO 队列投递，支持指数退避重试。

详细说明请参阅 [服务端插件文档 (doc/server_plugin.md)](doc/server_plugin.md)。

---

### 6. 独立监控与可观测性端口 (`metricsServer`)

将 Prometheus 指标、健康检查与性能分析接口与管理仪表板隔离：

```toml
# 在 frps.toml 或 frpc.toml 中配置
[metricsServer]
addr = "127.0.0.1"
port = 9100
user = "admin"
password = "metrics_secret_password"
pprofEnable = true
```

---

## 高并发与稳定性加固

| 场景 | 上游高负载挑战 | 本分支解决方案 |
| :--- | :--- | :--- |
| **保活超时** | 半窗口（$T/2$）阈值在轻微网络抖动或 GC 暂停下易产生误断开 | 修正为四分之一窗口（$3/4\,T$）刷新，数学上消除抖动塌陷 |
| **读工作协程** | 通道写满时可能造成读工作协程阻塞无法继续处理流量 | 采用 1024 容量非阻塞丢弃缓冲区，符合 RFC 768 规范 |
| **套接字释放** | 转发器停止时连接需等待 30 秒超时才能释放 | 停止信号触发时立即同步关闭底层连接，瞬间释放临时端口 |
| **堆内存开销** | 频繁连接创建产生大量未释放的 `time.After` 计时器 | 采用显式可停止计时器（`timer.Stop()`），大幅降低 GC 压力 |
| **内存安全** | 缓冲写入器使用池化缓冲区时存在潜在内存复用别名风险 | 限制在单次本地切片分配内，消除 Use-after-free 风险 |

---

## 文档索引

- [Linux 透明代理 (TPROXY) 配置指南 (doc/transparent_proxying.md)](doc/transparent_proxying.md)
- [RFC 9221 QUIC 数据报中继指南 (doc/quic_datagrams.md)](doc/quic_datagrams.md)
- [Wire Protocol v2 密码学设计 (doc/wire_protocol_v2.md)](doc/wire_protocol_v2.md)
- [服务端插件两阶段确认 (doc/server_plugin.md)](doc/server_plugin.md)
- [虚拟网络 TUN 配置 (doc/virtual_net.md)](doc/virtual_net.md)
- [SSH 隧道网关指南 (doc/ssh_tunnel_gateway.md)](doc/ssh_tunnel_gateway.md)
- [弃用特性生命周期追踪 (doc/deprecations.md)](doc/deprecations.md)

---

## 配置兼容性

本分支与官方 upstream [fatedier/frp](https://github.com/fatedier/frp)（`v0.71.1`）保持完全的配置兼容。现有的 `.ini`、`.toml`、`.yaml` 及 `.json` 格式配置文件均可直接使用，无需修改任何历史配置。

---

## 开源协议

遵循 [Apache 2.0 开源协议](LICENSE)。
