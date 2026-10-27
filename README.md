# frp (Performance & Optimization Fork)

[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

[README](README.md) | [中文文档](README_zh.md)

This repository is a personal fork of [fatedier/frp](https://github.com/fatedier/frp) (based on `v0.71.1`) focused on performance optimizations, low-latency UDP tunneling, and protocol enhancements. It uses identical configuration syntax and options as upstream frp, so existing configuration files work directly.

---

## Table of Contents

- [Highlights & Enhancements](#highlights--enhancements)
- [Quick Start](#quick-start)
  - [1. Server (`frps`)](#1-server-frps)
  - [2. Client (`frpc`)](#2-client-frpc)
- [Advanced Features & Configuration](#advanced-features--configuration)
  - [1. Linux Transparent Proxying (`tproxy`)](#1-linux-transparent-proxying-tproxy)
  - [2. RFC 9221 QUIC Datagram Relay](#2-rfc-9221-quic-datagram-relay)
  - [3. Per-Packet PROXY Protocol for UDP](#3-per-packet-proxy-protocol-for-udp)
  - [4. Wire Protocol v2 & AEAD Encryption](#4-wire-protocol-v2--aead-encryption)
  - [5. Two-Phase Server Plugin Admission (`NewProxyResult`)](#5-two-phase-server-plugin-admission-newproxyresult)
  - [6. Dedicated Observability Listener (`metricsServer`)](#6-dedicated-observability-listener-metricsserver)
- [Concurrency & Reliability Hardening](#concurrency--reliability-hardening)
- [Documentation Guides](#documentation-guides)
- [Configuration Compatibility](#configuration-compatibility)
- [License](#license)

---

## Highlights & Enhancements

* **True Client IP Spoofing (Linux TPROXY):** Exposes authentic remote client IP and port addresses to backend services via Linux `IP_TRANSPARENT`. Eliminates application modifications or L7 PROXY protocol parsing on backend servers. Includes automatic ephemeral port fallback on local port collision.
* **RFC 9221 QUIC Datagram Relay:** Relays UDP packets as independent, out-of-order QUIC datagrams, completely eliminating Head-of-Line (HoL) blocking. Features optional strict lane isolation (`quicDatagramStrict = true`) for **WireGuard**, **IPsec**, and **DTLS** to preserve anti-replay windows.
* **Per-Packet PROXY Protocol:** UDP proxies support `transport.proxyProtocolMode = "per-packet"` alongside `"first-packet"`, enabling stateless load balancing with HAProxy (`mode udp`) and NGINX Stream.
* **Wire Protocol v2 with Hardware AEAD:** Control channels support ephemeral X25519 ECDH key agreement with HKDF-SHA256 transcript binding and hardware-accelerated AES-256-GCM / XChaCha20-Poly1305 AEAD framing. Enforceable via `transport.requireKeyShare = true`.
* **Zero-Allocation Latency Optimizations:** Enabled `TCP_NODELAY` across client local dials and control connections to eliminate 40ms Nagle delays; zero-allocation v2 message dispatch table; timer and ticker allocation reuse.
* **Jitter-Immune Network Deadlines:** Quarter-window `IdleReadDeadline` refresh mathematically eliminates spurious connection drops caused by timer jitter or Go GC pauses under continuous traffic.
* **Non-Blocking UDP Pipelines:** Worker queues utilize 1024-packet drop-on-saturation buffers adhering strictly to RFC 768 UDP semantics; active flow sockets are closed immediately on forwarder stop without leaking ephemeral ports.
* **Two-Phase Server Plugin Pipeline:** Server plugins can participate in atomic proxy creation transactions via `NewProxyResult` with immutable 128-bit `attempt_id` correlation and a high-capacity 2048-entry non-blocking `CloseProxy` FIFO.
* **Programmatic Embedding & System CA:** `ServiceOptions.OnFirstLoginSuccess` hook to inspect and veto authenticated sessions before proxy registration; sanitized error logging for Unix domain socket paths; `useSystemCa` supported across all TLS transports including QUIC and WSS.

---

## Quick Start

### 1. Server (`frps`)

Create `frps.toml`:

```toml
bindPort = 7000
vhostHTTPPort = 80
vhostHTTPSPort = 443

auth.method = "token"
auth.token = "your_secure_cluster_token"

# Enable TCP stream multiplexing
transport.tcpMux = true

# Optional: enable Wire Protocol v2 with mandatory key agreement
transport.wireProtocol = "v2"
transport.requireKeyShare = true
```

Start the server:
```bash
./frps -c frps.toml
```

### 2. Client (`frpc`)

Create `frpc.toml`:

```toml
serverAddr = "x.x.x.x"
serverPort = 7000

auth.method = "token"
auth.token = "your_secure_cluster_token"

transport.tcpMux = true
transport.wireProtocol = "v2"
transport.requireKeyShare = true

# Standard TCP proxy
[[proxies]]
name = "ssh-internal"
type = "tcp"
localIP = "127.0.0.1"
localPort = 22
remotePort = 6000
```

Start the client:
```bash
./frpc -c frpc.toml
```

---

## Advanced Features & Configuration

### 1. Linux Transparent Proxying (`tproxy`)

To deliver genuine client IP addresses directly to local backend applications without PROXY protocol headers:

```toml
# frpc.toml
[[proxies]]
name = "web-tproxy"
type = "tcp"
localIP = "127.0.0.1"
localPort = 8080
remotePort = 80
transport.proxyProtocol = "tproxy"
transport.socketMark = 1 # Linux fwmark, defaults to 1
```

**Host Setup on Linux:**
```bash
# Grant networking capabilities to frpc binary (or run via systemd AmbientCapabilities)
sudo setcap cap_net_admin,cap_net_raw+ep $(which frpc)

# Enable routing sysctls
sudo sysctl -w net.ipv4.ip_forward=1
sudo sysctl -w net.ipv4.conf.all.route_localnet=1

# Configure policy routing for the socket mark
sudo ip rule add fwmark 1 lookup 100
sudo ip route add local 0.0.0.0/0 dev lo table 100
```
For complete details, see the [Transparent Proxying Guide](doc/transparent_proxying.md).

---

### 2. RFC 9221 QUIC Datagram Relay

For real-time UDP applications (gaming, VoIP, DNS, and VPNs):

```toml
# frpc.toml
serverAddr = "x.x.x.x"
serverPort = 7000
transport.protocol = "quic"

# Low-latency UDP proxy with datagram relay
[[proxies]]
name = "dns-udp"
type = "udp"
localIP = "127.0.0.1"
localPort = 53
remotePort = 5353
quicDatagrams = true

# WireGuard / DTLS with strict lane isolation (drops oversized packets to prevent replay-window degradation)
[[proxies]]
name = "wireguard"
type = "udp"
localIP = "127.0.0.1"
localPort = 51820
remotePort = 51820
quicDatagrams = true
quicDatagramStrict = true
```
For complete details, see the [QUIC Datagrams Guide](doc/quic_datagrams.md).

---

### 3. Per-Packet PROXY Protocol for UDP

For stateless upstream load balancers requiring PROXY headers on every packet:

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

### 4. Wire Protocol v2 & AEAD Encryption

Secure control channels with ephemeral X25519 ECDH key agreement and hardware AEAD:

```toml
# In both frps.toml and frpc.toml
[transport]
wireProtocol = "v2"
requireKeyShare = true
```
For cryptographic design and transcript hashing details, see the [Wire Protocol v2 Guide](doc/wire_protocol_v2.md).

---

### 5. Two-Phase Server Plugin Admission (`NewProxyResult`)

Server plugins can inspect and commit proxy creations within a synchronous transaction:
- `NewProxy`: Plugin prepares external routing or quota state.
- `NewProxyResult`: Receives the immutable `attempt_id` and boolean `admitted` status. Returning `reject: true` rolls back proxy publication.
- `CloseProxy`: Delivered via a bounded 2048-entry non-blocking FIFO with exponential backoff retries.

For complete schema and implementation details, see [Server Plugin Documentation](doc/server_plugin.md).

---

### 6. Dedicated Observability Listener (`metricsServer`)

Isolate health checks, Prometheus metrics, and profiling from public admin dashboards:

```toml
# In frps.toml or frpc.toml
[metricsServer]
addr = "127.0.0.1"
port = 9100
user = "admin"
password = "metrics_secret_password"
pprofEnable = true
```
Scrape metrics at `http://127.0.0.1:9100/metrics` and probe health at `http://127.0.0.1:9100/healthz`.

---

## Concurrency & Reliability Hardening

This fork incorporates deep runtime concurrency auditing and fixes:

| Area | Challenge in Upstream FRP | Solution in This Fork |
| :--- | :--- | :--- |
| **Deadlines** | Half-window ($T/2$) deadline refresh collapses under scheduler jitter or GC pauses. | Quarter-window ($3/4\,T$) refresh mathematically guarantees jitter immunity. |
| **Worker Channels** | Full channels block UDP/SUDP read workers indefinitely. | Non-blocking drop-on-saturation 1024-packet buffers (RFC 768). |
| **Socket Teardown** | Ephemeral UDP sockets lingered until 30s read timeout on forwarder stop. | Synchronous socket closure immediately unblocks kernel reads and frees ports. |
| **Timer Heaps** | Unbounded `time.After` allocations under rapid connection churn. | Stopped timers (`timer.Stop()`) prevent timer struct accumulation in GC heaps. |
| **Buffer Safety** | Frame buffer pool aliasing with asynchronous or buffered writers. | Local MTU slice allocation eliminates use-after-free and memory corruption risks. |

---

## Documentation Guides

- [Linux Transparent Proxying (TPROXY)](doc/transparent_proxying.md) — Kernel setup, policy routing, and client IP spoofing.
- [RFC 9221 QUIC Datagram Relay](doc/quic_datagrams.md) — Unordered UDP forwarding, sizing bounds, and strict mode.
- [Wire Protocol v2](doc/wire_protocol_v2.md) — Cryptographic architecture, X25519 key exchange, and AEAD framing.
- [Server Plugins](doc/server_plugin.md) — Two-phase admission control, attempt IDs, and lifecycle webhooks.
- [Virtual Network (TUN)](doc/virtual_net.md) — IP-level virtual networking over TUN interfaces.
- [SSH Tunnel Gateway](doc/ssh_tunnel_gateway.md) — Connecting via standard SSH clients.
- [Deprecations & Migration Policy](doc/deprecations.md) — API and protocol lifecycle tracking.

---

## Configuration Compatibility

This fork maintains full configuration compatibility with upstream `fatedier/frp` (v0.71.1). Existing `.toml`, `.ini`, `.yaml`, and `.json` configuration files can be used without modification.

---

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
