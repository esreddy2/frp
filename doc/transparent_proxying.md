# Linux Transparent Proxying (TPROXY) & Client IP Spoofing

## Overview

In standard reverse proxying, the backend service sees incoming connections as originating from the reverse proxy (`frpc` IP address) rather than the genuine remote client. While the PROXY protocol (v1/v2) can convey the original client IP to the backend, it requires explicit support and modification in the backend application or web server.

This fork provides native support for **Linux Transparent Proxying (`IP_TRANSPARENT`)**, allowing `frpc` to spoof the remote client's genuine IP address and port when establishing local connections to backend services. The backend receives connections directly from the original client's IP address without requiring any application-level protocol support or code changes.

Both **TCP** and **UDP** proxies are supported.

---

## How It Works

1. **Client to Server:** The remote client connects to `frps`, which negotiates the proxy connection.
2. **Control & Data Forwarding:** `frps` forwards the client connection and metadata (including the client's public IP and port) to `frpc`.
3. **Transparent Local Dial:** When `transport.proxyProtocol = "tproxy"` is configured:
   - `frpc` creates a local socket with the `IP_TRANSPARENT` (`SO_MARK` / `SOL_IP`) socket option.
   - It binds the local socket to the original client's source IP and port before connecting to the backend service.
   - It marks outgoing packets with `SO_MARK` (by default `0x01` / `1`).
4. **Backend Reception:** The backend service accepts the connection and inspects `RemoteAddr`, observing the genuine client's IP and port.
5. **Return Traffic Routing:** When the backend replies, its packets are addressed to the client IP. Linux policy routing (`ip rule` and `ip route`) ensures response packets are routed back to loopback so `frpc` can read and return them through the tunnel.

### Source Port Collision Handling
If a local connection is already using the exact same `(ClientIP, ClientPort)` tuple, the Linux kernel rejects binding with `EADDRINUSE`. The client detects this condition and automatically falls back to binding to `(ClientIP, 0)`—allowing the kernel to assign an ephemeral source port while still preserving the authentic client IP address.

---

## Configuration

In `frpc.toml`, set `transport.proxyProtocol = "tproxy"`:

```toml
# frpc.toml
serverAddr = "x.x.x.x"
serverPort = 7000

[[proxies]]
name = "web-transparent"
type = "tcp"
localIP = "127.0.0.1"
localPort = 8080
remotePort = 8000
transport.proxyProtocol = "tproxy"
# Optional: specify socket mark (fwmark). Defaults to 1 if omitted.
transport.socketMark = 1
```

For UDP services (e.g. DNS, Game Servers, Syslog):

```toml
[[proxies]]
name = "dns-transparent"
type = "udp"
localIP = "127.0.0.1"
localPort = 53
remotePort = 5353
transport.proxyProtocol = "tproxy"
transport.socketMark = 1
```

---

## Linux Host Setup & Policy Routing

To allow `frpc` to bind to non-local IP addresses and route response traffic properly, configure the Linux kernel and routing table:

### 1. Enable Routing Sysctls
```bash
sudo sysctl -w net.ipv4.ip_forward=1
sudo sysctl -w net.ipv4.conf.all.route_localnet=1
sudo sysctl -w net.ipv4.conf.default.route_localnet=1
```
To persist across reboots, add to `/etc/sysctl.d/99-frp-tproxy.conf`:
```ini
net.ipv4.ip_forward = 1
net.ipv4.conf.all.route_localnet = 1
net.ipv4.conf.default.route_localnet = 1
```

### 2. Configure Policy Routing (`ip rule` & `ip route`)
Route packets tagged with socket mark `1` to the local loopback interface:

```bash
# Create policy rule for fwmark 1 looking up table 100
sudo ip rule add fwmark 1 lookup 100
sudo ip route add local 0.0.0.0/0 dev lo table 100

# For IPv6 (if using IPv6 client addresses):
sudo ip -6 rule add fwmark 1 lookup 100
sudo ip -6 route add local ::/0 dev lo table 100
```

To persist policy routing across reboots, add them to your network manager configuration (e.g., `systemd-networkd`, `/etc/network/interfaces`, or a network startup script).

---

## Capabilities & Permissions

Setting `IP_TRANSPARENT` and `SO_MARK` requires the `CAP_NET_ADMIN` capability. You can run `frpc` as a non-root user by granting file capabilities:

```bash
sudo setcap cap_net_admin,cap_net_raw+ep /usr/local/bin/frpc
```

### Systemd Service Example (Non-Root)

```ini
[Unit]
Description=FRP Client with TPROXY
After=network.target

[Service]
Type=simple
User=frp
Group=frp
ExecStart=/usr/local/bin/frpc -c /etc/frp/frpc.toml
Restart=on-failure
RestartSec=5s
LimitNOFILE=65536
CapabilityBoundingSet=CAP_NET_ADMIN CAP_NET_RAW
AmbientCapabilities=CAP_NET_ADMIN CAP_NET_RAW

[Install]
WantedBy=multi-user.target
```

---

## Troubleshooting & Diagnostics

Pre-flight checks provide actionable diagnostic error messages when TPROXY cannot initialize:

| Error / Symptom | Probable Cause | Action |
| :--- | :--- | :--- |
| `operation not permitted` (`EPERM`) | Missing `CAP_NET_ADMIN` capability | Grant capabilities: `sudo setcap cap_net_admin,cap_net_raw+ep $(which frpc)` or run with adequate permissions. |
| `cannot assign requested address` (`EADDRNOTAVAIL`) | `IP_TRANSPARENT` not active or disabled by kernel | Verify kernel supports `IP_TRANSPARENT` (standard in Linux 2.6.28+). Check `sysctl net.ipv4.ip_forward=1`. |
| Connection times out / SYN-ACK not received | Response routing loop or missing local table route | Ensure policy route exists: `ip rule show` and `ip route show table 100`. Verify `sysctl net.ipv4.conf.all.route_localnet=1`. |
| Proxy validation error on startup | Mutually exclusive proxy options configured | `transport.proxyProtocol = "tproxy"` cannot be combined with `transport.proxyProtocolMode` (which is only for PROXY protocol headers). |
