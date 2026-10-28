# RFC 9221 QUIC Datagram Relay for UDP Proxies

## Overview

In traditional reverse proxy implementations, UDP packets forwarded over an encrypted tunnel are encapsulated inside reliable, ordered byte streams (such as TCP or QUIC streams). While reliable streams guarantee delivery, they introduce **Head-of-Line (HoL) Blocking**: if a single packet is lost in transit, all subsequent packets must wait in kernel and proxy buffers until the lost packet is retransmitted and acknowledged.

For latency-sensitive, real-time protocols such as **WireGuard**, **DTLS**, **DNS**, **VoIP/SIP**, and **game servers**, packet retransmissions cause catastrophic latency spikes and jitter.

This fork introduces native support for **RFC 9221 QUIC Datagrams**, allowing UDP packets to be relayed across WANs as independent, unreliable, unordered datagrams directly within the QUIC transport connection.

---

## How It Works

1. **Transport Negotiation:** When `transport.protocol = "quic"` is configured between `frpc` and `frps`, both peers negotiate QUIC datagram support (`quic.enableDatagrams = true`, enabled by default).
2. **Per-Proxy Opt-In:** Datagram relay is disabled by default (`quicDatagrams = false`). Configuring `transport.protocol = "quic"` alone does not change proxy behavior; each UDP proxy must explicitly set `quicDatagrams = true` to route traffic over datagrams. Without this flag, UDP proxies continue to forward packets through reliable QUIC streams.
3. **Datagram Framing:** Each UDP packet is framed into an RFC 9221 datagram containing:
   - 1-byte wire identifier length
   - Wire-level proxy identifier
   - User connection / remote address encoding
   - Raw UDP payload
4. **Multiplexing via `DatagramMux`:** The datagram multiplexer inspects the proxy identifier upon receipt and dispatches the packet directly to the destination UDP flow without stream state or memory copying.

---

## Datagram Sizing & MTU Boundaries

QUIC datagrams are subject to the path Maximum Transmission Unit (PMTU) and the QUIC packet overhead, so every relayed packet is framed into a small binary header before it is sent:

```
[0]        version                       1 byte
[1]        len(proxy name)               1 byte
[2:2+n]    proxy name                    n bytes
[+0]       len(user address)             1 byte
[+1:+1+a]  user address ("ip:port")      a bytes
[rest]     raw UDP payload               p bytes
```

The whole frame has to fit into one QUIC packet, so frp caps it at **1180 bytes** (`DatagramFrameBudget` in `pkg/proto/udp/datagram.go`). The cap covers the header and the user address, so the **largest UDP payload that can be relayed as a datagram is smaller than 1180 bytes**:

```
max payload = 1180 - 3 - len(proxy name) - len("ip:port" address)
```

Worked examples:

| proxy name | user address | max UDP payload |
|------------|--------------|-----------------|
| `game` (4 bytes) | `10.0.0.7:40000` (14 bytes) | 1159 bytes |
| `dns-datagram` (12 bytes) | `192.168.1.100:51234` (19 bytes) | 1146 bytes |
| `game` (4 bytes) | `[2001:db8::1]:40000` (18 bytes) | 1155 bytes |

In practice the usable payload is roughly **1130–1165 bytes**. The number varies per packet, because the user address is part of every frame and its length depends on the address family and the source port. frp computes the exact limit for the packet it is about to send, so the boundary is never crossed by accident.

If the path itself is smaller than the frame budget (a tunnel with an MTU below 1200 bytes for example), the QUIC connection reports a lower limit and the same fallback happens for the packets above it.

### Fallback vs. Strict Mode (`quicDatagramStrict`)

When an incoming UDP packet exceeds the maximum datagram payload size:

1. **Default Mode (`quicDatagramStrict = false`):**
   - The oversized packet falls back to forwarding over the reliable QUIC stream work connection.
   - Ideal for general UDP applications that occasionally send large bursts (e.g., DNS queries with large EDNS0 responses).

2. **Strict Mode (`quicDatagramStrict = true`):**
   - The oversized packet is **dropped** rather than diverted to the reliable stream.
   - **Why Strict Mode Matters for VPNs & Replay-Sensitive Traffic:**
     Protocols like **WireGuard**, **IPsec**, and **DTLS** implement anti-replay sliding windows. If a sequence of packets is split between the unreliable datagram lane (low latency, immediate delivery) and the reliable stream lane (retransmitted, queued), the stream-delayed packets will arrive far behind the datagram packets. This causes the anti-replay window to advance past the delayed packets, resulting in spurious replay rejections and tunnel stalls.
   - Enabling `quicDatagramStrict = true` guarantees strict lane isolation: packets never experience dual-lane reordering.

Both modes report the event: `frpc` and `frps` log a warning that names the size of the packet and the limit that was reached, at most once per minute per proxy, so a stream of oversized packets cannot flood the log. The warning is the only place where the fallback is visible, so it is worth watching after a deployment.

---

## Configuration

QUIC datagram relay requires both QUIC transport on the client-server connection and an explicit opt-in on each UDP proxy:

- **Transport:** Set `transport.protocol = "quic"` in `frpc.toml`.
- **Server:** Set `quicBindPort` in `frps.toml` to the UDP port the client connects to. QUIC is disabled without it.
- **Proxy:** Set `quicDatagrams = true` on each UDP proxy. If omitted (or set to `false`), the proxy continues using reliable QUIC streams.
- **Constraints:** Because datagram forwarding bypasses stream-level framing, `quicDatagrams = true` cannot be combined with `transport.useEncryption` or `transport.useCompression` on the same proxy.

### Client Configuration (`frpc.toml`)

```toml
serverAddr = "x.x.x.x"
serverPort = 7000

# Set transport protocol to QUIC
transport.protocol = "quic"

# UDP Proxy with QUIC Datagram Relay
[[proxies]]
name = "dns-datagram"
type = "udp"
localIP = "127.0.0.1"
localPort = 53
remotePort = 5353
quicDatagrams = true

# Replay-Sensitive Tunnel (WireGuard / DTLS) with Strict Mode
[[proxies]]
name = "wireguard-tunnel"
type = "udp"
localIP = "127.0.0.1"
localPort = 51820
remotePort = 51820
quicDatagrams = true
quicDatagramStrict = true
```

### Server Configuration (`frps.toml`)

```toml
bindPort = 7000
# QUIC is a separate UDP listener. It is disabled unless quicBindPort is set,
# and it can use the same port number as bindPort, because bindPort is a TCP
# port.
quicBindPort = 7000
transport.quic.enableDatagrams = true
```

The client always dials QUIC over UDP on the port it is configured with (`serverPort` in `frpc.toml`), so `quicBindPort` has to be the port the client uses — usually the same number as `bindPort`. When `quicBindPort` is `0`, `frps` does not listen for QUIC at all: a client with `transport.protocol = "quic"` then fails to connect, and `frps` says so in its startup log. The client names the same setting in the error it reports, because a missing QUIC listener is otherwise indistinguishable from a broken path.

---

## Operational Best Practices

1. **WireGuard MTU Tuning:** A WireGuard packet adds 32 bytes to the tunneled IP packet (16-byte message header plus the 16-byte Poly1305 tag), and the relay header takes another 20 to 40 bytes of the datagram frame. With a usable payload of about 1145 bytes, the WireGuard interface MTU has to stay below 1115 bytes: configure `MTU = 1100` (e.g. `MTU = 1100` in `wg0.conf`). A larger MTU produces packets that do not fit into the datagram lane, so every one of them falls back to the reliable stream or is dropped in strict mode.
2. **Watch the oversize warning:** `frpc` and `frps` log a warning when a packet does not fit into the datagram lane. A steady stream of those warnings means that the tunneled traffic carries packets that are too large, and the fix is a smaller MTU on the tunneled interface rather than a larger limit in frp.
3. **Buffer Sizing:** This fork uses 1024-packet non-blocking drop-on-saturation worker queues for UDP datagrams, conforming strictly to RFC 768 semantics under burst loads.