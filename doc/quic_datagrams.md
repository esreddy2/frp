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

QUIC datagrams are subject to the path Maximum Transmission Unit (PMTU) and the QUIC packet overhead:
- The maximum datagram payload size supported by frp is **1200 bytes** (`MaxQuicDatagramSize = 1200`), chosen to reliably avoid fragmentation across standard Internet paths with a 1280-byte IPv6 minimum MTU.
- Accounted for framing headers (identifier, addresses), the maximum single UDP packet payload relayed via datagram is approximately **1140–1180 bytes**.

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

1. **WireGuard MTU Tuning:** When tunneling WireGuard over FRP QUIC datagrams, configure the WireGuard interface MTU to `1180` (e.g. `MTU = 1180` in `wg0.conf`). This ensures all encrypted WireGuard packets fit cleanly within the QUIC datagram budget without triggering fragmentation or drops.
2. **Buffer Sizing:** This fork uses 1024-packet non-blocking drop-on-saturation worker queues for UDP datagrams, conforming strictly to RFC 768 semantics under burst loads.