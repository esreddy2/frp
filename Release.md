# FRP Optimization Fork Release Notes (v0.71.1-fork)

This repository is a personal fork of [fatedier/frp](https://github.com/fatedier/frp) (based on `v0.71.1`) focused on performance optimizations, low-latency UDP tunneling, and protocol enhancements. Configuration format and options remain identical to upstream frp.

---

## Highlights & Major Features

### Linux Transparent Proxying (TPROXY) & True Client IP Spoofing
- **Client IP & Port Preservation:** Local connections dial backend services using Linux `IP_TRANSPARENT`, allowing backend services to see authentic client source IPs and ports without requiring application-layer PROXY protocol headers or application modifications.
- **Port Collision Fallback:** If a client port is already active locally, the client automatically falls back to an ephemeral source port (`Port: 0`) while still preserving the original client IP.
- **Unified Configuration:** Configured via `transport.proxyProtocol = "tproxy"`, with automatic smart socket mark defaulting (`transport.socketMark = 1`).
- **Pre-Flight Diagnostics:** Validates kernel capabilities (`CAP_NET_ADMIN`) and sysctls (`route_localnet`, `ip_forward`), emitting actionable errors rather than opaque timeouts.

### RFC 9221 QUIC Datagram Relay for UDP
- **Head-of-Line Blocking Elimination:** Proxies UDP traffic as unreliable, unordered QUIC datagrams across the WAN, avoiding TCP-style retransmission delays and latency spikes.
- **Strict Lane Isolation (`quicDatagramStrict`):** Optional strict mode drops oversized packets instead of falling back to reliable streams, eliminating cross-lane reordering and protecting anti-replay windows in **WireGuard**, **IPsec**, and **DTLS** tunnels.
- **Per-Packet PROXY Protocol:** UDP proxies support `transport.proxyProtocolMode = "per-packet"` alongside `"first-packet"`, enabling seamless integration with stateless load balancers (HAProxy `mode udp` and NGINX Stream).

### Wire Protocol v2 with Hardware AEAD & Ephemeral Key Agreement
- **X25519 ECDH Key Exchange:** Control channel keys are derived from ephemeral X25519 Diffie-Hellman key shares and HKDF-SHA256 transcript hashing.
- **Key Share Enforcement:** Enforceable via `transport.requireKeyShare = true`.
- **Hardware-Accelerated AEAD Framing:** Automatic selection of AES-256-GCM (on hardware with AES-NI / ARMv8 crypto extensions) or XChaCha20-Poly1305.

### Two-Phase Server Plugin Admission
- **Two-Phase Admission Control (`NewProxyResult`):** Server plugins can validate and commit proxy creation transactions before `frps` confirms registration to the client. If admission fails, compensating `CloseProxy` transactions are triggered automatically.
- **Attempt ID Correlation:** Cryptographically random 128-bit `attempt_id` accompanies all `NewProxy`, `NewProxyResult`, and `CloseProxy` notifications to uniquely identify proxy generations.
- **High-Capacity Non-Blocking Queue:** Expanded `CloseProxy` asynchronous notification FIFO from 256 to **2048** entries with a 30-second exponential backoff retry budget, bounded 25-second HTTP timeouts, and strict redirect prevention.

### Programmatic Embedding & System CA Support
- **Client Authentication Hooks:** Embedders of `client.NewService` can configure `ServiceOptions.OnFirstLoginSuccess` to inspect and veto authenticated sessions before proxies are registered, and supply `InitialRunID` to resume sessions.
- **System Root CAs on All Transports:** `transport.tls.useSystemCa` is now respected across all TLS transports, including implicit TLS transports (`quic` and `wss`), with support for combining system roots with custom `trustedCaFile`.
- **Unix Domain Socket Privacy:** Dial failure logs report sanitized OS error codes without leaking private Unix socket filesystem paths.

---

## Concurrency, Performance & Stability Hardening

- **Jitter-Immune `IdleReadDeadline`:** Replaced the vulnerable half-window ($T/2$) deadline refresh with a quarter-window threshold ($3/4\,T$), mathematically eliminating spurious disconnects caused by timer jitter or Go runtime GC pauses under continuous traffic.
- **Immediate Ephemeral Socket Cleanup:** Active UDP flow sockets are closed immediately upon forwarder termination, unblocking kernel `ReadFromUDP` calls and freeing ephemeral ports without waiting for idle deadlines.
- **Non-Blocking Drop-on-Saturation Queues:** UDP and SUDP reader workers utilize 1024-packet non-blocking queues, adhering strictly to RFC 768 UDP semantics and preventing reader goroutines from locking up under bursts.
- **Safe Buffer Lifecycle in Wire Framing:** Eliminated buffer pool aliasing hazards in `WriteFrame` by ensuring frames are cleanly allocated without use-after-free risks under buffered or asynchronous writers.
- **Timer Heap Retention Fix:** Replaced dormant `time.After` allocations in `GetWorkConn` with stopped timers, eliminating GC memory churn during connection spikes.
- **TCP_NODELAY Defaults:** Enabled `TCP_NODELAY` across client local dials and control connections, eliminating 40ms Nagle delays on latency-sensitive traffic.
- **Zero-Allocation v2 Dispatcher:** Replaced reflection-based message instantiation with a pre-allocated constructor table.

---

## Configuration Reference Summary

| Scope | Option | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| Proxy | `transport.proxyProtocol` | string | `""` | Origin IP delivery mode: `""`, `"v1"`, `"v2"`, or `"tproxy"`. |
| Proxy | `transport.proxyProtocolMode` | string | `"first-packet"` | UDP proxy protocol header delivery: `"first-packet"` or `"per-packet"`. |
| Proxy | `transport.socketMark` | int | `1` | Linux socket mark (`fwmark`) for `tproxy` policy routing. |
| Proxy | `quicDatagrams` | bool | `false` | Relays UDP packets as RFC 9221 QUIC datagrams. |
| Proxy | `quicDatagramStrict` | bool | `false` | Drops oversized datagrams instead of falling back to stream. |
| Transport | `transport.wireProtocol` | string | `"v1"` | Wire protocol version (`"v1"` or `"v2"`). |
| Transport | `transport.requireKeyShare` | bool | `false` | Enforces ephemeral X25519 key agreement on wire v2. |
| Transport | `transport.tls.useSystemCa` | bool | `false` | Verifies server certificates using system root CAs (supported on all TLS transports). |
| Server | `metricsServer.port` | int | `0` | Dedicated listener for `/healthz`, `/metrics`, and pprof. |


