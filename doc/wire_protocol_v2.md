# Wire Protocol v2 & Cryptographic Framing

## Overview

This fork provides **Wire Protocol v2**, a modern, high-performance transport framing layer for client-server control channels. Wire Protocol v2 provides cryptographically verified session establishment, ephemeral key agreement, and hardware-accelerated Authenticated Encryption with Associated Data (AEAD).

---

## Cryptographic Design

### 1. Ephemeral X25519 Key Agreement
During initial handshake:
- `frpc` and `frps` generate ephemeral X25519 private/public key pairs for each connection session.
- The public key shares are exchanged inside the `ClientHello` and `ServerHello` handshake messages.
- Both sides compute an ephemeral ECDH shared secret:
  $$\text{SharedSecret} = \text{X25519}(\text{PrivKey}_{\text{local}}, \text{PubKey}_{\text{peer}})$$

### 2. Transcript Hashing & HKDF Key Derivation
- To defend against man-in-the-middle tampering, both peers compute a SHA-256 transcript hash covering the handshake payloads.
- Session encryption and authentication keys are derived using **HKDF-SHA256**:
  $$\text{PRK} = \text{HKDF-Extract}(\text{Salt} = \text{TranscriptHash}, \text{IKM} = \text{SharedSecret} \parallel \text{AuthToken})$$
  $$\text{SessionKey} = \text{HKDF-Expand}(\text{PRK}, \text{Info} = \text{"frp-wire-v2-key"}, \text{Len} = 32)$$
- The derivation binds both the configured authentication token and the ephemeral key exchange to the exact handshake transcript.

### 3. Hardware-Accelerated AEAD Framing
All subsequent control messages are encrypted and framed using standard AEAD ciphers:
- **AES-256-GCM:** Automatically selected on architectures with hardware acceleration (Intel/AMD AES-NI and ARMv8 Cryptography Extensions).
- **XChaCha20-Poly1305:** Fallback on platforms without dedicated AES instructions.
- Framing includes an escalating 64-bit frame counter as part of the authenticated additional data (AAD), defending against frame replay, reordering, and injection attacks.

---

## Configuration

Wire Protocol v2 can be enabled in the `transport` configuration block:

### Client Configuration (`frpc.toml`)

```toml
serverAddr = "x.x.x.x"
serverPort = 7000

auth.method = "token"
auth.token = "your_secure_cluster_token"

transport.wireProtocol = "v2"
# Enforce X25519 key agreement (rejects connections if server does not perform key exchange)
transport.requireKeyShare = true
```

### Server Configuration (`frps.toml`)

```toml
bindPort = 7000

auth.method = "token"
auth.token = "your_secure_cluster_token"

transport.wireProtocol = "v2"
# Refuse clients that do not provide an X25519 key share
transport.requireKeyShare = true
```

---

## Security Recommendations

1. **Enable `requireKeyShare`:** In multi-tenant environments or when connecting across untrusted networks, configure `transport.requireKeyShare = true` to guarantee that every session establishes fresh ephemeral forward secrecy.
2. **Combine with TLS:** Wire Protocol v2 provides cryptographic control framing at the message layer. For defense-in-depth, configure `transport.tls.enable = true` (or transport protocols with built-in TLS, such as QUIC or WSS) to protect lower-level transport metadata.
