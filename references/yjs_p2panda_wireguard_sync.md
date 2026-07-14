# Building a P2P Local-First File Sync System with Yjs, p2panda, and WireGuard

## Overview

This report examines how to build a peer-to-peer, local-first file synchronization system combining three technologies: **Yjs** (CRDT engine for collaborative editing), **p2panda** (P2P networking and sync layer), and **WireGuard** (encrypted tunneling). The goal is a system that works offline-first, syncs eventually, and communicates directly between devices without central servers.

---

## 1. Yjs — The CRDT Engine

### What Yjs Is

Yjs is a high-performance CRDT (Conflict-free Replicated Data Type) framework for collaborative editing, with ~920K weekly npm downloads and 17K GitHub stars. It is explicitly designed to be **network-agnostic** — it handles the CRDT logic while leaving the transport layer to pluggable providers. [[https://github.com/yjs/yjs][Yjs GitHub]] [[https://yjs.dev][Yjs]]

Yjs implements an adaptation of the **YATA CRDT** ("Near Real-Time Peer-to-Peer Shared Editing on Extensible Data Types"), which uses a linked-list-based structure with compound representations for efficiency. It provides shared types: `Y.Text`, `Y.Map`, `Y.Array`, and `Y.XmlElement`. [[https://docs.yjs.dev/api/internals][Yjs Internals]] [[https://www.bartoszsypytkowski.com/yata-move][YATA Move Paper]]

### Yjs Sync Protocol

The sync protocol is defined in the `@y/protocols` package and uses binary wire formats:

| Message | ID | Encoding |
|---------|-----|----------|
| `SyncStep1` | 0 | `varUint(0) • varBuffer(stateVector)` |
| `SyncStep2` | 1 | `varUint(1) • varBuffer(documentUpdate)` |
| `Update` | 2 | `varUint(2) • varBuffer(documentUpdate)` |

**Handshake process:**
1. Each peer sends `SyncStep1` containing its state vector (`Y.encodeStateVector(doc)`)
2. On receiving `SyncStep1`, reply with `SyncStep2` containing missing updates (`Y.encodeStateAsUpdate(doc, stateVector)`)
3. After receiving `SyncStep2`, the local document is up to date
4. Subsequent changes propagate as `Update` messages

In P2P topologies, **both peers send SyncStep1** upon connecting. In client-server topologies, only the client initiates. [[https://github.com/yjs/y-protocols/blob/master/PROTOCOL.md][y-protocols PROTOCOL.md]]

The **Awareness protocol** (message type 1) propagates ephemeral per-client state (cursor position, username, selection) using a state-based CRDT. Clients re-broadcast every 15 seconds; clients not refreshed for 30 seconds are marked offline.

### Yjs P2P Providers

| Provider | Transport | Status | Stars |
|----------|-----------|--------|-------|
| `y-webrtc` | WebRTC | Active | 586 |
| `y-libp2p` | libp2p | Community | — |
| `y-dat` / `y-hyper` | Dat/Hypercore | Archived | — |
| `y-websocket` | WebSocket | Active | — |
| `y-socket.io` | Socket.io | Community | — |
| `Matrix-CRDT` | Matrix (federated) | Active | — |
| `yrs-webrtc` | WebRTC (Rust/Yrs) | Active | — |
| `y-webrtc-trystero` | WebRTC + Trystero | Active | — |

**y-webrtc** is the most widely used P2P connector. It uses public signaling servers (`wss://signaling.yjs.dev` in EU/US) and `simple-peer` internally. Tabs within the same browser share data via BroadcastChannel. However, some users report reliability issues between browsers. [[https://github.com/yjs/y-webrtc][y-webrtc]] [[https://www.reddit.com/r/javascript/comments/1kwrgqb][Reddit: y-webrtc issues]]

### Yjs Local Persistence

| Library | Storage | Platform |
|---------|---------|----------|
| `y-indexeddb` | IndexedDB | Browser |
| `y-leveldb` | LevelDB | Node.js (archived) |
| `y-redis` | Redis | Server |
| PowerSync | SQLite + Postgres | Mobile/Web |

`y-indexeddb` is the primary browser persistence layer: `new IndexeddbPersistence(docName, ydoc)` fires a `"synced"` event when loaded. Content created offline syncs when the peer reconnects. [[https://docs.yjs.dev/ecosystem/database-provider/y-indexeddb][y-indexeddb Docs]]

---

## 2. p2panda — The P2P Networking Layer

### What p2panda Is

p2panda is a Rust framework for building local-first, offline-first P2P applications. It is **not** a single application but a modular toolkit providing networking, sync, discovery, gossip, and storage layers. The project is funded by NLNet/NGI and is currently at version 0.7.0 (pre-v1.0). [[https://p2panda.org][p2panda.org]] [[https://github.com/p2panda/p2panda][p2panda GitHub]]

### Architecture

p2panda does **not** use libp2p. Instead, it uses **iroh** — a QUIC-based Rust P2P library with built-in relay fallback. Iroh provides:

- **QUIC transport** for encrypted, multiplexed connections
- **STUN/ICE** for NAT traversal
- **Relay fallback** (DERP-like) when direct P2P fails
- **Peer discovery** via rendezvous servers

p2panda's networking layer supports "networks from the internet to packet radio, LoRa or BLE" — it is designed for challenged communication infrastructure. [[https://p2panda.org/2024/12/06/p2panda-release.html][p2panda 2024 Release]]

### Data Model

p2panda's core data type is a **single-writer append-only log** with pruning support and fork resistance. Multiple logs form **causally-ordered graphs** (DAGs) when arranged in multi-writer streams over a topic. This provides ordering — knowledge of "what happened before/after/simultaneously."

The framework also has its own CRDT work: `p2panda-auth` is a convergent, offline-first Access Control CRDT for managing group permissions. [[https://p2panda.org/2025/08/27/notes-convergent-access-control-crdt.html][p2panda-auth CRDT]]

### Sync Protocol

p2panda's sync mechanism uses:
- **State vectors** exchanged in constant size (cheap even over radio meshes)
- **Log-height comparison** and **range-based set reconciliation** (RIBLT)
- **Topic-based pubsub** for partial replication
- **At-least-once delivery** with acknowledgements and replay support

The `p2panda-sync` crate provides "Local-first sync for append-only logs and traits to build your own." [[https://crates.io/crates/p2panda-sync][p2panda-sync crate]]

### Gossip Protocols

p2panda uses **PlumTree** and **HyParView** for gossip-based peer discovery and message propagation, rather than libp2p's gossipsub.

### Storage

Synchronized operations are persisted in a **local SQLite database**, together with address books, causal ordering buffers, topic mappings, and stream cursors.

### APIs

p2panda exposes:
- **Rust** — primary API via crates
- **JavaScript/WASM** — bindings for browser and Node.js use

### Data-Type Agnostic Design

**This is the key insight for integration:** p2panda's Dec 2024 rewrite explicitly states:

> "Support any CRDT or application data — Previous versions of p2panda came with their own approaches to CRDTs and schema validation. While we still believe this is great for future high-level modules, we wanted to offer you the option of combining all p2panda modules with Automerge, Yjs or any other CRDT of your choice."

The sync, discovery, and networking layers are **data-type agnostic** — they work with raw bytes, so you can bring your own CRDT (including Yjs) and develop your protocol on top. [[https://p2panda.org/2024/12/06/p2panda-release.html][p2panda 2024 Release]]

### The "Always-On" Shared Node Pattern

p2panda's "always-on" pattern refers to **Shared Nodes** — always-available peers that act as relay/sync hubs. When direct P2P connections fail (due to NAT, firewalls, or offline peers), shared nodes:
- Buffer updates for offline peers
- Relay messages between peers that cannot connect directly
- Provide persistent availability for sync

This is similar to the "very available peer" concept in Automerge's sync server model, where the sync server is just "a very available peer." [[https://automerge.org/docs/tutorial/network-sync][Automerge Network Sync]]

---

## 3. WireGuard — Encrypted P2P Tunneling

### What WireGuard Is

WireGuard is a Layer 3 secure network tunnel operating as a kernel virtual network interface. It uses **crypto-key routing**: each peer is identified by its public key, and packets are encrypted using **ChaCha20-Poly1305** encapsulated in **UDP**. Key exchange uses the **Noise_IK** protocol. [[https://www.wireguard.com][WireGuard]] [[https://www.wireguard.com/papers/wireguard.pdf][WireGuard Paper]]

WireGuard supports **mesh topologies** natively — each device can connect directly to every other device. Every peer's configuration lists all other peers with their public keys, endpoints (IP:port), and AllowedIPs.

### WireGuard and NAT Traversal

WireGuard **does not include built-in NAT traversal** — it is "just usual UDP." This is both a strength (simplicity) and a limitation.

**What WireGuard provides:**
- **PersistentKeepalive**: Sends keepalive packets at a configurable interval (recommended: 25 seconds) to maintain NAT mappings
- **Endpoint roaming**: Automatically updates a peer's endpoint when it receives a valid packet from a new address

**What WireGuard does NOT provide:**
- No STUN/ICE support
- No hole-punching coordination
- No relay fallback

| NAT Type | P2P Likelihood | Notes |
|----------|---------------|-------|
| Full Cone NAT | High | Consistent port mapping |
| Symmetric NAT | Very Low | Different port per destination |
| CGNAT | Low-Medium | Double NAT, short timeouts |
| 1:1 NAT (cloud VMs) | High | Behaves like easy NAT |

[[https://tailscale.com/blog/nat-traversal-improvements-pt-1][Tailscale NAT Traversal]] [[https://docs.netbird.io/about-netbird/understanding-nat-and-connectivity][NetBird NAT Docs]]

### The "Always-On" WireGuard Tunnel Pattern

The always-on pattern relies on **PersistentKeepalive** to maintain NAT mappings:
- Set `PersistentKeepalive = 25` (seconds) on the NATed peer side
- WireGuard sends an empty encrypted packet every 25 seconds
- This keeps the NAT/firewall mapping alive during idle periods
- Default is 0 (disabled) — WireGuard is silent when idle by design

Additional techniques:
- **DNS-based endpoints**: WireGuard resolves DNS names, supporting dynamic IPs
- **Scripts/cron jobs**: Periodically re-resolve DNS and update peer endpoints

[[https://www.wireguard.com/quickstart][WireGuard Quickstart]]

### WireGuard vs. Tailscale vs. ZeroTier

| Feature | WireGuard | Tailscale | ZeroTier |
|---------|-----------|-----------|----------|
| Protocol | Layer 3 VPN (UDP) | WireGuard-based overlay | Custom Layer 2 |
| NAT Traversal | Manual only | Built-in (STUN/ICE + DERP) | Built-in (root servers) |
| Peer Discovery | Manual config | Automatic (control server) | Automatic (root servers) |
| Relay Fallback | None | DERP (TCP/443) | Built-in |
| Self-Hostable | Yes (fully) | Partially (Headscale) | Yes |
| Open Source | Fully | Clients yes, server no | Yes |

**Key insight:** Tailscale is essentially "WireGuard + NAT traversal + coordination server." It uses WireGuard for the data plane but adds DERP relays, STUN/ICE hole punching, and a coordination server. [[https://tailscale.com/compare/zerotier][Tailscale vs ZeroTier]]

### Dynamic Peer Management

WireGuard has **no built-in dynamic peer management**. External tools exist:

1. **Headscale** — Self-hosted Tailscale control server for WireGuard key distribution and peer coordination [[https://github.com/juanfont/headscale][Headscale]]
2. **wgsd** — CoreDNS plugin for WireGuard peer discovery via DNS-SD [[https://www.jordanwhited.com/posts/wireguard-endpoint-discovery-nat-traversal][DNS-SD for WireGuard]]
3. **natpunch-go** — NAT hole punching tool for WireGuard mesh [[https://github.com/malcolmseyd/natpunch-go][natpunch-go]]
4. **wireguard-dynamic** — Auto-discovery peers using key-value stores [[https://github.com/segator/wireguard-dynamic][wireguard-dynamic]]

### WireGuard's Role in Local-First Architecture

WireGuard fits as the **encrypted transport layer** that enables:
1. **Direct device-to-device communication** over an encrypted mesh
2. **Privacy and security** — end-to-end encryption with modern cryptography
3. **LAN-like experience** — devices appear on the same virtual network
4. **Data sovereignty** — self-hosted WireGuard means no third-party sees your topology

**Limitations:** WireGuard is not local-first itself — it requires coordination (manual config or a server). It does not handle offline-first scenarios or conflict resolution.

---

## 4. Integration: How the Three Technologies Fit Together

### No Existing Combined Project

**No existing project directly combines Yjs + p2panda + WireGuard.** The ecosystem is fragmented:

- **Yjs + p2panda**: No production app combines them yet, but p2panda explicitly supports Yjs as a data type
- **Yjs + WireGuard**: No project combines them; WireGuard is a VPN protocol, not a data-sync protocol
- **p2panda + WireGuard**: No direct combination; p2panda uses iroh for networking, not WireGuard

### Architecture Options

#### Option A: Yjs + p2panda (Recommended)

```
┌─────────────────────────────────────────────┐
│  Application (collaborative editor)          │
├─────────────────────────────────────────────┤
│  Yjs (CRDT engine)                          │
│  └── y-indexeddb (local persistence)        │
├─────────────────────────────────────────────┤
│  p2panda sync layer (data-type agnostic)    │
│  └── p2panda networking (iroh/QUIC)         │
│      └── STUN/ICE + relay fallback          │
├─────────────────────────────────────────────┤
│  p2panda storage (SQLite)                   │
└─────────────────────────────────────────────┘
```

This is the most natural integration. p2panda's data-type agnostic design means you can:
1. Use Yjs as the CRDT engine for collaborative editing
2. Use p2panda's networking (iroh) for P2P connectivity with NAT traversal
3. Use p2panda's sync layer to propagate Yjs updates between peers
4. Use p2panda's SQLite storage for local persistence
5. Use p2panda's Shared Nodes for always-on relay

**WireGuard is not needed** because p2panda already provides encrypted P2P networking with NAT traversal via iroh.

#### Option B: Yjs + WireGuard (Simpler, but limited)

```
┌─────────────────────────────────────────────┐
│  Application (collaborative editor)          │
├─────────────────────────────────────────────┤
│  Yjs (CRDT engine)                          │
│  └── y-indexeddb (local persistence)        │
├─────────────────────────────────────────────┤
│  Custom sync server over WireGuard tunnel   │
│  └── y-websocket or custom provider         │
├─────────────────────────────────────────────┤
│  WireGuard mesh (encrypted tunnel)          │
│  └── PersistentKeepalive for NAT            │
└─────────────────────────────────────────────┘
```

This approach:
1. Sets up a WireGuard mesh between all devices
2. Runs a Yjs sync server (or y-websocket) inside the tunnel
3. Uses standard Yjs providers over the encrypted tunnel

**Limitations:** No NAT traversal beyond PersistentKeepalive, no automatic peer discovery, no relay fallback. Requires manual WireGuard configuration or a coordination server like Headscale.

#### Option C: Full Three-Layer Stack

```
┌─────────────────────────────────────────────┐
│  Application (collaborative editor)          │
├─────────────────────────────────────────────┤
│  Yjs (CRDT engine)                          │
│  └── y-indexeddb (local persistence)        │
├─────────────────────────────────────────────┤
│  p2panda sync + discovery                   │
│  └── p2panda networking (iroh)              │
├─────────────────────────────────────────────┤
│  WireGuard mesh (encrypted overlay)         │
│  └── Headscale (coordination server)        │
└─────────────────────────────────────────────┘
```

This combines p2panda's sync/discovery with WireGuard's encrypted overlay. WireGuard provides the encrypted network fabric, while p2panda handles sync logic and Yjs handles CRDTs. This is the most complex but most flexible option.

### Why WireGuard May Be Redundant

p2panda already uses iroh, which provides:
- **QUIC encryption** (TLS 1.3-level security)
- **STUN/ICE NAT traversal** (better than WireGuard's PersistentKeepalive)
- **Relay fallback** (WireGuard has none)
- **Peer discovery** (WireGuard has none)

Adding WireGuard on top adds complexity without clear benefit unless you specifically need:
- A unified encrypted network for multiple applications (not just your sync app)
- Kernel-level performance (WireGuard is in-kernel; iroh is userspace)
- Integration with existing WireGuard infrastructure

---

## 5. Document State Sync and Eventual Consistency

### How Sync Works

CRDT-based sync achieves **Strong Eventual Consistency (SEC)**: correct replicas that have delivered the same updates are immediately in equivalent states. The sufficient conditions are:

1. **Eventual delivery** — every update reaches every replica
2. **Commutativity** — merge order doesn't matter
3. **Associativity** — grouping doesn't matter
4. **Idempotency** — applying the same update twice has no extra effect

Yjs document updates satisfy all three properties. [[https://docs.yjs.dev/api/document-updates][Yjs Document Updates]]

### Sync Flow in Practice

1. **Peer A** makes an edit → Yjs generates an `Update` message (binary blob)
2. **Peer A** persists the update locally (y-indexeddb or SQLite)
3. **Peer A** sends the update to connected peers via the P2P network
4. **Peer B** receives the update → applies it to its local Yjs document
5. **Peer B** persists the update locally
6. **Peer B** forwards the update to its connected peers (gossip)
7. When **Peer C** comes online, it syncs with any available peer using the state vector handshake

### Offline-First Behavior

- All edits work locally, even when offline
- Updates are queued and persisted locally
- When a peer reconnects, it exchanges state vectors and syncs missing updates
- CRDTs guarantee convergence regardless of sync order or timing

### Binary Encoding

Yjs uses a highly compressed binary format (`Uint8Array`) with LEB128-style variable-length encoding. State vectors are compact summaries of what updates each peer has seen, making sync efficient even over constrained networks.

---

## 6. Missing Components and Gaps

### Gap 1: No Yjs + p2panda Integration Library

p2panda supports Yjs as a data type, but no production-ready integration library exists. You would need to:
- Implement a p2panda sync adapter for Yjs updates
- Map Yjs `Update` messages to p2panda's append-only log format
- Handle Yjs awareness messages through p2panda's pubsub layer

### Gap 2: NAT Traversal for True P2P

- **y-webrtc** has known reliability issues between browsers
- **iroh** (used by p2panda) solves this with relay fallback
- **WireGuard** alone cannot handle symmetric NAT or CGNAT
- **Gap:** A reliable, self-hosted NAT traversal solution without cloud infrastructure

### Gap 3: Peer Discovery Without Central Servers

- Most P2P systems need at least a signaling/discovery server
- p2panda uses rendezvous servers for iroh
- Earthstar uses mDNS for local network discovery
- **Gap:** Truly decentralized discovery that works across WAN without any central infrastructure

### Gap 4: End-to-End Encryption + CRDTs

- p2panda is working on group encryption with UCAN-based access control (2025 roadmap)
- Combining CRDTs with E2E encryption is an active research area
- **Gap:** Production-ready E2E encrypted CRDT sync that works offline-first

### Gap 5: Large File Sync + CRDT Sync

- Syncthing handles files but not CRDT-based collaborative editing
- Hyperdrive handles P2P file sharing but not CRDT merging
- **Gap:** A unified system that syncs both structured CRDT data (documents) and binary blobs (images, attachments) over the same P2P connection

### Gap 6: Cross-Platform P2P Runtime

- **Pear Runtime** exists but is relatively new
- **Gap:** A mature, widely-adopted P2P runtime that works seamlessly across desktop, mobile, and browser

---

## 7. Recommended Architecture

For a P2P local-first file sync system with collaborative editing, the recommended stack is:

```
┌──────────────────────────────────────────────────────────┐
│  APPLICATION LAYER                                       │
│  Collaborative editor (ProseMirror, Tiptap, etc.)        │
├──────────────────────────────────────────────────────────┤
│  CRDT LAYER                                              │
│  Yjs (collaborative text editing)                        │
│  └── y-indexeddb (browser persistence)                   │
│  └── SQLite (desktop/mobile persistence)                 │
├──────────────────────────────────────────────────────────┤
│  SYNC LAYER                                              │
│  p2panda-sync (data-type agnostic sync)                  │
│  └── Custom Yjs adapter (maps Yjs updates to p2panda)   │
├──────────────────────────────────────────────────────────┤
│  NETWORKING LAYER                                        │
│  p2panda networking (iroh/QUIC)                          │
│  └── STUN/ICE NAT traversal                              │
│  └── Relay fallback for unreachable peers                │
│  └── PlumTree/HyParView gossip                           │
├──────────────────────────────────────────────────────────┤
│  STORAGE LAYER                                           │
│  p2panda storage (SQLite)                                │
│  └── Local-first, offline-first                          │
├──────────────────────────────────────────────────────────┤
│  OPTIONAL: WireGuard mesh (if you need a unified         │
│  encrypted network for multiple applications)            │
│  └── Headscale for coordination                          │
│  └── PersistentKeepalive for NAT                         │
└──────────────────────────────────────────────────────────┘
```

**Key decisions:**
1. **Use p2panda's iroh instead of WireGuard** for P2P networking — it provides NAT traversal and relay fallback that WireGuard lacks
2. **Build a custom Yjs adapter** for p2panda's sync layer — this is the main missing piece
3. **Use SQLite for local persistence** — works across platforms and integrates with p2panda
4. **Use p2panda's Shared Nodes** for always-on relay when direct P2P fails
5. **Add WireGuard only if** you need a unified encrypted network for multiple applications beyond the sync system

---

## 8. Existing Projects in the Local-First Ecosystem

| Project | CRDT | P2P Network | Notes |
|---------|------|-------------|-------|
| **Anytype** | Any-Sync (custom) | Custom P2P + mDNS | Local-first knowledge OS |
| **Excalidraw** | Yjs | y-webrtc | Collaborative drawing |
| **tldraw** | Yjs | y-webrtc | Infinite canvas whiteboard |
| **AFFiNE** | Yjs | WebSocket | Workspace app |
| **OrbitDB** | Merkle-CRDTs | IPFS PubSub | P2P database |
| **Earthstar** | Author versions | Encrypted TCP + mDNS | Offline-first P2P KV store |
| **Jazz (CoJSON)** | CoJSON | Custom | Local-first relational DB |
| **Automerge + MPL** | Automerge | WebRTC P2P | Magic Persistence Layer |
| **y-libp2p** | Yjs | libp2p/gossipsub | Community project |
| **Matrix-CRDT** | Yjs | Matrix (federated) | Distributed collaboration |
| **Pear Runtime** | Hypercore | Hyperswarm | P2P runtime for apps |
| **Syncthing** | None (file-level) | Custom P2P | Continuous file sync |

**No project combines all three layers (CRDT + P2P networking + encrypted tunneling) into a single cohesive system.** p2panda comes closest with its modular approach. [[https://github.com/alexanderop/awesome-local-first][awesome-local-first]] [[https://www.localfirst.fm/landscape][Local-First Landscape]]

---

## 9. Conclusion

Building a P2P local-first file sync system with Yjs, p2panda, and WireGuard is feasible but requires custom integration work:

1. **Yjs** provides excellent CRDT support for collaborative editing with a mature ecosystem of providers and persistence layers
2. **p2panda** provides a modular P2P networking stack with NAT traversal (via iroh), sync protocols, and local-first storage — and explicitly supports Yjs as a data type
3. **WireGuard** provides encrypted mesh networking but lacks NAT traversal and peer discovery — it is largely redundant when p2panda's iroh is already providing encrypted P2P with NAT traversal

The main missing piece is a **Yjs adapter for p2panda's sync layer**. This would map Yjs `Update` messages to p2panda's append-only log format and leverage p2panda's networking, discovery, and storage. WireGuard is optional and only adds value if you need a unified encrypted network for multiple applications.

The recommended path is **Yjs + p2panda (iroh)**, with WireGuard as an optional overlay for broader network encryption needs.

---

## Sources

[1] Yjs GitHub: https://github.com/yjs/yjs
[2] Yjs Docs: https://yjs.dev
[3] Yjs Internals: https://docs.yjs.dev/api/internals
[4] Yjs Document Updates: https://docs.yjs.dev/api/document-updates
[5] Yjs Awareness: https://docs.yjs.dev/api/about-awareness
[6] Yjs IndexedDB: https://docs.yjs.dev/ecosystem/database-provider/y-indexeddb
[7] Yjs LevelDB: https://docs.yjs.dev/ecosystem/database-provider/y-leveldb
[8] y-webrtc: https://github.com/yjs/y-webrtc
[9] y-libp2p: https://github.com/MarcoPolo/y-libp2p
[10] y-protocols PROTOCOL.md: https://github.com/yjs/y-protocols/blob/master/PROTOCOL.md
[11] y-protocols: https://github.com/yjs/y-protocols
[12] y-leveldb: https://github.com/yjs/y-leveldb
[13] Matrix-CRDT: https://github.com/YousefED/Matrix-CRDT
[14] YATA Move Paper: https://www.bartoszsypytkowski.com/yata-move
[15] p2panda.org: https://p2panda.org
[16] p2panda GitHub: https://github.com/p2panda/p2panda
[17] p2panda 2024 Release: https://p2panda.org/2024/12/06/p2panda-release.html
[18] p2panda Rust Docs: https://docs.rs/p2panda
[19] p2panda-sync crate: https://crates.io/crates/p2panda-sync
[20] p2panda-auth CRDT: https://p2panda.org/2025/08/27/notes-convergent-access-control-crdt.html
[21] p2panda Group Encryption: https://p2panda.org/2025/02/24/group-encryption.html
[22] WireGuard: https://www.wireguard.com
[23] WireGuard Quickstart: https://www.wireguard.com/quickstart
[24] WireGuard Paper: https://www.wireguard.com/papers/wireguard.pdf
[25] Tailscale NAT Traversal: https://tailscale.com/blog/nat-traversal-improvements-pt-1
[26] Tailscale vs ZeroTier: https://tailscale.com/compare/zerotier
[27] NetBird NAT Docs: https://docs.netbird.io/about-netbird/understanding-nat-and-connectivity
[28] Headscale: https://github.com/juanfont/headscale
[29] natpunch-go: https://github.com/malcolmseyd/natpunch-go
[30] wireguard-dynamic: https://github.com/segator/wireguard-dynamic
[31] DNS-SD for WireGuard: https://www.jordanwhited.com/posts/wireguard-endpoint-discovery-nat-traversal
[32] Automerge Network Sync: https://automerge.org/docs/tutorial/network-sync
[33] Automerge Binary Format: https://automerge.org/automerge-binary-format-spec
[34] awesome-local-first: https://github.com/alexanderop/awesome-local-first
[35] Local-First Landscape: https://www.localfirst.fm/landscape
[36] Earthstar: https://earthstar-project.org/docs/what-is-it
[37] OrbitDB: https://orbitdb.org
[38] Jazz (CoJSON): https://jazz.tools
[39] Pear Runtime: https://pears.com/
[40] Yjs vs Automerge vs Loro: https://www.pkgpulse.com/guides/yjs-vs-automerge-vs-loro-crdt-libraries-2026
[41] Yjs UndoManager: https://docs.yjs.dev/api/undo-manager
[42] Yjs WebSocket: https://docs.yjs.dev/ecosystem/connection-provider/y-websocket
[43] Yjs Hyper: https://docs.yjs.dev/ecosystem/connection-provider/y-hyper
[44] SyncedStore: https://syncedstore.org/docs/sync-providers
[45] Reddit y-webrtc issues: https://www.reddit.com/r/javascript/comments/1kwrgqb/yjs_is_not_working_with_ywebrtc
[46] Yjs Yrs WebRTC: https://github.com/Horusiath/yrs-webrtc
[47] Yjs Trystero: https://github.com/WinstonFassett/y-webrtc-trystero
[48] Yjs Beta Docs: https://beta.yjs.dev/docs/introduction
[49] Yjs GitHub Issue #170: https://github.com/yjs/yjs/issues/170
[50] Yjs y-dat: https://github.com/yjs/y-dat
[51] Yjs y-sync (Rust): https://github.com/y-crdt/y-sync
[52] Yjs y-socket.io: https://www.npmjs.com/package/y-socket.io
[53] Yjs Demos: https://github.com/yjs/yjs-demos
[54] Yjs Discuss Matrix-CRDT: https://discuss.yjs.dev/t/matrix-crdt-yjs-provider-that-connects-to-matrix/958
[55] Yjs Discuss ProseMirror: https://discuss.prosemirror.net/t/offline-peer-to-peer-collaborative-editing-using-yjs/2488
[56] Yjs Discuss y-leveldb: https://discuss.yjs.dev/t/how-is-y-leveldb-coming-along/126
[57] Yjs Discuss State Vectors: https://discuss.yjs.dev/t/question-regarding-updates-and-state-vectors-in-y-leveldb/399
[58] Yjs Discuss Sync Protocol: https://discuss.yjs.dev/t/sync-protocol-basic-example/540
[59] Yjs Discuss Loro: https://discuss.yjs.dev/t/yjs-vs-loro-new-crdt-lib/2567
[60] Yjs Tag1 Deep Dive: https://www.tag1.com/blog/yjs-deep-dive-part-4
[61] Yjs Tag1 Signal: https://www.tag1.com/blog/signal-y-webrtc-part2
[62] Yjs Dovetail Engineering: https://medium.com/dovetail-engineering/yjs-fundamentals-part-1-theory-232a450dad7b
[63] Yjs Dovetail Sync: https://medium.com/dovetail-engineering/yjs-fundamentals-part-2-sync-awareness-73b8fabc223b
[64] Yjs StackOverflow History: https://stackoverflow.com/questions/77117000/is-it-possible-to-store-the-document-edit-history-when-using-yjs-as-the-collaboa
[65] Yjs StackOverflow Gossipsub: https://stackoverflow.com/questions/71157652/gossipsub-scalability-in-terms-of-topic-size
[66] p2panda NGI Impact: https://ngi.eu/impact-stories/decentralised-social-media/p2panda
[67] p2panda Mastodon: https://autonomous.zone/@p2panda/116296878828115857
[68] p2panda FOSDEM 2026: https://fosdem.org/2026/events/attachments/J3FLC3-walkaway-stack/slides/267628/260201_wa_kner30x.pdf
[69] p2panda Releases: https://github.com/p2panda/p2panda/releases
[70] p2panda-sync latest: https://docs.rs/p2panda-sync/latest
[71] libp2p Gossipsub Spec: https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/README.md
[72] libp2p Gossipsub v1.0: https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.0.md
[73] libp2p Gossipsub npm: https://npmjs.com/package/@libp2p/gossipsub
[74] libp2p CRDT Sync: https://discuss.libp2p.io/t/libp2p-crdt-synchronization/1781
[75] libp2p Gossipsub Discovery: https://discuss.libp2p.io/t/gossipsub-how-does-it-work-without-peer-discovery/2373
[76] libp2p Gossipsub Rust: https://docs.rs/gossipsub/latest/gossipsub
[77] libp2p Gossipsub Crate: https://docs.rs/crate/libp2p-gossipsub/latest
[78] Automerge MPL: https://github.com/automerge/mpl
[79] Automerge Binary Format Spec: https://github.com/automerge/automerge-binary-format-spec
[80] Automerge Columnar Encoding: https://mohakchugh.is-a.dev/blog/crdts-automerge-columnar-encoding-collaborative-editing
[81] Automerge Rust: https://lib.rs/crates/automerge
[82] Automerge Swift: https://automerge.org/automerge-swift/documentation/automerge/document/save()
[83] Loro CRDT: https://loro.dev/docs/concepts/crdt
[84] Loro Extended: https://github.com/schoolAI/loro-extended
[85] Anytype Any-Sync: https://tech.anytype.io/any-sync/overview
[86] Anytype HN: https://news.ycombinator.com/item?id=36799548
[87] Anytype Reddit: https://www.reddit.com/r/Anytype/comments/1nr45io/local_p2p_sync
[88] ElectricSQL: https://electric.ax
[89] ElectricSQL Landscape: https://www.localfirst.fm/landscape/electricsql
[90] PowerSync: https://docs.powersync.com/resources/local-first-software
[91] Hyperdrive: https://hypercore-protocol.github.io/new-website/guides/modules/hyperdrive
[92] RxDB: https://rxdb.info
[93] RxDB WebRTC: https://rxdb.info/replication-webrtc.html
[94] RxDB Local-First: https://rxdb.info/articles/local-first-future.html
[95] Syncthing: https://itsfoss.com/syncthing
[96] LocalSend: https://localsend.org
[97] ZeroTier + Syncthing: https://forum.syncthing.net/t/use-zerotier-as-network-backend/13411
[98] ZeroTier Blog: https://blog.rico-j.de/zerotier-one
[99] WireGuard Reddit P2P: https://www.reddit.com/r/WireGuard/comments/1rxevtx/how_can_p2p_be_done_ovee_wireguard
[100] WireGuard Reddit Selfhosted: https://www.reddit.com/r/selfhosted/comments/15log01/a_complete_p2p_wireguard_network_with_user
[101] WireGuard Reddit CGNAT: https://www.reddit.com/r/WireGuard/comments/1besy7d/wireguard_to_bypass_cgnat
[102] WireGuard Home Assistant: https://community.home-assistant.io/t/wire-guard-over-cg-nat-on-raspberry-pi/745695
[103] WireGuard Galaxy Tutorial: https://training.galaxyproject.org/training-material/topics/admin/tutorials/wireguard/tutorial.html
[104] WireGuard Unofficial Docs: https://docs.sweeting.me/s/wireguard
[105] WireGuard Keepalive Talos: https://oneuptime.com/blog/post/2026-03-03-configure-wireguard-keepalive-on-talos-linux/view
[106] WireGuard Dynamic Endpoints: https://oneuptime.com/blog/post/2026-03-03-set-up-wireguard-with-dynamic-endpoints-on-talos-linux/view
[107] WireGuard Mesh Scaleway: https://www.scaleway.com/en/docs/tutorials/wireguard-mesh-vpn
[108] WireGuard Palo Alto: https://www.paloaltonetworks.com/cyberpedia/what-is-wireguard
[109] WireGuard Wiki Teltonika: https://wiki.teltonika-networks.com/view/Wireguard_Peer_To_Peer_Configuration_example
[110] WireGuard Mailing List: https://lists.zx2c4.com/pipermail/wireguard/2016-August/000372.html
[111] Tailscale MongoDB: https://www.youtube.com/watch?v=zIjV1vJD_lE
[112] Tailscale Issue: https://github.com/tailscale/tailscale/issues/14622
[113] Tauri + Yjs WebRTC: https://www.youtube.com/shorts/dLNW8URHCyM
[114] Tailscale vs ZeroTier Medium: https://afeiszli.medium.com/tailscale-vs-zerotier-b6da7f66d7b6
[115] Tailscale vs ZeroTier GL.iNet: https://www.gl-inet.com/blogs/blog/openvpn-vs-wireguard-vs-tailscale-which-vpn-to-choose
[116] Duplicacy Forum: https://forum.duplicacy.com/t/tailscale-vs-zerotier-vs/9795
[117] CRDT Eventual Consistency: https://dev.to/foxgem/crdts-achieving-eventual-consistency-in-distributed-systems-296g
[118] Eventual Consistency Part 1: https://www.mydistributed.systems/2022/02/eventual-consistency-part-1.html
[119] Local-First Stack: https://www.ersin.nz/articles/creating-the-local-first-stack
[120] CRDT E2E Encryption: https://kerkour.com/crdt-end-to-end-encryption-research-notes
[121] CRDTs as Database: https://jackson.dev/post/crdts_as_database
[122] TinyBase Persistence: https://tinybase.org/guides/persistence
[123] Local-First Database Search: https://jaredforsyth.com/posts/in-search-of-a-local-first-database
[124] Kleppmann Convergence: https://martin.kleppmann.com/papers/convergence-cacm.pdf
[125] Type-Checking CRDT: https://programming-group.com/assets/pdf/papers/2023_Type-Checking-CRDT-Convergence.pdf
[126] CRDT Causal Consistency: https://cs.stackexchange.com/questions/155289/proof-that-state-based-convergent-crdts-are-causally-consistent
[127] CRDT Research 2020: https://arxiv.org/pdf/2006.09823
[128] CRDT Research 2022: https://arxiv.org/pdf/2212.05197
[129] CRDT Research 2024: https://arxiv.org/html/2404.11308v1
[130] CRDT Research 2025: https://arxiv.org/html/2503.17826v1
[131] CRDT Multi-Agent: https://zylos.ai/research/2026-03-17-crdts-distributed-state-sync-multi-agent-systems
[132] CRDT Eventual Consistency Medium: https://medium.com/@rangavamsi5/eventual-consistency-423744066919
[133] CRDT Shared Editing: https://blog.kevinjahns.de/are-crdts-suitable-for-shared-editing
[134] CRDT Gossipsub Research: https://research.protocol.ai/blog/2019/a-new-lab-for-resilient-networks-research/PL-TechRep-gossipsub-v0.1-Dec30.pdf
[135] CRDT Gossipsub 2022: https://arxiv.org/pdf/2402.03773
[136] Yjs HN 2021: https://news.ycombinator.com/item?id=29978659
[137] Yjs HN 2023: https://news.ycombinator.com/item?id=37212462
[138] Yjs HN 2024: https://news.ycombinator.com/item?id=42731582
[139] Yjs HN 2022: https://news.ycombinator.com/item?id=29507948
[140] Yjs HN 2022: https://news.ycombinator.com/item?id=28717848
[141] Yjs Best of JS: https://bestofjs.org/projects/yjs
[142] Yjs SOLID Forum: https://forum.solidproject.org/t/application-of-crdts-to-solid/3321
[143] Yjs Patent: https://patents.google.com/patent/US10740350B2
[144] Yjs YouTube 1: https://www.youtube.com/watch?v=oyUHd894w18
[145] Yjs YouTube 2: https://www.youtube.com/watch?v=NB7HRfyufLk
[146] Yjs YouTube 3: https://www.youtube.com/watch?v=Gmj6vSvYHds
[147] Yjs YouTube 4: https://www.youtube.com/watch?v=aVTOKGLFuLo
[148] Yjs YouTube 5: https://www.youtube.com/watch?v=CDNGdrJajRc
[149] Yjs YouTube 6: https://www.youtube.com/watch?v=4QkLD7JhD_I
[150] Yjs YouTube 7: https://www.youtube.com/watch?v=eWsJ14xw26I
[151] Nettica NAT Traversal: https://nettica.com/nat-traversal-hole-punch
[152] Local-First Essay: https://www.inkandswitch.com/essay/local-first
[153] Local-First Expo: https://docs.expo.dev/guides/local-first
[154] Local-First MongoDB: https://www.mongodb.com/company/blog/innovation/mongodb-atlas-power-sync-future-offline-first-enterprise-apps
[155] Local-First Queryplane: https://queryplane.com/blog/powersync-offline-first-sync
[156] Local-First Ditto: https://www.ditto.com/blog/how-to-build-robust-offline-first-apps-a-technical-guide-to-conflict-resolution-with-crdts-and-ditto
[157] Local-First SQLite Sync: https://github.com/sqliteai/sqlite-sync
[158] Local-First P2P Architecture: https://xylentis.com/blog/building-local-first-and-offline-first-applications-p2p-data-sync-with-crdts-and-hybrid-vpshome-backend-architecture
[159] Local-First Sync Engines: https://www.sandromaglione.com/articles/local-first-vs-sync-engines
[160] Local-First Frameworks: https://neon.com/blog/comparing-local-first-frameworks-and-approaches
[161] Local-First Web Dev: https://medium.com/@arunseetharaman/local-first-web-development-the-future-of-resilient-user-centric-applications-3368e22170e7
[162] Local-First Landscape GitHub: https://github.com/localfirstfm/local-first-landscape
[163] IPFS OrbitDB: https://ecosystem.ipfs.tech/project/orbitdb
[164] libp2p Wikipedia: https://en.wikipedia.org/wiki/Libp2p
[165] PowerSync + Yjs: https://powersync.com/blog/postgres-and-yjs-crdt-collaborative-text-editing-using-powersync
