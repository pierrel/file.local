# Automerge vs Yjs vs Seafile: P2P Local-First File Sync Comparison

## Overview

This report compares three approaches to file synchronization and collaborative editing: **Automerge** (a CRDT library with Git-like branching), **Yjs** (the dominant CRDT for real-time collaboration), and **Seafile** (a mature self-hosted file sync platform). The focus is on building a P2P, local-first file sync stack, with particular attention to the Automerge + p2panda + iroh combination versus Seafile's traditional client-server model.

---

## Part 1: Automerge vs Yjs for P2P File Sync

### Automerge Sync Protocol

Automerge's sync protocol is a **set-reconciliation algorithm for a hash-linked DAG of Git-like commits**, based on the paper at arxiv.org/abs/2012.00472. It assumes a reliable in-order stream between two peers.

**Protocol flow:**

1. The **initiating peer** creates an empty `SyncState` and calls `generateSyncMessage()` to produce a binary sync message, then sends it to the receiving peer.
2. The **receiving peer** creates a new `SyncState`, calls `receiveSyncMessage()` on its document, then generates a response message to send back.
3. Both peers loop — receiving a message, generating a response — until neither side has anything new to send (both return `null`/`None`).

**Binary format:** Sync messages are raw binary (`Uint8Array`/`Vec<u8>`). Each message contains up to three components:

- **`Have`**: A Bloom filter (4000 bits, 10 bits per item, 7 hashes, ~1% false positive rate) summarizing which changes the sender already has
- **`Want`**: Specific change hashes the sender needs
- **`Changes`**: Actual change objects to apply

The protocol converges in typically 3-4 rounds for simple documents. After the Bloom filter step, a second reconciliation phase transfers items missed due to false positives. Because Automerge commits form a DAG with parent references, the system always knows when the commit set is incomplete.

**SyncState persistence:** The `SyncState` is document-independent and can be persisted across sessions via `encodeSyncState()`/`decodeSyncState()`. Persisting sync state is critical — it prevents redundant data transfer on reconnection. The state tracks `shared_heads`, `their_heads`, `their_need`, `their_have` (Bloom filters), `sent_hashes`, and `their_capabilities` per peer.

**Transport-agnostic:** The sync protocol works over WebSockets, WebRTC, HTTP, Bluetooth, QUIC, or even unidirectional transfer (email attachment, USB drive).

Sources: [[https://automerge.org/automerge/automerge/sync/index.html][Automerge Rust sync docs]], [[https://posit-dev.github.io/automerge-r/articles/sync-protocol.html][Automerge sync protocol tutorial]], [[https://github.com/hoytech/automerge-poison][Bloom filter analysis]]

### Automerge P2P Sync Providers

Unlike Yjs, which has mature, well-maintained providers (`y-webrtc`, `y-websocket`, `y-indexeddb`, `y-redis`), Automerge's P2P ecosystem is **fragmented**:

| Provider | Transport | Status |
|---|---|---|
| `@automerge/automerge-repo-network-websocket` | WebSocket (client + server) | Official, maintained |
| `@automerge/automerge-repo-network-broadcastchannel` | BroadcastChannel (same-browser) | Official, maintained |
| **MPL** (`github.com/automerge/mpl`) | WebRTC P2P | Standalone project, 284 stars |
| **Hypermerge** (`github.com/automerge/hypermerge`) | hypercore/DAT/Hyperswarm | **Archived** (Jan 2023) |
| **iroh-automerge** | QUIC via iroh | Working example, third-party |

**MPL (Magic Persistence Layer)** is the closest thing to Yjs's `y-webrtc` provider. It provides WebRTC-based P2P document sync with a Redux-like `Store` API, automatic peer group discovery by document ID, and built-in actions for `NEW_DOCUMENT`, `OPEN_DOCUMENT`, and `FORK_DOCUMENT`. However, it is a standalone project, not officially maintained by the core Automerge team.

**Hypermerge** was Automerge's original libp2p/DAT integration but is deprecated. Its README explicitly warns: "Hypermerge is deprecated. This library is no longer maintained and uses an ancient and slow version of Automerge."

**iroh-automerge** demonstrates Automerge sync over iroh's QUIC streams — length-prefixed binary frames with bidirectional sync loops. This is the most promising path for P2P file sync.

Sources: [[https://automerge.org/docs/reference/repositories/networking][Automerge networking docs]], [[https://github.com/automerge/mpl][MPL GitHub]], [[https://github.com/automerge/hypermerge][Hypermerge (archived)]], [[https://docs.iroh.computer/protocols/automerge][iroh-automerge]]

### Yjs Sync Protocol

Yjs uses a **state vector-based sync protocol** defined in the `@y/protocols` package with binary wire formats:

| Message | ID | Encoding |
|---|---|---|
| `SyncStep1` | 0 | `varUint(0) • varBuffer(stateVector)` |
| `SyncStep2` | 1 | `varUint(1) • varBuffer(documentUpdate)` |
| `Update` | 2 | `varUint(2) • varBuffer(documentUpdate)` |

**Handshake process:**
1. Each peer sends `SyncStep1` containing its state vector (`Y.encodeStateVector(doc)`)
2. On receiving `SyncStep1`, reply with `SyncStep2` containing missing updates (`Y.encodeStateAsUpdate(doc, stateVector)`)
3. After receiving `SyncStep2`, the local document is up to date
4. Subsequent changes propagate as `Update` messages

In P2P topologies, **both peers send SyncStep1** upon connecting. In client-server topologies, only the client initiates. The **Awareness protocol** (message type 1) propagates ephemeral per-client state (cursor position, username, selection) using a state-based CRDT.

Yjs implements an adaptation of the **YATA CRDT** ("Near Real-Time Peer-to-Peer Shared Editing on Extensible Data Types"), which uses a linked-list-based structure with compound representations for efficiency. It provides shared types: `Y.Text`, `Y.Map`, `Y.Array`, and `Y.XmlElement`.

Sources: [[https://github.com/yjs/y-protocols/blob/master/PROTOCOL.md][y-protocols PROTOCOL.md]], [[https://docs.yjs.dev/api/internals][Yjs Internals]]

### Yjs P2P Providers

Yjs has a **mature provider ecosystem** with well-maintained options for multiple transports:

| Provider | Transport | Status |
|---|---|---|
| `y-webrtc` | WebRTC | Active, 586 stars |
| `y-websocket` | WebSocket | Active, official |
| `y-indexeddb` | IndexedDB (persistence) | Active, official |
| `y-redis` | Redis (server persistence) | Active |
| `y-libp2p` | libp2p | Community |
| `y-webrtc-trystero` | WebRTC + Trystero | Active |
| `Matrix-CRDT` | Matrix (federated) | Active |

**y-webrtc** is the most widely used P2P connector. It uses public signaling servers (`wss://signaling.yjs.dev` in EU/US) and `simple-peer` internally. Tabs within the same browser share data via BroadcastChannel. Some users report reliability issues between browsers.

**y-indexeddb** is the primary browser persistence layer: `new IndexeddbPersistence(docName, ydoc)` fires a `"synced"` event when loaded. Content created offline syncs when the peer reconnects.

Sources: [[https://github.com/yjs/y-webrtc][y-webrtc]], [[https://docs.yjs.dev/ecosystem/database-provider/y-indexeddb][y-indexeddb Docs]]

### Automerge + p2panda Integration

**p2panda** (created by the p2panda team, with Martin Kleppmann as a prominent figure in the local-first software movement) does **not** have a native, built-in Automerge integration. However, p2panda's December 2024 rewrite is explicitly designed to be **CRDT-agnostic**:

> "We wanted to offer you the option of combining all p2panda modules with Automerge, Yjs or any other CRDT of your choice."

p2panda provides:
- **Modular Rust crates** for networking, sync, discovery, gossip, blobs, authentication, ordering, deletion, access control, and encryption
- **iroh-based networking** for P2P connectivity (STUN/ICE, QUIC, TLS)
- **Data-type agnostic payloads** — raw bytes that can wrap Automerge documents
- **Append-only log data type** for ordering and persistence
- **Group encryption** with planned UCAN-based access control

To use Automerge with p2panda, you would use p2panda's networking/sync/discovery modules for P2P connectivity and serialize Automerge sync messages as the payload. This is similar to the `iroh-automerge` example but with p2panda's additional features (gossip, access control, encryption).

Sources: [[https://p2panda.org/2024/12/06/p2panda-release.html][p2panda 2024 release]], [[https://crates.io/crates/p2panda-net][p2panda-net crate]]

### Branching/Merging: Automerge vs Yjs

**Automerge — Git-like DAG model:**

Every edit creates a "change" object (analogous to a Git commit) with a unique hash, dependencies (parent change hashes), timestamp, actor ID, and sequence number. This creates a **Directed Acyclic Graph (DAG)** of changes with:

- **Fork**: Clone a document, make independent changes
- **Merge**: Automatically merge any two divergent documents
- **History**: View the document at any point in history
- **Diff**: Compare any two versions
- **Cherry-pick**: Apply specific changes selectively

**Yjs — Single-document linear model:**

Yjs uses a single-document model with a linear timeline. Every change is immediately merged into the document. Yjs provides:
- **Y.UndoManager** for undo/redo
- **No built-in branching** — all edits go into one document
- **No built-in history browsing** — cannot view the document at a past point in time
- **Garbage collection** — old operations can be cleaned up

**For P2P file sync, Automerge's branching model is superior** because:
1. Devices work fully offline, creating divergent histories
2. When they reconnect, any two states merge automatically
3. Full history enables audit trails and version comparison
4. No central server needed to maintain a single timeline

| Feature | Automerge | Yjs |
|---|---|---|
| Branching | Native (clone + merge) | Not supported |
| History | Full DAG, view any version | Limited (undo/redo only) |
| Divergent sync | Excellent | Good (single timeline) |
| Conflict resolution | Automatic (LWW for scalars, merge for collections) | Automatic (YATA algorithm) |
| Garbage collection | No (stores full history) | Yes (GC mode available) |

Sources: [[https://automerge.org/docs/hello][Automerge docs]], [[https://www.pkgpulse.com/guides/yjs-vs-automerge-vs-loro-crdt-libraries-2026][Yjs vs Automerge comparison]]

### Performance Comparison

**Bundle size:**

| Library | Bundle Size (min+gz) | Language |
|---|---|---|
| Yjs | ~18 kB | Pure JavaScript |
| Automerge | ~320 kB (WASM) | Rust compiled to WASM |
| Loro | ~180 kB (WASM) | Rust compiled to WASM |

Automerge's WASM compilation means a **~18x larger bundle** than Yjs and a one-time initialization cost of ~50ms.

**Performance benchmarks (260K-character document):**

| Operation | Yjs | Automerge | Loro |
|---|---|---|---|
| Apply 260K edits | 430ms | 680ms | 290ms |
| Encode document | 4ms | 12ms | 2ms |
| Decode document | 8ms | 45ms | 5ms |
| Document size (encoded) | 160 kB | 250 kB | 68 kB |
| Memory (loaded doc) | 28 MB | 41 MB | 15 MB |

**Automerge 3.0 memory breakthrough:** Automerge 3.0 achieved a **10x+ reduction in memory usage** by using the compressed columnar format at runtime (previously only used at rest):

- **Automerge 2**: Pasting Moby Dick consumed **700 MB** of memory
- **Automerge 3**: Same operation consumes only **1.3 MB**

Load times also improved dramatically (one document that took 17 hours to load now loads in 9 seconds).

**Ecosystem size:**

| Metric | Yjs | Automerge |
|---|---|---|
| Weekly npm downloads | ~920K | ~85K |
| GitHub stars | 17K | 4.2K |

Sources: [[https://www.pkgpulse.com/guides/yjs-vs-automerge-vs-loro-crdt-libraries-2026][CRDT comparison benchmarks]], [[https://automerge.org/blog/automerge-3][Automerge 3.0 release]]

### Offline-First Support

Automerge is **designed from the ground up for local-first software**:

> "Automerge is designed for creating local-first software, i.e. software that treats a user's local copy of their data (on their own device) as primary, rather than centralising data in a cloud service."

**How it works:**
1. **Local storage**: Every change stored locally via `StorageAdapter` (IndexedDB in browsers, filesystem in Node.js)
2. **Local editing**: Users read and modify data even while offline
3. **Deferred sync**: When a network connection becomes available, Automerge figures out which changes need syncing
4. **Automatic merge**: Concurrent changes from different devices merge automatically — no changes lost
5. **Multiple sync paths**: Works with client/server, P2P, or unidirectional transfer

**Storage adapters:**

| Adapter | Package | Platform |
|---|---|---|
| IndexedDB | `@automerge/automerge-repo-storage-indexeddb` | Browser |
| NodeFS | `@automerge/automerge-repo-storage-nodefs` | Node.js |
| Postgres | `automerge-repo-storage-postgres` | Server |

Storage adapters are safe for concurrent use — multiple browser tabs or processes can use the same storage, and changes merge on refresh.

The Automerge team distinguishes **offline-first** (app works without internet, syncs when connected) from **local-first** (the local copy is the primary source of truth; the cloud is optional). Automerge supports both, but is specifically designed for the stronger local-first guarantee.

Yjs has comparable offline support through `y-indexeddb` (browser) and `y-leveldb` (Node.js, archived). Both CRDTs are equally capable offline — the difference is that Automerge's branching model handles divergent offline histories more naturally.

Sources: [[https://automerge.org/docs/hello][Automerge docs]], [[https://automerge.org/docs/reference/repositories/storage][Storage adapters]], [[https://martin.kleppmann.com/papers/local-first.pdf][Local-First Software paper]]

### Automerge Peritext — Rich Text CRDT

**Peritext** is a novel CRDT algorithm for **rich-text collaboration**, developed by Ink & Switch (the Automerge team). It solves the problem of merging concurrent edits to rich-text documents (with formatting like bold, italic, links, comments) while preserving user intent.

**Key design:**
1. **Plain text CRDT base**: Uses RGA/Causal Trees for the underlying text sequence
2. **Mark operations**: Formatting represented as `addMark` and `removeMark` operations anchored to character positions using stable `opId` references (not indexes)
3. **Anchor points**: Each character has two anchor points (before/after) where formatting spans attach
4. **Last-write-wins**: Conflicting marks of the same type resolved by comparing `opId` (Lamport timestamps)
5. **Growing vs non-growing marks**: Bold/italic spans grow when text is inserted at boundaries; links/comments do not

**Automerge integration:**
- Rich text support officially released in **Automerge 2.2** with `Automerge.mark()`, `Automerge.marks()`, and `Automerge.spans()` APIs
- **ProseMirror binding** as the reference implementation
- In **Automerge 3.0**, the text API was cleaned up: collaborative text is now the default (plain JavaScript strings), non-collaborative strings use `ImmutableString`

**Limitations:** Currently handles **inline formatting only** (bold, italic, links, comments, text color). Block elements (headings, bullet points, block quotes, tables) are planned for a future release.

Yjs handles rich text through `Y.Text` with external bindings (Quill, Slate, ProseMirror, Tiptap, CodeMirror). Yjs has a broader ecosystem of editor bindings but lacks Automerge's Peritext algorithm for sophisticated mark handling.

Sources: [[https://www.inkandswitch.com/peritext][Peritext paper]], [[https://automerge.org/blog/rich-text][Automerge 2.2 rich text]], [[https://automerge.org/docs/reference/documents/rich-text][Rich text API]]

### Binary File Handling

**This is a critical distinction between the CRDT stack and Seafile.**

**Automerge and Yjs** are designed for **structured, text-based data**. They excel at collaborative editing of documents, JSON, and other structured formats. Binary files (images, PDFs, executables, databases) cannot be meaningfully synced at the CRDT level — they would need to be stored as opaque blobs, which defeats the purpose of CRDT-based sync. For binary files, you would need a separate blob storage layer (like iroh-blobs or a traditional file store).

**Seafile** handles binary files natively through its block-level storage. Any file type can be synced, versioned, and delta-synced efficiently. This makes Seafile suitable for general-purpose file sync (code repos, media files, databases, etc.) while the CRDT stack is best suited for text-based collaborative editing.

### Always-On Peer Pattern

Automerge's sync server model treats the server as **"a very available peer"** — it runs the exact same version of Automerge as clients:

> "When you configure automerge to run on an internet server, listen for connections, and store data on disk, then we call that a 'sync server'. There's nothing really special about a sync server: it runs the exact same version of Automerge as you run locally."

**Self-hosted sync server:** The `automerge-repo-sync-server` package provides a simple Express-based sync server:
- Runs via `npx @automerge/automerge-repo-sync-server`
- Configurable via `PORT` and `DATA_DIR` environment variables
- Available as a Docker image: `ghcr.io/automerge/automerge-repo-sync-server:main`
- Implements the automerge-repo WebSocket protocol

**Public community sync server:** Available at `wss://sync.automerge.org` for prototyping.

**WebSocket wire protocol:**
1. **Handshake**: Each peer sends a `join` message with `senderId` and metadata
2. **Storage ID**: Peers may optionally advertise a `storageId` tied to persistent storage
3. **Sync loop**: Peers exchange sync messages and ephemeral messages

The server acts as a **mesh relay** — when a client connects, the server shares all documents it knows about, and the client shares its documents. The server stores documents persistently, surviving client disconnections.

**For P2P file sync:** The sync server is **optional** — Automerge works perfectly in pure P2P mode (via MPL or iroh). The server serves as a bootstrap peer, persistent relay, and backup.

Sources: [[https://automerge.org/docs/tutorial/network-sync][Sync server tutorial]], [[https://github.com/automerge/automerge-repo-sync-server][Sync server repo]]

---

## Part 2: The P2P CRDT Stack (Automerge + p2panda + iroh) vs Seafile

### Seafile Architecture

Seafile consists of three main server components:

- **Seahub**: Django-based web interface
- **Seafile Server (seaf-server)**: File server handling storage, block management, and sync protocol
- **Ccnet Server**: Messaging/networking service (merged into seafile-server in v8.0, but its database remains required)

Each component requires its own separate database (`ccnet_db`, `seafile_db`, `seahub_db`). Clients communicate via a proprietary sync protocol over HTTP.

**Block-level delta sync:** Seafile stores files in a proprietary block-based format (similar to Git's object model). Files are split into blocks, and only changed blocks transfer during sync — not entire files. This enables:
- Efficient deduplication (identical blocks stored once)
- Fast delta sync (only changed chunks transferred)
- Efficient versioning
- Resumable interrupted transfers

Sources: [[https://homeserver.page/app/seafile][Seafile overview]], [[https://forum.seafile.com/t/seafile-block-deduplication/11774][Block deduplication]], [[https://github.com/haiwen/seafile/issues/1311][Block sync issue]]

### Seafile Offline Access

Seafile offers **two distinct client models**:

**Seafile Sync Client (traditional):**
- Keeps **full local copies** of synced libraries on disk
- Full offline access to all synced content
- Selective sync allows choosing which libraries/folders to keep locally

**SeaDrive Client (virtual drive, newer):**
- Uses a **VFS (Virtual File System)** via FUSE (Linux) or Dokany (Windows)
- Files appear as a virtual drive but are **not fully downloaded** — cached on demand
- Configurable cache modes: **Full** (cache contents locally) or **Names-only** (only cache names, download on access)
- Cache size is configurable with a limit
- Files work offline only if previously cached/accessed

**Key limitation:** Offline access requires locally stored data. There is no pure RAM-only cache mode.

Sources: [[https://forum.seafile.com/t/caching-files-to-local-disk/2382][Caching discussion]], [[https://haiwen.github.io/seafile-user-manual/drive_client/drive_client_for_linux][SeaDrive Linux docs]]

### Conflict Resolution: Seafile vs CRDT

**Seafile — Three-way merge at file level:**

Seafile uses a three-way merge algorithm (comparing Base/ancestor, Local/head, and Remote trees) to automatically reconcile non-conflicting changes. When conflicts occur (both sides modified the same file differently):

- The **first version synced to the cloud remains unchanged**
- The second version is **renamed to a conflict file**: `filename (SFConflict username@domain.com YYYY-MM-DD-HH-MM-SS).extension`
- The user must **manually resolve** the conflict
- There is **no automatic merge** of conflicting file contents — it is a "winner-takes-all" approach

**CRDT-based resolution:**

CRDTs automatically merge concurrent edits at the **character/operation level**, producing a single consistent result without conflicts. No manual resolution needed.

| Aspect | Seafile | CRDT (Automerge/Yjs) |
|---|---|---|
| Resolution level | File-level | Character/operation-level |
| Automatic merge | No (conflict files) | Yes (always converges) |
| Manual intervention | Required for conflicts | Never required |
| History | Version snapshots | Full operation DAG |

Sources: [[https://help.seafile.com/syncing_client/file_conflicts/][Seafile conflicts]], [[https://deepwiki.com/haiwen/seafile-server/4.3-merge-and-conflict-resolution][Seafile merge system]]

### Real-Time Collaboration

**Seafile's collaboration is browser-only and depends on external office suites:**

- **Collabora Online**: Real-time co-editing in the browser (LibreOffice-based). Develop edition limited to 5 concurrent sessions.
- **ONLYOFFICE**: Real-time co-editing with both browser and desktop editors. Desktop editors connect to Seafile for editing server-stored files.

**Key limitations:**
- Collaboration is **limited to supported Office file types**
- Interface between Seafile and editors is via WebDAV
- **No simultaneous collaborative editing** through the desktop sync client — uses file-locking instead
- No native collaborative editing for code, markdown, or other file types

**CRDT stack:** Real-time collaboration is **built into the sync protocol**. Any data type supported by the CRDT (text, maps, arrays) can be collaboratively edited in real-time, regardless of file format.

Sources: [[https://forum.seafile.com/t/seafile-collabora/918][Seafile Collabora]], [[https://www.onlyoffice.com/blog/2020/04/integrating-onlyoffice-in-seafile-within-ucs][ONLYOFFICE integration]]

### Data Sovereignty

**Seafile — Self-hosted client-server:**
- Available as **Community Edition** (free, open-source) and **Professional Edition** (paid)
- Deploy on your own server, enterprise data center, or private cloud
- Full control over data, storage, and encryption keys
- Client-side end-to-end encryption via "encrypted libraries" (optional — server never sees the decryption key)
- Supports LDAP/AD, SAML 2.0, OAuth, Shibboleth for authentication
- **All sync flows through the central server** — not P2P

**P2P CRDT stack — Fully distributed:**
- No central server required
- Peers sync directly via P2P (iroh QUIC connections)
- Data lives on each peer's device
- End-to-end encryption via QUIC/TLS
- No single point of failure or data collection
- Optional "always-on" shared nodes for relay (self-hostable)

| Aspect | Seafile | P2P CRDT Stack |
|---|---|---|
| Architecture | Central server (client-server) | Peer-to-peer, no central server |
| Data location | Server + local copies | Each peer's device |
| Encryption | Optional (encrypted libraries) | Built-in (QUIC/TLS) |
| Single point of failure | Yes (server) | No (distributed) |
| Self-hostable | Yes | Yes (relays optional) |

Sources: [[https://www.seafile.com/en/product/seafile_on_premise][Seafile on-premise]], [[https://help.seafile.com/security_and_encryption/use_encrypted_libraries][Encrypted libraries]]

### Setup Complexity

**Seafile requires a non-trivial stack:**

**Minimum hardware:** 2 cores CPU (>2GHz), 2GB RAM

**Required software components:**
1. **MySQL or MariaDB** — three separate databases
2. **Memcached or Redis** — for caching
3. **Python 3** with virtual environment and ~20 pip packages (Django 4.2, mysqlclient, pymysql, pillow, pylibmc, captcha, markupsafe, jinja2, sqlalchemy, psd-tools, django-pylibmc, django_simple_captcha, djangosaml2, pysaml2, pycryptodome, cffi, lxml, python-ldap, gevent)
4. **System packages**: libmemcached-dev, libmysqlclient-dev, ldap-utils, libldap2-dev, build-essential, pkg-config
5. **Reverse proxy** (recommended): Nginx or Apache for HTTPS termination
6. **Docker** (alternative): Docker Compose deployment available

**P2P CRDT stack:** Typically requires **no server setup, no database, no reverse proxy** — just install the client on each peer. Optional shared nodes for relay are simple to deploy.

Sources: [[https://manual.seafile.com/12.0/setup_binary/installation_ce][Seafile installation]], [[https://www.tecmint.com/install-seafile-in-linux][Seafile setup guide]]

### Sync Latency

**Seafile — Polling-based, not real-time:**
- Client automatically detects local changes via filesystem watch
- Falls back to **periodic polling** for network shares
- Default polling interval is approximately **30 seconds**
- Some configurations report refresh intervals of ~5 minutes
- No WebSocket-based real-time push for file changes
- Sync throughput: 80-100 MB/s on local networks; high-latency connections (50-350ms RTT) can cause significant slowdowns
- Each block may make a separate HTTP request, adding overhead

**CRDT stack — Near-instant:**
- Uses WebSocket or QUIC persistent connections for near-instant propagation
- Operates at the operation/character level
- Latency measured in **milliseconds**, not seconds
- Seafile's 30-second polling interval is **orders of magnitude slower**

| Metric | Seafile | CRDT Stack |
|---|---|---|
| Sync mechanism | HTTP polling | Persistent connection (WebSocket/QUIC) |
| Default latency | ~30 seconds | Milliseconds |
| Granularity | File/block level | Operation/character level |
| Real-time push | No | Yes |

Sources: [[https://haiwen.github.io/seafile-user-manual/syncing_client/setting_sync_interval][Sync interval docs]], [[https://forum.seafile.com/t/increase-sync-frequency-to-more-often-than-every-30-seconds/14863][Sync frequency discussion]]

### Simultaneous Editing

**Seafile does NOT support true simultaneous editing via the sync client:**

- If two users edit the same file simultaneously, the **first to sync wins**
- The second version becomes a **conflict file**
- **File Locking (Professional Edition only)**: Auto-locking when a user opens a Microsoft Office file; other users see a red "stop sign" and the file is read-only. Manual locking available via right-click. Known issues with auto-locking reliability in recent versions.
- **Browser-based editing** (Collabora/ONLYOFFICE): True simultaneous co-editing supported, limited to Office file formats

**CRDT stack:** Natively supports simultaneous editing by any number of users with automatic conflict-free merging. No locking needed — concurrent edits merge automatically at the operation level.

Sources: [[https://help.seafile.com/sharing_collaboration/file_locking][File locking]], [[https://forum.seafile.com/t/multiple-edits-a-file-at-the-same-time/5217][Simultaneous editing discussion]]

### Seafile Linux Client Quality and Resource Usage

**Seafile Sync Client (Linux):**
- Available as **AppImage** (since v9.0.7), **Flatpak**, and **Snap**
- Built with Qt for the GUI
- Cross-platform (Linux, Windows, macOS, iOS, Android)

**SeaDrive Client (Linux):**
- Available as **AppImage** (GUI and CLI versions)
- Requires **FUSE version 2**
- Provides virtual drive functionality

**Reported resource issues:**
- **High CPU**: Client pegging a CPU core when editing files (GitHub issue from 2014, still referenced)
- **High memory**: SeaDrive reported consuming 95% of 20GB RAM due to I-intensive FUSE operations
- **High power consumption**: Idle power consumption jumping from normal to 44 Watts with Seafile client running
- **Server-side CPU**: Uploading many large files causes high server CPU load from hash calculation and block chunking
- **Windows client**: Constant 15-20% CPU usage even when idle

**Quality concerns:**
- Linux client update cadence is slow — client version 9.0.8 while server is at 11.x
- FUSE-based SeaDrive can have memory growth issues
- Functional but not as polished as commercial alternatives

Sources: [[https://forum.seafile.com/t/seafile-client-9-0-7-released-with-appimage-for-linux/22135][Client 9.0.7 release]], [[https://forum.seafile.com/t/why-is-seadrive-using-so-much-memory/14907][Memory usage]], [[https://forum.seafile.com/t/high-power-consumtion-of-seafile-client-on-linux/4606][Power consumption]], [[https://github.com/haiwen/seafile-client/issues/220][CPU issue]]

### Additional Considerations

**Scalability:** Seafile scales vertically (bigger server, more storage). The P2P CRDT stack scales horizontally — each peer adds capacity. However, P2P sync complexity grows with the number of peers (O(n²) connections in a full mesh), which may require shared nodes or relay infrastructure at scale.

**Cost:** Seafile Community Edition is free; Professional Edition costs money (per-server licensing). The P2P CRDT stack is entirely free and open-source, though self-hosted relays add infrastructure cost.

**Mobile support:** Seafile has mature iOS and Android clients. The P2P CRDT stack's mobile support depends on the implementation — iroh has Swift and Kotlin bindings (since v1.0), making mobile integration feasible but not yet production-ready for most use cases.

**Backup and disaster recovery:** Seafile has built-in versioning and backup mechanisms. The P2P CRDT stack relies on each peer maintaining its own copy — if all peers lose data, there is no central backup unless an "always-on" shared node is configured.

**Access control:** Seafile has granular permissions (read, write, admin per folder) with LDAP/AD/SAML integration. The p2panda stack has planned UCAN-based access control, but this is not yet fully implemented.

---

## Part 3: The iroh Networking Layer

iroh is the P2P networking foundation used by p2panda. Understanding iroh is essential to evaluating the P2P CRDT stack.

### What is iroh?

**iroh** is a Rust-based networking library enabling direct P2P connections between devices, identified by Ed25519 public keys rather than IP addresses. Tagline: "IP addresses break, dial keys instead."

- **Architecture**: Pool of encrypted QUIC connections with pluggable application-level protocols (iroh-blobs, iroh-gossip, iroh-docs)
- **Protocol negotiation**: ALPN TLS extension during QUIC handshake
- **Current version**: **iroh 1.0** released **June 15, 2026** — first stable release after 65 pre-releases over 4+ years
- **Production use**: Running on millions of devices (DeltaChat, UniClipboard, and others)
- **Organization**: Developed by **n0-computer** (n0), open source (Apache-2.0)
- **FFI bindings**: Python, Node.js, Swift, Kotlin (since 1.0)

### NAT Traversal

iroh's NAT traversal is a core strength, built on QUIC with tight relay integration:

1. **Initial contact through relay**: Both peers connect to a shared relay server
2. **Sharing connection info**: Peers exchange public IPs, ports, and local addresses via relay
3. **Simultaneous outbound connection (holepunching)**: Both nodes send UDP datagrams simultaneously; firewalls recognize incoming packets as matching outbound traffic
4. **Fallback to relay**: If NAT traversal fails, traffic routes through the relay

**Key details:**
- **~90% success rate** in real-world network configurations
- **Moved from STUN to QUIC Address Discovery (QAD)** since v0.90 — encrypted, reliable, with congestion control
- Deterministic: if it works once, it continues working with stable networking

### Transport and Peer Discovery

**Transport:** Built entirely on **QUIC** (RFC 9000). Uses `noq` (n0's fork of `quinn`) for multipath support. TLS with raw Ed25519 public keys as identity — no self-signed certificates needed.

**Peer discovery:**
- **DNS/Pkarr** (default): Endpoints publish signed records mapping Endpoint ID to relay URL via HTTPS PUT to `dns.iroh.link`; resolution via standard DNS lookup
- **mDNS**: Local network discovery, no relay needed
- **Mainline DHT (BitTorrent)**: Fully decentralized pkarr-based publish/lookup
- **Custom**: `AddressLookup` trait allows custom implementations

### Relay Network

- **Public relays**: Hardcoded set provided by n0.computer, free to use, rate-limited, no guarantees
- **Dedicated relays**: Provisioned exclusively for your project, authenticated, with uptime guarantees
- **Self-hosting**: Fully supported via `iroh-relay` crate, CLI binary with TOML config
- **Stateless**: Relays don't store application data, just facilitate connections
- **End-to-end encrypted**: Relay servers cannot read any traffic

### iroh vs libp2p

| Aspect | iroh | libp2p |
|---|---|---|
| Design goal | Maximize effectiveness (reliable connections) | Minimize central points of failure |
| Connection guarantee | "Almost always get a connection" | "Much more dependent on network conditions" |
| NAT traversal | ~90% success rate | ~70% hole punching success |
| Simplicity | Streamlined, fewer options | Extensive configurability, steep learning curve |
| Transport | QUIC only | Multiple (TCP, WebSocket, QUIC, etc.) |
| Addressing | Ed25519 public key | PeerId (multi-format) |
| Scope | Narrow: connections + pluggable protocols | Broad: DHT, pubsub, file transfer built-in |

> "Libp2p is built to keep its reliance on central points of failure at an absolute minimum, which comes at the cost of effectiveness. Iroh is built to maximize effectiveness, which comes at the cost of a little centralization."

Sources: [[https://github.com/n0-computer/iroh][iroh GitHub]], [[https://www.iroh.computer/blog/v1][iroh 1.0 release]], [[https://www.iroh.computer/blog/the-road-to-iroh-1-0][Road to iroh 1.0]], [[https://www.iroh.computer/blog/comparing-iroh-and-libp2p][iroh vs libp2p]], [[https://docs.iroh.computer/concepts/nat-traversal][NAT traversal docs]], [[https://docs.iroh.computer/concepts/relays][Relay docs]]

---

## Part 4: Summary Comparison

### Architecture Comparison

| Aspect | Automerge + p2panda + iroh | Yjs + p2panda + iroh | Seafile |
|---|---|---|---|
| **Architecture** | P2P, no central server | P2P, no central server | Central server (client-server) |
| **CRDT model** | Git-like DAG, branching/merging | Single-document, linear timeline | N/A (block-level sync) |
| **Sync protocol** | Bloom filter set-reconciliation | State vector delta sync | HTTP polling, block-level delta |
| **Sync latency** | Milliseconds (QUIC) | Milliseconds (QUIC/WebSocket) | ~30 seconds (polling) |
| **Conflict resolution** | Automatic (CRDT merge) | Automatic (YATA merge) | File-level (conflict files) |
| **Simultaneous editing** | Native, unlimited | Native, unlimited | Browser-only (Office files) |
| **Offline support** | Full local-first | Full local-first | Full copies or cached (SeaDrive) |
| **History** | Full DAG, any version | Undo/redo only | Version snapshots |
| **Branching** | Native | Not supported | Not supported |
| **Bundle size** | ~320 kB (WASM) | ~18 kB (JS) | N/A (native client) |
| **P2P providers** | Fragmented (MPL, iroh example) | Mature (y-webrtc, y-libp2p) | N/A |
| **Setup complexity** | Low (install client per peer) | Low (install client per peer) | High (MySQL + Memcached + Python + 20+ packages) |
| **Data sovereignty** | Fully distributed | Fully distributed | Self-hosted central server |
| **NAT traversal** | ~90% (iroh QUIC) | ~90% (iroh QUIC) | N/A (server-initiated) |
| **Rich text** | Peritext (inline formatting) | Y.Text + bindings | Browser editors (Collabora/ONLYOFFICE) |
| **Linux client** | Depends on implementation | Depends on implementation | AppImage/Flatpak/Snap, resource-heavy |
| **Binary file support** | No (needs separate blob layer) | No (needs separate blob layer) | Yes (block-level delta sync) |
| **Mobile support** | Feasible (Swift/Kotlin bindings) | Feasible (JS/WASM) | Mature (iOS + Android clients) |
| **Access control** | Planned (UCAN-based) | Depends on implementation | Mature (granular permissions, LDAP/SAML) |
| **Ecosystem maturity** | Growing (85K npm/wk) | Mature (920K npm/wk) | Mature (10+ years) |

### When to Choose Each Approach

**Choose Automerge + p2panda + iroh when:**
- Version history and branching are important features
- You need Git-like semantics (fork, merge, cherry-pick, diff)
- Full audit trails and version comparison matter
- You want a truly distributed architecture with no central server
- You can tolerate a larger bundle size (~320 kB WASM)
- You're comfortable building P2P networking on top of iroh/p2panda
- Your primary use case is text/structured data collaboration

**Choose Yjs + p2panda + iroh when:**
- Real-time collaboration is the primary use case
- You need a mature ecosystem with many editor bindings
- Bundle size matters (18 kB vs 320 kB)
- You don't need branching or full history
- You want the most battle-tested CRDT for collaboration
- You want mature P2P providers (y-webrtc) as a fallback
- Your primary use case is text/structured data collaboration

**Choose Seafile when:**
- You need a proven, production-ready file sync solution
- Your team is comfortable managing a central server
- You need Office document collaboration (Collabora/ONLYOFFICE)
- Block-level delta sync for large binary files is important
- You need enterprise features (LDAP, SAML, audit logs)
- You don't need real-time collaborative editing of arbitrary file types
- You can accept 30-second sync latency and file-level conflict resolution
- You need mobile clients (iOS/Android)
- Your primary use case is general-purpose file sync (any file type)

**Hybrid approach:** For teams that need both binary file sync and real-time text collaboration, consider using Seafile for binary files and a CRDT stack for collaborative text editing. This is the most pragmatic approach for mixed workloads.

---

## Sources

[1] Automerge Rust sync docs: https://automerge.org/automerge/automerge/sync/index.html
[2] Automerge sync protocol tutorial (R): https://posit-dev.github.io/automerge-r/articles/sync-protocol.html
[3] Bloom filter analysis: https://github.com/hoytech/automerge-poison
[4] Automerge networking docs: https://automerge.org/docs/reference/repositories/networking
[5] MPL (Magic Persistence Layer): https://github.com/automerge/mpl
[6] Hypermerge (archived): https://github.com/automerge/hypermerge
[7] iroh-automerge integration: https://docs.iroh.computer/protocols/automerge
[8] p2panda 2024 release (CRDT-agnostic): https://p2panda.org/2024/12/06/p2panda-release.html
[9] p2panda-net crate: https://crates.io/crates/p2panda-net
[10] Automerge docs (local-first design): https://automerge.org/docs/hello
[11] Automerge storage adapters: https://automerge.org/docs/reference/repositories/storage
[12] Local-First Software paper: https://martin.kleppmann.com/papers/local-first.pdf
[13] Yjs vs Automerge vs Loro comparison: https://www.pkgpulse.com/guides/yjs-vs-automerge-vs-loro-crdt-libraries-2026
[14] Automerge 3.0 release (memory improvements): https://automerge.org/blog/automerge-3
[15] Peritext rich-text CRDT paper: https://www.inkandswitch.com/peritext
[16] Automerge 2.2 rich text release: https://automerge.org/blog/rich-text
[17] Automerge rich text API: https://automerge.org/docs/reference/documents/rich-text
[18] Automerge sync server tutorial: https://automerge.org/docs/tutorial/network-sync
[19] Automerge sync server repo: https://github.com/automerge/automerge-repo-sync-server
[20] y-protocols PROTOCOL.md: https://github.com/yjs/y-protocols/blob/master/PROTOCOL.md
[21] Yjs Internals: https://docs.yjs.dev/api/internals
[22] y-webrtc: https://github.com/yjs/y-webrtc
[23] y-indexeddb Docs: https://docs.yjs.dev/ecosystem/database-provider/y-indexeddb
[24] Seafile overview: https://homeserver.page/app/seafile
[25] Seafile block deduplication: https://forum.seafile.com/t/seafile-block-deduplication/11774
[26] Seafile block sync issue: https://github.com/haiwen/seafile/issues/1311
[27] Seafile caching discussion: https://forum.seafile.com/t/caching-files-to-local-disk/2382
[28] SeaDrive Linux docs: https://haiwen.github.io/seafile-user-manual/drive_client/drive_client_for_linux
[29] Seafile conflict resolution: https://help.seafile.com/syncing_client/file_conflicts/
[30] Seafile merge system: https://deepwiki.com/haiwen/seafile-server/4.3-merge-and-conflict-resolution
[31] Seafile Collabora integration: https://forum.seafile.com/t/seafile-collabora/918
[32] ONLYOFFICE Seafile integration: https://www.onlyoffice.com/blog/2020/04/integrating-onlyoffice-in-seafile-within-ucs
[33] Seafile on-premise: https://www.seafile.com/en/product/seafile_on_premise
[34] Seafile encrypted libraries: https://help.seafile.com/security_and_encryption/use_encrypted_libraries
[35] Seafile installation guide: https://manual.seafile.com/12.0/setup_binary/installation_ce
[36] Seafile setup guide: https://www.tecmint.com/install-seafile-in-linux
[37] Seafile sync interval docs: https://haiwen.github.io/seafile-user-manual/syncing_client/setting_sync_interval
[38] Seafile sync frequency discussion: https://forum.seafile.com/t/increase-sync-frequency-to-more-often-than-every-30-seconds/14863
[39] Seafile file locking: https://help.seafile.com/sharing_collaboration/file_locking
[40] Seafile simultaneous editing: https://forum.seafile.com/t/multiple-edits-a-file-at-the-same-time/5217
[41] Seafile Linux client release: https://forum.seafile.com/t/seafile-client-9-0-7-released-with-appimage-for-linux/22135
[42] SeaDrive memory usage: https://forum.seafile.com/t/why-is-seadrive-using-so-much-memory/14907
[43] Seafile power consumption: https://forum.seafile.com/t/high-power-consumtion-of-seafile-client-on-linux/4606
[44] Seafile CPU issue: https://github.com/haiwen/seafile-client/issues/220
[45] iroh GitHub: https://github.com/n0-computer/iroh
[46] iroh 1.0 release: https://www.iroh.computer/blog/v1
[47] Road to iroh 1.0: https://www.iroh.computer/blog/the-road-to-iroh-1-0
[48] iroh vs libp2p: https://www.iroh.computer/blog/comparing-iroh-and-libp2p
[49] iroh NAT traversal docs: https://docs.iroh.computer/concepts/nat-traversal
[50] iroh relay docs: https://docs.iroh.computer/concepts/relays
[51] iroh address lookup: https://docs.iroh.computer/concepts/address-lookup
[52] iroh QUIC usage: https://docs.iroh.computer/protocols/using-quic
[53] iroh QAD blog: https://www.iroh.computer/blog/qad
[54] LambdaClass on iroh: https://blog.lambdaclass.com/the-wisdom-of-iroh
