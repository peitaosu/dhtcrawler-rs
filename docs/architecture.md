# Architecture

dhtcrawler is a DHT crawler and torrent search engine in Rust (Tokio, single binary). It joins the BitTorrent mainline DHT with multiple UDP nodes, observes `get_peers`/`announce_peer` traffic to collect info hashes, downloads torrent metadata from peers over BEP-9, stores documents in MongoDB (or an in-memory store), and serves a JSON API with axum.

## Modules

| Module | Role |
| --- | --- |
| `common` | Bencode codec, TOML config, hex helpers, text segmentation, time formatting |
| `dht` | Kademlia DHT: node IDs, messages, routing table, node actor, iterative search, token issuer, peer store, persisted state |
| `engine` | Orchestration: `Crawler` (fleet of DHT nodes + hash pipeline), `Reader` (metadata download pipeline), caches, stats |
| `metadata` | Torrent parsing, BEP-9 client (BT handshake + `ut_metadata`), local torrent-file cache |
| `store` | `Store` trait with `MongoStore` and `MemoryStore` implementations; queues, torrent docs, day stats |
| `httpd` | axum web server: JSON API, result cache |

## Runtime topology

`main.rs` parses CLI (clap) and loads `dhtcrawler.toml` (created with defaults if missing). Subcommands:

- `crawler` — DHT fleet only
- `reader` — download pipeline only (no peer source; warns)
- `http` — web server only
- `all` — crawler + reader + web server sharing one store
- `import <dir>` — bulk import local `.torrent` files
- `reindex` — re-segment all stored names
- `export-xml <output>` — dump torrents to XML

`store.backend` selects `mongodb` (default) or `memory`; `--backend` overrides the config.

### Task graph (`all`)

```
CLI / config
    |
    +--> Crawler::start
    |      |-- N x DhtNode actor (UDP bind on start_port..start_port+N-1)
    |      |      \-- one bootstrap task per node
    |      |-- per-node random get_peers search loop
    |      |-- event consumer (MonitorEvent -> stats + AnnouncePeerCache + HashCache)
    |      |-- HashCache flush timer
    |      +-- stats dump timer, announce cache sweep timer
    |
    +--> Reader::run loop
    |      \-- per-hash download tasks (bounded by semaphore)
    |
    +--> httpd::run (axum)
    |      \-- background top/stats cache refresher
    |
    \--> store (Arc<dyn Store>): MongoDB or Memory
```

## Data flow

1. **Discovery.** Each `DhtNode` actor handles UDP packets and internal commands. Incoming `get_peers`/`announce_peer` queries are answered and emitted as `MonitorEvent`.
2. **Hash collection.** The crawler's event consumer counts events, remembers announced peers in `AnnouncePeerCache` (peer source for the reader), and feeds info hashes into `HashCache`.
3. **Hash batching.** `HashCache` merges counts in memory; it flushes to the store's pending queue when full (`hash_max_cache`) or on timer (`cache_max_time_secs`), via `Store::insert_pending_hashes`, and bumps daily stats (`recv`, `unique`).
4. **Download scheduling.** The `Reader` polls `pop_pending_hash`; unknown hashes go to BEP-9 download (bounded by `max_download_per_reader`), known hashes take the fast path (`inc_announce` + daterange/stats updates). When the in-memory `DownloadCache` is full it spills the oldest third to the store's `wait_download` queue; failed downloads are requeued there up to `MAX_REQUEUE_ATTEMPTS` (3).
5. **Metadata download.** `Reader` picks the nearest node's peer source (`SearchPeerSource` = announce cache first, then a DHT `get_peers` search). `bep9::fetch_metadata` connects over TCP, exchanges BT handshake + extension handshake, requests `ut_metadata` pieces, and verifies SHA-1 against the info hash.
6. **Indexing.** Verified content is parsed into `TorrentDoc` (name, length, files, segmented `name_array`) and upserted; daterange and daily stats are updated.
7. **Serving.** The web layer reads through `ResultCache`: search results per keyword+page (fresh-only), today-top and stats refreshed by a background task every 10 minutes. Search uses the MongoDB `name_array` text index or in-memory token matching.
## Storage layout (MongoDB)

| Database | Collection | Contents |
| --- | --- | --- |
| `dht_hash` | `hash` | pending hash queue (`_id`=hash, `req_cnt`, `req_at`, `attempts`) |
| `dht_hash` | `wait_download` | requeued hashes awaiting download |
| `torrents` | `hashes` | torrent documents (`_id`=hash, name, length, created_at, announce, files, `name_array` text index) |
| `hash_date` | `hashes` | per-day hash request counts (`_id`={hash,date}, `reqs`) |
| `dht_system` | `stats` | daily counters (`_id`=day) |
| `torfiles` | `torrents` | raw torrent file contents (binary) |

Daterange entries older than 5 days are eligible for cleanup (`daterange_cleanup`).

## State persistence

Each DHT node persists its node ID and routing-table addresses to `dhtstate/state<port>` as JSON every 5 minutes and on shutdown; it reloads them on start and prefers saved peers before public bootstrap hosts.
