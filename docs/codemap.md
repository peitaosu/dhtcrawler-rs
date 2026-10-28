# Code Map

Entry point: `src/main.rs` (CLI) → library `src/lib.rs` re-exports `common::AppConfig`.

## `src/common`

| File | Contents |
| --- | --- |
| `mod.rs` | Module list; re-exports `AppConfig` |
| `config.rs` | `AppConfig`, `StoreConfig`, `CrawlerConfig`, `ReaderConfig`, `HttpConfig`, `StoreBackend`, `SegMethod`; `load_or_create`/`save` TOML handling |
| `bencode.rs` | Bencode `Value`, `decode`, `decode_prefix`, `encode`; depth limit (32), strict integer/string validation, duplicate-key last-wins |
| `hex.rs` | `encode_upper`, `decode` (≤ 4096 bytes), `is_hash` (40 uppercase hex chars) |
| `segment.rs` | `seg_tokens` for `SegMethod::{None,Simple,Rmmseg}`; bundled dictionary from `dicts/words.dic` loaded once via `OnceLock` |
| `time_util.rs` | `now_seconds`, `now_day_seconds`, `format_datetime`, `format_date` (local time) |

## `src/dht`

| File | Contents |
| --- | --- |
| `mod.rs` | Module list; re-exports `NodeId`, `DhtNode`, `bootstrap`, `RoutingTable`, `MonitorEvent`, `SearchResult` |
| `id.rs` | `NodeId` (20 bytes): random, hex, XOR `distance`/`distance_cmp`, validity, `random_discrete_ids`; big-int helpers `big_sub`/`big_add`/`big_div2`/`big_mul_small`/`range_width_leq` |
| `node.rs` | `DhtNode` handle + `Actor` loop: command enum, UDP select loop (commands, packets, save/keepalive/sweep/pending timers), query answering (ping/find_node/get_peers/announce_peer), token check, `MonitorEvent` emission, keepalive eviction, state persistence; `bootstrap` retry loop (saved addresses first, max 16 sampled, then DNS hosts) |
| `message.rs` | KRPC codec: `Method`, `QueryArgs`, `ResponseValues`, `Inbound`; encode/decode for queries, responses, errors; compact peer (6/18-byte) and node (26/38-byte) codecs |
| `routing.rs` | `Node`, ordered-range `Bucket`, `RoutingTable` (`K = 8`, split only own-containing buckets, `closest`, `inactive_since`, `remove`) |
| `search.rs` | Iterative `find_node`/`get_peers` search (`SEARCH_WIDTH = 32`, `SEARCH_RETRIES = 4`), returns `SearchResult { trackers, peers, alive }` |
| `state.rs` | `SavedState` JSON save/load per node (`state<port>`), node ID + peer addresses |
| `storage.rs` | `PeerStore`: peers per info hash, 30-minute expiry, 64/hash cap, 50/query cap, `sweep` |
| `token.rs` | `TokenIssuer`: HMAC-SHA1 over (epoch, IP), 600 s epochs, 8-byte tokens, constant-time compare, current+previous epoch accepted |

## `src/engine`

| File | Contents |
| --- | --- |
| `mod.rs` | Module list; re-exports `Crawler`, `PeerSource`, `Reader`, stats types |
| `crawler.rs` | `Crawler::start`: builds node fleet from config, spawns bootstrap/search/flush/stats/sweep tasks; `SearchPeerSource` (announce cache → nearest-node `get_peers` search); `save_state`, `flush_caches` |
| `reader.rs` | `Reader::run`: pending-hash loop with semaphore, fast path via `inc_announce`, `process_download` → `bep9_phase` with budgets (120 s total, 30 s peer search, 8 s per peer), content verification, doc building, requeue logic (`MAX_REQUEUE_ATTEMPTS`) |
| `hash_cache.rs` | `HashCache`: merge counts in memory, flush by size or timer to `insert_pending_hashes`, restore on failure, stats updates (`recv`, `unique`) |
| `download_cache.rs` | `DownloadCache`: FIFO `PendingHash` queue, spill oldest third to `wait_download` when full, refill from store, shutdown flush |
| `stats.rs` | `CrawlerStats`/`ReaderStats` atomic counters with snapshots; `AnnouncePeerCache` (16 peers/hash, 30-min TTL, sweep) |

## `src/metadata`

| File | Contents |
| --- | --- |
| `mod.rs` | Module list; re-exports `parse`, `Torrent`, `FileEntry`, `TorrentKind`, `fetch_metadata`, `Bep9Error`, `LocalCache` |
| `torrent.rs` | `Torrent`/`TorrentKind` (single vs multi), `parse` (accepts full torrent or bare info dict, prefers `.utf-8` keys), `info_hash` (SHA-1 of re-encoded info dict), `wrap_as_torrent_file` |
| `bep9.rs` | `fetch_metadata`: TCP connect, BT handshake, extension handshake, `ut_metadata` piece requests until complete, SHA-1 verification, `MAX_METADATA_SIZE = 8 MiB`, `random_peer_id` (`-TR4050-`) |
| `bt_message.rs` | BT wire codec: handshake encode/decode (extension bit 0x10), frame decoder (`MAX_FRAME_SIZE = 256 KiB`), extended-message encoders, `UtMetadataMessage` request/data/reject parsing |
| `local_cache.rs` | `LocalCache`: torrent files under `<root>/<hh>/<hh>/<hash>.torrent`, empty files treated as missing |

## `src/store`

| File | Contents |
| --- | --- |
| `mod.rs` | `Store` trait (pending queue, wait_download, torrent CRUD/search/pages, daterange, day stats, torrent content), `PendingHash`, `TorrentDoc`, `DayStats`, `SearchStats`, `StoreError`, `MAX_REQUEUE_ATTEMPTS = 3`, `DATERANGE_RETENTION_DAYS = 5` |
| `memory.rs` | `MemoryStore`: mutex-guarded maps/queues implementing `Store`; token-based search (all query tokens must match name words or tokens) |
| `mongo.rs` | `MongoStore`: databases `dht_hash`, `torrents`, `hash_date`, `dht_system`, `torfiles`; text index on `name_array`, date index; upserts with `$inc`/`$setOnInsert`; `insert_pending_hashes` with concurrency 16; BSON encode/decode helpers |

## `src/httpd`

| File | Contents |
| --- | --- |
| `mod.rs` | `run` (axum serve), `AppState` with result caches, router (`/api/*`), handlers, `search_page` (cache + paging via `SEARCH_LIMIT = 50`), `compute_today_top` (daterange with 2-day fallback), `compute_stats` (total + 3 days), background refresher |
| `cache.rs` | `ResultCache<T>`: TTL 10 min, capacity 1000, evict-oldest-half, `get_fresh`/`get_stale` |
| `api.rs` | JSON response builders: search/detail/stats/list/error; files sorted by size, optional per-result limit |

## `tests`

| File | Contents |
| --- | --- |
| `dht_node.rs` | Two-node UDP integration: ping, self-query refusal, get_peers event/token/peer storage |
| `httpd_httpd.rs` | Router tests via `tower::oneshot`: API search/detail/stats, invalid input, unrouted paths return 404 |
| `pipeline/main.rs` | BEP-9 end-to-end against a fake peer: fetch, wrong-hash rejection, multi-piece metadata |
| `pipeline/fake_peer.rs` | Minimal BT peer: handshake, `ut_metadata` with a different extension id, serves info dict |

## `dicts/words.dic`

Frequency-sorted Chinese word list consumed by `segment.rs` for `rmmseg` segmentation.
