# dhtcrawler

DHT crawler and torrent search engine in Rust. Joins the BitTorrent mainline DHT with a fleet of UDP nodes, collects info hashes from `get_peers`/`announce_peer` traffic, downloads torrent metadata from peers over BEP-9, indexes it, and serves a JSON API.

## Features

- Multi-node DHT crawler with state persistence across restarts
- BEP-9 (ut_metadata) metadata download with hash verification
- Pluggable store backend: MongoDB or in-memory
- Search backed by a MongoDB text index over segmented names (none / simple / rmmseg)
- axum JSON API with result caching

## Build

Requires Rust 2024 edition toolchain.

```sh
cargo build --release
```

## Run

```sh
dhtcrawler --config dhtcrawler.toml [--backend memory|mongo] <COMMAND>
```

| Command | Description |
| --- | --- |
| `crawler` | Run the DHT crawler only |
| `reader` | Run the metadata download pipeline only (no peer source) |
| `http` | Run the web server only |
| `all` | Crawler + reader + web server sharing one store |
| `import <dir>` | Import `.torrent` files from a directory |
| `reindex` | Re-segment all stored names |
| `export-xml <output>` | Export torrents to XML |

On first run the config file is created with defaults: store `mongodb` at `mongodb://localhost:27017`, 50 DHT nodes from port 6776, HTTP on `0.0.0.0:8000`. All sections and fields have defaults; unknown keys are rejected.

## HTTP API

`/api/search?q=&p=&files=N` (files limit per result, default all), `/api/today_top?limit=N` (default 50), `/api/stats`, `/api/index?q=<hash>`

## Storage

MongoDB databases: `dht_hash` (pending/waiting hashes), `torrents` (index), `hash_date` (per-day request counts), `dht_system` (daily stats), `torfiles` (raw torrent contents). DHT node state is persisted under `dhtstate/`; optional torrent files under `torrents/`. See [docs/architecture.md](docs/architecture.md).

## Tests

```sh
cargo test
```

Unit tests are inline; integration tests cover DHT node pairs, HTTP routes, and BEP-9 against a fake peer.

## Documentation

- [Architecture](docs/architecture.md)
- [Design](docs/design.md)
- [Code map](docs/codemap.md)

## License

MIT
