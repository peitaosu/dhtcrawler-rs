use anyhow::Result;
use async_trait::async_trait;
use clap::{Parser, Subcommand};
use dhtcrawler::common::config::{AppConfig, StoreBackend};
use dhtcrawler::common::segment;
use dhtcrawler::engine::{Crawler, Reader};
use dhtcrawler::store::{memory::MemoryStore, mongo::MongoStore, Store, TorrentDoc};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "dhtcrawler", about = "DHT crawler and torrent search engine")]
struct Cli {
    #[arg(short, long, default_value = "dhtcrawler.toml")]
    config: PathBuf,

    #[arg(short, long, value_enum)]
    backend: Option<BackendArg>,

    #[command(subcommand)]
    command: Command,
}

#[derive(clap::ValueEnum, Clone, Copy)]
enum BackendArg {
    Memory,
    Mongo,
}

#[derive(Subcommand)]
enum Command {
    Crawler,
    Reader,
    Http,
    All,
    Import { dir: PathBuf },
    Reindex,
    ExportXml { output: PathBuf },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cli = Cli::parse();
    let config = AppConfig::load_or_create(&cli.config).map_err(anyhow::Error::msg)?;
    let config = apply_backend_override(config, cli.backend);
    let store = open_store(&config).await?;
    match cli.command {
        Command::Crawler => run_crawler(&config, store).await,
        Command::Reader => run_reader(&config, store).await,
        Command::Http => run_http(&config, store).await,
        Command::All => run_all(&config, store).await,
        Command::Import { dir } => run_import(&config, store, &dir).await,
        Command::Reindex => run_reindex(&config, store).await,
        Command::ExportXml { output } => run_export_xml(store, &output).await,
    }
}

fn apply_backend_override(mut config: AppConfig, backend: Option<BackendArg>) -> AppConfig {
    if let Some(backend) = backend {
        config.store.backend = match backend {
            BackendArg::Memory => StoreBackend::Memory,
            BackendArg::Mongo => StoreBackend::Mongodb,
        };
    }
    config
}

async fn open_store(config: &AppConfig) -> Result<Arc<dyn Store>> {
    match config.store.backend {
        StoreBackend::Mongodb => {
            let store = MongoStore::connect(&config.store.mongo_url).await.map_err(anyhow::Error::msg)?;
            store.init().await.map_err(anyhow::Error::msg)?;
            Ok(Arc::new(store))
        }
        StoreBackend::Memory => {
            tracing::warn!("using in-memory store, data will be lost on exit");
            Ok(Arc::new(MemoryStore::new()))
        }
    }
}

async fn run_crawler(config: &AppConfig, store: Arc<dyn Store>) -> Result<()> {
    let crawler = Crawler::start(&config.crawler, store).await.map_err(anyhow::Error::msg)?;
    println!(
        "crawler started with {} dht instances on ports {}..{}",
        crawler.nodes.len(),
        config.crawler.start_port,
        config.crawler.start_port + crawler.nodes.len().saturating_sub(1) as u16
    );
    tokio::signal::ctrl_c().await?;
    crawler.save_state().await;
    crawler.flush_caches().await;
    println!("crawler stats: {:?}", crawler.stats.snapshot());
    Ok(())
}

struct NoPeers;

const TORRENT_PAGE_SIZE: u64 = 1000;

#[async_trait]
impl dhtcrawler::engine::PeerSource for NoPeers {
    async fn peers_for(&self, _info_hash: &str) -> Vec<SocketAddr> {
        Vec::new()
    }
}

async fn run_reader(config: &AppConfig, store: Arc<dyn Store>) -> Result<()> {
    tracing::warn!("standalone reader has no peer source, BEP-9 metadata download is unavailable, use the all subcommand for full downloads");
    let reader = Arc::new(Reader::new(store, config.reader.clone(), Arc::new(NoPeers)));
    let running = reader.clone();
    tokio::spawn(async move {
        running.run().await;
    });
    tokio::signal::ctrl_c().await?;
    reader.flush_pending().await;
    Ok(())
}

async fn run_http(config: &AppConfig, store: Arc<dyn Store>) -> Result<()> {
    dhtcrawler::httpd::run(store, &config.http.bind, config.http.port)
        .await
        .map_err(anyhow::Error::msg)?;
    Ok(())
}

async fn run_all(config: &AppConfig, store: Arc<dyn Store>) -> Result<()> {
    let crawler = Crawler::start(&config.crawler, store.clone()).await.map_err(anyhow::Error::msg)?;
    let reader = Arc::new(Reader::new(
        store.clone(),
        config.reader.clone(),
        crawler.peer_source.clone(),
    ));
    tokio::spawn({
        let reader = reader.clone();
        async move {
            reader.run().await;
        }
    });
    println!(
        "running crawler ({} instances), reader and httpd on {}",
        crawler.nodes.len(),
        config.http.port
    );
    let http_task = tokio::spawn({
        let store = store.clone();
        let bind = config.http.bind.clone();
        let port = config.http.port;
        async move { dhtcrawler::httpd::run(store, &bind, port).await }
    });
    tokio::signal::ctrl_c().await?;
    crawler.save_state().await;
    crawler.flush_caches().await;
    reader.flush_pending().await;
    println!("crawler stats: {:?}", crawler.stats.snapshot());
    http_task.abort();
    Ok(())
}

async fn run_import(config: &AppConfig, store: Arc<dyn Store>, dir: &Path) -> Result<()> {
    let mut files = Vec::new();
    collect_torrent_files(dir, &mut files)?;
    println!("importing {} torrent files", files.len());
    let mut imported = 0usize;
    for path in files {
        let Ok(content) = std::fs::read(&path) else {
            continue;
        };
        let Ok(hash_bytes) = dhtcrawler::metadata::torrent::info_hash(&content) else {
            tracing::warn!("skip {}: not a valid torrent", path.display());
            continue;
        };
        let hash = dhtcrawler::common::hex::encode_upper(&hash_bytes);
        let Ok(torrent) = dhtcrawler::metadata::parse(&content) else {
            tracing::warn!("skip {}: parse failed", path.display());
            continue;
        };
        if let Ok(Some(existing)) = store.get_torrent(&hash).await {
            store
                .inc_announce(&hash, 1)
                .await
                .map_err(anyhow::Error::msg)?;
            tracing::debug!("skip {}: already imported as {}", path.display(), existing.name);
            continue;
        }
        let doc = TorrentDoc {
            hash,
            name: torrent.name().to_string(),
            length: torrent.length(),
            created_at: dhtcrawler::common::time_util::now_seconds(),
            announce: 1,
            files: torrent.files(),
            name_array: segment::seg_tokens(config.reader.text_seg, torrent.name()),
        };
        store.insert_torrent(doc).await?;
        imported += 1;
    }
    println!("imported {} torrents", imported);
    Ok(())
}

fn collect_torrent_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    let entries = std::fs::read_dir(dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_torrent_files(&path, out)?;
        } else if path.extension().is_some_and(|e| e == "torrent") {
            out.push(path);
        }
    }
    Ok(())
}

async fn run_reindex(config: &AppConfig, store: Arc<dyn Store>) -> Result<()> {
    let mut offset = 0u64;
    let mut total = 0usize;
    loop {
        let docs = store.torrents_page(offset, TORRENT_PAGE_SIZE).await?;
        if docs.is_empty() {
            break;
        }
        offset += docs.len() as u64;
        for mut doc in docs {
            doc.name_array = segment::seg_tokens(config.reader.text_seg, &doc.name);
            store.insert_torrent(doc).await?;
            total += 1;
        }
    }
    println!("resegmented {} torrents", total);
    Ok(())
}

async fn run_export_xml(store: Arc<dyn Store>, output: &Path) -> Result<()> {
    let mut offset = 0u64;
    let mut total = 0usize;
    let mut file = std::fs::File::create(output)?;
    std::io::Write::write_all(&mut file, b"<documents>\n")?;
    loop {
        let docs = store.torrents_page(offset, TORRENT_PAGE_SIZE).await?;
        if docs.is_empty() {
            break;
        }
        offset += docs.len() as u64;
        for doc in &docs {
            let line = format!(
                "<document><id>{}</id><hash>{}</hash><name>{}</name><announce>{}</announce><created_at>{}</created_at><files>{}</files></document>\n",
                total + 1,
                escape_xml_text(&doc.hash),
                escape_xml_text(&doc.name),
                doc.announce,
                doc.created_at,
                escape_xml_text(&doc.files.iter().map(|f| f.path.clone()).collect::<Vec<_>>().join(" "))
            );
            std::io::Write::write_all(&mut file, line.as_bytes())?;
            total += 1;
        }
    }
    std::io::Write::write_all(&mut file, b"</documents>\n")?;
    println!("exported {} documents to {}", total, output.display());
    Ok(())
}

fn escape_xml_text(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(ch),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::escape_xml_text;

    #[test]
    fn escapes_xml_specials() {
        assert_eq!(escape_xml_text("a<&>\"'"), "a&lt;&amp;&gt;&quot;&apos;");
        assert_eq!(escape_xml_text("plain"), "plain");
    }
}
