use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StoreBackend {
    Memory,
    Mongodb,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SegMethod {
    None,
    Simple,
    Rmmseg,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct StoreConfig {
    pub backend: StoreBackend,
    pub mongo_url: String,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            backend: StoreBackend::Mongodb,
            mongo_url: "mongodb://localhost:27017".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct CrawlerConfig {
    pub start_port: u16,
    pub node_count: u16,
    pub hash_max_cache: usize,
    pub cache_max_time_secs: u64,
    pub query_interval_secs: u64,
    pub state_dir: String,
    pub bootstrap: Vec<String>,
}

impl Default for CrawlerConfig {
    fn default() -> Self {
        Self {
            start_port: 6776,
            node_count: 50,
            hash_max_cache: 300,
            cache_max_time_secs: 120,
            query_interval_secs: 60,
            state_dir: "dhtstate".into(),
            bootstrap: vec![
                "router.bittorrent.com:6881".into(),
                "dht.transmissionbt.com:6881".into(),
                "dht.libtorrent.org:6881".into(),
            ],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct ReaderConfig {
    pub max_download_cache: usize,
    pub max_download_per_reader: usize,
    pub save_torrent: bool,
    pub save_to_file: bool,
    pub save_to_db: bool,
    pub load_from_db: bool,
    pub torrent_path: String,
    pub text_seg: SegMethod,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            max_download_cache: 100,
            max_download_per_reader: 100,
            save_torrent: false,
            save_to_file: true,
            save_to_db: false,
            load_from_db: false,
            torrent_path: "torrents/".into(),
            text_seg: SegMethod::Simple,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct HttpConfig {
    pub bind: String,
    pub port: u16,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0".into(),
            port: 8000,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct AppConfig {
    pub store: StoreConfig,
    pub crawler: CrawlerConfig,
    pub reader: ReaderConfig,
    pub http: HttpConfig,
}

impl AppConfig {
    pub fn load_or_create(path: &Path) -> Result<Self, String> {
        if path.exists() {
            let content = fs::read_to_string(path).map_err(|e| format!("read {}: {}", path.display(), e))?;
            let config: AppConfig =
                toml::from_str(&content).map_err(|e| format!("parse {}: {}", path.display(), e))?;
            return Ok(config);
        }
        let config = AppConfig::default();
        config.save(path)?;
        Ok(config)
    }

    pub fn save(&self, path: &Path) -> Result<(), String> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(|e| format!("create {}: {}", parent.display(), e))?;
            }
        }
        let content = toml::to_string_pretty(self).map_err(|e| format!("serialize config: {}", e))?;
        fs::write(path, content).map_err(|e| format!("write {}: {}", path.display(), e))
    }
}

pub fn default_config_path() -> PathBuf {
    PathBuf::from("dhtcrawler.toml")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_original() {
        let config = AppConfig::default();
        assert_eq!(config.crawler.start_port, 6776);
        assert_eq!(config.crawler.node_count, 50);
        assert_eq!(config.crawler.hash_max_cache, 300);
        assert_eq!(config.crawler.cache_max_time_secs, 120);
        assert_eq!(config.http.port, 8000);
        assert_eq!(config.reader.max_download_cache, 100);
        assert_eq!(config.reader.max_download_per_reader, 100);
        assert_eq!(config.reader.torrent_path, "torrents/");
        assert!(matches!(config.reader.text_seg, SegMethod::Simple));
    }

    #[test]
    fn load_or_create_roundtrip() {
        let dir = std::env::temp_dir().join(format!("dhcfg-{}", std::process::id()));
        let path = dir.join("dhtcrawler.toml");
        let _ = std::fs::remove_file(&path);
        let first = AppConfig::load_or_create(&path).unwrap();
        assert!(path.exists());
        let second = AppConfig::load_or_create(&path).unwrap();
        assert_eq!(first, second);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reject_unknown_fields() {
        let bad = "[store]\nunknown_key = 1\n";
        assert!(toml::from_str::<AppConfig>(bad).is_err());
    }
}
