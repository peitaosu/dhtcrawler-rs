use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct LocalCache {
    root: PathBuf,
}

impl LocalCache {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        LocalCache { root: root.into() }
    }

    pub fn torrent_path(&self, hash: &str) -> PathBuf {
        let prefix = hash.get(0..2).unwrap_or("00");
        let suffix = hash.get(2..4).unwrap_or("00");
        self.root
            .join(prefix)
            .join(suffix)
            .join(format!("{}.torrent", hash))
    }

    pub fn save(&self, hash: &str, content: &[u8]) -> Result<(), String> {
        let path = self.torrent_path(hash);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("create {}: {}", parent.display(), e))?;
        }
        std::fs::write(&path, content).map_err(|e| format!("write {}: {}", path.display(), e))
    }

    pub fn load(&self, hash: &str) -> Option<Vec<u8>> {
        let path = self.torrent_path(hash);
        std::fs::read(path).ok().filter(|c| !c.is_empty())
    }

    pub fn exists(&self, hash: &str) -> bool {
        self.torrent_path(hash).exists()
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root() -> LocalCache {
        let dir = std::env::temp_dir().join(format!("loccache-{}-{}", std::process::id(), std::time::SystemTime::now().elapsed().unwrap_or_default().as_nanos()));
        LocalCache::new(&dir)
    }

    #[test]
    fn path_layout_matches_original() {
        let cache = LocalCache::new("torrents");
        let path = cache.torrent_path("7C6932E7EC1CF5B00AE991871E57B2375DADA5A0");
        assert_eq!(
            path,
            PathBuf::from("torrents/7C/69/7C6932E7EC1CF5B00AE991871E57B2375DADA5A0.torrent")
        );
    }

    #[test]
    fn save_load_roundtrip() {
        let cache = temp_root();
        let hash = "AABBCCDDEEFF00112233445566778899AABBCCDD";
        assert!(!cache.exists(hash));
        cache.save(hash, b"content").unwrap();
        assert!(cache.exists(hash));
        assert_eq!(cache.load(hash), Some(b"content".to_vec()));
        assert!(cache.load("0000000000000000000000000000000000000000").is_none());
    }

    #[test]
    fn empty_file_treated_as_missing() {
        let cache = temp_root();
        let hash = "1111111111111111111111111111111111111111";
        cache.save(hash, b"").unwrap();
        assert!(cache.load(hash).is_none());
    }
}
