use crate::dht::id::NodeId;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedState {
    pub node_id: String,
    pub nodes: Vec<String>,
}

pub fn state_file_path(dir: &str, port: u16) -> PathBuf {
    Path::new(dir).join(format!("state{}", port))
}

pub fn save(path: &Path, node_id: &NodeId, nodes: &[SocketAddr]) -> Result<(), String> {
    let state = SavedState {
        node_id: node_id.to_hex(),
        nodes: nodes.iter().map(|a| a.to_string()).collect(),
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create {}: {}", parent.display(), e))?;
    }
    let content = serde_json::to_string(&state).map_err(|e| e.to_string())?;
    std::fs::write(path, content).map_err(|e| format!("write {}: {}", path.display(), e))
}

pub fn load(path: &Path) -> Result<(NodeId, Vec<SocketAddr>), String> {
    let content = std::fs::read_to_string(path).map_err(|e| format!("read {}: {}", path.display(), e))?;
    let state: SavedState = serde_json::from_str(&content).map_err(|e| e.to_string())?;
    let node_id = NodeId::from_hex(&state.node_id).ok_or("invalid saved node id")?;
    let mut nodes = Vec::new();
    for addr_str in state.nodes {
        if let Ok(addr) = addr_str.parse::<SocketAddr>() {
            nodes.push(addr);
        }
    }
    Ok((node_id, nodes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn save_load_roundtrip() {
        let dir = std::env::temp_dir().join(format!("dhtstate-{}", std::process::id()));
        let path = state_file_path(dir.to_str().unwrap(), 6776);
        let id = NodeId::random();
        let nodes: Vec<SocketAddr> = vec!["1.2.3.4:6881".parse().unwrap()];
        save(&path, &id, &nodes).unwrap();
        let (loaded_id, loaded_nodes) = load(&path).unwrap();
        assert_eq!(loaded_id, id);
        assert_eq!(loaded_nodes, nodes);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn load_missing_fails() {
        assert!(load(Path::new("/nonexistent/state1")).is_err());
    }
}
