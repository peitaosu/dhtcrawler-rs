use crate::common::bencode::{self, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    pub path: String,
    pub length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TorrentKind {
    Single {
        name: String,
        length: u64,
    },
    Multi {
        root: String,
        files: Vec<FileEntry>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Torrent {
    pub kind: TorrentKind,
}

impl Torrent {
    pub fn name(&self) -> &str {
        match &self.kind {
            TorrentKind::Single { name, .. } => name,
            TorrentKind::Multi { root, .. } => root,
        }
    }

    pub fn length(&self) -> u64 {
        match &self.kind {
            TorrentKind::Single { length, .. } => *length,
            TorrentKind::Multi { files, .. } => files.iter().map(|f| f.length).sum(),
        }
    }

    pub fn files(&self) -> Vec<FileEntry> {
        match &self.kind {
            TorrentKind::Single { name, length } => vec![FileEntry {
                path: name.clone(),
                length: *length,
            }],
            TorrentKind::Multi { files, .. } => files.clone(),
        }
    }
}

pub fn parse(content: &[u8]) -> Result<Torrent, String> {
    let value = bencode::decode(content).map_err(|e| e.to_string())?;
    let info = value
        .str_key("info")
        .or_else(|| looks_like_info(&value).then_some(&value))
        .ok_or_else(|| "no info dictionary".to_string())?;
    let kind = parse_info(info)?;
    Ok(Torrent { kind })
}

fn looks_like_info(value: &Value) -> bool {
    value.as_dict().is_some()
        && (value.str_key("name").is_some() || value.str_key("name.utf-8").is_some())
        && (value.str_key("piece length").is_some() || value.str_key("files").is_some())
}

fn parse_info(info: &Value) -> Result<TorrentKind, String> {
    if info.str_key("files").and_then(|f| f.as_list()).is_some() {
        let root = read_string(info, "name")?;
        let mut files = Vec::new();
        for item in info.str_key("files").unwrap().as_list().unwrap() {
            let length = item
                .str_key("length")
                .and_then(|v| v.as_int())
                .ok_or("file entry missing length")?;
            let length = u64::try_from(length).map_err(|_| "negative file length")?;
            let path = read_path(item)?;
            files.push(FileEntry { path, length });
        }
        Ok(TorrentKind::Multi { root, files })
    } else {
        let name = read_string(info, "name")?;
        let length = info
            .str_key("length")
            .and_then(|v| v.as_int())
            .ok_or("single torrent missing length")?;
        let length = u64::try_from(length).map_err(|_| "negative length")?;
        Ok(TorrentKind::Single { name, length })
    }
}

fn read_string(dict: &Value, key: &str) -> Result<String, String> {
    let utf8_key = format!("{}.utf-8", key);
    if let Some(bytes) = dict.str_key(&utf8_key).and_then(|v| v.as_bytes()) {
        return Ok(String::from_utf8_lossy(bytes).into_owned());
    }
    dict.str_key(key)
        .and_then(|v| v.as_bytes())
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .ok_or_else(|| format!("missing {}", key))
}

fn read_path(file: &Value) -> Result<String, String> {
    let parts = file
        .str_key("path.utf-8")
        .or_else(|| file.str_key("path"))
        .and_then(|v| v.as_list())
        .ok_or("file entry missing path")?;
    let segments: Vec<String> = parts
        .iter()
        .filter_map(|p| p.as_bytes())
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .collect();
    Ok(segments.join("/"))
}

pub fn wrap_as_torrent_file(info_dict: &[u8]) -> Vec<u8> {
    let mut out = b"d4:info".to_vec();
    out.extend_from_slice(info_dict);
    out.push(b'e');
    out
}

pub fn info_hash(content: &[u8]) -> Result<[u8; 20], String> {
    use sha1::{Digest, Sha1};
    let value = bencode::decode(content).map_err(|e| e.to_string())?;
    if let Some(info) = value.str_key("info") {
        let encoded = bencode::encode(info);
        Ok(Sha1::digest(&encoded).into())
    } else if looks_like_info(&value) {
        Ok(Sha1::digest(content).into())
    } else {
        Err("no info dictionary".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::bencode::Value;

    fn sorted_dict(pairs: Vec<(&str, Value)>) -> Value {
        let mut pairs: Vec<(Vec<u8>, Value)> = pairs
            .into_iter()
            .map(|(k, v)| (k.as_bytes().to_vec(), v))
            .collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        Value::Dict(pairs)
    }

    fn single_torrent() -> Vec<u8> {
        let info = sorted_dict(vec![
            ("length", Value::Int(100)),
            ("name", Value::Bytes(b"single".to_vec())),
            ("piece length", Value::Int(16384)),
        ]);
        bencode::encode(&sorted_dict(vec![("info", info)]))
    }

    fn multi_torrent() -> Vec<u8> {
        let file = |path: Vec<&str>, length: i64| {
            sorted_dict(vec![
                ("length", Value::Int(length)),
                (
                    "path",
                    Value::List(path.into_iter().map(|p| Value::Bytes(p.as_bytes().to_vec())).collect()),
                ),
            ])
        };
        let info = sorted_dict(vec![
            ("files", Value::List(vec![file(vec!["abc"], 10), file(vec!["abc", "def"], 20)])),
            ("name", Value::Bytes(b"root".to_vec())),
            ("piece length", Value::Int(16384)),
        ]);
        bencode::encode(&sorted_dict(vec![("info", info)]))
    }

    #[test]
    fn parse_single() {
        let torrent = parse(&single_torrent()).unwrap();
        assert_eq!(
            torrent.kind,
            TorrentKind::Single {
                name: "single".into(),
                length: 100
            }
        );
        assert_eq!(torrent.length(), 100);
        assert_eq!(torrent.files().len(), 1);
    }

    #[test]
    fn parse_multi() {
        let torrent = parse(&multi_torrent()).unwrap();
        assert_eq!(torrent.length(), 30);
        match torrent.kind {
            TorrentKind::Multi { root, files } => {
                assert_eq!(root, "root");
                assert_eq!(files.len(), 2);
                assert_eq!(files[0].path, "abc");
                assert_eq!(files[1].path, "abc/def");
                assert_eq!(files[1].length, 20);
            }
            other => panic!("expected multi, got {:?}", other),
        }
    }

    #[test]
    fn parse_prefers_utf8_names() {
        let info = sorted_dict(vec![
            ("length", Value::Int(1)),
            ("name", Value::Bytes(b"raw".to_vec())),
            ("name.utf-8", Value::Bytes(b"pretty ok".to_vec())),
        ]);
        let content = bencode::encode(&sorted_dict(vec![("info", info)]));
        let torrent = parse(&content).unwrap();
        assert_eq!(torrent.name(), "pretty ok");
    }

    #[test]
    fn parse_bare_info_dict() {
        let full = single_torrent();
        let value = bencode::decode(&full).unwrap();
        let info = value.str_key("info").unwrap();
        let info_bytes = bencode::encode(info);
        let torrent = parse(&full).unwrap();
        let torrent2 = parse(&info_bytes).unwrap();
        assert_eq!(torrent, torrent2);
    }

    #[test]
    fn parse_rejects_garbage() {
        assert!(parse(b"i42e").is_err());
        assert!(parse(b"d4:name3:abc e").is_err());
        let empty = bencode::encode(&sorted_dict(vec![("info", sorted_dict(vec![]))]));
        assert!(parse(&empty).is_err());
    }

    #[test]
    fn wrap_torrent_file() {
        let full = single_torrent();
        let value = bencode::decode(&full).unwrap();
        let info_bytes = bencode::encode(value.str_key("info").unwrap());
        let wrapped = wrap_as_torrent_file(&info_bytes);
        let _ = parse(&wrapped);
    }

    #[test]
    fn info_hash_is_sha1_of_info_dict() {
        let full = single_torrent();
        let hash = info_hash(&full).unwrap();
        let value = bencode::decode(&full).unwrap();
        let info_bytes = bencode::encode(value.str_key("info").unwrap());
        use sha1::{Digest, Sha1};
        assert_eq!(hash, <[u8; 20]>::from(Sha1::digest(&info_bytes)));
        assert_eq!(hash.len(), 20);
        assert!(info_hash(b"i42e").is_err());
        let bare = bencode::encode(value.str_key("info").unwrap());
        assert_eq!(info_hash(&bare).unwrap(), hash);
    }
}
