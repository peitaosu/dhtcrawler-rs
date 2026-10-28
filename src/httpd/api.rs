use crate::store::{DayStats, TorrentDoc};
use serde_json::{json, Value};

pub fn search_response(
    keyword: &str,
    docs: &[TorrentDoc],
    page: u64,
    cost_micros: u64,
    files_limit: Option<usize>,
) -> Value {
    json!({
        "keyword": keyword,
        "found": docs.len(),
        "cost": cost_micros / 1000,
        "page": page,
        "results": docs.iter().map(|doc| one_result(doc, files_limit)).collect::<Vec<Value>>(),
    })
}

pub fn torrent_detail(doc: &TorrentDoc) -> Value {
    one_result(doc, None)
}

pub fn stats_response(total: u64, stats: &[DayStats]) -> Value {
    json!({
        "total": total,
        "stats": stats.iter().map(one_day).collect::<Vec<Value>>(),
    })
}

pub fn list_response(docs: &[TorrentDoc]) -> Value {
    json!({ "results": docs.iter().map(|doc| one_result(doc, None)).collect::<Vec<Value>>() })
}

pub fn error_response(message: &str) -> Value {
    json!({ "error": message })
}

fn one_result(doc: &TorrentDoc, files_limit: Option<usize>) -> Value {
    json!({
        "hash": doc.hash,
        "name": doc.name,
        "created_at": doc.created_at,
        "req": doc.announce,
        "file_cnt": doc.files.len(),
        "total": doc.files.iter().map(|f| f.length).sum::<u64>(),
        "files": format_files(doc, files_limit),
    })
}

fn format_files(doc: &TorrentDoc, limit: Option<usize>) -> Vec<Value> {
    let mut sorted: Vec<&crate::metadata::FileEntry> = doc.files.iter().collect();
    sorted.sort_by(|a, b| b.length.cmp(&a.length));
    sorted
        .into_iter()
        .take(limit.unwrap_or(usize::MAX))
        .map(|f| json!({ "name": f.path, "size": f.length }))
        .collect()
}

fn one_day(stats: &DayStats) -> Value {
    json!({
        "day_secs": stats.day,
        "recv": stats.recv,
        "process": stats.process,
        "update": stats.update,
        "new": stats.new,
        "unique": stats.unique,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::FileEntry;

    fn doc() -> TorrentDoc {
        TorrentDoc {
            hash: "CD".repeat(20),
            name: "some torrent".into(),
            length: 1,
            created_at: 42,
            announce: 5,
            files: vec![
                FileEntry { path: "big.bin".into(), length: 300 },
                FileEntry { path: "small.txt".into(), length: 1 },
                FileEntry { path: "mid.iso".into(), length: 100 },
                FileEntry { path: "tail.rar".into(), length: 10 },
            ],
            name_array: vec![],
        }
    }

    #[test]
    fn search_response_shape() {
        let value = search_response("kw", &[doc()], 2, 123_456, None);
        assert_eq!(value["keyword"], "kw");
        assert_eq!(value["found"], 1);
        assert_eq!(value["page"], 2);
        assert_eq!(value["cost"], 123);
        assert_eq!(value["results"].as_array().unwrap().len(), 1);
        assert_eq!(value["results"][0]["hash"], "CD".repeat(20));
        let files = value["results"][0]["files"].as_array().unwrap();
        assert_eq!(files.len(), 4);
        assert_eq!(files[0]["name"], "big.bin");
        assert_eq!(files[0]["size"], 300);
    }

    #[test]
    fn files_limit_returns_largest_first() {
        let value = search_response("kw", &[doc()], 1, 0, Some(2));
        let files = value["results"][0]["files"].as_array().unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0]["name"], "big.bin");
        assert_eq!(files[1]["name"], "mid.iso");
    }

    #[test]
    fn detail_and_list_return_all_files() {
        assert_eq!(torrent_detail(&doc())["files"].as_array().unwrap().len(), 4);
        assert_eq!(list_response(&[doc()])["results"][0]["files"].as_array().unwrap().len(), 4);
    }

    #[test]
    fn stats_response_shape() {
        let stats = DayStats {
            day: 1690000000,
            recv: 1,
            process: 2,
            update: 3,
            new: 4,
            unique: 5,
        };
        let value = stats_response(9, &[stats]);
        assert_eq!(value["total"], 9);
        assert_eq!(value["stats"][0]["day_secs"], 1690000000);
        assert_eq!(value["stats"][0]["recv"], 1);
        assert_eq!(value["stats"][0]["unique"], 5);
        assert!(value["stats"][0].get("filtered").is_none());
    }
}
