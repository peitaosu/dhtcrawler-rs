use crate::common::config::SegMethod;
use std::collections::HashMap;
use std::sync::OnceLock;

const WORDS_DIC: &str = include_str!("../../dicts/words.dic");

pub fn seg_tokens(method: SegMethod, text: &str) -> Vec<String> {
    match method {
        SegMethod::None => vec![text.to_string()],
        SegMethod::Simple => simple_seg(text),
        SegMethod::Rmmseg => rmmseg_seg(text),
    }
}

fn is_spliter(ch: char) -> bool {
    let code = ch as u32;
    code < '0' as u32
        || ('[' as u32..='`' as u32).contains(&code)
        || ('{' as u32..='~' as u32).contains(&code)
        || (':' as u32..='@' as u32).contains(&code)
}

fn tokens_by_separator(text: &str) -> Vec<String> {
    text.split(|c: char| is_spliter(c))
        .filter(|t| !t.is_empty())
        .map(|t| t.to_string())
        .collect()
}

fn simple_seg(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for token in tokens_by_separator(text) {
        let chars: Vec<char> = token.chars().collect();
        for start in 0..chars.len() {
            for end in start + 1..=chars.len() {
                out.push(chars[start..end].iter().collect());
            }
        }
    }
    out
}

struct Dictionary {
    words: HashMap<String, u32>,
    max_chars: usize,
}

impl Dictionary {
    fn load() -> Self {
        let mut words = HashMap::new();
        let mut max_chars = 1;
        for line in WORDS_DIC.lines() {
            if let Some((freq, word)) = split_dic_line(line) {
                max_chars = max_chars.max(word.chars().count());
                words.insert(word, freq);
            }
        }
        Dictionary { words, max_chars }
    }
}

fn split_dic_line(line: &str) -> Option<(u32, String)> {
    let line = line.trim();
    if line.is_empty() {
        return None;
    }
    let (freq, word) = line.split_once(' ')?;
    let freq: u32 = freq.trim().parse().ok()?;
    Some((freq, word.trim().to_string()))
}

fn dictionary() -> &'static Dictionary {
    static DICT: OnceLock<Dictionary> = OnceLock::new();
    DICT.get_or_init(Dictionary::load)
}

fn rmmseg_seg(text: &str) -> Vec<String> {
    let dict = dictionary();
    let mut out = Vec::new();
    for token in tokens_by_separator(text) {
        if token.is_ascii() {
            out.push(token);
            continue;
        }
        let chars: Vec<char> = token.chars().collect();
        let mut end = chars.len();
        let mut pieces = Vec::new();
        while end > 0 {
            let max_len = end.min(dict.max_chars);
            let mut matched = None;
            for len in (1..=max_len).rev() {
                let candidate: String = chars[end - len..end].iter().collect();
                if dict.words.contains_key(&candidate) {
                    matched = Some(candidate);
                    break;
                }
            }
            match matched {
                Some(word) => {
                    end -= word.chars().count();
                    pieces.push(word);
                }
                None => {
                    end -= 1;
                    let ch = chars[end];
                    pieces.push(ch.to_string());
                }
            }
        }
        pieces.reverse();
        out.extend(pieces);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_method() {
        assert_eq!(seg_tokens(SegMethod::None, "hello world"), vec!["hello world"]);
    }

    #[test]
    fn simple_splits_and_expands() {
        let tokens = seg_tokens(SegMethod::Simple, "ab cd");
        assert_eq!(
            tokens,
            vec!["a", "ab", "b", "c", "cd", "d"]
        );
        let cjk = seg_tokens(SegMethod::Simple, "中文");
        assert_eq!(cjk, vec!["中", "中文", "文"]);
    }

    #[test]
    fn simple_filters_separators() {
        let tokens = seg_tokens(SegMethod::Simple, "Big.Buck Bunny-1080p");
        assert!(tokens.contains(&"Big".to_string()));
        assert!(tokens.contains(&"Buck".to_string()));
        assert!(tokens.contains(&"1080p".to_string()));
        assert!(!tokens.iter().any(|t| t.contains('.') || t.contains('-')));
    }

    #[test]
    fn rmmseg_ascii_passthrough() {
        let tokens = seg_tokens(SegMethod::Rmmseg, "Big Buck");
        assert_eq!(tokens, vec!["Big", "Buck"]);
    }

    #[test]
    fn rmmseg_chinese_uses_dictionary() {
        let dict = dictionary();
        assert!(dict.max_chars >= 2);
        let tokens = seg_tokens(SegMethod::Rmmseg, "化为自己");
        assert!(tokens.iter().all(|t| !t.is_empty()));
        assert_eq!(tokens.concat(), "化为自己");
    }

    #[test]
    fn rmmseg_unknown_chars_still_emitted() {
        let tokens = seg_tokens(SegMethod::Rmmseg, "漢字語言");
        assert_eq!(tokens.concat(), "漢字語言");
    }
}
