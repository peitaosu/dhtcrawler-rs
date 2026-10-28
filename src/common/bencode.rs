use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Dict(Vec<(Vec<u8>, Value)>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BencodeError {
    pub message: String,
    pub offset: usize,
}

impl fmt::Display for BencodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bencode error at offset {}: {}", self.offset, self.message)
    }
}

impl std::error::Error for BencodeError {}

impl Value {
    pub fn dict_key(&self, key: &[u8]) -> Option<&Value> {
        match self {
            Value::Dict(pairs) => pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn str_key(&self, key: &str) -> Option<&Value> {
        self.dict_key(key.as_bytes())
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        self.as_bytes().and_then(|b| std::str::from_utf8(b).ok())
    }

    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_dict(&self) -> Option<&[(Vec<u8>, Value)]> {
        match self {
            Value::Dict(v) => Some(v),
            _ => None,
        }
    }
}

pub fn decode(data: &[u8]) -> Result<Value, BencodeError> {
    let mut pos = 0usize;
    let value = decode_value(data, &mut pos, 0)?;
    if pos != data.len() {
        return Err(BencodeError {
            message: "trailing bytes after value".into(),
            offset: pos,
        });
    }
    Ok(value)
}

pub fn decode_prefix(data: &[u8]) -> Result<(Value, usize), BencodeError> {
    let mut pos = 0usize;
    let value = decode_value(data, &mut pos, 0)?;
    Ok((value, pos))
}

const MAX_DEPTH: usize = 32;

fn decode_value(data: &[u8], pos: &mut usize, depth: usize) -> Result<Value, BencodeError> {
    if depth > MAX_DEPTH {
        return Err(BencodeError {
            message: "nesting too deep".into(),
            offset: *pos,
        });
    }
    let Some(&tag) = data.get(*pos) else {
        return Err(BencodeError {
            message: "unexpected end of input".into(),
            offset: *pos,
        });
    };
    match tag {
        b'i' => decode_int(data, pos),
        b'l' => decode_list(data, pos, depth),
        b'd' => decode_dict(data, pos, depth),
        b'0'..=b'9' => decode_bytes(data, pos),
        _ => Err(BencodeError {
            message: format!("invalid type tag 0x{:02x}", tag),
            offset: *pos,
        }),
    }
}

fn decode_int(data: &[u8], pos: &mut usize) -> Result<Value, BencodeError> {
    let start = *pos;
    let Some(end) = data[start + 1..].iter().position(|&b| b == b'e') else {
        return Err(BencodeError {
            message: "unterminated integer".into(),
            offset: start,
        });
    };
    let digits = &data[start + 1..start + 1 + end];
    if digits.is_empty() || (digits.len() > 1 && (digits[0] == b'0' || digits == b"-0")) {
        return Err(BencodeError {
            message: "invalid integer literal".into(),
            offset: start,
        });
    }
    let negative = digits[0] == b'-';
    let body = if negative { &digits[1..] } else { digits };
    if body.is_empty() || !body.iter().all(u8::is_ascii_digit) {
        return Err(BencodeError {
            message: "invalid integer literal".into(),
            offset: start,
        });
    }
    let mut value: i64 = 0;
    for &d in body {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add((d - b'0') as i64))
            .ok_or(BencodeError {
                message: "integer overflow".into(),
                offset: start,
            })?;
    }
    *pos = start + 1 + end + 1;
    Ok(Value::Int(if negative { -value } else { value }))
}

fn decode_bytes(data: &[u8], pos: &mut usize) -> Result<Value, BencodeError> {
    let start = *pos;
    let Some(colon) = data[start..].iter().position(|&b| b == b':') else {
        return Err(BencodeError {
            message: "unterminated string length".into(),
            offset: start,
        });
    };
    let len_digits = &data[start..start + colon];
    if len_digits.is_empty() || (len_digits.len() > 1 && len_digits[0] == b'0') {
        return Err(BencodeError {
            message: "invalid string length".into(),
            offset: start,
        });
    }
    if !len_digits.iter().all(u8::is_ascii_digit) {
        return Err(BencodeError {
            message: "invalid string length".into(),
            offset: start,
        });
    }
    let len: usize = std::str::from_utf8(len_digits)
        .map_err(|_| BencodeError {
            message: "invalid string length".into(),
            offset: start,
        })?
        .parse()
        .map_err(|_| BencodeError {
            message: "string length overflow".into(),
            offset: start,
        })?;
    let body_start = start + colon + 1;
    let body_end = body_start
        .checked_add(len)
        .ok_or(BencodeError {
            message: "string length overflow".into(),
            offset: start,
        })?;
    if body_end > data.len() {
        return Err(BencodeError {
            message: "string body out of range".into(),
            offset: start,
        });
    }
    *pos = body_end;
    Ok(Value::Bytes(data[body_start..body_end].to_vec()))
}

fn decode_list(data: &[u8], pos: &mut usize, depth: usize) -> Result<Value, BencodeError> {
    *pos += 1;
    let mut items = Vec::new();
    loop {
        if data.get(*pos) == Some(&b'e') {
            *pos += 1;
            return Ok(Value::List(items));
        }
        items.push(decode_value(data, pos, depth + 1)?);
    }
}

fn decode_dict(data: &[u8], pos: &mut usize, depth: usize) -> Result<Value, BencodeError> {
    *pos += 1;
    let mut pairs: Vec<(Vec<u8>, Value)> = Vec::new();
    loop {
        if data.get(*pos) == Some(&b'e') {
            *pos += 1;
            return Ok(Value::Dict(pairs));
        }
        let key = match decode_value(data, pos, depth + 1)? {
            Value::Bytes(k) => k,
            _ => {
                return Err(BencodeError {
                    message: "dictionary key must be a string".into(),
                    offset: *pos,
                })
            }
        };
        let value = decode_value(data, pos, depth + 1)?;
        match pairs.iter_mut().find(|(existing, _)| *existing == key) {
            Some(slot) => slot.1 = value,
            None => pairs.push((key, value)),
        }
    }
}

pub fn encode(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    encode_value(value, &mut out);
    out
}

fn encode_value(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Int(v) => {
            out.push(b'i');
            out.extend_from_slice(v.to_string().as_bytes());
            out.push(b'e');
        }
        Value::Bytes(v) => {
            out.extend_from_slice(v.len().to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(v);
        }
        Value::List(items) => {
            out.push(b'l');
            for item in items {
                encode_value(item, out);
            }
            out.push(b'e');
        }
        Value::Dict(pairs) => {
            out.push(b'd');
            for (key, item) in pairs {
                encode_value(&Value::Bytes(key.clone()), out);
                encode_value(item, out);
            }
            out.push(b'e');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_ints() {
        assert_eq!(decode(b"i42e").unwrap(), Value::Int(42));
        assert_eq!(decode(b"i-7e").unwrap(), Value::Int(-7));
        assert_eq!(decode(b"i0e").unwrap(), Value::Int(0));
        assert!(decode(b"i-0e").is_err());
        assert!(decode(b"i01e").is_err());
        assert!(decode(b"ie").is_err());
    }

    #[test]
    fn decode_strings() {
        assert_eq!(decode(b"4:spam").unwrap(), Value::Bytes(b"spam".to_vec()));
        assert_eq!(decode(b"0:").unwrap(), Value::Bytes(vec![]));
        assert!(decode(b"04:spam").is_err());
        assert!(decode(b"5:spam").is_err());
    }

    #[test]
    fn decode_containers() {
        let v = decode(b"l4:spami1ed3:bar4:spamee").unwrap();
        assert_eq!(
            v,
            Value::List(vec![
                Value::Bytes(b"spam".to_vec()),
                Value::Int(1),
                Value::Dict(vec![(b"bar".to_vec(), Value::Bytes(b"spam".to_vec()))]),
            ])
        );
    }

    #[test]
    fn decode_rejects_trailing() {
        assert!(decode(b"i1ei2e").is_err());
    }

    #[test]
    fn decode_allows_unsorted_and_duplicate_keys_last_wins() {
        assert_eq!(
            decode(b"d1:bi1e1:ai2ee").unwrap(),
            Value::Dict(vec![
                (b"b".to_vec(), Value::Int(1)),
                (b"a".to_vec(), Value::Int(2)),
            ])
        );
        assert_eq!(
            decode(b"d1:ai1e1:ai2ee").unwrap(),
            Value::Dict(vec![(b"a".to_vec(), Value::Int(2))])
        );
    }

    #[test]
    fn decode_prefix_returns_consumed_offset() {
        let (value, consumed) = decode_prefix(b"d1:ai1eeTRAILING").unwrap();
        assert_eq!(value.str_key("a").unwrap().as_int(), Some(1));
        assert_eq!(consumed, 8);
        assert!(decode_prefix(b"i42e").is_ok());
        assert!(decode_prefix(b"x").is_err());
    }

    #[test]
    fn roundtrip() {
        let value = Value::Dict(vec![
            (b"a".to_vec(), Value::List(vec![Value::Int(1), Value::Int(-2)])),
            (b"b".to_vec(), Value::Bytes(vec![0xff, 0x00, 0x7f])),
        ]);
        let encoded = encode(&value);
        assert_eq!(decode(&encoded).unwrap(), value);
    }

    #[test]
    fn accessors() {
        let value = Value::Dict(vec![(b"id".to_vec(), Value::Bytes(b"x".repeat(20)))]);
        assert_eq!(value.str_key("id").unwrap().as_bytes().unwrap().len(), 20);
        assert!(value.str_key("missing").is_none());
    }

    #[test]
    fn depth_limit() {
        let mut data = Vec::new();
        for _ in 0..64 {
            data.push(b'l');
        }
        for _ in 0..64 {
            data.push(b'e');
        }
        assert!(decode(&data).is_err());
    }
}
