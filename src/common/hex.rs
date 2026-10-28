pub fn encode_upper(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len() * 2);
    for byte in data {
        out.push_str(&format!("{:02X}", byte));
    }
    out
}

pub fn decode(data: &[u8]) -> Option<Vec<u8>> {
    if data.len() % 2 != 0 || data.len() > 4096 {
        return None;
    }
    let mut out = Vec::with_capacity(data.len() / 2);
    for pair in data.chunks(2) {
        let high = (pair[0] as char).to_digit(16)?;
        let low = (pair[1] as char).to_digit(16)?;
        out.push((high * 16 + low) as u8);
    }
    Some(out)
}

pub fn is_hash(data: &[u8]) -> bool {
    data.len() == 40 && data.iter().all(|b| b.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upper_case_roundtrip() {
        let data = [0xde, 0xad, 0xbe, 0xef, 0x01, 0x0a];
        let hex_str = encode_upper(&data);
        assert_eq!(hex_str, "DEADBEEF010A");
        assert_eq!(decode(hex_str.as_bytes()).unwrap(), data);
    }

    #[test]
    fn decode_invalid() {
        assert!(decode(b"XYZ").is_none());
        assert!(decode(b"ZZ").is_none());
        assert!(decode(b"0").is_none());
    }

    #[test]
    fn is_hash_check() {
        assert!(is_hash(b"7C6932E7EC1CF5B00AE991871E57B2375DADA5A0"));
        assert!(!is_hash(b"7c6932e7ec1cf5b00ae991871e57b2375dada5a"));
        assert!(!is_hash(b"short"));
    }
}
