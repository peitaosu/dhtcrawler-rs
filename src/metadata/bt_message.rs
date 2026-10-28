use crate::common::bencode::{self, Value};

pub const EXTENSION_MESSAGE_ID: u8 = 20;
pub const EXT_HANDSHAKE_ID: u8 = 0;
pub const PROTOCOL_STRING: &[u8] = b"BitTorrent protocol";
pub const METADATA_PIECE_SIZE: usize = 16384;
pub const OUR_UT_METADATA_ID: u8 = 1;
pub const MAX_FRAME_SIZE: usize = 256 * 1024;

pub fn encode_handshake(info_hash: &[u8; 20], peer_id: &[u8; 20]) -> Vec<u8> {
    let mut out = Vec::with_capacity(68);
    out.push(PROTOCOL_STRING.len() as u8);
    out.extend_from_slice(PROTOCOL_STRING);
    out.extend_from_slice(&[0, 0, 0, 0, 0, 0x10, 0, 0]);
    out.extend_from_slice(info_hash);
    out.extend_from_slice(peer_id);
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedHandshake {
    pub reserved: [u8; 8],
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub supports_extension: bool,
}

pub fn decode_handshake(data: &[u8]) -> Result<DecodedHandshake, String> {
    if data.len() < 49 {
        return Err("handshake too short".into());
    }
    let protocol_len = data[0] as usize;
    if data.len() < 1 + protocol_len + 8 + 20 + 20 {
        return Err("handshake too short".into());
    }
    let start = 1 + protocol_len;
    if &data[1..start] != PROTOCOL_STRING {
        return Err("unexpected protocol string".into());
    }
    let reserved: [u8; 8] = data[start..start + 8].try_into().unwrap();
    let info_hash: [u8; 20] = data[start + 8..start + 28].try_into().unwrap();
    let peer_id: [u8; 20] = data[start + 28..start + 48].try_into().unwrap();
    Ok(DecodedHandshake {
        supports_extension: reserved[5] & 0x10 != 0,
        reserved,
        info_hash,
        peer_id,
    })
}

pub fn encode_extended_message(extended_id: u8, payload: &Value) -> Vec<u8> {
    let body = bencode::encode(payload);
    let mut out = Vec::with_capacity(body.len() + 6);
    out.extend_from_slice(&((body.len() + 2) as u32).to_be_bytes());
    out.push(EXTENSION_MESSAGE_ID);
    out.push(extended_id);
    out.extend_from_slice(&body);
    out
}

pub fn encode_extended_raw(extended_id: u8, dict: &Value, trailing: &[u8]) -> Vec<u8> {
    let body = bencode::encode(dict);
    let total = body.len() + trailing.len() + 2;
    let mut out = Vec::with_capacity(total + 4);
    out.extend_from_slice(&(total as u32).to_be_bytes());
    out.push(EXTENSION_MESSAGE_ID);
    out.push(extended_id);
    out.extend_from_slice(&body);
    out.extend_from_slice(trailing);
    out
}

pub fn encode_ext_handshake(metadata_size: Option<u64>) -> Vec<u8> {
    let m = Value::Dict(vec![(
        b"ut_metadata".to_vec(),
        Value::Int(OUR_UT_METADATA_ID as i64),
    )]);
    let mut dict = vec![(b"m".to_vec(), m)];
    if let Some(size) = metadata_size {
        dict.push((b"metadata_size".to_vec(), Value::Int(size as i64)));
    }
    dict.sort_by(|a, b| a.0.cmp(&b.0));
    encode_extended_message(EXT_HANDSHAKE_ID, &Value::Dict(dict))
}

pub fn encode_metadata_request(extended_id: u8, piece: u32) -> Vec<u8> {
    let dict = Value::Dict(vec![
        (b"msg_type".to_vec(), Value::Int(0)),
        (b"piece".to_vec(), Value::Int(piece as i64)),
    ]);
    encode_extended_message(extended_id, &dict)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireMessage {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have { index: u32 },
    Bitfield,
    Request,
    Piece,
    Cancel,
    Unknown(u8),
    Extended { extended_id: u8, payload: Vec<u8> },
}

pub fn decode_frame(data: &[u8]) -> Result<Option<(WireMessage, usize)>, String> {
    if data.len() < 4 {
        return Ok(None);
    }
    let length = u32::from_be_bytes(data[..4].try_into().unwrap()) as usize;
    if length > MAX_FRAME_SIZE {
        return Err("frame exceeds maximum size".into());
    }
    if data.len() < 4 + length {
        return Ok(None);
    }
    let frame = &data[4..4 + length];
    let consumed = 4 + length;
    if length == 0 {
        return Ok(Some((WireMessage::KeepAlive, consumed)));
    }
    let message_id = frame[0];
    let message = match message_id {
        0 => WireMessage::Choke,
        1 => WireMessage::Unchoke,
        2 => WireMessage::Interested,
        3 => WireMessage::NotInterested,
        4 => WireMessage::Have {
            index: u32::from_be_bytes(frame.get(1..5).ok_or("bad have")?.try_into().unwrap()),
        },
        5 => WireMessage::Bitfield,
        6 => WireMessage::Request,
        7 => WireMessage::Piece,
        8 => WireMessage::Cancel,
        9 => WireMessage::Unknown(9),
        EXTENSION_MESSAGE_ID => WireMessage::Extended {
            extended_id: *frame.get(1).ok_or("extended message missing id")?,
            payload: frame[2..].to_vec(),
        },
        other => WireMessage::Unknown(other),
    };
    Ok(Some((message, consumed)))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UtMetadataMessage {
    Request { piece: u32 },
    Data { piece: u32, total_size: u64, data: Vec<u8> },
    Reject { piece: u32 },
}

pub fn decode_ut_metadata(payload: &[u8]) -> Result<UtMetadataMessage, String> {
    let (dict, dict_len) = split_bencoded_prefix(payload)?;
    let msg_type = dict
        .str_key("msg_type")
        .and_then(|v| v.as_int())
        .ok_or("ut_metadata missing msg_type")?;
    let piece = dict
        .str_key("piece")
        .and_then(|v| v.as_int())
        .ok_or("ut_metadata missing piece")?;
    let piece = u32::try_from(piece).map_err(|_| "bad piece")?;
    match msg_type {
        0 => Ok(UtMetadataMessage::Request { piece }),
        1 => {
            let total_size = dict
                .str_key("total_size")
                .and_then(|v| v.as_int())
                .ok_or("ut_metadata data missing total_size")?;
            Ok(UtMetadataMessage::Data {
                piece,
                total_size: u64::try_from(total_size).map_err(|_| "bad total_size")?,
                data: payload[dict_len..].to_vec(),
            })
        }
        2 => Ok(UtMetadataMessage::Reject { piece }),
        other => Err(format!("unknown ut_metadata msg_type {}", other)),
    }
}

fn split_bencoded_prefix(payload: &[u8]) -> Result<(Value, usize), String> {
    let (value, consumed) = bencode::decode_prefix(payload).map_err(|e| e.to_string())?;
    if !matches!(value, Value::Dict(_)) {
        return Err("ut_metadata payload is not a dictionary".into());
    }
    Ok((value, consumed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_roundtrip() {
        let info_hash = [7u8; 20];
        let peer_id = [9u8; 20];
        let encoded = encode_handshake(&info_hash, &peer_id);
        assert_eq!(encoded.len(), 68);
        let decoded = decode_handshake(&encoded).unwrap();
        assert_eq!(decoded.info_hash, info_hash);
        assert_eq!(decoded.peer_id, peer_id);
        assert!(decoded.supports_extension);
    }

    #[test]
    fn handshake_rejects_short() {
        assert!(decode_handshake(&[0u8; 10]).is_err());
        assert!(decode_handshake(&[0u8; 100]).is_err());
    }

    #[test]
    fn ext_handshake_decode() {
        let encoded = encode_ext_handshake(Some(39000));
        let (message, consumed) = decode_frame(&encoded).unwrap().unwrap();
        assert_eq!(consumed, encoded.len());
        match message {
            WireMessage::Extended { extended_id, payload } => {
                assert_eq!(extended_id, 0);
                let dict =
                    bencode::decode(&payload).expect("valid bencode");
                assert_eq!(
                    dict.str_key("m")
                        .unwrap()
                        .str_key("ut_metadata")
                        .unwrap()
                        .as_int(),
                    Some(1)
                );
                assert_eq!(
                    dict.str_key("metadata_size").and_then(|v| v.as_int()),
                    Some(39000)
                );
            }
            other => panic!("expected extended, got {:?}", other),
        }
    }

    #[test]
    fn metadata_request_roundtrip() {
        let encoded = encode_metadata_request(1, 3);
        let (message, _) = decode_frame(&encoded).unwrap().unwrap();
        match message {
            WireMessage::Extended { extended_id, payload } => {
                assert_eq!(extended_id, 1);
                assert_eq!(
                    decode_ut_metadata(&payload).unwrap(),
                    UtMetadataMessage::Request { piece: 3 }
                );
            }
            other => panic!("expected extended, got {:?}", other),
        }
    }

    #[test]
    fn metadata_data_and_reject_decode() {
        let dict = Value::Dict(vec![
            (b"msg_type".to_vec(), Value::Int(1)),
            (b"piece".to_vec(), Value::Int(2)),
            (b"total_size".to_vec(), Value::Int(100)),
        ]);
        let mut payload = bencode::encode(&dict);
        payload.extend_from_slice(b"rawdata");
        assert_eq!(
            decode_ut_metadata(&payload).unwrap(),
            UtMetadataMessage::Data {
                piece: 2,
                total_size: 100,
                data: b"rawdata".to_vec()
            }
        );
        let reject = Value::Dict(vec![
            (b"msg_type".to_vec(), Value::Int(2)),
            (b"piece".to_vec(), Value::Int(4)),
        ]);
        assert_eq!(
            decode_ut_metadata(&bencode::encode(&reject)).unwrap(),
            UtMetadataMessage::Reject { piece: 4 }
        );
    }

    #[test]
    fn frame_needs_more_bytes() {
        assert_eq!(decode_frame(b"").unwrap(), None);
        assert_eq!(decode_frame(b"\x00\x00").unwrap(), None);
        let encoded = encode_metadata_request(1, 0);
        assert_eq!(decode_frame(&encoded[..encoded.len() - 1]).unwrap(), None);
    }

    #[test]
    fn oversized_frame_rejected() {
        let length = (MAX_FRAME_SIZE + 1) as u32;
        let mut data = length.to_be_bytes().to_vec();
        data.extend_from_slice(&[0u8; 32]);
        assert!(decode_frame(&data).is_err());
    }

    #[test]
    fn keep_alive_frame() {
        let (message, consumed) = decode_frame(&[0, 0, 0, 0]).unwrap().unwrap();
        assert_eq!(message, WireMessage::KeepAlive);
        assert_eq!(consumed, 4);
    }

    #[test]
    fn other_wire_messages() {
        let frame = [0, 0, 0, 1, 5, 0xff, 0xff];
        let (message, consumed) = decode_frame(&frame).unwrap().unwrap();
        assert_eq!(message, WireMessage::Bitfield);
        assert_eq!(consumed, 5);
        let have = [0, 0, 0, 5, 4, 0, 0, 0, 9];
        let (message, _) = decode_frame(&have).unwrap().unwrap();
        assert_eq!(message, WireMessage::Have { index: 9 });
    }
}
