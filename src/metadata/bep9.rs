use crate::metadata::bt_message::{
    decode_frame, decode_handshake, decode_ut_metadata, encode_ext_handshake, encode_handshake,
    encode_metadata_request, UtMetadataMessage, WireMessage, METADATA_PIECE_SIZE,
    OUR_UT_METADATA_ID,
};
use sha1::{Digest, Sha1};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub const MAX_METADATA_SIZE: usize = 8 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum Bep9Error {
    #[error("connect failed: {0}")]
    Connect(String),
    #[error("handshake failed: {0}")]
    Handshake(String),
    #[error("peer does not support ut_metadata")]
    NotSupported,
    #[error("rejected")]
    Rejected,
    #[error("invalid metadata: {0}")]
    Invalid(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("timeout")]
    Timeout,
}

pub async fn fetch_metadata(
    addr: SocketAddr,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    overall_timeout: Duration,
) -> Result<Vec<u8>, Bep9Error> {
    let result = tokio::time::timeout(overall_timeout, fetch_metadata_inner(addr, info_hash, peer_id)).await;
    match result {
        Ok(inner) => inner,
        Err(_) => Err(Bep9Error::Timeout),
    }
}

async fn fetch_metadata_inner(
    addr: SocketAddr,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
) -> Result<Vec<u8>, Bep9Error> {
    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|e| Bep9Error::Connect(e.to_string()))?;
    stream
        .write_all(&encode_handshake(&info_hash, &peer_id))
        .await
        .map_err(|e| Bep9Error::Io(e.to_string()))?;
    stream
        .write_all(&encode_ext_handshake(None))
        .await
        .map_err(|e| Bep9Error::Io(e.to_string()))?;
    let handshake = read_handshake(&mut stream).await?;
    if handshake.info_hash != info_hash {
        return Err(Bep9Error::Handshake("info_hash mismatch".into()));
    }
    if !handshake.supports_extension {
        return Err(Bep9Error::NotSupported);
    }
    let mut peer_ut_metadata_id: Option<u8> = None;
    let mut metadata_size: Option<usize> = None;
    let mut pieces: Option<Vec<Vec<u8>>> = None;
    let mut read_buffer: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let len = stream
            .read(&mut chunk)
            .await
            .map_err(|e| Bep9Error::Io(e.to_string()))?;
        if len == 0 {
            return Err(Bep9Error::Io("peer closed connection".into()));
        }
        read_buffer.extend_from_slice(&chunk[..len]);
        while let Some((message, consumed)) = decode_frame(&read_buffer)
            .map_err(Bep9Error::Invalid)?
        {
            read_buffer.drain(..consumed);
            match message {
                WireMessage::Extended { extended_id, payload } => {
                    if extended_id == 0 {
                        let dict = crate::common::bencode::decode(&payload)
                            .map_err(|e| Bep9Error::Invalid(e.to_string()))?;
                        let id = dict
                            .str_key("m")
                            .and_then(|m| m.str_key("ut_metadata"))
                            .and_then(|v| v.as_int());
                        peer_ut_metadata_id = id.map(|v| v as u8);
                        let announced_size = dict
                            .str_key("metadata_size")
                            .and_then(|v| v.as_int())
                            .and_then(|v| usize::try_from(v).ok());
                        if announced_size.is_none_or(|size| size == 0 || size > MAX_METADATA_SIZE) {
                            return Err(Bep9Error::NotSupported);
                        }
                        metadata_size = announced_size;
                        let id = peer_ut_metadata_id.ok_or(Bep9Error::NotSupported)?;
                        let expected_pieces = announced_size
                            .unwrap_or_default()
                            .div_ceil(METADATA_PIECE_SIZE);
                        pieces = Some(vec![Vec::new(); expected_pieces]);
                        stream
                            .write_all(&encode_metadata_request(id, 0))
                            .await
                            .map_err(|e| Bep9Error::Io(e.to_string()))?;
                    } else if extended_id == OUR_UT_METADATA_ID {
                        let Some(store) = pieces.as_mut() else {
                            return Err(Bep9Error::Invalid("metadata data before handshake".into()));
                        };
                        match decode_ut_metadata(&payload).map_err(Bep9Error::Invalid)? {
                            UtMetadataMessage::Data { piece, total_size, data } => {
                                if data.len() > METADATA_PIECE_SIZE {
                                    return Err(Bep9Error::Invalid("metadata piece exceeds piece size".into()));
                                }
                                if total_size != metadata_size.unwrap_or_default() as u64 {
                                    return Err(Bep9Error::Invalid("metadata total_size mismatch".into()));
                                }
                                let index = piece as usize;
                                let slot = store
                                    .get_mut(index)
                                    .ok_or_else(|| Bep9Error::Invalid("piece out of range".into()))?;
                                *slot = data;
                                if let Some(next) = next_missing_piece(store) {
                                    let id = peer_ut_metadata_id.unwrap();
                                    stream
                                        .write_all(&encode_metadata_request(id, next as u32))
                                        .await
                                        .map_err(|e| Bep9Error::Io(e.to_string()))?;
                                } else {
                                    let metadata = pieces.as_deref().unwrap_or_default().concat();
                                    verify_metadata(&metadata, &info_hash)?;
                                    return Ok(metadata);
                                }
                            }
                            UtMetadataMessage::Reject { .. } => return Err(Bep9Error::Rejected),
                            UtMetadataMessage::Request { .. } => {}
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

async fn read_handshake(stream: &mut TcpStream) -> Result<crate::metadata::bt_message::DecodedHandshake, Bep9Error> {
    let mut buffer = vec![0u8; 68];
    match stream.read_exact(&mut buffer).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Err(Bep9Error::Handshake("peer closed during handshake".into()));
        }
        Err(e) => return Err(Bep9Error::Io(e.to_string())),
    }
    decode_handshake(&buffer).map_err(Bep9Error::Handshake)
}

fn next_missing_piece(pieces: &[Vec<u8>]) -> Option<usize> {
    pieces.iter().position(|p| p.is_empty())
}

fn verify_metadata(metadata: &[u8], info_hash: &[u8; 20]) -> Result<(), Bep9Error> {
    let digest = Sha1::digest(metadata);
    if digest.as_slice() == info_hash {
        Ok(())
    } else {
        Err(Bep9Error::Invalid("metadata hash mismatch".into()))
    }
}

pub fn random_peer_id() -> [u8; 20] {
    let mut peer_id = [0u8; 20];
    peer_id[..8].copy_from_slice(b"-TR4050-");
    for byte in &mut peer_id[8..] {
        *byte = rand::random::<u8>();
    }
    peer_id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_id_format() {
        let peer_id = random_peer_id();
        assert_eq!(&peer_id[..8], b"-TR4050-");
        assert_eq!(peer_id.len(), 20);
    }

    #[test]
    fn piece_count_math() {
        assert_eq!(39000usize.div_ceil(METADATA_PIECE_SIZE), 3);
        assert_eq!(16384usize.div_ceil(METADATA_PIECE_SIZE), 1);
        assert_eq!(16385usize.div_ceil(METADATA_PIECE_SIZE), 2);
    }

    #[test]
    fn metadata_size_cap_is_sane() {
        assert!(MAX_METADATA_SIZE >= 1024 * 1024);
        assert_eq!(MAX_METADATA_SIZE % METADATA_PIECE_SIZE, 0);
        assert_eq!(MAX_METADATA_SIZE / METADATA_PIECE_SIZE, 512);
    }

    #[test]
    fn next_missing() {
        assert_eq!(next_missing_piece(&[vec![1], vec![]]), Some(1));
        assert_eq!(next_missing_piece(&[vec![1]]), None);
    }
}
