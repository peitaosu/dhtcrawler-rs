use dhtcrawler::common::bencode::{self, Value};
use sha1::{Digest, Sha1};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub const FAKE_PEER_UT_METADATA_ID: u8 = 3;

pub fn build_info_dict(name: &str, length: i64) -> Vec<u8> {
    let mut pairs = vec![
        (b"length".to_vec(), Value::Int(length)),
        (b"name".to_vec(), Value::Bytes(name.as_bytes().to_vec())),
        (b"piece length".to_vec(), Value::Int(16384)),
    ];
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    bencode::encode(&Value::Dict(pairs))
}

pub fn info_hash_of(info_dict: &[u8]) -> [u8; 20] {
    Sha1::digest(info_dict).into()
}

pub struct FakeBtPeer {
    pub addr: SocketAddr,
}

pub async fn spawn_fake_peer(info_hash: [u8; 20], metadata: Vec<u8>) -> FakeBtPeer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        serve_peer(listener, info_hash, metadata).await;
    });
    FakeBtPeer { addr }
}

async fn serve_peer(listener: TcpListener, info_hash: [u8; 20], metadata: Vec<u8>) {
    loop {
        let Ok((mut socket, _)) = listener.accept().await else {
            return;
        };
        let metadata = metadata.clone();
        let info_hash = info_hash;
        let result = serve_connection(&mut socket, info_hash, &metadata).await;
        if result.is_err() {
            let _ = socket.shutdown().await;
        }
    }
}

async fn serve_connection(
    socket: &mut TcpStream,
    info_hash: [u8; 20],
    metadata: &[u8],
) -> Result<(), String> {
    let mut buffer = vec![0u8; 68];
    socket.read_exact(&mut buffer).await.map_err(|e| e.to_string())?;
    let handshake =
        dhtcrawler::metadata::bt_message::decode_handshake(&buffer).map_err(|e| e)?;
    if handshake.info_hash != info_hash {
        return Err("unexpected info_hash".into());
    }
    let peer_id = [0xabu8; 20];
    socket
        .write_all(&dhtcrawler::metadata::bt_message::encode_handshake(
            &info_hash,
            &peer_id,
        ))
        .await
        .map_err(|e| e.to_string())?;
    let _ = socket.read_frame().await?;
    let ext_handshake_dict = Value::Dict(vec![
        (
            b"m".to_vec(),
            Value::Dict(vec![(b"ut_metadata".to_vec(), Value::Int(FAKE_PEER_UT_METADATA_ID as i64))]),
        ),
        (b"metadata_size".to_vec(), Value::Int(metadata.len() as i64)),
    ]);
    socket
        .write_all(&dhtcrawler::metadata::bt_message::encode_extended_raw(
            dhtcrawler::metadata::bt_message::EXT_HANDSHAKE_ID,
            &ext_handshake_dict,
            b"",
        ))
        .await
        .map_err(|e| e.to_string())?;
    loop {
        let frame = socket.read_frame().await?;
        let message =
            dhtcrawler::metadata::bt_message::decode_frame(&frame).map_err(|e| e)?;
        let Some((message, _)) = message else {
            continue;
        };
        match message {
            dhtcrawler::metadata::bt_message::WireMessage::Extended { extended_id: FAKE_PEER_UT_METADATA_ID, payload } => {
                match dhtcrawler::metadata::bt_message::decode_ut_metadata(&payload)? {
                    dhtcrawler::metadata::bt_message::UtMetadataMessage::Request { piece } => {
                        let start = piece as usize * dhtcrawler::metadata::bt_message::METADATA_PIECE_SIZE;
                        let end = (start + dhtcrawler::metadata::bt_message::METADATA_PIECE_SIZE)
                            .min(metadata.len());
                        if start >= metadata.len() {
                            return Err("piece out of range".into());
                        }
                        let dict = Value::Dict(vec![
                            (b"msg_type".to_vec(), Value::Int(1)),
                            (b"piece".to_vec(), Value::Int(piece as i64)),
                            (b"total_size".to_vec(), Value::Int(metadata.len() as i64)),
                        ]);
                        socket
                            .write_all(&dhtcrawler::metadata::bt_message::encode_extended_raw(
                                dhtcrawler::metadata::bt_message::OUR_UT_METADATA_ID,
                                &dict,
                                &metadata[start..end],
                            ))
                            .await
                            .map_err(|e| e.to_string())?;
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
}

trait ReadFrame {
    async fn read_frame(&mut self) -> Result<Vec<u8>, String>;
}

impl ReadFrame for TcpStream {
    async fn read_frame(&mut self) -> Result<Vec<u8>, String> {
        let mut length_bytes = [0u8; 4];
        self.read_exact(&mut length_bytes)
            .await
            .map_err(|e| e.to_string())?;
        let length = u32::from_be_bytes(length_bytes) as usize;
        let mut frame = vec![0u8; length];
        self.read_exact(&mut frame).await.map_err(|e| e.to_string())?;
        frame.splice(0..0, length_bytes);
        Ok(frame)
    }
}
