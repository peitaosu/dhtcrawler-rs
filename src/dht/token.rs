use crate::dht::id::NodeId;
use sha1::{Digest, Sha1};
use std::net::IpAddr;
use std::time::{SystemTime, UNIX_EPOCH};

pub const EPOCH_SECONDS: u64 = 600;

const HMAC_BLOCK_SIZE: usize = 64;
const TOKEN_LEN: usize = 8;

pub struct TokenIssuer {
    secret: [u8; 20],
}

impl TokenIssuer {
    pub fn new() -> Self {
        let secret = NodeId::random().0;
        TokenIssuer { secret }
    }

    fn epoch(now_seconds: u64) -> u64 {
        now_seconds / EPOCH_SECONDS
    }

    fn compute(&self, ip: IpAddr, epoch: u64) -> [u8; 20] {
        let epoch_bytes = epoch.to_be_bytes();
        let ip_bytes: &[u8] = match ip {
            IpAddr::V4(v4) => &v4.octets(),
            IpAddr::V6(v6) => &v6.octets(),
        };
        hmac_sha1(&self.secret, &[&epoch_bytes, ip_bytes])
    }

    fn current_epoch() -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self::epoch(now)
    }

    pub fn generate(&self, ip: IpAddr) -> Vec<u8> {
        self.compute(ip, Self::current_epoch())[..TOKEN_LEN].to_vec()
    }

    pub fn verify(&self, ip: IpAddr, token: &[u8]) -> bool {
        let epoch = Self::current_epoch();
        constant_time_eq(token, &self.compute(ip, epoch)[..TOKEN_LEN])
            || constant_time_eq(token, &self.compute(ip, epoch.saturating_sub(1))[..TOKEN_LEN])
    }
}

fn hmac_sha1(key: &[u8], message: &[&[u8]]) -> [u8; 20] {
    let mut key_block = [0u8; HMAC_BLOCK_SIZE];
    if key.len() > HMAC_BLOCK_SIZE {
        key_block.copy_from_slice(&Sha1::digest(key));
    } else {
        key_block[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha1::new();
    for byte in &key_block {
        inner.update([*byte ^ 0x36]);
    }
    for chunk in message {
        inner.update(chunk);
    }
    let inner_digest = inner.finalize();
    let mut outer = Sha1::new();
    for byte in &key_block {
        outer.update([*byte ^ 0x5c]);
    }
    outer.update(inner_digest);
    outer.finalize().into()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |diff, (x, y)| diff | (x ^ y))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn generate_and_verify() {
        let issuer = TokenIssuer::new();
        let ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        let token = issuer.generate(ip);
        assert_eq!(token.len(), TOKEN_LEN);
        assert!(issuer.verify(ip, &token));
    }

    #[test]
    fn wrong_ip_rejected() {
        let issuer = TokenIssuer::new();
        let token = issuer.generate(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)));
        assert!(!issuer.verify(IpAddr::V4(Ipv4Addr::new(5, 6, 7, 8)), &token));
    }

    #[test]
    fn tokens_differ_per_ip() {
        let issuer = TokenIssuer::new();
        let a = issuer.generate(IpAddr::V4(Ipv4Addr::new(9, 9, 9, 9)));
        let b = issuer.generate(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)));
        assert_ne!(a, b);
    }

    #[test]
    fn tokens_differ_per_secret() {
        let ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        assert_ne!(TokenIssuer::new().generate(ip), TokenIssuer::new().generate(ip));
    }

    #[test]
    fn hmac_matches_reference_structure() {
        let digest = hmac_sha1(b"key", &[b"The quick brown fox jumps over the lazy dog"]);
        assert_eq!(digest.len(), 20);
        assert_eq!(hmac_sha1(b"key", &[b"The quick brown fox jumps over the lazy dog"]), digest);
        assert_ne!(hmac_sha1(b"key2", &[b"The quick brown fox jumps over the lazy dog"]), digest);
    }

    #[test]
    fn constant_time_compare() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }
}
