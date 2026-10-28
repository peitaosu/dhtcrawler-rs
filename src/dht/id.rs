use std::fmt;

pub const ID_SIZE: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub [u8; ID_SIZE]);

impl NodeId {
    pub fn random() -> Self {
        use rand::RngExt;
        let mut bytes = [0u8; ID_SIZE];
        rand::rng().fill(&mut bytes);
        NodeId(bytes)
    }

    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        let bytes: [u8; ID_SIZE] = data.try_into().ok()?;
        Some(NodeId(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn distance(&self, other: &NodeId) -> [u8; ID_SIZE] {
        let mut out = [0u8; ID_SIZE];
        for i in 0..ID_SIZE {
            out[i] = self.0[i] ^ other.0[i];
        }
        out
    }

    pub fn distance_cmp(&self, a: &NodeId, b: &NodeId) -> std::cmp::Ordering {
        self.distance(a).cmp(&self.distance(b))
    }

    pub fn to_hex(&self) -> String {
        crate::common::hex::encode_upper(&self.0)
    }

    pub fn from_hex(data: &str) -> Option<Self> {
        let bytes = crate::common::hex::decode(data.as_bytes())?;
        NodeId::from_bytes(&bytes)
    }

    pub fn min() -> Self {
        NodeId([0u8; ID_SIZE])
    }

    pub fn max() -> Self {
        NodeId([0xff; ID_SIZE])
    }

    pub fn is_valid(&self) -> bool {
        self.0.iter().any(|&b| b != 0x00) && self.0.iter().any(|&b| b != 0xff)
    }

    pub fn random_discrete_ids(count: usize) -> Vec<NodeId> {
        if count <= 1 {
            return vec![NodeId::random()];
        }
        if count > 65536 {
            return (0..count).map(|_| NodeId::random()).collect();
        }
        let mut ids = Vec::with_capacity(count);
        for index in 0..count {
            let prefix = ((index * 65536) / count) as u16;
            let mut bytes = [0u8; ID_SIZE];
            bytes[..2].copy_from_slice(&prefix.to_be_bytes());
            {
                use rand::RngExt;
                rand::rng().fill(&mut bytes[2..]);
            }
            ids.push(NodeId(bytes));
        }
        ids
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

pub fn big_sub(a: &[u8; ID_SIZE], b: &[u8; ID_SIZE]) -> [u8; ID_SIZE] {
    let mut out = [0u8; ID_SIZE];
    let mut borrow = 0i16;
    for i in (0..ID_SIZE).rev() {
        let diff = a[i] as i16 - b[i] as i16 - borrow;
        if diff < 0 {
            out[i] = (diff + 256) as u8;
            borrow = 1;
        } else {
            out[i] = diff as u8;
            borrow = 0;
        }
    }
    out
}

pub fn big_add(a: &[u8; ID_SIZE], b: &[u8; ID_SIZE]) -> [u8; ID_SIZE] {
    let mut out = [0u8; ID_SIZE];
    let mut carry = 0u16;
    for i in (0..ID_SIZE).rev() {
        let sum = a[i] as u16 + b[i] as u16 + carry;
        out[i] = (sum & 0xff) as u8;
        carry = sum >> 8;
    }
    out
}

pub fn big_div2(a: &[u8; ID_SIZE]) -> [u8; ID_SIZE] {
    let mut out = [0u8; ID_SIZE];
    let mut carry = 0u8;
    for i in 0..ID_SIZE {
        out[i] = (a[i] >> 1) | (carry << 7);
        carry = a[i] & 1;
    }
    out
}

pub fn big_mul_small(a: &[u8; ID_SIZE], n: usize) -> [u8; ID_SIZE] {
    let mut out = [0u8; ID_SIZE];
    if n == 0 {
        return out;
    }
    let mut carry = 0u32;
    for i in (0..ID_SIZE).rev() {
        let prod = a[i] as u32 * n as u32 + carry;
        out[i] = (prod & 0xff) as u8;
        carry = prod >> 8;
    }
    out
}

pub fn range_width_leq(min: &[u8; ID_SIZE], max: &[u8; ID_SIZE], value: u8) -> bool {
    let diff = big_sub(max, min);
    let leading_zero = diff[..ID_SIZE - 1].iter().all(|&b| b == 0);
    leading_zero && diff[ID_SIZE - 1] <= value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrip() {
        let id = NodeId::random();
        let hex = id.to_hex();
        assert_eq!(hex.len(), 40);
        assert_eq!(NodeId::from_hex(&hex).unwrap(), id);
        assert_eq!(hex, hex.to_uppercase());
    }

    #[test]
    fn distance_is_xor() {
        let a = NodeId([1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let b = NodeId([3, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(a.distance(&b), [2, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert!(a.distance_cmp(&a, &b) != a.distance_cmp(&b, &b));
    }

    #[test]
    fn validity() {
        assert!(!NodeId::min().is_valid());
        assert!(!NodeId::max().is_valid());
        assert!(NodeId::random().is_valid());
    }

    #[test]
    fn discrete_ids_are_ordered_and_valid() {
        let ids = NodeId::random_discrete_ids(8);
        assert_eq!(ids.len(), 8);
        for id in &ids {
            assert!(id.is_valid());
        }
    }

    #[test]
    fn discrete_ids_cover_distinct_prefixes() {
        let count = 50usize;
        let ids = NodeId::random_discrete_ids(count);
        let mut prefixes: Vec<u16> = ids
            .iter()
            .map(|id| u16::from_be_bytes([id.0[0], id.0[1]]))
            .collect();
        prefixes.sort();
        prefixes.dedup();
        assert_eq!(prefixes.len(), count);
        let spread = prefixes[count - 1] - prefixes[0];
        assert!(spread > 60000);
    }

    #[test]
    fn big_math() {
        let one = {
            let mut v = [0u8; ID_SIZE];
            v[ID_SIZE - 1] = 1;
            v
        };
        let two = big_add(&one, &one);
        assert_eq!(two[ID_SIZE - 1], 2);
        assert_eq!(big_sub(&two, &one), one);
        assert_eq!(big_div2(&two), one);
        let min = NodeId::min().0;
        let max = NodeId::max().0;
        assert!(!range_width_leq(&min, &max, 2));
        let two_id = NodeId(two);
        let three = big_add(&two, &one);
        let three_id = NodeId(three);
        assert!(range_width_leq(&min, &two_id.0, 2));
        assert!(!range_width_leq(&min, &three_id.0, 2));
    }
}
