use crate::dht::id::{big_sub, range_width_leq, NodeId, ID_SIZE};
use std::net::SocketAddr;
use std::time::Instant;

pub const K: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub last_active: Instant,
}

impl Node {
    pub fn new(id: NodeId, addr: SocketAddr) -> Self {
        Node {
            id,
            addr,
            last_active: Instant::now(),
        }
    }

    pub fn touch(&mut self) {
        self.last_active = Instant::now();
    }
}

#[derive(Debug, Clone)]
struct Bucket {
    min: [u8; ID_SIZE],
    max: [u8; ID_SIZE],
    members: Vec<Node>,
}

impl Bucket {
    fn root() -> Self {
        Bucket {
            min: NodeId::min().0,
            max: NodeId::max().0,
            members: Vec::new(),
        }
    }

    fn contains(&self, id: &NodeId) -> bool {
        self.min <= id.0 && id.0 < self.max
    }
}

#[derive(Debug, Clone)]
pub struct RoutingTable {
    own_id: NodeId,
    buckets: Vec<Bucket>,
}

impl RoutingTable {
    pub fn new(own_id: NodeId) -> Self {
        RoutingTable {
            own_id,
            buckets: vec![Bucket::root()],
        }
    }

    pub fn own_id(&self) -> NodeId {
        self.own_id
    }

    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    pub fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.members.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn find_bucket_index(&self, id: &NodeId) -> usize {
        self.buckets
            .iter()
            .position(|b| b.contains(id))
            .unwrap_or(self.buckets.len() - 1)
    }

    pub fn insert(&mut self, id: NodeId, addr: SocketAddr) -> bool {
        if !id.is_valid() {
            return false;
        }
        let index = self.find_bucket_index(&id);
        let bucket = &mut self.buckets[index];
        if let Some(existing) = bucket.members.iter_mut().find(|n| n.id == id && n.addr == addr) {
            existing.touch();
            return true;
        }
        if bucket.members.len() < K {
            bucket.members.push(Node::new(id, addr));
            return true;
        }
        if bucket.contains(&self.own_id) && !range_width_leq(&bucket.min, &bucket.max, 2) {
            self.split_bucket(index);
            let index = self.find_bucket_index(&id);
            let bucket = &mut self.buckets[index];
            if bucket.members.len() < K {
                bucket.members.push(Node::new(id, addr));
                return true;
            }
            return false;
        }
        false
    }

    fn split_bucket(&mut self, index: usize) {
        let bucket = &self.buckets[index];
        let diff = big_sub(&bucket.max, &bucket.min);
        let half = crate::dht::id::big_div2(&diff);
        let mid = crate::dht::id::big_sub(&bucket.max, &half);
        let mut lower = Bucket {
            min: bucket.min,
            max: mid,
            members: Vec::new(),
        };
        let mut upper = Bucket {
            min: mid,
            max: bucket.max,
            members: Vec::new(),
        };
        let members = std::mem::take(&mut self.buckets[index].members);
        for node in members {
            if lower.contains(&node.id) {
                lower.members.push(node);
            } else {
                upper.members.push(node);
            }
        }
        self.buckets.remove(index);
        self.buckets.insert(index, lower);
        self.buckets.insert(index + 1, upper);
    }

    pub fn remove(&mut self, id: &NodeId, addr: &SocketAddr) -> bool {
        let index = self.find_bucket_index(id);
        let bucket = &mut self.buckets[index];
        let before = bucket.members.len();
        bucket.members.retain(|n| !(n.id == *id && n.addr == *addr));
        bucket.members.len() < before
    }

    pub fn get(&self, id: &NodeId, addr: &SocketAddr) -> Option<&Node> {
        let index = self.find_bucket_index(id);
        self.buckets[index]
            .members
            .iter()
            .find(|n| n.id == *id && n.addr == *addr)
    }

    pub fn all(&self) -> Vec<Node> {
        self.buckets
            .iter()
            .flat_map(|b| b.members.iter().cloned())
            .collect()
    }

    pub fn closest(&self, target: &NodeId, count: usize) -> Vec<Node> {
        let mut nodes = self.all();
        nodes.sort_by(|a, b| target.distance_cmp(&a.id, &b.id));
        nodes.truncate(count);
        nodes
    }

    pub fn inactive_since(&self, max_idle: std::time::Duration) -> Vec<Node> {
        self.all()
            .into_iter()
            .filter(|n| n.last_active.elapsed() >= max_idle)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{}", port).parse().unwrap()
    }

    fn id_with_prefix(byte: u8) -> NodeId {
        let mut bytes = [0u8; ID_SIZE];
        bytes[0] = byte;
        bytes[ID_SIZE - 1] = byte.max(1);
        NodeId(bytes)
    }

    #[test]
    fn insert_and_get() {
        let own = NodeId::random();
        let mut table = RoutingTable::new(own);
        let id = NodeId::random();
        assert!(table.insert(id, addr(1000)));
        assert!(table.get(&id, &addr(1000)).is_some());
        assert_eq!(table.len(), 1);
        assert_eq!(table.bucket_count(), 1);
    }

    #[test]
    fn duplicate_touches_not_inserts() {
        let mut table = RoutingTable::new(NodeId::random());
        let id = NodeId::random();
        assert!(table.insert(id, addr(1000)));
        assert!(table.insert(id, addr(1000)));
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn invalid_ids_rejected() {
        let mut table = RoutingTable::new(NodeId::random());
        assert!(!table.insert(NodeId::min(), addr(1)));
        assert!(!table.insert(NodeId::max(), addr(1)));
        assert!(table.is_empty());
    }

    #[test]
    fn full_bucket_not_containing_own_does_not_split() {
        let own = id_with_prefix(0x80);
        let mut table = RoutingTable::new(own);
        for i in 0..K {
            let mut bytes = id_with_prefix(0x10).0;
            bytes[ID_SIZE - 2] = i as u8;
            let id = NodeId(bytes);
            assert!(table.insert(id, addr(1000 + i as u16)));
        }
        let overflow = {
            let mut bytes = id_with_prefix(0x10).0;
            bytes[ID_SIZE - 2] = 0xff;
            NodeId(bytes)
        };
        let inserted = table.insert(overflow, addr(2000));
        assert_eq!(table.len(), K);
        assert_eq!(table.bucket_count(), 2);
        assert!(!inserted || table.get(&overflow, &addr(2000)).is_some());
        assert!(table.get(&overflow, &addr(2000)).is_none());
    }

    #[test]
    fn full_bucket_on_own_range_splits() {
        let own = NodeId::random();
        let mut table = RoutingTable::new(own);
        for i in 0..K {
            let id = NodeId::random();
            assert!(table.insert(id, addr(1000 + i as u16)));
        }
        assert!(table.insert(NodeId::random(), addr(2000)));
        assert!(table.bucket_count() > 1);
        assert_eq!(table.len(), K + 1);
    }

    #[test]
    fn closest_orders_by_distance() {
        let mut table = RoutingTable::new(NodeId::random());
        let target = id_with_prefix(0x50);
        let near = id_with_prefix(0x51);
        let far = id_with_prefix(0xf0);
        table.insert(far, addr(1));
        table.insert(near, addr(2));
        let closest = table.closest(&target, 1);
        assert_eq!(closest[0].id, near);
    }

    #[test]
    fn remove_works() {
        let mut table = RoutingTable::new(NodeId::random());
        let id = NodeId::random();
        table.insert(id, addr(1000));
        assert!(table.remove(&id, &addr(1000)));
        assert!(table.get(&id, &addr(1000)).is_none());
        assert!(!table.remove(&id, &addr(1000)));
    }
}
