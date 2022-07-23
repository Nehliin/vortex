use std::ops::Deref;

use bytes::Bytes;
use serde_derive::{Deserialize, Serialize};

// BE endian large nums that
// can use lexographical order
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Deserialize, Serialize)]
pub struct NodeId([u8; 20]);

pub const ID_ZERO: NodeId = NodeId([0; 20]);
pub const ID_MAX: NodeId = NodeId([0xFF; 20]);

impl NodeId {
    // TODO: don't change in place?
    fn halve(&mut self) {
        if let Some(most_significant) = self.0.iter_mut().find(|byte| **byte != 0) {
            *most_significant >>= 1;
        }
    }

    // a bit odd to return another node id here
    pub fn distance(&self, other: &NodeId) -> NodeId {
        // Almost optimal asm generated but can be improved
        let mut dist = [0; 20];
        self.0
            .iter()
            .zip(other.0.iter())
            .zip(dist.iter_mut())
            .for_each(|((a, b), res)| *res = a ^ b);
        NodeId(dist)
    }

    pub fn to_bytes(self) -> Bytes {
        Bytes::copy_from_slice(&self.0)
    }
}

impl From<Bytes> for NodeId {
    fn from(bytes: Bytes) -> Self {
        bytes[..].into()
    }
}

impl From<&[u8]> for NodeId {
    fn from(slice: &[u8]) -> Self {
        assert!(slice.len() == 20);
        // use maybe uninit
        let mut id = [0; 20];
        id.copy_from_slice(slice);
        NodeId(id)
    }
}

impl Deref for NodeId {
    type Target = [u8; 20];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl core::fmt::Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("NodeId")
            .field(&format!("{:02x?}", &self.0))
            .finish()
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct Node {
    pub id: NodeId,
    // socket addr?
    pub host: String,
    pub port: u16,
}

// TODO implement PartialEq manually to only check min,max
#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct Bucket {
    min: NodeId,
    max: NodeId,
    nodes: [Option<Node>; 8],
}

impl Bucket {
    #[inline]
    fn covers(&self, node_id: &NodeId) -> bool {
        &self.min <= node_id && node_id < &self.max
    }

    #[inline]
    fn empty_spot(&mut self) -> Option<&mut Option<Node>> {
        self.nodes.iter_mut().find(|spot| spot.is_none())
    }

    fn split(&mut self) -> Bucket {
        let old_max = self.max;
        // modify max limit by divinding by 2
        self.max.halve();
        // max should never be 0
        if self.max == ID_ZERO {
            panic!("should never happen");
        }

        let new_min = self.max;

        let mut bucket = Bucket {
            min: new_min,
            max: old_max,
            nodes: [None, None, None, None, None, None, None, None],
        };

        // wtf why do I have to write these out manually
        let mut i = 0;
        for node in self.nodes.iter_mut() {
            if node.as_ref().map_or(false, |node| node.id >= self.max) {
                bucket.nodes[i] = node.take();
                i += 1;
            }
        }
        bucket
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RoutingTable {
    buckets: Vec<Bucket>,
    // TODO: Perhaps worth passing this along with insert_node and not keep it here
    own_id: NodeId,
}

impl RoutingTable {
    pub fn new(own_id: NodeId) -> RoutingTable {
        RoutingTable {
            buckets: vec![Bucket {
                min: ID_ZERO,
                max: ID_MAX,
                nodes: [None, None, None, None, None, None, None, None],
            }],
            own_id,
        }
    }

    pub fn insert_node(&mut self, node: Node) -> bool {
        // TODO: naive
        for bucket in self.buckets.iter_mut() {
            if bucket.covers(&node.id) {
                if let Some(empty_spot) = bucket.empty_spot() {
                    *empty_spot = Some(node);
                    return true;
                } else if bucket.covers(&self.own_id) {
                    let new_bucket = bucket.split();
                    self.buckets.push(new_bucket);
                    // not efficient
                    return self.insert_node(node);
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod test {
    use num_bigint::BigInt;

    use super::*;

    fn verify_bucket(bucket: &Bucket) {
        assert!(bucket.min < bucket.max, "Bucket min max limits are invalid");
        for node in bucket.nodes.iter().filter(|node| node.is_some()) {
            let node = node.as_ref().unwrap();
            assert!(
                bucket.covers(&node.id),
                "Node id is not within bucket range"
            );
        }
    }

    fn assert_non_overlapping(bucket_a: &Bucket, bucket_b: &Bucket) {
        if bucket_a.max < bucket_b.max {
            assert!(bucket_a.max <= bucket_b.min);
            assert!(bucket_a.min < bucket_a.max);
            assert!(bucket_b.min < bucket_b.max);
        } else {
            assert!(bucket_b.max <= bucket_a.min);
            assert!(bucket_b.min < bucket_b.max);
            assert!(bucket_a.min < bucket_a.max);
        }
    }

    #[test]
    fn test_bucket_split_basic() {
        let mut routing_table = RoutingTable::new(ID_ZERO);

        let end = BigInt::new(
            num_bigint::Sign::Plus,
            vec![u32::MAX, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
        );
        for i in 1..17 {
            let id: BigInt = end.clone() - (end.clone() / 16) * i;
            let (_, mut id) = id.to_bytes_be();
            while id.len() < 20 {
                id.push(0);
            }
            routing_table.insert_node(Node {
                id: id.as_slice().into(),
                host: "host".to_string(),
                port: 0,
            });

            if i < 9 {
                assert_eq!(routing_table.buckets.len(), 1);
                assert_eq!(
                    routing_table.buckets[0]
                        .nodes
                        .iter()
                        .filter(|node| node.is_some())
                        .count(),
                    i
                );
                assert_eq!(routing_table.buckets[0].min, ID_ZERO);
                assert_eq!(routing_table.buckets[0].max, ID_MAX);
            } else {
                assert_eq!(routing_table.buckets.len(), 2);

                // Check bucket limits
                assert_eq!(routing_table.buckets[0].min, ID_ZERO);
                let max = BigInt::from_bytes_be(
                    num_bigint::Sign::Plus,
                    routing_table.buckets[0].max.as_slice(),
                );
                assert_eq!(max, end.clone() / 2);

                let min = BigInt::from_bytes_be(
                    num_bigint::Sign::Plus,
                    routing_table.buckets[1].min.as_slice(),
                );

                assert_eq!(min, end.clone() / 2);
                assert_eq!(routing_table.buckets[1].max, ID_MAX);

                for bucket in routing_table.buckets.iter() {
                    verify_bucket(bucket);
                    assert!(bucket.nodes.len() == 8);
                }
                assert_non_overlapping(&routing_table.buckets[0], &routing_table.buckets[1])
            }
        }
    }

    #[test]
    fn test_only_split_own_id() {
        let mut routing_table = RoutingTable::new(ID_ZERO);

        let end = BigInt::new(
            num_bigint::Sign::Plus,
            vec![u32::MAX, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
        );
        for i in 1..32 {
            let id: BigInt = end.clone() - (end.clone() / 32) * i;
            let (_, mut id) = id.to_bytes_be();
            while id.len() < 20 {
                id.push(0);
            }
            routing_table.insert_node(Node {
                id: id.as_slice().into(),
                host: "host".to_string(),
                port: 0,
            });
        }

        for bucket_a in routing_table.buckets.iter() {
            verify_bucket(bucket_a);
            for bucket_b in routing_table.buckets.iter() {
                if bucket_b == bucket_a {
                    continue;
                }
                verify_bucket(bucket_b);
                assert_non_overlapping(bucket_a, bucket_b);
            }
        }
    }
}
