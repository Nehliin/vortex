use serde_derive::{Deserialize, Serialize};

use crate::node::{Node, NodeId, ID_MAX, ID_ZERO};

// TODO implement PartialEq manually to only check min,max
#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct Bucket {
    min: NodeId,
    max: NodeId,
    pub nodes: [Option<Node>; 8],
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
        // modify max limit by finding midpoint
        self.max = crate::node::midpoint(&self.min, &self.max);
        // max should never be 0
        if self.max == ID_ZERO {
            panic!("should never happen");
        }

        let new_min = self.max;

        let mut bucket = Bucket {
            min: new_min,
            max: old_max,
            // wtf why do I have to write these out manually
            nodes: [None, None, None, None, None, None, None, None],
        };

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
    pub buckets: Vec<Bucket>,
    // TODO: Perhaps worth passing this along with insert_node and not keep it here
    pub own_id: NodeId,
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
                    println!("Splitting bucket for: {:?}", node.id);
                    println!("bucket min {:?}", bucket.min);
                    println!("bucket max {:?}", bucket.max);
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

        let (_, end_bytes) = end.to_bytes_be();
        // Sanity check
        assert!(ID_MAX == end_bytes.as_slice().into());

        for i in 1..17 {
            let id: BigInt = end.clone() - (end.clone() / 16) * i;
            let (_, mut id) = id.to_bytes_be();
            while id.len() < 20 {
                id.push(0);
            }
            routing_table.insert_node(Node {
                id: id.as_slice().into(),
                addr: "0.0.0.0:0".parse().unwrap(),
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
        let mut id = ID_MAX;
        id.halve();
        let mut routing_table = RoutingTable::new(id);

        let end = BigInt::new(
            num_bigint::Sign::Plus,
            vec![u32::MAX, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
        );
        for i in 1..128 {
            let id: BigInt = end.clone() - (end.clone() / 128) * i;
            let (_, mut id) = id.to_bytes_be();
            while id.len() < 20 {
                id.push(0);
            }
            routing_table.insert_node(Node {
                id: id.as_slice().into(),
                addr: "0.0.0.0:0".parse().unwrap(),
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

    #[test]
    fn test_no_stack_overflow() {
        let mut min = [0xFF; 20];
        min[0] = 0x14;
        let mut max = [0xFF; 20];
        max[0] = 0x15;
        let mut bucket = Bucket {
            min: min.as_slice().into(),
            max: max.as_slice().into(),
            nodes: [None, None, None, None, None, None, None, None],
        };
        

        let new_bucket = bucket.split();

        verify_bucket(&bucket);
        verify_bucket(&new_bucket);
        assert_non_overlapping(&bucket, &new_bucket);
    }
}
