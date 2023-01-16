
use serde_derive::{Deserialize, Serialize};
use slotmap::{new_key_type, DenseSlotMap, Key};
use time::OffsetDateTime;

use crate::{
    node::{Node, NodeId, NodeStatus, ID_MAX, ID_ZERO},
};

// TODO implement PartialEq manually to only check min,max
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Bucket {
    min: NodeId,
    max: NodeId,
    nodes: [Option<Node>; 16],
    last_changed: OffsetDateTime,
}

impl Bucket {
    #[inline]
    fn covers(&self, node_id: &NodeId) -> bool {
        &self.min <= node_id && node_id < &self.max
    }

    #[inline(always)]
    pub fn random_id(&self) -> NodeId {
        NodeId::new_in_range(&self.min, &self.max)
    }

    #[inline]
    fn empty_spot(&mut self) -> Option<&mut Option<Node>> {
        let empty_spot = self.nodes.iter_mut().find(|spot| spot.is_none());
        if empty_spot.is_some() {
            return empty_spot;
        } else {
            drop(empty_spot);
            self.nodes.iter_mut().find(|spot| {
                spot.map(|node| node.last_status == NodeStatus::Bad)
                    .unwrap_or(true)
            })
        }
    }

    #[inline]
    fn is_full(&self) -> bool {
        let empty_spot = self.nodes.iter().find(|spot| spot.is_none());
        if empty_spot.is_some() {
            return false;
        } else {
            drop(empty_spot);
            self.nodes.iter().any(|spot| {
                spot.map(|node| node.last_status == NodeStatus::Bad)
                    .unwrap_or(true)
            })
        }
    }

    #[inline]
    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.nodes
            .iter()
            .filter_map(|maybe_node| maybe_node.as_ref())
    }

    #[inline]
    pub fn nodes_mut(&mut self) -> impl Iterator<Item = &mut Node> {
        self.nodes
            .iter_mut()
            .filter_map(|maybe_node| maybe_node.as_mut())
    }

    fn split(&mut self) -> Bucket {
        let old_max = self.max;
        // modify max limit by finding midpoint
        self.max = crate::node::midpoint(&self.min, &self.max);
        // max should never be 0
        assert!(self.max != ID_ZERO);

        let new_min = self.max;

        let last_changed = OffsetDateTime::now_utc();

        let mut bucket = Bucket {
            min: new_min,
            max: old_max,
            // wtf why do I have to write these out manually
            nodes: [
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None,
            ],
            last_changed,
        };

        self.last_changed = last_changed;
        let mut i = 0;
        for node in self.nodes.iter_mut() {
            if node.as_ref().map_or(false, |node| bucket.covers(&node.id)) {
                // Assert it's covered by only one bucket
                debug_assert!(
                    !(self.min <= node.as_ref().unwrap().id
                        && node.as_ref().unwrap().id < self.max)
                );
                bucket.nodes[i] = node.take();
                i += 1;
            } else {
                // Assert it's covered by at least one bucket
                debug_assert!(node
                    .as_ref()
                    .map_or(true, |node| self.min <= node.id && node.id < self.max))
            }
        }
        bucket
    }

    #[inline]
    pub fn last_changed(&self) -> OffsetDateTime {
        self.last_changed
    }
}

new_key_type! {
    pub struct BucketId;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingTable {
    pub buckets: DenseSlotMap<BucketId, Bucket>,
    pub own_id: NodeId,
}

impl RoutingTable {
    pub fn new(own_id: NodeId) -> RoutingTable {
        let mut buckets = DenseSlotMap::with_key();
        buckets.insert(Bucket {
            min: ID_ZERO,
            max: ID_MAX,
            nodes: [
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None,
            ],
            last_changed: OffsetDateTime::now_utc(),
        });
        RoutingTable { buckets, own_id }
    }

    pub fn insert_node(&mut self, node: Node) -> [BucketId; 2] {
        let mut result = [BucketId::null(), BucketId::null()];
        for (bucket_id, bucket) in self.buckets.iter_mut() {
            if bucket.covers(&node.id) {
                if let Some(empty_spot) = bucket.empty_spot() {
                    *empty_spot = Some(node);
                    bucket.last_changed = OffsetDateTime::now_utc();
                    // If the first result is null there was no recursion
                    // so we need to set the bucket id here. If this is non null
                    // we know the recursion happend so both result ids have already been set
                    if result[0].is_null() {
                        result[0] = bucket_id;
                    }
                    return result;
                } else if bucket.covers(&self.own_id) {
                    result[0] = bucket_id;
                    let new_bucket = bucket.split();
                    result[1] = self.buckets.insert(new_bucket);
                    return self.insert_node(node);
                }
            }
        }
        result
    }

    //    fn insert_node_helper(&mut self, node: Node, mut result: [BucketId; 2], )

    // TODO: properly add last_changed to buckets and
    // periodically ping nodes accoriding to
    // https://www.bittorrent.org/beps/bep_0005.html
    /*pub async fn ping_all_nodes(&mut self, service: &KrpcSocket, progress: &MultiProgress) {
        // Will live long enough and this is temporary
        let this: &'static mut Self = unsafe { std::mem::transmute(self) };
        let ping_progress = progress.add(ProgressBar::new_spinner());
        ping_progress.set_style(ProgressStyle::with_template("{spinner:.blue} {msg}").unwrap());
        ping_progress.enable_steady_tick(Duration::from_millis(100));
        ping_progress.set_message("Pinging nodes...");
        let futures = this
            .buckets
            .iter_mut()
            .flat_map(|bucket| bucket.nodes.iter_mut())
            .map(|maybe_node| {
                let service_clone = service.clone();
                let own_id = this.own_id;
                tokio_uring::spawn(async move {
                    if let Some(node) = maybe_node {
                        if let Err(err) = service_clone.ping(&own_id, node).await {
                            log::warn!("Ping failed for node: {node:?}, error: {err}");
                            maybe_node.take();
                        } else {
                            log::info!("Ping succeeded");
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        for fut in futures {
            fut.await.unwrap();
        }
        ping_progress.finish_with_message("Pinged all nodes");
    }*/

    // TODO maybe not use nodeid as type for info_hash
    pub fn get_closest_mut(&mut self, info_hash: &NodeId) -> Option<&mut Node> {
        let closest = self
            .buckets
            .iter_mut()
            .map(|(_, v)| v)
            .flat_map(|bucket| &mut bucket.nodes)
            .min_by_key(|node| {
                node.as_ref()
                    .map(|node| info_hash.distance(&node.id))
                    .unwrap_or(ID_MAX)
            })? // never empty
            .as_mut()?;

        // Santify check TODO FIX AND REMOVE
        /*let mut found = 0;
        for bucket in self.buckets.iter() {
            if bucket.covers(&closest.id) {
                found += 1;
                assert!(bucket.nodes.contains(&Some(closest.clone())));
            }
        }
        assert_eq!(found, 1);*/
        Some(closest)
    }

    pub fn is_full(&self, bucket: &Bucket) -> bool {
        !bucket.covers(&self.own_id) && !bucket.is_full()
    }

    pub fn get_mut(&mut self, id: &NodeId) -> Option<&mut Node> {
        self.buckets
            .iter_mut()
            .map(|(_, bucket)| bucket)
            .flat_map(|bucket| &mut bucket.nodes)
            .find(|node| node.map_or(false, |node| node.id == *id))?
            .as_mut()
    }

    pub fn find_bucket(&self, id: &NodeId) -> Option<(BucketId, &Bucket)> {
        self.buckets
            .iter()
            .find(|(_, bucket)| bucket.covers(id))
            .filter(|(_, bucket)| {
                bucket
                    .nodes
                    .iter()
                    .any(|node| node.map_or(false, |node| node.id == *id))
            })
    }

    #[inline(always)]
    pub fn get_bucket_mut(&mut self, bucket_id: BucketId) -> Option<&mut Bucket> {
        self.buckets.get_mut(bucket_id)
    }

    pub fn remove(&mut self, to_remove: &Node) -> anyhow::Result<()> {
        let to_remove = self
            .buckets
            .iter_mut()
            .map(|(_, bucket)| bucket)
            .flat_map(|bucket| bucket.nodes.iter_mut())
            .find(|node| node.as_ref().map(|node| node == to_remove).unwrap_or(false))
            .ok_or_else(|| anyhow::anyhow!("Node not found in routing table"))?;

        // TODO: Remove buckets?
        to_remove.take();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use num_bigint::BigInt;
    use time::OffsetDateTime;

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
    fn test_get_closest() {
        let mut routing_table: RoutingTable =
            serde_json::from_reader(std::fs::File::open("get_closest.json").unwrap()).unwrap();
        for (_, bucket_a) in routing_table.buckets.iter() {
            verify_bucket(bucket_a);
            for (_, bucket_b) in routing_table.buckets.iter() {
                if bucket_b == bucket_a {
                    continue;
                }
                verify_bucket(bucket_b);
                assert_non_overlapping(bucket_a, bucket_b);
            }
        }

        let info_bytes: &[u8] = &[
            0xaa, 0x12, 0x73, 0xc9, 0xb8, 0xf0, 0x90, 0x29, 0xfc, 0xa8, 0x0d, 0xd8, 0x14, 0x22,
            0xad, 11, 0xeb, 0xca, 0x20, 0x60,
        ];
        let info_hash = NodeId::from(info_bytes);

        let buckets_clone = routing_table.buckets.clone();
        let closest = routing_table.get_closest_mut(&info_hash).unwrap();

        // Santify check
        let mut found = 0;
        for (_, bucket) in buckets_clone.iter() {
            if bucket.covers(&info_hash) {
                found += 1;
                assert!(bucket.nodes.contains(&Some(closest.clone())));
            }
        }
        assert_eq!(found, 1);
    }

    /*#[test]
    fn test_bucket_split_basic() {
        let mut routing_table = RoutingTable::new(ID_ZERO);

        let end = BigInt::new(
            num_bigint::Sign::Plus,
            vec![u32::MAX, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
        );

        let (_, end_bytes) = end.to_bytes_be();
        // Sanity check
        assert!(ID_MAX == end_bytes.as_slice().into());

        for i in 1..33 {
            let id: BigInt = end.clone() - (end.clone() / 32) * i;
            let (_, mut id) = id.to_bytes_be();
            while id.len() < 20 {
                id.push(0);
            }
            routing_table.insert_node(Node {
                id: id.as_slice().into(),
                addr: "0.0.0.0:0".parse().unwrap(),
                last_seen: OffsetDateTime::now_utc(),
                last_status: NodeStatus::Unknown,
            });

            if i < 17 {
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

                for (_, bucket) in routing_table.buckets.iter() {
                    verify_bucket(bucket);
                    assert!(bucket.nodes.len() == 16);
                }
                assert_non_overlapping(&routing_table.buckets[0], &routing_table.buckets[1])
            }
        }
    }*/

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
                last_seen: OffsetDateTime::now_utc(),
                last_status: NodeStatus::Unknown,
            });
        }

        for (_, bucket_a) in routing_table.buckets.iter() {
            verify_bucket(bucket_a);
            for (_, bucket_b) in routing_table.buckets.iter() {
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
            nodes: [
                None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                None, None,
            ],
            last_changed: OffsetDateTime::now_utc(),
        };

        let new_bucket = bucket.split();

        verify_bucket(&bucket);
        verify_bucket(&new_bucket);
        assert_non_overlapping(&bucket, &new_bucket);
    }

    #[test]
    fn is_bucket_full() {
        assert!(false);
    }

    #[test]
    fn empty_spot() {
        assert!(false);
    }
}
