use std::{
    net::SocketAddr,
    ops::{Add, Deref, Sub},
};

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
    pub fn halve(&mut self) {
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

impl Add for &NodeId {
    type Output = NodeId;

    fn add(self, rhs: Self) -> Self::Output {
        let mut carry = false;
        let mut result = [0; 20];
        self.0
            .iter()
            .rev()
            .zip(rhs.0.iter().rev())
            .zip(result.iter_mut().rev())
            .for_each(|((own, other), res)| {
                let (num, new_carry) = own.overflowing_add(*other);
                *res = num;
                if carry {
                    let (num, extra_carry) = res.overflowing_add(1);
                    *res = num;
                    carry = new_carry | extra_carry;
                } else {
                    carry = new_carry;
                }
            });
        NodeId(result)
    }
}

impl Sub for &NodeId {
    type Output = NodeId;

    fn sub(self, rhs: Self) -> Self::Output {
        let mut carry = false;
        let mut result = [0; 20];
        self.0
            .iter()
            .rev()
            .zip(rhs.0.iter().rev())
            .zip(result.iter_mut().rev())
            .for_each(|((own, other), res)| {
                let (num, new_carry) = own.overflowing_sub(*other);
                *res = num;
                if carry {
                    let (num, extra_carry) = res.overflowing_sub(1);
                    *res = num;
                    carry = new_carry | extra_carry;
                } else {
                    carry = new_carry;
                }
            });
        NodeId(result)
    }
}

#[inline]
pub fn midpoint(low: &NodeId, high: &NodeId) -> NodeId {
    let mut diff = high - low;
    diff.halve();
    low + &diff
}

impl From<Bytes> for NodeId {
    fn from(bytes: Bytes) -> Self {
        bytes[..].into()
    }
}

impl From<&[u8]> for NodeId {
    fn from(slice: &[u8]) -> Self {
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
    pub addr: SocketAddr,
}

#[cfg(test)]
mod test {
    use num_bigint::BigInt;

    use super::*;

    impl From<BigInt> for NodeId {
        fn from(bigint: BigInt) -> Self {
            let (_, bytes) = bigint.to_bytes_be();
            bytes.as_slice().into()
        }
    }

    #[test]
    fn test_addition() {
        // Sanity check with big int
        let bigint_a = BigInt::new(
            num_bigint::Sign::Plus,
            // LE bytes
            vec![u32::MAX, u32::MAX, u32::MAX - 1, u32::MAX, u32::MAX - 1],
        );

        let bigint_b = BigInt::new(num_bigint::Sign::Plus, vec![0, 0, 2, 1, 0]);

        let expected: BigInt = bigint_a + bigint_b;

        assert_eq!(
            BigInt::new(
                num_bigint::Sign::Plus,
                vec![u32::MAX, u32::MAX, 0, 1, u32::MAX],
            ),
            expected
        );

        let expected: NodeId = expected.into();

        // BE bytes
        let nodeid_a = NodeId::from(
            [
                0xFF,
                0xFF,
                0xFF,
                0xFF - 1,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF - 1,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
                0xFF,
            ]
            .as_slice(),
        );

        let nodeid_b =
            NodeId::from([0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0].as_slice());

        let actual = &nodeid_b + &nodeid_a;

        assert_eq!(expected, actual);
    }

    #[test]
    fn find_midpoint() {
        let high = BigInt::new(
            num_bigint::Sign::Plus,
            vec![u32::MAX, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
        );

        let low = high.clone() / 2;

        let mid: BigInt = (high + low) / 2;

        let mid_id: NodeId = mid.into();

        let high = ID_MAX;
        let mut low = ID_MAX;
        low.halve();

        let mut calculated_mid = &high - &low;
        calculated_mid.halve();
        let calculated_mid = &low + &calculated_mid;

        assert_eq!(mid_id, calculated_mid);
    }

    #[test]
    fn test_subtraction() {
        // Sanity check with big int
        let bigint_a = BigInt::new(
            num_bigint::Sign::Plus,
            // LE bytes
            vec![u32::MAX, u32::MAX, 0, 0, 1],
        );

        let bigint_b = BigInt::new(num_bigint::Sign::Plus, vec![0, 0, 2, 1, 0]);

        let expected: BigInt = bigint_a - bigint_b;

        assert_eq!(
            BigInt::new(
                num_bigint::Sign::Plus,
                vec![u32::MAX, u32::MAX, u32::MAX - 1, u32::MAX - 1],
            ),
            expected
        );

        let (_, mut expected_bytes) = expected.to_u32_digits();
        expected_bytes.push(0);
        let expected_bytes: Vec<u8> = expected_bytes
            .iter()
            .rev()
            .flat_map(|num| num.to_be_bytes())
            .collect();
        let expected: NodeId = expected_bytes.as_slice().into();

        // BE bytes
        let nodeid_a = NodeId::from(
            [
                0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            ]
            .as_slice(),
        );

        let nodeid_b =
            NodeId::from([0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0].as_slice());

        let actual = &nodeid_a - &nodeid_b;

        assert_eq!(expected, actual);
    }
}
