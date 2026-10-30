use std::cmp::Ordering;
use std::net::{IpAddr, SocketAddr};

/// A peer's priority score calculated according to BEP 40 (Canonical Peer Priority).
/// A higher CRC32C hash means a higher priority.
#[derive(Debug, PartialEq)]
pub struct PeerPriority(Option<u32>);

impl PeerPriority {
    /// Calculates the priority between our own address and a remote peer's address.
    /// Returns `None` if the IP versions do not match (e.g., mixing IPv4 and IPv6).
    ///
    /// See [BEP 40](https://www.bittorrent.org/beps/bep_0040.html) for the specification.
    pub fn new(addr_a: &SocketAddr, addr_b: &SocketAddr) -> Self {
        Self(Self::calculate_priority(addr_a, addr_b))
    }

    fn calculate_priority(addr_a: &SocketAddr, addr_b: &SocketAddr) -> Option<u32> {
        if addr_a.is_ipv4() != addr_b.is_ipv4() {
            return None;
        }

        let mut buff = Vec::new();

        if addr_a.ip() == addr_b.ip() {
            let (p1, p2) = if addr_a.port() <= addr_b.port() {
                (addr_a.port(), addr_b.port())
            } else {
                (addr_b.port(), addr_a.port())
            };
            buff.extend_from_slice(&p1.to_be_bytes());
            buff.extend_from_slice(&p2.to_be_bytes());
        } else {
            match (addr_a.ip(), addr_b.ip()) {
                (IpAddr::V4(ip1), IpAddr::V4(ip2)) => {
                    let a = u32::from(ip1);
                    let b = u32::from(ip2);

                    let mask = if (a >> 8) == (b >> 8) {
                        0xffff_ffff
                    } else if (a >> 16) == (b >> 16) {
                        0xffff_ff55
                    } else {
                        0xffff_5555
                    };

                    let (lo, hi) = {
                        let ma = a & mask;
                        let mb = b & mask;

                        if ma <= mb { (ma, mb) } else { (mb, ma) }
                    };

                    buff.extend_from_slice(&lo.to_be_bytes());
                    buff.extend_from_slice(&hi.to_be_bytes());
                }
                (IpAddr::V6(ip1), IpAddr::V6(ip2)) => {
                    let a = u128::from(ip1);
                    let b = u128::from(ip2);

                    let c_bytes = ((a ^ b).leading_zeros() as usize) / 8;
                    let ff_bytes = (c_bytes + 1).clamp(6, 16);

                    const FIVES: u128 = 0x5555_5555_5555_5555_5555_5555_5555_5555;
                    let ff_part = u128::MAX << ((16 - ff_bytes) * 8);
                    let mask = ff_part | (FIVES & !ff_part);

                    let (lo, hi) = {
                        let ma = a & mask;
                        let mb = b & mask;

                        if ma <= mb { (ma, mb) } else { (mb, ma) }
                    };

                    buff.extend_from_slice(&lo.to_be_bytes());
                    buff.extend_from_slice(&hi.to_be_bytes());
                }
                _ => {}
            }
        }
        Some(crc32c::crc32c(&buff))
    }
}

impl PartialOrd for PeerPriority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PeerPriority {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.0, other.0) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some(a), Some(b)) => b.cmp(&a),
        }
    }
}

impl Eq for PeerPriority {}

impl From<(&SocketAddr, &SocketAddr)> for PeerPriority {
    fn from(value: (&SocketAddr, &SocketAddr)) -> Self {
        Self::new(value.0, value.1)
    }
}

impl From<u32> for PeerPriority {
    fn from(value: u32) -> Self {
        Self(Some(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash_hex(hex: &str) -> u32 {
        let bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        crc32c::crc32c(&bytes)
    }

    #[test]
    fn test_peer_priority_same_ip_address() {
        let peer1: SocketAddr = ([230, 12, 123, 3], 1234).into();
        let peer2: SocketAddr = ([230, 12, 123, 3], 300).into();
        assert_eq!(
            Some(hash_hex("012c04d2")),
            PeerPriority::new(&peer1, &peer2).0
        );
    }

    #[test]
    fn test_peer_priority_matching_24_prefix() {
        let peer1: SocketAddr = ([230, 12, 123, 1], 1234).into();
        let peer2: SocketAddr = ([230, 12, 123, 3], 300).into();
        assert_eq!(
            Some(hash_hex("e60c7b01e60c7b03")),
            PeerPriority::new(&peer1, &peer2).0
        );
    }

    #[test]
    fn test_peer_priority_matching_16_prefix() {
        let peer1: SocketAddr = ([230, 12, 23, 1], 1234).into();
        let peer2: SocketAddr = ([230, 12, 123, 3], 300).into();
        assert_eq!(
            Some(hash_hex("e60c1701e60c7b01")),
            PeerPriority::new(&peer1, &peer2).0
        );
    }

    #[test]
    fn test_peer_priority_different_16_prefix() {
        let peer1: SocketAddr = ([230, 120, 23, 1], 1234).into();
        let peer2: SocketAddr = ([230, 12, 123, 3], 300).into();
        assert_eq!(
            Some(hash_hex("e60c5101e6781501")),
            PeerPriority::new(&peer1, &peer2).0
        );
    }

    #[test]
    fn test_different_ip_versions_returns_none() {
        let peer1: SocketAddr = ([123, 213, 0, 1], 5000).into();
        let peer2: SocketAddr = (
            [0x20d, 0x20c, 0x20b, 0x20a, 0x209, 0x208, 0x207, 0x206],
            4000,
        )
            .into();
        assert_eq!(None, PeerPriority::new(&peer1, &peer2).0);
    }

    #[test]
    fn test_peer_priority_ordering() {
        let p_none = PeerPriority(None);
        let p_low = PeerPriority(Some(10));
        let p_high = PeerPriority(Some(20));

        assert_eq!(Ordering::Less, p_high.cmp(&p_low));
        assert_eq!(Ordering::Greater, p_low.cmp(&p_high));
        assert_eq!(Ordering::Equal, p_high.cmp(&p_high));

        assert_eq!(Ordering::Greater, p_none.cmp(&p_low));
        assert_eq!(Ordering::Less, p_low.cmp(&p_none));
    }

    #[test]
    fn test_sorting_high_to_low_priority() {
        // Create some fake addresses (the IPs don't matter for the test,
        // we will manually assign them fake priorities to prove the sorting works)
        let addr1: SocketAddr = "127.0.0.1:1000".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:2000".parse().unwrap();
        let addr3: SocketAddr = "127.0.0.1:3000".parse().unwrap();

        // Create a list out of order: Low, None, High
        let mut ranked: Vec<(PeerPriority, SocketAddr)> = vec![
            (PeerPriority::from(10), addr1), // Low
            (PeerPriority(None), addr2),     // Unrankable (None)
            (PeerPriority::from(50), addr3), // High
        ];

        ranked.sort();

        // Assert they are sorted correctly:
        // 1. Highest priority first (50)
        // 2. Then lower priority (10)
        // 3. Then None at the very end
        assert_eq!(ranked[0].1, addr3); // Highest priority
        assert_eq!(ranked[1].1, addr1); // Lower priority
        assert_eq!(ranked[2].1, addr2); // None
    }
}
