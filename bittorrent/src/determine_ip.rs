use std::{
    net::IpAddr,
    time::{Duration, Instant},
};

use ahash::HashMapExt;
use slotmap::SecondaryMap;

use crate::connection_manager::ConnectionId;

/// Used to determine this clients ip
/// based off data from the incoming `yourip`
/// extension messages
#[derive(Default, Debug)]
pub struct DeterminedIp {
    ip: Option<IpAddr>,
    votes: SecondaryMap<ConnectionId, IpAddr>,
    last_changed: Option<Instant>,
    stable: bool,
}

impl DeterminedIp {
    pub fn add_vote(&mut self, conn_id: ConnectionId, addr: IpAddr) {
        if addr.is_loopback() || addr.is_unspecified() || addr.is_multicast() {
            // don't count invalid addrs
            return;
        }
        self.votes.insert(conn_id, addr);
        if self.votes.len() == 50 && !self.stable {
            self.stable = true;
            self.reevaluate();
            return;
        }
        if self.ip.is_none() {
            self.update(addr);
        }
    }

    #[inline]
    pub fn remove_vote(&mut self, conn_id: ConnectionId) {
        self.votes.remove(conn_id);
    }

    fn update(&mut self, addr: IpAddr) {
        log::debug!("Updating external ip: {addr}");
        self.ip = Some(addr);
        self.last_changed = Some(Instant::now());
    }

    pub fn maybe_reevaluate(&mut self) {
        if self
            .last_changed
            .is_none_or(|change| change.elapsed() < Duration::from_mins(5))
        {
            return;
        }
        self.reevaluate();
    }

    fn reevaluate(&mut self) {
        let votes_per_ip = self
            .votes
            .values()
            .fold(ahash::HashMap::new(), |mut acc, e| {
                acc.entry(*e).and_modify(|count| *count += 1).or_insert(1);
                acc
            });
        if let Some((ip, votes)) = votes_per_ip
            .into_iter()
            .max_by(|(_, count_a), (_, count_b)| count_a.cmp(count_b))
        {
            log::debug!("top voted ip: {ip} (votes: {votes})");
            self.stable = true;
            self.update(ip);
        }
    }

    pub fn determined_ip(&self) -> Option<IpAddr> {
        self.ip
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slotmap::SlotMap;

    #[test]
    fn first_valid_report_is_used_provisionally() {
        for addr in ["203.0.113.10", "2001:db8::10"] {
            let mut peers = SlotMap::<ConnectionId, ()>::with_key();
            let mut determined = DeterminedIp::default();
            let addr = addr.parse().unwrap();

            determined.add_vote(peers.insert(()), addr);

            assert_eq!(determined.determined_ip(), Some(addr));
        }
    }

    #[test]
    fn invalid_reports_do_not_select_an_address_or_count_as_votes() {
        let mut peers = SlotMap::<ConnectionId, ()>::with_key();
        let mut determined = DeterminedIp::default();
        for addr in ["0.0.0.0", "127.0.0.1", "224.0.0.1", "::", "::1", "ff02::1"] {
            determined.add_vote(peers.insert(()), addr.parse().unwrap());
        }
        assert_eq!(determined.determined_ip(), None);
        assert!(determined.votes.is_empty());

        let valid = "203.0.113.10".parse().unwrap();
        let peer = peers.insert(());
        determined.add_vote(peer, valid);
        determined.add_vote(peer, "0.0.0.0".parse().unwrap());

        assert_eq!(determined.determined_ip(), Some(valid));
        assert_eq!(determined.votes.get(peer), Some(&valid));
    }

    #[test]
    fn fifteen_reports_select_majority_without_waiting_for_timer() {
        let mut peers = SlotMap::<ConnectionId, ()>::with_key();
        let mut determined = DeterminedIp::default();
        let initial = "203.0.113.200".parse().unwrap();
        let majority = "203.0.113.10".parse().unwrap();
        determined.add_vote(peers.insert(()), initial);
        for _ in 0..13 {
            determined.add_vote(peers.insert(()), majority);
        }
        assert_eq!(determined.determined_ip(), Some(initial));

        determined.add_vote(peers.insert(()), majority);

        assert_eq!(determined.determined_ip(), Some(majority));
    }

    #[test]
    fn repeated_reports_from_one_connection_do_not_manufacture_a_majority() {
        let mut peers = SlotMap::<ConnectionId, ()>::with_key();
        let mut determined = DeterminedIp::default();
        let repeated_peer = peers.insert(());
        let minority = "203.0.113.200".parse().unwrap();
        let majority = "203.0.113.10".parse().unwrap();
        for _ in 0..50 {
            determined.add_vote(repeated_peer, minority);
        }
        for _ in 0..14 {
            determined.add_vote(peers.insert(()), majority);
        }

        assert_eq!(determined.determined_ip(), Some(majority));
    }

    #[test]
    fn periodic_reevaluation_waits_five_minutes_then_uses_updated_reports() {
        let mut peers = SlotMap::<ConnectionId, ()>::with_key();
        let mut determined = DeterminedIp::default();
        let initial = "203.0.113.10".parse().unwrap();
        let changed = "203.0.113.200".parse().unwrap();
        let voters: Vec<_> = (0..15).map(|_| peers.insert(())).collect();
        for &peer in &voters {
            determined.add_vote(peer, initial);
        }
        for &peer in &voters {
            determined.add_vote(peer, changed);
        }

        determined.last_changed = Some(Instant::now() - Duration::from_secs(240));
        determined.maybe_reevaluate();
        assert_eq!(determined.determined_ip(), Some(initial));

        determined.last_changed = Some(Instant::now() - Duration::from_secs(301));
        determined.maybe_reevaluate();
        assert_eq!(determined.determined_ip(), Some(changed));
    }
}
