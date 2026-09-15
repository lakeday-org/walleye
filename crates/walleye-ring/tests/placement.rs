//! Placement invariants recovered from the Verglas ring contract.
use std::time::{Duration, Instant};
use walleye_ring::{Node, Ring};

fn nodes() -> Vec<Node> {
    (0..3)
        .map(|i| Node::new(format!("n{i}"), format!("http://n{i}"), 1.0).unwrap())
        .collect()
}
#[test]
fn membership_order_does_not_change_placement() {
    let members = nodes();
    let a = Ring::new(members.clone()).unwrap();
    let mut reversed = members;
    reversed.reverse();
    let b = Ring::new(reversed).unwrap();
    for key in 0u64..1000 {
        assert_eq!(
            a.owner(&key.to_le_bytes()).id,
            b.owner(&key.to_le_bytes()).id
        );
    }
}
#[test]
fn adding_node_only_moves_keys_to_that_node_and_exposes_previous_owner() {
    let before = Ring::new(nodes()).unwrap();
    let mut after = before.clone();
    let now = Instant::now();
    let mut members = nodes();
    members.push(Node::new("new", "http://new", 1.0).unwrap());
    after
        .replace(members, now, Duration::from_secs(30))
        .unwrap();
    let mut moved = 0;
    for key in 0u64..10000 {
        let key = key.to_le_bytes();
        let old = before.owner(&key);
        let next = after.owner(&key);
        if next.id != old.id {
            moved += 1;
            assert_eq!(next.id, "new");
            assert_eq!(after.donor(&key, now).unwrap().id, old.id);
        }
        assert!(after.donor(&key, now + Duration::from_secs(31)).is_none());
    }
    assert!(moved > 1500 && moved < 3500);
}
#[test]
fn weights_distribute_capacity() {
    let ring = Ring::new(vec![
        Node::new("small", "http://small", 1.0).unwrap(),
        Node::new("large", "http://large", 3.0).unwrap(),
    ])
    .unwrap();
    let mut large = 0;
    for i in 0u64..10000 {
        let key = i.to_le_bytes();
        if ring.owner(&key).id == "large" {
            large += 1;
        }
    }
    assert!((7000..8000).contains(&large));
}
#[test]
fn invalid_membership_is_rejected_without_changing_the_ring() {
    assert!(Ring::new(vec![]).is_err());
    assert!(Node::new("a", "http://a", f64::NAN).is_err());
    let mut ring = Ring::new(nodes()).unwrap();
    let epoch = ring.epoch();
    assert!(
        ring.replace(vec![], Instant::now(), Duration::from_secs(1))
            .is_err()
    );
    assert_eq!(ring.epoch(), epoch);
    let n = Node::new("same", "http://a", 1.0).unwrap();
    assert!(Ring::new(vec![n.clone(), n]).is_err());
}
#[test]
fn draining_node_is_only_a_temporary_donor() {
    let mut ring = Ring::new(nodes()).unwrap();
    let old = ring.clone();
    let now = Instant::now();
    ring.replace(
        nodes().into_iter().filter(|n| n.id != "n0").collect(),
        now,
        Duration::from_secs(10),
    )
    .unwrap();
    for i in 0u64..1000 {
        let k = i.to_le_bytes();
        assert_ne!(ring.owner(&k).id, "n0");
        if old.owner(&k).id == "n0" {
            assert_eq!(ring.donor(&k, now).unwrap().id, "n0");
        }
    }
}

#[test]
fn discovery_updates_keep_inflight_snapshots_and_ignore_endpoint_order() {
    use walleye_ring::Membership;
    let live = Membership::new(nodes()).unwrap();
    let before = live.snapshot();
    let now = Instant::now();
    let mut reversed = nodes();
    reversed.reverse();
    assert!(!live.update(reversed, now, Duration::from_secs(30)).unwrap());
    assert_eq!(live.snapshot().epoch(), 0);
    let mut added = nodes();
    added.push(Node::new("new", "http://new", 1.0).unwrap());
    assert!(live.update(added, now, Duration::from_secs(30)).unwrap());
    assert_eq!(before.members().len(), 3);
    assert_eq!(live.snapshot().members().len(), 4);
    assert!(live.update(vec![], now, Duration::from_secs(30)).is_err());
    assert_eq!(live.snapshot().members().len(), 4);
}
