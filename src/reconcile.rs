use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::model::{Entry, Record, RelativePath};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Conflict {
    pub path: RelativePath,
    pub winner: Record,
    pub loser: Record,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Plan {
    pub records: Vec<Record>,
    pub conflicts: Vec<Conflict>,
}

pub fn reconcile(local: &[Record], remote: &[Record]) -> Plan {
    let local: BTreeMap<_, _> = local.iter().map(|r| (r.path.as_bytes(), r)).collect();
    let remote: BTreeMap<_, _> = remote.iter().map(|r| (r.path.as_bytes(), r)).collect();
    let paths: BTreeSet<_> = local.keys().chain(remote.keys()).copied().collect();
    let mut records = Vec::new();
    let mut conflicts = Vec::new();

    for path in paths {
        match (local.get(path), remote.get(path)) {
            (Some(a), Some(b)) if a.version.id() == b.version.id() => {
                let mut record = (*a).clone();
                merge_seen(&mut record.version.seen, &b.version);
                records.push(record);
            }
            (Some(a), Some(b)) if a.version.has_seen(&b.version.id()) => {
                records.push((*a).clone());
            }
            (Some(a), Some(b)) if b.version.has_seen(&a.version.id()) => {
                records.push((*b).clone());
            }
            (Some(a), Some(b)) if a.version.entry == b.version.entry => {
                records.push(if a.version.lww_cmp(&b.version).is_ge() {
                    (*a).clone()
                } else {
                    (*b).clone()
                });
            }
            (Some(a), Some(b)) => {
                let (winner, loser) = if a.version.lww_cmp(&b.version).is_ge() {
                    ((*a).clone(), (*b).clone())
                } else {
                    ((*b).clone(), (*a).clone())
                };
                let mut winner = winner;
                merge_seen(&mut winner.version.seen, &loser.version);
                if matches!(winner.version.entry, Entry::File { .. })
                    && matches!(loser.version.entry, Entry::File { .. })
                {
                    conflicts.push(Conflict {
                        path: winner.path.clone(),
                        winner: winner.clone(),
                        loser,
                    });
                }
                records.push(winner);
            }
            (Some(a), None) => records.push((*a).clone()),
            (None, Some(b)) => records.push((*b).clone()),
            (None, None) => unreachable!(),
        }
    }
    Plan { records, conflicts }
}

fn merge_seen(seen: &mut Vec<crate::model::VersionId>, version: &crate::model::Version) {
    for item in version
        .seen
        .iter()
        .cloned()
        .chain(std::iter::once(version.id()))
    {
        if let Some(existing) = seen.iter_mut().find(|known| known.peer == item.peer) {
            existing.sequence = existing.sequence.max(item.sequence);
        } else {
            seen.push(item);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ObjectHash, PeerId, Version, VersionId};

    fn record(peer: &str, timestamp: i64, text: &str) -> Record {
        Record {
            path: RelativePath::from_bytes(b"a".to_vec()).unwrap(),
            version: Version {
                peer: PeerId(peer.into()),
                sequence: 1,
                timestamp_ns: timestamp,
                seen: Vec::new(),
                entry: Entry::File {
                    hash: ObjectHash::from_blake3(blake3::hash(text.as_bytes())),
                    size: 1,
                    executable: false,
                },
            },
        }
    }

    #[test]
    fn converges_independent_of_input_order() {
        let a = record("a", 1, "old");
        let b = record("b", 2, "new");
        assert_eq!(
            reconcile(std::slice::from_ref(&a), std::slice::from_ref(&b)).records,
            reconcile(&[b], &[a]).records
        );
    }

    #[test]
    fn causal_child_wins_despite_older_clock() {
        let parent = record("a", 100, "parent");
        let mut child = record("b", 1, "child");
        child.version.sequence = 2;
        child.version.seen = vec![VersionId {
            peer: parent.version.peer.clone(),
            sequence: parent.version.sequence,
        }];
        assert_eq!(
            reconcile(&[parent], std::slice::from_ref(&child)).records,
            vec![child]
        );
    }

    #[test]
    fn transitive_descendant_wins_over_ancestor() {
        let ancestor = record("a", 100, "ancestor");
        let mut grandchild = record("a", 1, "grandchild");
        grandchild.version.sequence = 3;
        grandchild.version.seen = vec![VersionId {
            peer: ancestor.version.peer.clone(),
            sequence: 2,
        }];
        assert_eq!(
            reconcile(&[ancestor], std::slice::from_ref(&grandchild)).records,
            vec![grandchild]
        );
    }

    #[test]
    fn equal_version_ids_union_causal_history() {
        let a = record("a", 1, "same");
        let mut b = a.clone();
        b.version.seen.push(VersionId {
            peer: PeerId("b".into()),
            sequence: 7,
        });
        let merged = reconcile(&[a], &[b]).records.pop().unwrap();
        assert!(merged.version.has_seen(&VersionId {
            peer: PeerId("b".into()),
            sequence: 7,
        }));
    }
}
