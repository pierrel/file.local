use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::merge::{ConflictHunk, FallbackReason};
use crate::model::{BaseVersion, Entry, Record, RelativePath, VersionId};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ConflictResolution {
    MergedWithOverlaps,
    WholeFile {
        winner: usize,
        reason: FallbackReason,
    },
    Destructive {
        winner: usize,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Conflict {
    pub path: RelativePath,
    pub resolution: ConflictResolution,
    pub base: Option<BaseVersion>,
    pub inputs: [Record; 2],
    pub merged: Option<Record>,
    pub hunks: Vec<ConflictHunk>,
}

impl Conflict {
    pub fn whole_file(winner: Record, loser: Record, reason: FallbackReason) -> Self {
        let path = winner.path.clone();
        let (inputs, winner) = canonical_inputs(winner, loser);
        Self {
            path,
            resolution: ConflictResolution::WholeFile { winner, reason },
            base: None,
            inputs,
            merged: None,
            hunks: Vec::new(),
        }
    }

    pub fn destructive(winner: Record, loser: Record) -> Self {
        let path = winner.path.clone();
        let (inputs, winner) = canonical_inputs(winner, loser);
        Self {
            path,
            resolution: ConflictResolution::Destructive { winner },
            base: None,
            inputs,
            merged: None,
            hunks: Vec::new(),
        }
    }

    pub fn merged(
        base: BaseVersion,
        winner: Record,
        loser: Record,
        merged: Record,
        hunks: Vec<ConflictHunk>,
    ) -> Self {
        let path = winner.path.clone();
        let (inputs, _) = canonical_inputs(winner, loser);
        Self {
            path,
            resolution: ConflictResolution::MergedWithOverlaps,
            base: Some(base),
            inputs,
            merged: Some(merged),
            hunks,
        }
    }

    pub fn winner_index(&self) -> usize {
        match self.resolution {
            ConflictResolution::WholeFile { winner, .. }
            | ConflictResolution::Destructive { winner } => winner,
            ConflictResolution::MergedWithOverlaps => usize::from(
                self.inputs[1]
                    .version
                    .lww_cmp(&self.inputs[0].version)
                    .is_gt(),
            ),
        }
    }

    pub fn winner(&self) -> &Record {
        self.merged
            .as_ref()
            .unwrap_or(&self.inputs[self.winner_index()])
    }

    pub fn loser(&self) -> &Record {
        &self.inputs[1 - self.winner_index()]
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MergeCandidate {
    pub path: RelativePath,
    pub base: BaseVersion,
    pub winner: Record,
    pub loser: Record,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Plan {
    pub records: Vec<Record>,
    pub conflicts: Vec<Conflict>,
    #[serde(default)]
    pub merges: Vec<MergeCandidate>,
}

pub fn conflict_id(conflict: &Conflict) -> String {
    let bytes = serde_json::to_vec(conflict).expect("conflict wire types always serialize");
    let digest = blake3::hash(&bytes);
    format!("c-{}", &digest.to_hex()[..12])
}

pub fn reconcile(local: &[Record], remote: &[Record]) -> Plan {
    let local: BTreeMap<_, _> = local.iter().map(|r| (r.path.as_bytes(), r)).collect();
    let remote: BTreeMap<_, _> = remote.iter().map(|r| (r.path.as_bytes(), r)).collect();
    let paths: BTreeSet<_> = local.keys().chain(remote.keys()).copied().collect();
    let mut records = Vec::new();
    let mut conflicts = Vec::new();
    let mut merges = Vec::new();

    for path in paths {
        match (local.get(path), remote.get(path)) {
            (Some(a), Some(b)) if a.version.id() == b.version.id() => {
                records.push((*a).clone());
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
                if matches!(winner.version.entry, Entry::File { .. })
                    && matches!(loser.version.entry, Entry::File { .. })
                {
                    if let (Some(winner_base), Some(loser_base)) =
                        (&winner.version.merge_base, &loser.version.merge_base)
                        && winner_base == loser_base
                        && winner.version.has_seen(&winner_base.id)
                        && loser.version.has_seen(&winner_base.id)
                    {
                        merges.push(MergeCandidate {
                            path: winner.path.clone(),
                            base: winner_base.clone(),
                            winner: winner.clone(),
                            loser: loser.clone(),
                        });
                    } else {
                        let reason = if winner.version.merge_base.is_none()
                            || loser.version.merge_base.is_none()
                        {
                            FallbackReason::AbsentBase
                        } else {
                            FallbackReason::UnequalBase
                        };
                        conflicts.push(Conflict::whole_file(winner.clone(), loser.clone(), reason));
                    }
                } else if matches!(winner.version.entry, Entry::File { .. })
                    || matches!(loser.version.entry, Entry::File { .. })
                {
                    conflicts.push(Conflict::destructive(winner.clone(), loser.clone()));
                }
                records.push(winner);
            }
            (Some(a), None) => records.push((*a).clone()),
            (None, Some(b)) => records.push((*b).clone()),
            (None, None) => unreachable!(),
        }
    }
    Plan {
        records,
        conflicts,
        merges,
    }
}

fn canonical_inputs(a: Record, b: Record) -> ([Record; 2], usize) {
    if compare_ids(&a.version.id(), &b.version.id()).is_le() {
        ([a, b], 0)
    } else {
        ([b, a], 1)
    }
}

fn compare_ids(a: &VersionId, b: &VersionId) -> std::cmp::Ordering {
    a.peer
        .0
        .cmp(&b.peer.0)
        .then_with(|| a.sequence.cmp(&b.sequence))
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
                id_authenticator: None,
                timestamp_ns: timestamp,
                seen: Vec::new(),
                merge_base: None,
                version_authenticator: None,
                base_authenticator: None,
                entry: Entry::File {
                    hash: ObjectHash::from_blake3(blake3::hash(text.as_bytes())),
                    size: text.len() as u64,
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
            authenticator: None,
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
            authenticator: None,
        }];
        assert_eq!(
            reconcile(&[ancestor], std::slice::from_ref(&grandchild)).records,
            vec![grandchild]
        );
    }

    #[test]
    fn destructive_and_unequal_base_conflicts_are_explicit() {
        let file = record("a", 1, "file");
        let mut tombstone = record("b", 2, "deleted");
        tombstone.version.entry = Entry::Tombstone;
        let destructive = reconcile(std::slice::from_ref(&file), &[tombstone]);
        assert!(matches!(
            destructive.conflicts[0].resolution,
            ConflictResolution::Destructive { .. }
        ));

        let mut left = record("a", 3, "left");
        let mut right = record("b", 4, "right");
        left.version.merge_base = file.version.as_base();
        let other_base = record("base-b", 1, "other");
        right.version.merge_base = other_base.version.as_base();
        let unequal = reconcile(&[left], &[right]);
        assert!(matches!(
            unequal.conflicts[0].resolution,
            ConflictResolution::WholeFile {
                reason: FallbackReason::UnequalBase,
                ..
            }
        ));
    }

    #[test]
    fn equal_version_ids_do_not_rewrite_authenticated_metadata() {
        let a = record("a", 1, "same");
        let mut b = a.clone();
        b.version.seen.push(VersionId {
            peer: PeerId("b".into()),
            sequence: 7,
            authenticator: None,
        });
        let merged = reconcile(&[a], &[b]).records.pop().unwrap();
        assert!(!merged.version.has_seen(&VersionId {
            peer: PeerId("b".into()),
            sequence: 7,
            authenticator: None,
        }));
    }
}
