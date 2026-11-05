use std::fs;

use anyhow::Result;
use flocal::model::{Entry, ObjectHash, PeerId, Record, RelativePath};
use flocal::state::{State, file_record};
use rusqlite::Connection;

fn record(path: &[u8], sequence: u64) -> Result<Record> {
    Ok(file_record(
        RelativePath::from_bytes(path.to_vec())?,
        PeerId("test-peer".into()),
        sequence,
        sequence as i64,
        Vec::new(),
        Entry::File {
            hash: ObjectHash::from_blake3(blake3::hash(path)),
            size: path.len() as u64,
            executable: false,
        },
    ))
}

fn audit(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "CREATE TABLE row_audit(kind TEXT NOT NULL,path BLOB NOT NULL);
         CREATE TRIGGER record_insert AFTER INSERT ON records BEGIN INSERT INTO row_audit VALUES('record',new.path); END;
         CREATE TRIGGER record_update AFTER UPDATE ON records BEGIN INSERT INTO row_audit VALUES('record',new.path); END;
         CREATE TRIGGER record_delete AFTER DELETE ON records BEGIN INSERT INTO row_audit VALUES('record',old.path); END;
         CREATE TRIGGER head_insert AFTER INSERT ON shared_heads BEGIN INSERT INTO row_audit VALUES('head',new.path); END;
         CREATE TRIGGER head_update AFTER UPDATE ON shared_heads BEGIN INSERT INTO row_audit VALUES('head',new.path); END;
         CREATE TRIGGER head_delete AFTER DELETE ON shared_heads BEGIN INSERT INTO row_audit VALUES('head',old.path); END;"
    )?;
    Ok(())
}

fn changes(connection: &Connection) -> Result<Vec<(String, Vec<u8>)>> {
    let mut query = connection.prepare("SELECT kind,path FROM row_audit ORDER BY kind,path")?;
    let rows = query
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    connection.execute("DELETE FROM row_audit", [])?;
    Ok(rows)
}

#[test]
fn snapshots_only_write_changed_rows_and_keep_full_membership_semantics() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("root");
    fs::create_dir(&root)?;
    let mut state = State::open(temp.path().join("state"))?;
    let share = state.init_share(&root)?;
    let other_root = temp.path().join("other");
    fs::create_dir(&other_root)?;
    let other_share = state.init_share(&other_root)?;
    let other_records = vec![record(b"a", 99)?];
    state.replace_records(&other_share, &other_records)?;
    let mut records = vec![record(b"a", 1)?, record(b"b", 2)?, record(b"c", 3)?];
    state.replace_records(&share, &records)?;
    let connection = Connection::open(temp.path().join("state/state.sqlite3"))?;
    audit(&connection)?;
    state.replace_records(&share, &records)?;
    assert!(
        changes(&connection)?.is_empty(),
        "unchanged snapshot rewrote canonical rows"
    );
    records[1].version.sequence += 1;
    state.replace_records(&share, &records)?;
    assert_eq!(
        changes(&connection)?,
        vec![("record".into(), b"b".to_vec())]
    );
    records.remove(0);
    state.replace_records(&share, &records)?;
    assert_eq!(
        changes(&connection)?,
        vec![("record".into(), b"a".to_vec())]
    );
    assert_eq!(state.records(&share)?, records);
    let mut duplicate = records.clone();
    duplicate.push(records[0].clone());
    assert!(state.replace_records(&share, &duplicate).is_err());
    assert_eq!(state.records(&share)?, records);
    assert!(
        changes(&connection)?.is_empty(),
        "failed duplicate snapshot leaked writes"
    );
    state.replace_records(&share, &[])?;
    assert!(state.records(&share)?.is_empty());
    assert_eq!(state.records(&other_share)?, other_records);
    Ok(())
}

#[test]
fn legacy_json_equality_preserves_bytes_and_malformed_values_are_replaced() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("root");
    fs::create_dir(&root)?;
    let mut state = State::open(temp.path().join("state"))?;
    let share = state.init_share(&root)?;
    let records = vec![record(b"a", 1)?];
    state.replace_records(&share, &records)?;
    state.acknowledge_shared_heads(&share, &records)?;
    let connection = Connection::open(temp.path().join("state/state.sqlite3"))?;
    let mut version = serde_json::to_value(&records[0].version)?;
    for field in [
        "seen",
        "merge_base",
        "id_authenticator",
        "version_authenticator",
        "base_authenticator",
    ] {
        version.as_object_mut().unwrap().remove(field);
    }
    let version = serde_json::to_string_pretty(&version)?;
    let mut base = serde_json::to_value(records[0].version.as_base().unwrap())?;
    base.as_object_mut().unwrap().remove("authenticator");
    base["id"].as_object_mut().unwrap().remove("authenticator");
    let base = serde_json::to_string_pretty(&base)?;
    connection.execute("UPDATE records SET version_json=?1", [&version])?;
    connection.execute("UPDATE shared_heads SET base_json=?1", [&base])?;
    audit(&connection)?;
    state.replace_records(&share, &records)?;
    state.acknowledge_shared_heads(&share, &records)?;
    assert!(changes(&connection)?.is_empty());
    assert_eq!(
        connection.query_row("SELECT version_json FROM records", [], |row| row
            .get::<_, String>(0))?,
        version
    );
    assert_eq!(
        connection.query_row("SELECT base_json FROM shared_heads", [], |row| row
            .get::<_, String>(0))?,
        base
    );

    connection.execute("UPDATE records SET version_json='invalid'", [])?;
    connection.execute("UPDATE shared_heads SET base_json='invalid'", [])?;
    changes(&connection)?;
    state.replace_records(&share, &records)?;
    state.acknowledge_shared_heads(&share, &records)?;
    assert_eq!(
        changes(&connection)?,
        vec![
            ("head".into(), b"a".to_vec()),
            ("record".into(), b"a".to_vec())
        ]
    );
    assert_eq!(state.records(&share)?, records);
    Ok(())
}

#[test]
fn canonical_changes_roll_back_with_failed_intent_retirement_and_retry() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("root");
    fs::create_dir(&root)?;
    let mut state = State::open(temp.path().join("state"))?;
    let share = state.init_share(&root)?;
    let records = vec![record(b"a", 1)?];
    state.replace_records(&share, &records)?;
    let connection = Connection::open(temp.path().join("state/state.sqlite3"))?;
    audit(&connection)?;
    for invalidated in [false, true] {
        let before = state.records(&share)?;
        let mut after = before.clone();
        after[0].version.sequence += 1;
        let (intent, _) = state.set_install_intent(&share, &after)?;
        connection.execute_batch("CREATE TRIGGER refuse_retirement BEFORE DELETE ON install_intents BEGIN SELECT RAISE(ABORT,'injected retirement failure'); END;")?;
        let result = if invalidated {
            state.retire_invalidated_install(&share, &intent, &after, &[], &after[0].path)
        } else {
            state.finish_install(&share, &intent, &after)
        };
        assert!(result.is_err());
        assert_eq!(state.records(&share)?, before);
        assert_eq!(
            serde_json::to_value(state.install_intent(&share)?.unwrap())?,
            serde_json::to_value(&intent)?
        );
        assert!(state.unsettled_paths(&share)?.is_empty());
        assert!(changes(&connection)?.is_empty());
        connection.execute_batch("DROP TRIGGER refuse_retirement;")?;
        if invalidated {
            state.retire_invalidated_install(&share, &intent, &after, &[], &after[0].path)?;
        } else {
            state.finish_install(&share, &intent, &after)?;
        }
        assert_eq!(state.records(&share)?, after);
        assert_eq!(
            changes(&connection)?,
            vec![("record".into(), b"a".to_vec())]
        );
    }
    Ok(())
}

#[test]
fn duplicate_eligible_acks_roll_back_but_nonfile_duplicates_remain_skipped() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("root");
    fs::create_dir(&root)?;
    let mut state = State::open(temp.path().join("state"))?;
    let share = state.init_share(&root)?;
    let mut records = vec![record(b"a", 1)?, record(b"b", 2)?];
    state.replace_records(&share, &records)?;
    state.acknowledge_shared_heads(&share, &records)?;
    let old_heads = state.shared_heads(&share)?;
    records[0].version.sequence += 1;
    state.replace_records(&share, &records)?;
    let connection = Connection::open(temp.path().join("state/state.sqlite3"))?;
    audit(&connection)?;
    let duplicated = vec![records[0].clone(), records[0].clone()];
    assert!(state.acknowledge_shared_heads(&share, &duplicated).is_err());
    assert_eq!(state.shared_heads(&share)?, old_heads);
    assert!(changes(&connection)?.is_empty());
    state.acknowledge_shared_heads(&share, &records[..1])?;
    assert_eq!(
        changes(&connection)?.len(),
        2,
        "one changed base and one omitted base"
    );
    records[0].version.entry = Entry::Tombstone;
    state.replace_records(&share, &records)?;
    changes(&connection)?;
    state.acknowledge_shared_heads(&share, &[records[0].clone(), records[0].clone()])?;
    assert!(state.shared_heads(&share)?.is_empty());
    assert_eq!(changes(&connection)?, vec![("head".into(), b"a".to_vec())]);
    Ok(())
}

#[test]
fn install_completion_and_invalidation_preserve_unchanged_canonical_rows() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("root");
    fs::create_dir(&root)?;
    let mut state = State::open(temp.path().join("state"))?;
    let share = state.init_share(&root)?;
    let mut records = vec![record(b"a", 1)?, record(b"b", 2)?];
    state.replace_records(&share, &records)?;
    let connection = Connection::open(temp.path().join("state/state.sqlite3"))?;
    audit(&connection)?;
    let (intent, _) = state.set_install_intent(&share, &records)?;
    state.finish_install(&share, &intent, &records)?;
    assert!(
        changes(&connection)?.is_empty(),
        "install completion rewrote unchanged records"
    );
    records[1].version.sequence += 1;
    let (intent, _) = state.set_install_intent(&share, &records)?;
    state.retire_invalidated_install(&share, &intent, &records, &[], &records[1].path)?;
    assert_eq!(
        changes(&connection)?,
        vec![("record".into(), b"b".to_vec())]
    );
    assert_eq!(
        state.unsettled_paths(&share)?,
        vec![records[1].path.clone()]
    );
    assert!(state.install_intent(&share)?.is_none());
    Ok(())
}

#[test]
fn exact_shared_heads_are_retained_without_rewriting_and_stale_acks_do_not_advance() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("root");
    fs::create_dir(&root)?;
    let mut state = State::open(temp.path().join("state"))?;
    let share = state.init_share(&root)?;
    let mut records = vec![record(b"a", 1)?, record(b"b", 2)?];
    state.replace_records(&share, &records)?;
    state.acknowledge_shared_heads(&share, &records)?;
    let connection = Connection::open(temp.path().join("state/state.sqlite3"))?;
    audit(&connection)?;
    state.acknowledge_shared_heads(&share, &records)?;
    assert!(
        changes(&connection)?.is_empty(),
        "unchanged ACK rewrote shared heads"
    );
    let old = records.clone();
    records[1].version.sequence += 1;
    state.replace_records(&share, &records)?;
    changes(&connection)?;
    state.acknowledge_shared_heads(&share, &old)?;
    assert_eq!(changes(&connection)?, vec![("head".into(), b"b".to_vec())]);
    assert!(!state.shared_heads(&share)?.contains_key(b"b".as_slice()));
    state.acknowledge_shared_heads(&share, &records)?;
    assert_eq!(changes(&connection)?, vec![("head".into(), b"b".to_vec())]);
    assert_eq!(
        state.shared_heads(&share)?[b"b".as_slice()],
        records[1].version.as_base().unwrap()
    );
    Ok(())
}
