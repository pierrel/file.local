//! Catalog #13 (watch notices an editor-style atomic-rename save) and #14
//! (watch catches up unattended after the peer goes offline and returns).
//! Both run the watcher at the one-second rescan floor through the
//! `FLOCAL_WATCH_RESCAN_SECONDS` hook of design section 14.1; their only
//! checks are `wait_*` primitives, which are assertions with a deadline.

use anyhow::Result;

use crate::harness as e2e;

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn watch_notices_an_atomic_rename_save() -> Result<()> {
    let (a, b) = e2e::pair_with(e2e::Config {
        watch_rescan_seconds: 1,
    })?;
    a.write("notes.txt", "v1")?;
    a.sync()?;

    let watch = a.watch_start()?;
    a.write(".notes.txt.tmp", "v2")?; // editor safe-save: temp file...
    a.rename(".notes.txt.tmp", "notes.txt")?; // ...renamed over the original
    b.wait_for_file("notes.txt", "v2")?; // the assertion: fails (with the dump)
    watch.stop() // if v2 never arrives by the deadline
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn watch_catches_up_unattended_after_the_peer_returns() -> Result<()> {
    let (a, b) = e2e::pair_with(e2e::Config {
        watch_rescan_seconds: 1,
    })?;
    a.write("kept.txt", "stays")?;
    a.write("dropped.txt", "goes")?;
    a.sync()?;
    b.assert_file("dropped.txt", "goes")?;

    let watch = a.watch_start()?;
    b.offline()?;
    a.write("made-offline.txt", "while b was away")?;
    a.remove("dropped.txt")?;
    b.online()?;
    // Unattended catch-up: nothing drives a sync here — the running
    // watcher's own cycles must notice the reconnection and converge.
    b.wait_for_file("made-offline.txt", "while b was away")?;
    b.wait_absent("dropped.txt")?;
    watch.stop()?;
    e2e::assert_trees_equal(&a, &b)
}
