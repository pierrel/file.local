//! Catalog #12 (ignore rules) and #17 (`.git` is never synchronized).

use anyhow::Result;

use crate::harness as e2e;

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn ignored_paths_stay_local_and_newly_ignored_paths_are_kept() -> Result<()> {
    let (a, b) = e2e::containers()?;
    a.write(".gitignore", "secret.txt\n")?;
    a.write("secret.txt", "local only")?;
    a.write("shared.txt", "everywhere")?;
    a.init()?;
    let (a, b) = a.peer_add(b)?;
    a.sync()?;

    // A pre-existing ignored path never transfers; the rule file itself does.
    b.assert_file(".gitignore", "secret.txt\n")?;
    b.assert_absent("secret.txt")?;
    b.assert_file("shared.txt", "everywhere")?;

    // A newly ignored, already-synchronized path is deleted on neither peer,
    // and its edits stop propagating.
    a.write(".gitignore", "secret.txt\nshared.txt\n")?;
    a.sync()?;
    a.assert_file("shared.txt", "everywhere")?;
    b.assert_file("shared.txt", "everywhere")?;
    a.write("shared.txt", "changed locally")?;
    a.sync()?;
    b.assert_file("shared.txt", "everywhere")?;
    Ok(())
}

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn git_directories_are_never_scanned_transferred_or_deleted() -> Result<()> {
    let (a, b) = e2e::containers()?;
    a.write(".git/config", "[core]\n")?;
    a.write("tracked.txt", "content")?;
    a.init()?;
    let (a, b) = a.peer_add(b)?;
    a.sync()?;

    b.assert_absent(".git")?;
    b.assert_file("tracked.txt", "content")?;

    // The responder's own .git is equally untouched by later rounds.
    b.write(".git/HEAD", "ref: refs/heads/main\n")?;
    a.write("tracked.txt", "updated")?;
    a.sync()?;
    b.assert_file("tracked.txt", "updated")?;
    b.assert_file(".git/HEAD", "ref: refs/heads/main\n")?;
    a.assert_absent(".git/HEAD")?;
    e2e::assert_trees_equal(&a, &b)
}
