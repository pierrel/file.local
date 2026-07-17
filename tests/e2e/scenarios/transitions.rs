//! Catalog #18: an entry changes kind at the same path — file to directory
//! and back — and both peers converge.

use anyhow::Result;

use crate::harness as e2e;
use crate::harness::known_failure;

// Discovered by this harness: on main, the connector's self-apply of a
// kind transition fails the whole sync with "apply stopped; state was
// recovered from disk: File exists" — a path no unit test drives, because
// they apply plans only on the receiving side. Verified to pass with PR #2
// merged; promoted when it lands.
#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn file_and_directory_transitions_converge() -> Result<()> {
    known_failure(|| {
        let (a, b) = e2e::pair()?;
        a.write("node", "a plain file")?;
        a.sync()?;
        b.assert_file("node", "a plain file")?;

        a.remove("node")?;
        a.write("node/child", "now a directory")?;
        a.sync()?;
        b.assert_file("node/child", "now a directory")?;
        e2e::assert_trees_equal(&a, &b)?;

        a.remove_dir_all("node")?;
        a.write("node", "a file again")?;
        a.sync()?;
        b.assert_file("node", "a file again")?;
        e2e::assert_trees_equal(&a, &b)
    })
}
