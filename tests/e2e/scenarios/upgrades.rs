//! Base-revision to candidate installation and migration scenarios.

use anyhow::Result;

use crate::harness as e2e;

#[test]
#[ignore = "requires docker; run via `make e2e`"]
fn idle_managed_pair_survives_a_real_candidate_install() -> Result<()> {
    e2e::known_failure(|| {
        let (a, b) = e2e::upgrade_managed_pair()?;
        a.write("before-upgrade.txt", "created by the base connector")?;
        b.wait_for_file("before-upgrade.txt", "created by the base connector")?;

        a.install_candidate()?;
        b.install_candidate()?;

        a.write("after-upgrade-a.txt", "candidate connector resumed")?;
        b.write("after-upgrade-b.txt", "candidate responder resumed")?;
        b.wait_for_file("after-upgrade-a.txt", "candidate connector resumed")?;
        a.wait_for_file("after-upgrade-b.txt", "candidate responder resumed")?;
        e2e::assert_trees_equal(&a, &b)
    })
}
